// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use anyhow::anyhow;
use bytes::BufMut;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::indexed::columnar::ColumnarRecordsVecBuilder;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::storage::Atomicity;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use tracing::{debug, trace};
use uuid::Uuid;

use crate::error::Permanent;
use crate::machine::Machine;
use crate::paths::Paths;

// WIP do we actually want to be able to persist a WriterId? it's hard to
// imagine what the semantics are of two processes accidentally using the same
// id concurrently (e.g. restart after indeterminate failure)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct WriterId(pub(crate) [u8; 16]);

impl std::fmt::Display for WriterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&Uuid::from_bytes(self.0), f)
    }
}

impl WriterId {
    fn new() -> Self {
        WriterId(Uuid::new_v4().as_bytes().to_owned())
    }
}

pub struct WriteHandle<K, V, T, D> {
    pub(crate) machine: Machine<T>,
    pub(crate) writer_id: WriterId,
    pub(crate) _phantom: PhantomData<(K, V, D)>,

    pub(crate) upper: Antichain<T>,
}

/// A "capability" granting the ability to apply updates to some collection at
/// times greater or equal to `self.upper()`.
impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Codec + std::fmt::Debug,
    V: Codec + std::fmt::Debug,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    // This handle's upper frontier, not the global collection-level one.
    pub fn upper(&self) -> &Antichain<T> {
        &self.upper
    }

    /// Applies `updates` to this collection and downgrades this handle's upper
    /// to `new_upper`.
    ///
    /// All times in `updates` must be greater or equal to `self.upper()` and
    /// not greater or equal to `new_upper`. A `new_upper` of the empty
    /// antichain "finishes" this collection, promising
    // that no more data is ever incoming.
    ///
    /// It is probably an error to call this with `new_upper` equal to
    /// `self.upper()`, as it would mean `updates` must be empty.
    ///
    /// Multiple [WriteHandle]s may be used concurrently to write to the same
    /// collection, but in this case, the data being written must be identical
    /// (in the sense of "definite"-ness).
    pub async fn write_batch<'a, I: IntoIterator<Item = ((&'a K, &'a V), &'a T, &'a D)>>(
        &mut self,
        updates: I,
        new_upper: Antichain<T>,
    ) -> Result<(), Permanent> {
        let lower = self.upper.clone();
        let since = Antichain::from_elem(T::minimum());
        let desc = Description::new(lower, new_upper, since);

        // WIP verify lower vs upper

        let key = Paths::pending_batch_key(&self.machine.collection.id, &self.writer_id, &desc);
        let mut value = Vec::new();
        Self::encode_batch(&mut value, &desc, updates)?;

        debug!("writing batch {}", key);
        self.machine
            .collection
            .blob
            .set(&key, value, Atomicity::RequireAtomic)
            .await
            .expect("WIP retry loop");

        self.machine
            .write_batch(&self.writer_id, &key, &desc)
            .await
            .expect("WIP");
        self.upper = desc.upper().clone();
        Ok(())
    }

    pub async fn clone(&self) -> Result<Self, Permanent> {
        todo!();
    }

    fn encode_batch<'a, B, I>(
        buf: &mut B,
        desc: &Description<T>,
        updates: I,
    ) -> Result<(), Permanent>
    where
        B: BufMut,
        I: IntoIterator<Item = ((&'a K, &'a V), &'a T, &'a D)>,
    {
        let iter = updates.into_iter();
        let size_hint = iter.size_hint();

        let (mut key_buf, mut val_buf) = (Vec::new(), Vec::new());
        let mut builder = ColumnarRecordsVecBuilder::default();
        for ((k, v), t, d) in iter {
            if !desc.lower().less_equal(&t) || desc.upper().less_equal(&t) {
                return Err(Permanent::new(anyhow!(
                    "entry timestamp {:?} doesn't fit in batch desc: {:?}",
                    t,
                    desc
                )));
            }

            trace!("writing update {:?}", ((k, v), t, d));
            key_buf.clear();
            val_buf.clear();
            k.encode(&mut key_buf);
            v.encode(&mut val_buf);
            // WIP get rid of these from_le_bytes calls
            let t = u64::from_le_bytes(T::encode(t));
            let d = i64::from_le_bytes(D::encode(d));

            if builder.len() == 0 {
                // Use the first record to attempt to pre-size the builder
                // allocations. This uses the iter's size_hint's lower+1 to
                // match the logic in Vec.
                let (lower, _) = size_hint;
                let additional = usize::saturating_add(lower, 1);
                builder.reserve(additional, key_buf.len(), val_buf.len());
            }
            builder.push(((&key_buf, &val_buf), t, d))
        }

        // WIP get rid of these from_le_bytes calls
        let desc = Description::new(
            Antichain::from(
                desc.lower()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.upper()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
            Antichain::from(
                desc.since()
                    .elements()
                    .iter()
                    .map(|x| u64::from_le_bytes(T::encode(x)))
                    .collect::<Vec<_>>(),
            ),
        );

        let batch = BlobTraceBatchPart {
            desc,
            updates: builder.finish(),
            index: 0,
        };
        batch.encode(buf);
        Ok(())
    }
}

impl<K, V, T, D> Drop for WriteHandle<K, V, T, D> {
    fn drop(&mut self) {
        // WIP
    }
}
