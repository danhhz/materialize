// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::time::Duration;

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::Stream;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tracing::{debug, trace};
use uuid::Uuid;

use crate::collection::Collection;
use crate::descs::BatchDescs;
use crate::error::Permanent;
use crate::machine::{Machine, State};

// WIP This probably doesn't need to be Clone, so I've omitted it for now.
// Pretty sure we could make that work if necessary.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotPart {
    upper: Vec<[u8; 8]>,
    since: Vec<[u8; 8]>,
    batches: Vec<String>,
}

impl SnapshotPart {
    pub fn new<T: Codec64>(
        upper: &Antichain<T>,
        since: &Antichain<T>,
        batches: Vec<String>,
        num_parts: NonZeroUsize,
    ) -> Vec<Self> {
        let upper: Vec<[u8; 8]> = upper.elements().iter().map(|x| T::encode(x)).collect();
        let since: Vec<[u8; 8]> = since.elements().iter().map(|x| T::encode(x)).collect();
        let mut parts: Vec<SnapshotPart> = (0..num_parts.get())
            .map(|_| SnapshotPart {
                upper: upper.clone(),
                since: since.clone(),
                batches: Vec::new(),
            })
            .collect();
        for (idx, batch) in batches.into_iter().enumerate() {
            // WIP do something better than round-robin
            let part_idx = parts.len();
            parts[idx % part_idx].batches.push(batch);
        }
        parts
    }
}

pub struct SnapshotIter<K, V, T, D> {
    updates: Vec<((K, V), T, D)>,
}

impl<K, V, T, D> SnapshotIter<K, V, T, D> {
    // TODO: impl Stream directly on SnapshotIter
    pub fn into_stream(self) -> impl Stream<Item = ((K, V), T, D)> {
        futures_util::stream::iter(self.updates.into_iter())
    }
}

pub struct Listen<K, V, T, D> {
    collection: Collection,
    as_of: Antichain<T>,
    frontier: Antichain<T>,
    _phantom: PhantomData<(K, V, T, D)>,
}

// WIP: impl Stream directly on Listen
impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Codec + std::fmt::Debug,
    V: Codec + std::fmt::Debug,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    // WIP: We also need a way to delivery progress information from this.
    pub async fn poll_next(&mut self) -> Vec<((K, V), T, D)> {
        loop {
            let (_, meta) = self.collection.fetch_meta().await.expect("WIP");
            let state = State::from_meta(meta.as_ref());

            let all_batch_descs = state.trace.clone();
            debug!("all_batch_descs {:?}", all_batch_descs);
            for (key, desc) in all_batch_descs {
                trace!("listen frontier={:?} considering {:?}", self.frontier, desc);
                if PartialOrder::less_equal(desc.upper(), &self.frontier) {
                    trace!("listen skipping {:?}", desc);
                    continue;
                } else if PartialOrder::less_equal(desc.lower(), &self.frontier) {
                    trace!("listen emitting {:?}", desc);
                    let updates = self.fetch_batch_updates(&key).await.expect("WIP");
                    self.frontier = desc.upper().clone();
                    if updates.is_empty() {
                        continue;
                    }
                    return updates;
                } else {
                    todo!("TODO: More resilient implementation of this");
                }
            }
            // We didn't find anything, wait and try again.
            debug!("listen sleeping for 1s");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn fetch_batch_updates(&self, key: &str) -> Result<Vec<((K, V), T, D)>, Permanent> {
        let value = self
            .collection
            .blob
            .get(&key)
            .await
            .expect("WIP retry loop")
            .ok_or(Permanent::new(anyhow!("internal error: missing batch")))?;
        let batch =
            BlobTraceBatchPart::decode(&value).map_err(|err| Permanent::new(anyhow!(err)))?;
        let mut updates = Vec::new();
        for chunk in batch.updates {
            for ((k, v), t, d) in chunk.iter() {
                let k = K::decode(k).map_err(|err| Permanent::new(anyhow!(err)))?;
                let v = V::decode(&v).map_err(|err| Permanent::new(anyhow!(err)))?;
                let t = T::decode(t.to_le_bytes());
                let d = D::decode(d.to_le_bytes());
                trace!("listen read {:?}", ((&k, &v), &t, &d));
                // Well this is awkward.
                if !self.frontier.less_equal(&t) || !self.as_of.less_than(&t) {
                    continue;
                }
                updates.push(((k, v), t, d));
            }
        }
        Ok(updates)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&Uuid::from_bytes(self.0), f)
    }
}

impl ReaderId {
    fn new() -> Self {
        ReaderId(Uuid::new_v4().as_bytes().to_owned())
    }
}

/// A "capability" granting the ability to read the state of some collection at
/// times greater or equal to `self.since()`.
pub struct ReadHandle<K, V, T, D> {
    pub(crate) machine: Machine<T>,
    pub(crate) reader_id: ReaderId,
    pub(crate) _phantom: PhantomData<(K, V, D)>,

    pub(crate) since: Antichain<T>,
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Codec + std::fmt::Debug,
    V: Codec + std::fmt::Debug,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    /// This handle's since frontier, not the global collection-level one.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    /// Forwards the since frontier of this handle, giving up the ability to
    /// read at times not greater or equal to `new_since`.
    ///
    /// This may trigger (asynchronous) compaction and consolidation in the
    /// system. A `new_upper` of the empty antichain "finishes" this collection,
    /// promising that no more data will ever be read by this handle.
    //
    // WIP AntichainRef?
    pub async fn downgrade_since(&mut self, new_since: Antichain<T>) {
        self.machine
            .downgrade_since(&self.reader_id, &new_since)
            .await
            .expect("WIP");
        self.since = new_since;
    }

    /// Returns an ongoing stream of updates to a collection.
    ///
    /// The subscription includes all data at times greater than `as_of`.
    /// Combined with [Self::snapshot] it will produce exactly correct results:
    /// the snapshot is the TVCs contents at `as_of` and all subsequent updates
    /// occur at exactly their indicated time. The recipient should only
    /// downgrade their read capability when they are certain they have all data
    /// through the frontier they would downgrade to.
    //
    // WIP: This is an opportunity to push down Projection information to save
    // bandwidth. My thinking so far is that Map and Filter should stay
    // STORAGE-level concerns.
    //
    // WIP: My first attempt here was to return `impl Stream` but that didn't
    // compile with `todo!`. I don't yet have an opinion on which is better.
    pub async fn listen(&self, as_of: Antichain<T>) -> Result<Listen<K, V, T, D>, Permanent> {
        // WIP validate that it's okay to read at this as_of
        Ok(Listen {
            collection: self.machine.collection.clone(),
            frontier: as_of.clone(),
            as_of,
            _phantom: PhantomData,
        })
    }

    /// Returns a snapshot of the collection at `as_of`.
    ///
    /// This command returns the contents of this collection as of `as_of` once
    /// they are known. This may be in the future if `as_of` is greater or equal
    /// to the current `upper` of the collection. The recipient should only
    /// downgrade their read capability when they are certain they have all data
    /// through the frontier they would downgrade to.
    ///
    /// This snapshot may be split into a number of partitions, each of which
    /// may be exchanged (including over the network) to load balance the
    /// processing of this snapshot. These partitions are usable by anyone with
    /// access to the collection's [crate::Location]. The `len()` of the
    /// returned `Vec` is `num_parts`.
    //
    // WIP This is an opportunity to push down Projection information to save
    // bandwidth. My thinking so far is that Map and Filter should stay
    // STORAGE-level concerns.
    pub async fn snapshot(
        &self,
        as_of: Antichain<T>,
        num_parts: NonZeroUsize,
    ) -> Result<Vec<SnapshotPart>, Permanent> {
        let (_, meta) = self.machine.collection.fetch_meta().await.expect("WIP");
        let state = State::from_meta(meta.as_ref());
        let all_batch_descs = BatchDescs(state.trace);
        debug!("all_batch_descs {:?}", all_batch_descs);
        let query_desc = Description::new(
            Antichain::from_elem(T::minimum()),
            as_of.clone(),
            as_of.clone(),
        );
        debug!("query_desc {:?}", all_batch_descs);
        let relevant_batch_descs = all_batch_descs.cover(&query_desc).expect("WIP");
        debug!("relevant_batch_descs {:?}", relevant_batch_descs);
        let batches = relevant_batch_descs
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        debug!("batches {:?}", batches);
        let parts = SnapshotPart::new(&as_of, &as_of, batches, num_parts);
        debug!("parts {:?}", parts);
        Ok(parts)
    }

    /// Trade in an exchange-able [SnapshotPart] for its respective data.
    //
    // WIP: Should this live on ReadHandle or somewhere else? E.g. I think it
    // could also live on Client.
    //
    // WIP: Figure out the error story here. Retries should move inside this, but
    // what happens if we get data that we can't decode?
    //
    // WIP: My first attempt here was to return `impl Stream` but that didn't
    // compile with `todo!`. I don't yet have an opinion on which is better.
    pub async fn snapshot_part(
        &self,
        part: SnapshotPart,
    ) -> Result<SnapshotIter<K, V, T, D>, Permanent> {
        let upper = Antichain::from(
            part.upper
                .into_iter()
                .map(|x| T::decode(x))
                .collect::<Vec<_>>(),
        );
        let since = Antichain::from(
            part.since
                .into_iter()
                .map(|x| T::decode(x))
                .collect::<Vec<_>>(),
        );

        // TODO: Do all this iteration inside SnapshotIter.
        let mut updates = Vec::new();
        for key in part.batches {
            debug!("reading batch {}", key);
            let value = self
                .machine
                .collection
                .blob
                .get(&key)
                .await
                .expect("WIP retry loop")
                .ok_or(Permanent::new(anyhow!("internal error: missing batch")))?;
            let batch =
                BlobTraceBatchPart::decode(&value).map_err(|err| Permanent::new(anyhow!(err)))?;
            for chunk in batch.updates {
                for ((k, v), t, d) in chunk.iter() {
                    let k = K::decode(k).map_err(|err| Permanent::new(anyhow!(err)))?;
                    let v = V::decode(v).map_err(|err| Permanent::new(anyhow!(err)))?;
                    let mut t = T::decode(t.to_le_bytes());
                    let d = D::decode(d.to_le_bytes());
                    trace!("reading update {:?}", ((&k, &v), &t, &d));
                    if upper.less_than(&t) {
                        continue;
                    }
                    t.advance_by(since.borrow());
                    updates.push(((k, v), t, d));
                }
            }
        }
        Ok(SnapshotIter { updates })
    }

    pub async fn clone(&self) -> Result<Self, Permanent> {
        todo!();
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D> {
    fn drop(&mut self) {
        // WIP
    }
}
