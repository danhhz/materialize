// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use differential_dataflow::difference::Semigroup;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;

pub struct WriteHandle<K, V, T, D> {
    _phantom: PhantomData<(K, V, T, D)>,
}

/// A "capability" granting the ability to apply updates to some collection at
/// times greater or equal to `self.upper()`.
impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Codec64,
    D: Semigroup + Codec64,
{
    // This handle's upper frontier, not the global collection-level one.
    pub fn upper(&self) -> &Antichain<T> {
        todo!()
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
    ) -> Result<(), Error> {
        todo!("{:?}{:?}", updates.into_iter().size_hint(), new_upper);
    }

    pub async fn clone(&self) -> Result<Self, Error> {
        todo!();
    }
}

impl<K, V, T, D> Drop for WriteHandle<K, V, T, D> {
    fn drop(&mut self) {
        todo!()
    }
}
