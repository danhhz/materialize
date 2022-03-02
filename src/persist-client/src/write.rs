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
use tracing::warn;

use crate::error::Error;

pub struct WriteHandle<K, V, T, D> {
    _phantom: PhantomData<(K, V, T, D)>,
}

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

    // NB new_upper of the empty antichain "finishes" this collection, promising
    // that no more data is ever incoming
    //
    // TODO: Bound the write and space amplification as a function of how much
    // data has even been written to this collection as well as how much of it
    // is "live" (doesn't consolidate out at the collection frontier).
    pub async fn write_batch<I: IntoIterator<Item = ((K, V), T, D)>>(
        &mut self,
        data: I,
        new_upper: Antichain<T>,
    ) -> Result<(), Error> {
        todo!("{:?}{:?}", data.into_iter().size_hint(), new_upper);
    }

    pub async fn clone(&self) -> Result<Self, Error> {
        todo!();
    }
}

impl<K, V, T, D> WriteHandle<K, V, T, D> {
    pub async fn deregister(&mut self) -> Result<(), Error> {
        todo!()
    }
}

impl<K, V, T, D> Drop for WriteHandle<K, V, T, D> {
    fn drop(&mut self) {
        // WIP: Thread a tokio runtime down instead?
        futures_executor::block_on(async {
            if let Err(err) = self.deregister().await {
                warn!("failed to deregister WriteHandle on drop: {}", err)
            }
        })
    }
}
