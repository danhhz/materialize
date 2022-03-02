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
use std::pin::Pin;
use std::task::{Context, Poll};

use differential_dataflow::difference::Semigroup;
use futures_util::Stream;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use tracing::warn;

use crate::error::Error;

// WIP This probably doesn't need to be Clone, so I've omitted it for now.
// Pretty sure we could make that work if necessary.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotShard(PhantomData<()>);

pub struct SnapshotIter<K, V, T, D>(PhantomData<(K, V, T, D)>);

impl<K, V, T, D> Stream for SnapshotIter<K, V, T, D> {
    type Item = (K, V, T, D);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("{:?}", cx)
    }
}

pub struct Tail<K, V, T, D>(PhantomData<(K, V, T, D)>);

impl<K, V, T, D> Stream for Tail<K, V, T, D> {
    // WIP: We also need a way to delivery progress information from this.
    type Item = (K, V, T, D);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("{:?}", cx)
    }
}

pub struct ReadHandle<K, V, T, D> {
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Codec64,
    D: Semigroup + Codec64,
{
    // This handle's since frontier, not the global collection-level one.
    pub fn since(&self) -> &Antichain<T> {
        todo!()
    }

    // NB new_upper of the empty antichain "finishes" this collection, promising
    // that no more data will ever be read by this handle.
    //
    // WIP AntichainRef?
    pub fn downgrade_since(&self, new_since: Antichain<T>) {
        todo!("{:?}", new_since)
    }

    // WIP: The impl is far far easier if this is something we keep single
    // process at first (as opposed to being able to shard it like snapshots).
    // Do we think that's fine for short term (i.e. initial platform launch)? We
    // can definitely revisit this later if it ends up being a bottleneck.
    //
    // WIP: This is an opportunity to push down Projection information to save
    // bandwidth. My thinking so far is that Map and Filter should stay
    // STORAGE-level concerns.
    //
    // WIP: My first attempt here was to return `impl Stream` but that didn't
    // compile with `todo!`. I don't yet have an opinion on which is better.
    pub async fn tail(as_of: Antichain<T>) -> Result<Tail<K, V, T, D>, Error> {
        todo!("{:?}", as_of);
    }

    // NB the len of the returned vec is num_shards
    //
    // WIP This is an opportunity to push down Projection information to save
    // bandwidth. My thinking so far is that Map and Filter should stay
    // STORAGE-level concerns.
    pub async fn snapshot(
        &self,
        as_of: Antichain<T>,
        num_shards: NonZeroUsize,
    ) -> Result<Vec<SnapshotShard>, Error> {
        todo!("{:?}{:?}", as_of, num_shards);
    }

    // WIP: Should this live on ReadHandle or somewhere else? E.g. I think it
    // could also live on Client.
    //
    // WIP: Figure out the error story here. Retries should move inside this, but
    // what happens if we get data that we can't decode?
    //
    // WIP: My first attempt here was to return `impl Stream` but that didn't
    // compile with `todo!`. I don't yet have an opinion on which is better.
    pub async fn snapshot_shard(
        &self,
        shard: SnapshotShard,
    ) -> Result<SnapshotIter<K, V, T, D>, Error> {
        todo!("{:?}", shard);
    }

    pub async fn clone(&self) -> Result<Self, Error> {
        todo!();
    }
}

impl<K, V, T, D> ReadHandle<K, V, T, D> {
    pub async fn deregister(&mut self) -> Result<(), Error> {
        todo!()
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D> {
    fn drop(&mut self) {
        // WIP: Thread a tokio runtime down instead?
        futures_executor::block_on(async {
            if let Err(err) = self.deregister().await {
                warn!("failed to deregister ReadHandle on drop: {}", err)
            }
        })
    }
}
