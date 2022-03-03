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

use crate::error::Error;

// WIP This probably doesn't need to be Clone, so I've omitted it for now.
// Pretty sure we could make that work if necessary.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotPart(PhantomData<()>);

pub struct SnapshotIter<K, V, T, D>(PhantomData<(K, V, T, D)>);

impl<K, V, T, D> Stream for SnapshotIter<K, V, T, D> {
    type Item = (K, V, T, D);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("{:?}", cx)
    }
}

pub struct Listen<K, V, T, D>(PhantomData<(K, V, T, D)>);

impl<K, V, T, D> Stream for Listen<K, V, T, D> {
    // WIP: We also need a way to delivery progress information from this.
    type Item = (K, V, T, D);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!("{:?}", cx)
    }
}

/// A "capability" granting the ability to read the state of some collection at
/// times greater or equal to `self.since()`.
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

    /// Forwards the since frontier of this handle, giving up the ability to
    /// read at times not greater or equal to `new_since`.
    ///
    /// This may trigger (asynchronous) compaction and consolidation in the
    /// system. A `new_upper` of the empty antichain "finishes" this collection,
    /// promising that no more data will ever be read by this handle.
    //
    // WIP AntichainRef?
    pub fn downgrade_since(&self, new_since: Antichain<T>) {
        todo!("{:?}", new_since)
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
    pub async fn listen(as_of: Antichain<T>) -> Result<Listen<K, V, T, D>, Error> {
        todo!("{:?}", as_of);
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
    ) -> Result<Vec<SnapshotPart>, Error> {
        todo!("{:?}{:?}", as_of, num_parts);
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
    ) -> Result<SnapshotIter<K, V, T, D>, Error> {
        todo!("{:?}", part);
    }

    pub async fn clone(&self) -> Result<Self, Error> {
        todo!();
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D> {
    fn drop(&mut self) {
        todo!()
    }
}
