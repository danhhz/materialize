// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A proposal for alternate persist API, reinvented for platform.

use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use differential_dataflow::difference::Semigroup;
use futures_util::Stream;
use mz_persist_types::Codec;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use tracing::warn;

// Notes
// - Pretend that everything I've marked with Serialize and Deserialize instead
//   has some sort of encode/decode API that uses byte slices, Buf+BufMut, or
//   proto, depending on what we decide.
// - PhantomData is a placeholder for internal state that users don't care
//   about.

// WIP
// - Which of these need to be durably storable (for restarts) or transmittable
//   over the network? The formalism document describes certain things as having
//   this property, but does this translate to a persist requirement as well?
//   Candidates: Location, ReadHandle, WriteHandle, others? Definitely: Id,
//   SnapshotShard.
// - Same question for clone-able. The formalism document describes the
//   storage-level read and write capabilities as being clone-able, but does
//   this translate to a persist requirement as well?
// - Figure out the contract of tokio Runtimes and who is responsible for making
//   sure they're in the TLC.
// - I've split up getting a snapshot vs tailing for new changes below, because
//   I think it simplifies the story about multi-process snapshots. We could
//   also instead do something more analogous to SubscribeAt from the formalism
//   doc.
// - We'll want to expose overall collection frontier information (since and
//   upper) at the very least for introspection. There's a few options here so
//   we should think about how this will be used.
// - Leases for read and write handles, as described in formalism.
// - I think some things (readers at least?) will want to connect to the persist
//   controller over rpc. At the very least, this is something that we may want
//   to hook up for latency optimizations (making inputs to the state machine
//   push instead of pull). How does this information get plumbed around? Maybe
//   a dns name that cloud is responsible for making sure points at the right
//   thing?
// - To what extent should this adopt the naming conventions for STORAGE used in
//   the formalism doc? Many of them have direct analogues down at the persist
//   level.
// - At the moment, we have both a STORAGE collection concept as well as a
//   persist collection concept. One of these should be renamed :).

/// A location in s3, other cloud storage, or otherwise "durable storage" used
/// by persist. This location can contain any number of persist collections.
///
/// WIP: This also needs etcd (or whatever we pick) connection information.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Location {
    bucket: String,
    prefix: String,
}

/// An opaque identifier for a persist collection.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Id([u8; 16]);

// WIP: Define the semantics of this. In particular, we should use this
// opportunity to think through where we need to push retries into persist and
// which places need structured information back about errors we can't recover
// from.
#[derive(Debug)]
pub struct Error;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("{:?}", f.width())
    }
}

impl std::error::Error for Error {}

pub trait DurableTimestamp: Timestamp {
    // WIP: Figure out the requirements here. E.g. if we can require timestamps
    // be roundtrip-able as 8 bytes, persist can do some nice performance things
    // in it's internal columnar storage format.
    //
    // WIP: An alternative is to separate out this encode and decode into a
    // Codec64 trait and require only the necessary Timestamp vs Codec64 bounds
    // in various places.
    fn encode(&self) -> [u8; 8];
    fn decode(buf: [u8; 8]) -> Self;
}

pub trait DurableDiff: Semigroup {
    // WIP: Figure out the requirements here. E.g. if we can require timestamps
    // be roundtrip-able as 8 bytes, persist can do some nice performance things
    // in it's internal columnar storage format.
    //
    // WIP: An alternative is to separate out this encode and decode into a
    // Codec64 trait and require only the necessary Semigroup vs Codec64 bounds
    // in various places.
    fn encode(&self) -> [u8; 8];
    fn decode(buf: [u8; 8]) -> Self;
}

pub struct Client {
    _phantom: PhantomData<()>,
}

impl Client {
    pub async fn new(location: Location) -> Result<Self, Error> {
        todo!("{:?}", location)
    }

    pub async fn new_collection<K, V, T, D>(
        &self,
    ) -> Result<(ReadHandle<K, V, T, D>, WriteHandle<K, V, T, D>), Error>
    where
        K: Codec,
        V: Codec,
        T: DurableTimestamp,
        D: DurableDiff,
    {
        todo!();
    }

    pub async fn open_collection<K, V, T, D>(
        &self,
        id: Id,
    ) -> Result<(ReadHandle<K, V, T, D>, WriteHandle<K, V, T, D>), Error>
    where
        K: Codec,
        V: Codec,
        T: DurableTimestamp,
        D: DurableDiff,
    {
        todo!("{:?}", id)
    }
}

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
    T: DurableTimestamp,
    D: DurableDiff,
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

pub struct WriteHandle<K, V, T, D> {
    _phantom: PhantomData<(K, V, T, D)>,
}

impl<K, V, T, D> WriteHandle<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: DurableTimestamp,
    D: DurableDiff,
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
