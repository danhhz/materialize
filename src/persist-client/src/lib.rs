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

use differential_dataflow::difference::Semigroup;
use mz_persist_types::Codec;
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;

use crate::error::Error;
use crate::read::ReadHandle;
use crate::write::WriteHandle;

pub mod error;
pub mod read;
pub mod write;

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
