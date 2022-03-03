// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::todo)]

//! A proposal for alternate persist API, reinvented for platform.

use std::marker::PhantomData;

use differential_dataflow::difference::Semigroup;
use mz_persist_types::{Codec, Codec64};
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
// - I've split up getting a snapshot vs listening for new changes below,
//   because I think it simplifies the story about multi-process snapshots. We
//   could also instead do something more analogous to SubscribeAt from the
//   formalism doc.
// - We'll want to expose overall collection frontier information (since and
//   upper) at the very least for introspection. There's a few options here so
//   we should think about how this will be used.
// - Leases for read and write handles, as described in formalism.
// - To what extent should this adopt the naming conventions for STORAGE used in
//   the formalism doc? Many of them have direct analogues down at the persist
//   level.
// - At the moment, we have both a STORAGE collection concept as well as a
//   persist collection concept. One of these should be renamed :).

// TODO
// - Develop a model for understanding the write amplification and space
// amplification as a function of how much data has ever been written to this
// collection as well as how much of it is "live" (doesn't consolidate out at
// the collection frontier).

/// A location in s3, other cloud storage, or otherwise "durable storage" used
/// by persist. This location can contain any number of persist collections.
//
// WIP: This also needs etcd (or whatever we pick) connection information.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Location {
    bucket: String,
    prefix: String,
}

/// An opaque identifier for a persist collection.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Id([u8; 16]);

impl Id {
    /// Returns a random [Id] that is reasonably likely to have never been
    /// generated before.
    pub fn new() -> Self {
        todo!()
    }
}

/// A handle for interacting with the set of persist collection made durable at
/// a single [Location].
pub struct Client {
    _phantom: PhantomData<()>,
}

impl Client {
    /// Returns a new client for interfacing with persist collections made
    /// durable to the given `location`.
    ///
    /// The same `location` may be used concurrently from multiple processes.
    /// Concurrent usage is subject to the constraints documented on individual
    /// methods (mostly [WriteHandle::write_batch]).
    pub async fn new(location: Location) -> Result<Self, Error> {
        todo!("{:?}", location)
    }

    /// Binds `id` to an empty durable TVC.
    ///
    /// The command returns capabilities naming `id`, with frontiers set to
    /// initial values set to `Antichain::from_elem(T::minimum())`.
    ///
    /// It is an error to re-use a previously used `id`.
    pub async fn new_collection<K, V, T, D>(
        &self,
        id: Id,
    ) -> Result<(ReadHandle<K, V, T, D>, WriteHandle<K, V, T, D>), Error>
    where
        K: Codec,
        V: Codec,
        T: Timestamp + Codec64,
        D: Semigroup + Codec64,
    {
        todo!("{:?}", id)
    }

    /// Provides capabilities for `id` at its current since and upper frontiers.
    ///
    /// This method is a best-effort attempt to regain control of the frontiers
    /// of a collection. Its most common uses are to recover capabilities that
    /// have expired (leases) or to attempt to read a TVC that one did not
    /// create (or otherwise receive capabilities for). If the frontiers have
    /// been fully released by all other parties, this call may result in
    /// capabilities with empty frontiers (which are useless).
    pub async fn open_collection<K, V, T, D>(
        &self,
        id: Id,
    ) -> Result<(ReadHandle<K, V, T, D>, WriteHandle<K, V, T, D>), Error>
    where
        K: Codec,
        V: Codec,
        T: Timestamp + Codec64,
        D: Semigroup + Codec64,
    {
        todo!("{:?}", id)
    }
}
