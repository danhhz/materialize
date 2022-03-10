// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code, clippy::todo)]

//! A proposal for alternate persist API, reinvented for platform.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist::s3::{S3BlobConfig, S3BlobMultiWriter};
use mz_persist::storage::StorageError;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use tokio::sync::Mutex;
use tracing::trace;
use uuid::Uuid;

use crate::collection::Collection;
use crate::error::{InvalidUsage, Permanent};
use crate::machine::Machine;
use crate::read::{ReadHandle, ReaderId};
use crate::write::{WriteHandle, WriterId};

pub mod error;
pub mod read;
pub mod write;

mod collection;
mod descs;
mod machine;
mod metadata;
mod paths;

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

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&Uuid::from_bytes(self.0), f)
    }
}

impl Id {
    /// Returns a random [Id] that is reasonably likely to have never been
    /// generated before.
    fn new() -> Self {
        Id(Uuid::new_v4().as_bytes().to_owned())
    }
}

pub struct Client {
    blob: S3BlobMultiWriter,
    log: Arc<MemLog>,
}

impl Client {
    /// Returns a new client for interfacing with persist collections made
    /// durable to the given `location`.
    ///
    /// The same `location` may be used concurrently from multiple processes.
    /// Concurrent usage is subject to the constraints documented on individual
    /// methods (mostly [WriteHandle::write_batch]).
    pub async fn new(location: Location, role_arn: Option<String>) -> Result<Self, Permanent> {
        let config = S3BlobConfig::new(location.bucket, location.prefix, role_arn)
            .await
            .map_err(|err| Permanent::new(anyhow!(err)))?;
        let blob = S3BlobMultiWriter::open_multi_writer(config);
        let log = Arc::new(MemLog::default());
        // WIP verify that blob and log match to catch operator error, probably
        // by writing a new uuid to each of them when they're initialized
        Ok(Client { blob, log })
    }

    /// Binds `id` to an empty durable TVC.
    ///
    /// The command returns capabilities naming `id`, with frontiers set to
    /// initial values set to `Antichain::from_elem(T::minimum())`.
    ///
    /// It is an error to re-use a previously used `id`.
    pub async fn new_collection<K, V, T, D>(
        &self,
        timeout: Duration,
        id: Id,
    ) -> Result<Result<(WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>), InvalidUsage>, StorageError>
    where
        K: Codec + std::fmt::Debug,
        V: Codec + std::fmt::Debug,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let (write, read) = self.open_collection(timeout, id).await?;
        if read.since() != &Antichain::from_elem(T::minimum()) {
            return Ok(Err(InvalidUsage(anyhow!("id already exists"))));
        }
        if write.upper() != &Antichain::from_elem(T::minimum()) {
            return Ok(Err(InvalidUsage(anyhow!("id already exists"))));
        }
        Ok(Ok((write, read)))
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
        timeout: Duration,
        id: Id,
    ) -> Result<(WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>), StorageError>
    where
        K: Codec,
        V: Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        let deadline = Instant::now() + timeout;

        let collection = Collection {
            id,
            blob: self.blob.clone(),
            log: Arc::clone(&self.log),
        };
        let mut machine = Machine::new(collection);

        let writer_id = WriterId(Uuid::new_v4().as_bytes().to_owned());
        let reader_id = ReaderId(Uuid::new_v4().as_bytes().to_owned());
        let (write_cap, read_cap) = machine.register(deadline, &writer_id, &reader_id).await?;

        let write = WriteHandle {
            machine: machine.clone(),
            writer_id,
            _phantom: PhantomData,
            cap: write_cap,
        };
        let read = ReadHandle {
            machine,
            reader_id,
            _phantom: PhantomData,
            cap: read_cap,
        };

        Ok((write, read))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SeqNo(u64);

pub struct CompareAndSet {
    pub current: SeqNo,
}

#[async_trait]
pub trait Log: std::fmt::Debug + Send + 'static {
    async fn current(&self, deadline: Instant) -> Result<(SeqNo, Option<Vec<u8>>), StorageError>;

    async fn compare_and_set(
        &self,
        deadline: Instant,
        expected: SeqNo,
        value: Option<Vec<u8>>,
    ) -> Result<Result<SeqNo, CompareAndSet>, StorageError>;

    async fn compact(&self, deadline: Instant, since: SeqNo) -> Result<(), StorageError>;
}

#[derive(Debug, Default)]
pub struct MemLog {
    entries: Mutex<Vec<(SeqNo, Option<Vec<u8>>)>>,
}

#[async_trait]
impl Log for MemLog {
    async fn current(&self, _deadline: Instant) -> Result<(SeqNo, Option<Vec<u8>>), StorageError> {
        let current = self
            .entries
            .lock()
            .await
            .last()
            .map(|(seqno, entry)| (*seqno, entry.clone()))
            .unwrap_or_else(|| (SeqNo::default(), None));
        Ok(current)
    }

    async fn compare_and_set(
        &self,
        deadline: Instant,
        expected: SeqNo,
        value: Option<Vec<u8>>,
    ) -> Result<Result<SeqNo, CompareAndSet>, StorageError> {
        let (current, _) = self.current(deadline).await?;
        if current != expected {
            return Ok(Err(CompareAndSet { current }));
        }
        let next = SeqNo(current.0 + 1);
        self.entries.lock().await.push((next, value));
        trace!("Log::compare_and_set {:?}", self.entries);
        Ok(Ok(next))
    }

    async fn compact(&self, _deadline: Instant, _since: SeqNo) -> Result<(), StorageError> {
        // WIP no-op
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use futures_util::StreamExt;
    use mz_persist::s3::S3BlobConfig;
    use tokio::runtime::Runtime;

    use super::*;

    const NO_TIMEOUT: Duration = Duration::from_secs(1_000_000);

    #[test]
    fn sanity_check() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging_default("warn");
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (location, role_arn) = match S3BlobConfig::new_for_test().await? {
                None => return Ok(()),
                Some(cfg) => (
                    Location {
                        bucket: cfg.bucket,
                        prefix: cfg.prefix,
                    },
                    None,
                ),
            };
            let client = Client::new(location, role_arn).await?;
            let (mut write, mut read) = client
                .new_collection::<String, String, u64, i64>(NO_TIMEOUT, Id::new())
                .await??;

            let expected = vec![(("1".to_owned(), "one".to_owned()), 1, 1)];
            assert_eq!(write.upper(), &Antichain::from_elem(u64::minimum()));
            write
                .write_batch(
                    NO_TIMEOUT,
                    expected.iter().map(|((k, v), t, d)| ((k, v), t, d)),
                    Antichain::from_elem(2),
                )
                .await??;
            assert_eq!(write.upper(), &Antichain::from_elem(2));

            let mut snap = read
                .snapshot(
                    NO_TIMEOUT,
                    Antichain::from_elem(1),
                    NonZeroUsize::new(1).unwrap(),
                )
                .await??;
            assert_eq!(snap.len(), 1);
            let snap = snap.pop().unwrap();
            let mut listen = read.listen(Antichain::from_elem(1)).await?;

            let mut snap = read.snapshot_part(NO_TIMEOUT, snap).await?.into_stream();
            let actual = {
                let mut data = Vec::new();
                while let Some(((k, v), t, d)) = snap.next().await {
                    data.push(((k?, v?), t, d));
                }
                data
            };
            assert_eq!(actual, expected);

            assert_eq!(read.since(), &Antichain::from_elem(u64::minimum()));
            read.downgrade_since(NO_TIMEOUT, Antichain::from_elem(2))
                .await?;
            assert_eq!(read.since(), &Antichain::from_elem(2));

            let expected = vec![(("2".to_owned(), "two".to_owned()), 2, 1)];
            write
                .write_batch(
                    NO_TIMEOUT,
                    expected.iter().map(|((k, v), t, d)| ((k, v), t, d)),
                    Antichain::from_elem(3),
                )
                .await??;
            assert_eq!(write.upper(), &Antichain::from_elem(3));

            let actual = listen.poll_next(NO_TIMEOUT).await?;
            let actual = actual
                .into_iter()
                .map(|((k, v), t, d)| ((k.expect("WIP"), v.expect("WIP")), t, d))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);

            Ok(())
        })
    }
}
