// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)] // WIP

//! A source operator that emits metadata about a persist shard.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;

use differential_dataflow::lattice::Lattice;
use mz_persist::location::SeqNo;
use mz_persist::retry::Retry;
use mz_persist_types::Codec64;
use timely::progress::Timestamp;

use crate::internal::state::TypedState;
use crate::internal::state_versions::StateVersions;
use crate::{PersistClient, ShardId};

use crate::error::CodecMismatchT;
pub use crate::internal::state::{
    CriticalReaderState, HollowBatch, LeasedReaderState, WriterState,
};
pub use crate::internal::state_diff::{StateDiff, StateFieldDiff, StateFieldValDiff};

#[derive(Debug)]
pub struct ShardDiffsStream<T> {
    state_versions: StateVersions,
    shard_id: ShardId,
    seqno: SeqNo,
    diffs: VecDeque<StateDiff<T>>,
}

impl<T: Timestamp + Lattice + Codec64> ShardDiffsStream<T> {
    pub async fn new(
        client: PersistClient,
        shard_id: ShardId,
    ) -> Result<Self, Box<CodecMismatchT>> {
        let state_versions = StateVersions::new(
            client.cfg.clone(),
            Arc::clone(&client.consensus),
            Arc::clone(&client.blob),
            Arc::clone(&client.metrics),
        );
        let state_iter = state_versions
            .fetch_all_live_states(shard_id)
            .await
            .check_ts_codec()?;
        let (state, fetched_diffs) = state_iter.into_components();

        // Prepend (to the back) a diff from the initial state to the current
        // one.
        //
        // WIP this should search for some as_of and start there instead
        let initial_state = TypedState::<(), (), T, i64>::new(
            client.cfg.build_version,
            shard_id,
            client.cfg.hostname,
            0,
        );
        let initial_diff = StateDiff::from_diff(&initial_state.state, &state);
        let mut diffs = VecDeque::with_capacity(fetched_diffs.len() + 1);
        diffs.push_back(initial_diff);
        diffs.extend(fetched_diffs.into_iter());
        Ok(ShardDiffsStream {
            state_versions,
            shard_id,
            seqno: initial_state.seqno,
            diffs,
        })
    }

    pub async fn next(&mut self) -> Option<StateDiff<T>> {
        let mut retry = Retry::persist_defaults(SystemTime::now()).into_retry_stream();
        loop {
            if let Some(diff) = self.diffs.pop_front() {
                assert_eq!(diff.seqno_from, self.seqno);
                self.seqno = diff.seqno_to;
                return Some(diff);
            };
            // WIP shut this down (with a None) once the shard both becomes a
            // tombstone and also has finished any outstanding maintenance. maybe we
            // can detect the latter condition with last_gc_req or number of rollups
            // or something?

            self.state_versions
                .fetch_diffs_from(&self.shard_id, self.seqno.next(), &mut self.diffs)
                .await;
            if self.diffs.is_empty() {
                retry = retry.sleep().await;
            }
        }
    }
}
