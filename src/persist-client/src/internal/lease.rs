// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)] // WIP

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use mz_persist::location::SeqNo;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

#[derive(Debug)]
enum Frontier<T> {
    Inclusive(Antichain<T>),
    Exclusive(Antichain<T>),
}

pub struct ProgressSeqnoLease<T> {
    // Invariant: sorted by ascending SeqNo.
    leased_seqnos: Mutex<VecDeque<(SeqNo, Frontier<T>)>>,
}

pub struct ProgressSeqnoLeaseReturner<T> {
    wrapped: Arc<ProgressSeqnoLease<T>>,
}

impl<T> Default for ProgressSeqnoLease<T> {
    fn default() -> Self {
        Self {
            leased_seqnos: Default::default(),
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for ProgressSeqnoLease<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const MAX_ELEMENTS: usize = 10;
        let ProgressSeqnoLease { leased_seqnos } = self;
        let mut f = f.debug_struct("ProgressSeqnoLease");
        let leased_seqnos = leased_seqnos.lock().expect("lock");
        if leased_seqnos.len() > MAX_ELEMENTS {
            let prefix = leased_seqnos
                .iter()
                .take(MAX_ELEMENTS / 2)
                .collect::<Vec<_>>();
            let suffix = leased_seqnos
                .iter()
                .rev()
                .take(MAX_ELEMENTS / 2)
                .rev()
                .collect::<Vec<_>>();
            f.field("leased_seqnos_prefix", &prefix);
            f.field("leased_seqnos_suffix", &suffix);
        } else {
            f.field("leased_seqnos", &leased_seqnos);
        }
        f.finish()
    }
}

impl<T: Timestamp> ProgressSeqnoLease<T> {
    pub fn held_seqno(&self) -> Option<SeqNo> {
        self.leased_seqnos
            .lock()
            .expect("lock")
            .front()
            .map(|(seqno, _)| *seqno)
    }

    pub fn take_lease_frontier_inclusive(&self, seqno: SeqNo, frontier: Antichain<T>) {
        self.take_lease(seqno, Frontier::Inclusive(frontier))
    }

    pub fn take_lease_frontier_exclusive(&self, seqno: SeqNo, frontier: Antichain<T>) {
        self.take_lease(seqno, Frontier::Exclusive(frontier))
    }

    fn take_lease(&self, seqno: SeqNo, frontier: Frontier<T>) {
        let mut leased_seqnos = self.leased_seqnos.lock().expect("lock");
        let highest_seqno = leased_seqnos.back().map(|(seqno, _)| *seqno);
        let valid = highest_seqno.map_or(true, |x| x <= seqno);
        if valid {
            leased_seqnos.push_back((seqno, frontier));
        }
        // Make sure to drop the mutex guard before panic'ing, so we don't
        // poison the mutex.
        drop(leased_seqnos);
        assert!(valid, "{:?} vs {}", highest_seqno, seqno);
    }

    pub fn release_leases(&self, progress: &Antichain<T>) {
        let mut leased_seqnos = self.leased_seqnos.lock().expect("lock");
        while let Some((_seqno, frontier)) = leased_seqnos.front() {
            let past_frontier = match frontier {
                Frontier::Inclusive(frontier) => PartialOrder::less_than(frontier, progress),
                Frontier::Exclusive(frontier) => PartialOrder::less_equal(frontier, progress),
            };
            if past_frontier {
                leased_seqnos.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn returner(self: Arc<Self>) -> ProgressSeqnoLeaseReturner<T> {
        ProgressSeqnoLeaseReturner { wrapped: self }
    }
}

impl<T: Timestamp> ProgressSeqnoLeaseReturner<T> {
    pub fn release_leases(&self, progress: &Antichain<T>) {
        self.wrapped.release_leases(progress)
    }
}
