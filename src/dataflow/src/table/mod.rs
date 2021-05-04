// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL Tables

// WIP
#![allow(unused_variables, dead_code)]

use differential_dataflow::Collection;
use persist::{PersistableStream, PersistedV1};
use timely::dataflow::operators::unordered_input::{UnorderedHandle, UnorderedInput};
use timely::dataflow::operators::{ActivateCapability, Map};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::operator::CollectionExt;
use dataflow_types::{DataflowError, Update};
use repr::{Diff, Row, Timestamp};

// WIP
// - A bunch of the Manager calls previously got made directly from the
//   coordinator instead of the server. I assume because it's more okay to block
//   there? Investigate

pub struct Table {
    // persistence: Option<Box<dyn PersistedV1>>,
    handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    capability: ActivateCapability<Timestamp>,
}

impl Table {
    pub fn new<G>(
        scope: &mut G,
        persistence: Option<Box<dyn PersistedV1>>,
    ) -> (
        Self,
        (
            Stream<G, (Row, Timestamp, Diff)>,
            Collection<G, DataflowError, Diff>,
        ),
    )
    where
        G: Scope<Timestamp = Timestamp>,
    {
        // TODO: It seems like (when a persister is given) this pattern is now
        // buffering twice since new_unordered_input is (I think) buffering
        // input, but so is the persister.
        let ((handle, capability), stream) = scope.new_unordered_input();
        let err_collection = Collection::empty(scope);

        // WIP merge persistence errors into err_collection once this returns
        // errors
        let stream = if let Some(persistence) = persistence {
            let persistence: Box<dyn PersistedV1> = persistence;
            stream
                // TODO: Get rid of these 2 maps
                .map(|(row, ts, diff): (Row, Timestamp, Diff)| {
                    (row.data().to_vec(), ts as u64, diff as i64)
                })
                .persist_unary_sync(persistence)
                .map(|(row, ts, diff): (Vec<u8>, u64, i64)| {
                    (
                        unsafe { Row::from_bytes_unchecked(row) },
                        ts as Timestamp,
                        diff as Diff,
                    )
                })
        } else {
            stream
        };

        let table = Table {
            // persistence,
            handle,
            capability,
        };
        (table, (stream, err_collection))
    }

    pub fn advance(&mut self, ts: Timestamp) {
        self.capability.downgrade(&ts);
    }

    pub fn update(&mut self, updates: Vec<Update>) {
        let mut session = self.handle.session(self.capability.clone());
        for update in updates {
            assert!(update.timestamp >= *self.capability.time());
            session.give((update.row, update.timestamp, update.diff));
        }
    }

    pub fn allow_compaction(&mut self, frontier: &Antichain<Timestamp>) {
        // if let Some(persistence) = &mut self.persistence {
        //     // WIP
        //     persistence.allow_compaction(frontier.elements()[0]);
        // }
    }
}

// // TODO gross hack for now
// if !entry.id().is_system() {
//     if let Some(tables) = &mut self.persisted_tables {
//         if let Some(messages) = tables.resume(entry.id()) {
//             let mut updates = vec![];
//             for persisted_message in messages.into_iter() {
//                 match persisted_message {
//                     PersistedMessage::Progress(time) => {
//                         // Send the messages accumulated so far + update
//                         // progress
//                         // TODO: I think we need to avoid downgrading capabilities until
//                         // all rows have been sent so the table is not visible for reads
//                         // before being fully reloaded.
//                         let updates = std::mem::replace(&mut updates, vec![]);
//                         if !updates.is_empty() {
//                             self.broadcast(SequencedCommand::Insert {
//                                 id: entry.id(),
//                                 updates,
//                             });
//                             self.broadcast(
//                                 SequencedCommand::AdvanceAllLocalInputs {
//                                     advance_to: time,
//                                 },
//                             );
//                         }
//                     }
//                     PersistedMessage::Data(update) => {
//                         updates.push(update);
//                     }
//                 }
//             }
//         }
//     }
// }
