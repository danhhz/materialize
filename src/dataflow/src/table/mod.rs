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

use std::collections::HashMap;

use differential_dataflow::Collection;
use timely::dataflow::operators::unordered_input::{UnorderedHandle, UnorderedInput};
use timely::dataflow::operators::ActivateCapability;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::operator::CollectionExt;
use dataflow_types::{DataflowError, Update};
use expr::GlobalId;
use repr::{Diff, Row, Timestamp};

// WIP
// - A bunch of the Manager calls previously got made directly from the
//   coordinator instead of the server. I assume because it's more okay to block
//   there? Investigate

pub enum Durability {
    Ephemeral,
    Durable,
}

pub struct Table {
    durability: Durability,
    handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    capability: ActivateCapability<Timestamp>,
}

impl Table {
    fn new<G>(
        scope: &mut G,
        durability: Durability,
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
        let ((handle, capability), stream) = scope.new_unordered_input();
        let err_collection = Collection::empty(scope);

        let table = Table {
            durability,
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
}

pub struct Manager {
    pub tables: HashMap<GlobalId, Table>,
}

impl Manager {
    pub fn new() -> Self {
        Manager {
            tables: HashMap::new(),
        }
    }

    pub fn new_table<G>(
        &mut self,
        scope: &mut G,
        id: GlobalId,
        durability: Durability,
    ) -> (
        Stream<G, (Row, Timestamp, Diff)>,
        Collection<G, DataflowError, Diff>,
    )
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let (table, (stream, err_collection)) = Table::new(scope, durability);
        self.tables.insert(id, table);
        (stream, err_collection)
    }

    pub fn allow_compaction(&mut self, since: &[(GlobalId, Antichain<Timestamp>)]) {
        // WIP
    }

    pub fn remove(&mut self, id: &GlobalId) {
        self.tables.remove(id);
    }

    pub fn destroy(&mut self, id: &GlobalId) {
        // WIP
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
