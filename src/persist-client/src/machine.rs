// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::trace::Description;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};

use crate::read::ReaderId;
use crate::write::WriterId;

pub enum Cmd<T: Timestamp + Codec64> {
    WriteBatch {
        key: String,
        desc: Description<T>,
    },
    // WIP or should this return the upper?
    AddWriter {
        id: WriterId,
        upper: Antichain<T>,
    },
    RemoveWriter {
        id: WriterId,
    },
    // WIP or should this return the since?
    AddReader {
        id: ReaderId,
        since: Antichain<T>,
    },
    RemoveReader {
        id: ReaderId,
    },
    DowngradeSince {
        id: ReaderId,
        new_since: Antichain<T>,
    },
}
