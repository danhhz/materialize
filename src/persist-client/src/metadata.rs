// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{ReaderId, SeqNo, WriterId};

// WIP this is stored in a path containing CollectionId and WriterId. to keep it
// lean, these aren't denormalized here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterMeta {
    pub(crate) version: SeqNo,
    pub(crate) upper: Vec<[u8; 8]>,
}

// WIP this is stored in a path containing CollectionId and WriterId. to keep it
// lean, these aren't denormalized here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderMeta {
    pub(crate) version: SeqNo,
    pub(crate) since: Vec<[u8; 8]>,
}

// NB the upper of a collection is the max of all writer uppers. the since is
// the min of all reader sinces. WIP use actual antichain terminology here
//
// WIP this is stored in a path containing CollectionId. to keep it lean, this
// isn't denormalized here. note to self, now that we've added a bunch of stuff
// to this, may as well add CollectionId back.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CollectionMeta {
    pub(crate) writers: Vec<(WriterId, SeqNo, Vec<[u8; 8]>)>,
    pub(crate) readers: Vec<(ReaderId, SeqNo, Vec<[u8; 8]>)>,

    // These never change, pull them out into a separate thing?
    pub(crate) key_codec: String,
    pub(crate) val_codec: String,
    pub(crate) ts_codec: String,
    pub(crate) diff_codec: String,

    // WIP this is going to want to be a different structure for compaction, but
    // I think just moving data in is enough to de-risk it.
    pub(crate) trace: Vec<(String, TraceBatchMeta)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TraceBatchMeta {
    pub(crate) lower: Vec<[u8; 8]>,
    pub(crate) upper: Vec<[u8; 8]>,
    pub(crate) since: Vec<[u8; 8]>,
}
