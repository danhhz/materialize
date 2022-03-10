// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use differential_dataflow::trace::Description;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};
use uuid::Uuid;

use crate::read::ReaderId;
use crate::write::WriterId;
use crate::Id;

// structure of the data in s3 (all of these with some common prefix)
//
// - `<CollectionId>/versions/<UUID>` -> CollectionMeta
// - `<CollectionId>/writers/<WriterId>` -> WriterMeta
// - `<CollectionId>/readers/<ReaderId>` -> ReaderMeta
// - `<CollectionId>/pending/<WriterId>/<Desc>` -> ColumnarBatch
//
// structure of the data in etcd
//
// - log of <UUID> of the current CollectionMeta
pub struct Paths;

impl Paths {
    pub fn version_key(collection_id: &Id, version_id: Uuid) -> String {
        format!("{}/versions/{}", collection_id, version_id,)
    }

    pub fn writer_meta_key(collection_id: &Id, writer_id: &WriterId) -> String {
        format!("{}/writer/{}/META", collection_id, writer_id)
    }

    pub fn reader_meta_key(collection_id: &Id, reader_id: &ReaderId) -> String {
        format!("{}/reader/{}/META", collection_id, reader_id)
    }

    pub fn pending_batch_key<T: Codec64>(
        collection_id: &Id,
        writer_id: &WriterId,
        desc: &Description<T>,
    ) -> String {
        format!(
            "{}/pending/{}/{}",
            collection_id,
            writer_id,
            Self::desc(desc),
        )
    }

    pub fn pending_batch_prefix(collection_id: &Id) -> String {
        format!("{}/pending", collection_id)
    }

    pub fn parse_batch_key<T: Timestamp + Codec64>(
        key: &str,
    ) -> Result<(WriterId, Description<T>), anyhow::Error> {
        let mut parts = key.split('/');
        let _collection = parts.next().expect("WIP");
        let _pending = parts.next().expect("WIP");
        let writer_id = parts.next().expect("WIP");
        let desc = parts.next().expect("WIP");
        assert!(parts.next().is_none());

        let writer_id = Uuid::parse_str(writer_id).map_err(|err| anyhow!(err))?;
        let writer_id = WriterId(*writer_id.as_bytes());

        let desc = Self::parse_desc(desc)?;

        Ok((writer_id, desc))
    }

    fn desc<T: Codec64>(desc: &Description<T>) -> String {
        let lower = desc
            .lower()
            .elements()
            .iter()
            .map(|x| u64::from_le_bytes(T::encode(x)).to_string())
            .collect::<Vec<_>>()
            .join("-");
        let upper = desc
            .upper()
            .elements()
            .iter()
            .map(|x| u64::from_le_bytes(T::encode(x)).to_string())
            .collect::<Vec<_>>()
            .join("-");
        let since = desc
            .since()
            .elements()
            .iter()
            .map(|x| u64::from_le_bytes(T::encode(x)).to_string())
            .collect::<Vec<_>>()
            .join("-");
        format!("{}-{}-{}", lower, upper, since)
    }

    fn parse_desc<T: Timestamp + Codec64>(key: &str) -> Result<Description<T>, anyhow::Error> {
        let mut parts = key.split('-');
        let lower = parts.next().expect("WIP");
        let upper = parts.next().expect("WIP");
        let since = parts.next().expect("WIP");
        assert!(parts.next().is_none());

        let lower = lower
            .split('-')
            .map(|x| T::decode(x.parse::<u64>().expect("WIP").to_le_bytes()))
            .collect::<Vec<_>>();
        let upper = upper
            .split('-')
            .map(|x| T::decode(x.parse::<u64>().expect("WIP").to_le_bytes()))
            .collect::<Vec<_>>();
        let since = since
            .split('-')
            .map(|x| T::decode(x.parse::<u64>().expect("WIP").to_le_bytes()))
            .collect::<Vec<_>>();

        let desc = Description::new(
            Antichain::from(lower),
            Antichain::from(upper),
            Antichain::from(since),
        );

        Ok(desc)
    }
}
