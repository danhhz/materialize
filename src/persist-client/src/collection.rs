// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use mz_persist::s3::S3BlobMultiWriter;
use mz_persist::storage::Atomicity;
use uuid::Uuid;

use crate::metadata::CollectionMeta;
use crate::paths::Paths;
use crate::{Id, Log, MemLog, SeqNo};

#[derive(Debug, Clone)]
pub struct Collection {
    pub(crate) id: Id,
    pub(crate) blob: S3BlobMultiWriter,
    pub(crate) log: Arc<MemLog>,
}

impl Collection {
    pub async fn update_metadata<
        R,
        F: FnMut(SeqNo, &Option<CollectionMeta>) -> (CollectionMeta, R),
    >(
        &mut self,
        deadline: Instant,
        mut f: F,
    ) -> Result<(Option<CollectionMeta>, CollectionMeta, SeqNo, R), anyhow::Error> {
        let (prev_seqno, mut prev_meta) = self.fetch_meta(deadline).await?;
        loop {
            let (new_meta, ret) = f(prev_seqno, &prev_meta);
            // TODO: Retry on failure.
            let (seqno, durable_meta) = self.cas_meta(deadline, &prev_meta, &new_meta).await?;
            if durable_meta.as_ref() == Some(&new_meta) {
                return Ok((prev_meta.clone(), new_meta, seqno, ret));
            }
            // We lost a race, try again.
            prev_meta = durable_meta;
        }
    }

    async fn fetch_current_meta_key(
        &self,
        deadline: Instant,
    ) -> Result<(SeqNo, Option<String>), anyhow::Error> {
        let (current, value) = self.log.current(deadline).await?;
        let value = match value {
            Some(x) => x,
            None => return Ok((current, None)),
        };
        let value =
            String::from_utf8(value).map_err(|err| anyhow!("invalid CURRENT_META: {}", err))?;
        Ok((current, Some(value)))
    }

    // WIP I think we probably want to make this not an Option and just
    // specialize the new collection case?
    pub async fn fetch_meta(
        &self,
        deadline: Instant,
    ) -> Result<(SeqNo, Option<CollectionMeta>), anyhow::Error> {
        let (current, key) = self.fetch_current_meta_key(deadline).await?;
        let key = match key {
            Some(x) => x,
            None => return Ok((current, None)),
        };
        let value = self.blob.get(deadline, &key).await?;
        // NB: A missing current_meta_key means a new collection, but if we get
        // a key back and that key is missing, that's unexpected.
        let value = value.ok_or(anyhow!("missing collection metadata"))?;
        let meta: CollectionMeta = bincode::deserialize(value.as_slice())
            .map_err(|err| anyhow!("corrupted collection metadata: {}", err))?;
        Ok((current, Some(meta)))
    }

    async fn cas_meta(
        &self,
        deadline: Instant,
        expected: &Option<CollectionMeta>,
        new: &CollectionMeta,
    ) -> Result<(SeqNo, Option<CollectionMeta>), anyhow::Error> {
        // WIP this is an entirely incorrect implementation of compare and set.
        // we'll actually want to use something like etcd
        let (current, durable) = self.fetch_meta(deadline).await?;
        if &durable != expected {
            // WIP is current the right thing to return here or should we push
            // SeqNo inside the Option?
            return Ok((current, durable));
        }

        let key = Paths::version_key(&self.id, Uuid::new_v4());
        let value = bincode::serialize(&new)
            .map_err(|err| anyhow!("encoding collection metadata: {}", err))?;
        self.blob
            .set(deadline, &key, value, Atomicity::RequireAtomic)
            .await?;

        let seqno = self
            .log
            .compare_and_set(deadline, current, Some(key.into_bytes()))
            .await?;

        Ok((seqno, Some(new.clone())))
    }
}
