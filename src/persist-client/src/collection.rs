// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use anyhow::anyhow;
use mz_persist::s3::S3BlobMultiWriter;
use mz_persist::storage::Atomicity;
use uuid::Uuid;

use crate::metadata::{CollectionMeta, ReaderMeta, WriterMeta};
use crate::paths::Paths;
use crate::read::ReaderId;
use crate::write::WriterId;
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
        mut f: F,
    ) -> Result<(Option<CollectionMeta>, CollectionMeta, R), anyhow::Error> {
        // TODO: Retry on failure.
        let (prev_seqno, mut prev_meta) = self.fetch_meta().await?;
        loop {
            let (new_meta, ret) = f(prev_seqno, &prev_meta);
            // TODO: Retry on failure.
            let durable_meta = self.cas_meta(&prev_meta, &new_meta).await?;
            if durable_meta.as_ref() == Some(&new_meta) {
                return Ok((prev_meta.clone(), new_meta, ret));
            }
            // We lost a race, try again.
            prev_meta = durable_meta;
        }
    }

    async fn fetch_current_meta_key(&self) -> Result<(SeqNo, Option<String>), anyhow::Error> {
        let (current, value) = self.log.current().await?;
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
    pub async fn fetch_meta(&self) -> Result<(SeqNo, Option<CollectionMeta>), anyhow::Error> {
        let (current, key) = self.fetch_current_meta_key().await?;
        let key = match key {
            Some(x) => x,
            None => return Ok((current, None)),
        };
        let value = self.blob.get(&key).await?;
        // NB: A missing current_meta_key means a new collection, but if we get
        // a key back and that key is missing, that's unexpected.
        let value = value.ok_or(anyhow!("missing collection metadata"))?;
        let meta: CollectionMeta = bincode::deserialize(value.as_slice())
            .map_err(|err| anyhow!("corrupted collection metadata: {}", err))?;
        Ok((current, Some(meta)))
    }

    async fn cas_meta(
        &self,
        expected: &Option<CollectionMeta>,
        new: &CollectionMeta,
    ) -> Result<Option<CollectionMeta>, anyhow::Error> {
        // WIP this is an entirely incorrect implementation of compare and set.
        // we'll actually want to use something like etcd
        let (current, durable) = self.fetch_meta().await?;
        if &durable != expected {
            return Ok(durable);
        }

        let key = Paths::version_key(&self.id, Uuid::new_v4());
        let value = bincode::serialize(&new)
            .map_err(|err| anyhow!("encoding collection metadata: {}", err))?;
        self.blob.set(&key, value, Atomicity::RequireAtomic).await?;

        self.log
            .compare_and_set(current, Some(key.into_bytes()))
            .await?;

        Ok(Some(new.clone()))
    }

    async fn fetch_reader_meta(&self, reader_id: &ReaderId) -> Result<ReaderMeta, anyhow::Error> {
        let key = Paths::reader_meta_key(&self.id, reader_id);
        let value = self.blob.get(&key).await?;
        let value = value.ok_or(anyhow!("missing reader metadata"))?;
        let meta: ReaderMeta = bincode::deserialize(value.as_slice())
            .map_err(|err| anyhow!("corrupted reader metadata: {}", err))?;
        Ok(meta)
    }

    async fn fetch_writer_meta(&self, writer_id: &WriterId) -> Result<WriterMeta, anyhow::Error> {
        let key = Paths::writer_meta_key(&self.id, writer_id);
        let value = self.blob.get(&key).await?;
        let value = value.ok_or(anyhow!("missing writer metadata"))?;
        let meta: WriterMeta = bincode::deserialize(value.as_slice())
            .map_err(|err| anyhow!("corrupted writer metadata: {}", err))?;
        Ok(meta)
    }

    pub async fn write_reader_meta(
        &self,
        reader_id: &ReaderId,
        meta: &ReaderMeta,
    ) -> Result<(), anyhow::Error> {
        let key = Paths::reader_meta_key(&self.id, reader_id);
        let value =
            bincode::serialize(meta).map_err(|err| anyhow!("encoding reader metadata: {}", err))?;
        self.blob.set(&key, value, Atomicity::RequireAtomic).await?;
        Ok(())
    }

    pub async fn write_writer_meta(
        &self,
        writer_id: &WriterId,
        meta: &WriterMeta,
    ) -> Result<(), anyhow::Error> {
        let key = Paths::writer_meta_key(&self.id, writer_id);
        let value =
            bincode::serialize(meta).map_err(|err| anyhow!("encoding reader metadata: {}", err))?;
        self.blob.set(&key, value, Atomicity::RequireAtomic).await?;
        Ok(())
    }
}
