// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::s3::S3BlobMultiWriter;
use mz_persist::storage::Atomicity;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};
use uuid::Uuid;

use crate::descs::BatchDescs;
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
    pub async fn update_metadata<F: FnMut(&Option<CollectionMeta>) -> CollectionMeta>(
        &mut self,
        mut f: F,
    ) -> Result<(Option<CollectionMeta>, CollectionMeta), anyhow::Error> {
        // TODO: Retry on failure.
        let (_, mut prev_meta) = self.fetch_meta().await?;
        loop {
            let new_meta = f(&prev_meta);
            // TODO: Retry on failure.
            let durable_meta = self.cas_meta(&prev_meta, &new_meta).await?;
            if durable_meta.as_ref() == Some(&new_meta) {
                return Ok((prev_meta.clone(), new_meta));
            }
            // We lost a race, try again.
            prev_meta = durable_meta;
        }
    }

    pub async fn fetch_since<T: Timestamp + Lattice + Codec64>(
        &self,
    ) -> Result<Antichain<T>, anyhow::Error> {
        let (_, meta) = self.fetch_meta().await?;
        let meta = meta.expect("WIP");
        self.fetch_sinces(meta.readers.as_slice()).await
    }

    pub async fn fetch_sinces<T: Timestamp + Lattice + Codec64>(
        &self,
        readers: &[ReaderId],
    ) -> Result<Antichain<T>, anyhow::Error> {
        let metas = readers.iter().map(|x| self.fetch_reader_meta(x));
        let mut ret = Antichain::from_elem(T::minimum());
        for meta in metas {
            let meta = meta.await?;
            let mut since = Antichain::new();
            let _ = since.extend(meta.since.iter().map(|x| T::decode(*x)));
            ret.meet_assign(&since);
        }
        Ok(ret)
    }

    pub async fn fetch_upper<T: Timestamp + Lattice + Codec64>(
        &self,
    ) -> Result<Antichain<T>, anyhow::Error> {
        let (_, meta) = self.fetch_meta().await?;
        let meta = meta.expect("WIP");
        self.fetch_uppers(meta.writers.as_slice()).await
    }

    pub async fn fetch_uppers<T: Timestamp + Lattice + Codec64>(
        &self,
        writers: &[WriterId],
    ) -> Result<Antichain<T>, anyhow::Error> {
        let metas = writers.iter().map(|x| self.fetch_writer_meta(x));
        let mut ret = Antichain::from_elem(T::minimum());
        for meta in metas {
            let meta = meta.await?;
            let mut since = Antichain::new();
            let _ = since.extend(meta.upper.iter().map(|x| T::decode(*x)));
            ret.join_assign(&since);
        }
        Ok(ret)
    }

    pub async fn fetch_batch_descs<T: Timestamp + Codec64>(
        &self,
    ) -> Result<BatchDescs<T>, anyhow::Error> {
        let pending = self
            .blob
            .list_prefix(&Paths::pending_batch_prefix(&self.id))
            .await?;
        let mut pending_by_writer = HashMap::<_, Vec<(String, Description<T>)>>::new();
        for key in pending {
            let (writer, desc) = Paths::parse_batch_key(&key)?;
            pending_by_writer
                .entry(writer)
                .or_default()
                .push((key, desc));
        }
        // WIP: Pick the furthest writer?
        let descs = match pending_by_writer.into_iter().next() {
            Some((_, descs)) => descs,
            None => vec![],
        };
        Ok(BatchDescs(descs))
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
    async fn fetch_meta(&self) -> Result<(SeqNo, Option<CollectionMeta>), anyhow::Error> {
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
