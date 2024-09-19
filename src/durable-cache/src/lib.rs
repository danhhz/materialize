// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WIP

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use mz_ore::assert_none;
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::{ListenEvent, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::{Codec, ShardId};
use timely::progress::Antichain;

pub trait DurableCacheCodec {
    type Key: Ord + Clone + Debug;
    type Val: Ord + Clone + Debug;
    type KeyCodec: Codec + Debug;
    type ValCodec: Codec + Debug;

    fn schemas() -> (
        <Self::KeyCodec as Codec>::Schema,
        <Self::ValCodec as Codec>::Schema,
    );
    fn encode_key(key: &Self::Key) -> Self::KeyCodec;
    fn encode_val(val: &Self::Val) -> Self::ValCodec;
    fn decode_key(key: Self::KeyCodec) -> Self::Key;
    fn decode_val(val: Self::ValCodec) -> Self::Val;
}

#[derive(Debug)]
pub struct DurableCache<C: DurableCacheCodec> {
    write: WriteHandle<C::KeyCodec, C::ValCodec, u64, i64>,
    subscribe: Subscribe<C::KeyCodec, C::ValCodec, u64, i64>,

    local: BTreeMap<C::Key, C::Val>,
    local_progress: u64,
}

impl<C: DurableCacheCodec> DurableCache<C> {
    pub async fn new(persist: &PersistClient, shard_id: ShardId) -> Self {
        let use_critical_since = true;
        let (key_schema, val_schema) = C::schemas();
        let (mut write, read) = persist
            .open(
                shard_id,
                Arc::new(key_schema),
                Arc::new(val_schema),
                Diagnostics::from_purpose("WIP"),
                use_critical_since,
            )
            .await
            .expect("shard codecs should not change");
        // Ensure that at least one ts is immediately readable, for convenience.
        let res = write
            .compare_and_append_batch(&mut [], Antichain::from_elem(0), Antichain::from_elem(1))
            .await
            .expect("usage was valid");
        match res {
            // We made the ts readable.
            Ok(()) => {}
            // Someone else made it readable.
            Err(UpperMismatch { .. }) => {}
        }

        let as_of = read.since().clone();
        let subscribe = read
            .subscribe(as_of)
            .await
            .expect("capability should be held at this since");
        let mut ret = DurableCache {
            write,
            subscribe,
            local: BTreeMap::new(),
            local_progress: 0,
        };
        ret.sync_to(ret.write.upper().as_option().copied()).await;
        ret
    }

    async fn sync_to(&mut self, progress: Option<u64>) -> u64 {
        let progress = progress.expect("cache shard should not be closed");
        while self.local_progress < progress {
            let events = self.subscribe.fetch_next().await;
            for event in events {
                match event {
                    ListenEvent::Updates(x) => {
                        for ((k, v), _t, d) in x {
                            let (key, val) = (C::decode_key(k.unwrap()), C::decode_val(v.unwrap()));
                            if d <= 0 {
                                // WIP remove from local
                                continue;
                            }
                            let prev = self.local.insert(key, val);
                            assert_none!(prev);
                        }
                    }
                    ListenEvent::Progress(x) => {
                        self.local_progress =
                            x.into_option().expect("cache shard should not be closed")
                    }
                }
            }
        }
        progress
    }

    pub fn get_local(&self, key: &C::Key) -> Option<&C::Val> {
        self.local.get(key)
    }

    // WIP remove the Clone bound on C::Val and return a ref here
    pub async fn get(&mut self, key: &C::Key, val_fn: impl FnOnce() -> C::Val) -> C::Val {
        // Fast-path if it's already in the local cache.
        if let Some(val) = self.local.get(key) {
            return val.clone();
        }

        // Reduce wasted work by ensuring we're caught up to at least the
        // pubsub-updated shared_upper, and then trying again.
        self.sync_to(self.write.shared_upper().into_option()).await;
        if let Some(val) = self.local.get(key) {
            return val.clone();
        }

        // Okay compute it and write it durably to the cache.
        let val = val_fn();
        let mut expected_upper = self.local_progress;
        loop {
            let update = ((C::encode_key(key), C::encode_val(&val)), expected_upper, 1);
            let new_upper = expected_upper + 1;
            let ret = self
                .write
                .compare_and_append(
                    &[update],
                    Antichain::from_elem(expected_upper),
                    Antichain::from_elem(new_upper),
                )
                .await
                .expect("usage should be valid");
            match ret {
                Ok(()) => {
                    let prev = self.local.insert(key.clone(), val);
                    assert_none!(prev);
                    self.local_progress = new_upper;
                    return self.get_local(key).expect("just inserted").clone();
                }
                Err(err) => {
                    expected_upper = self.sync_to(err.current.into_option()).await;
                    if let Some(val) = self.local.get(key) {
                        return val.clone();
                    }
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_types::codec_impls::StringSchema;
    use mz_persist_types::PersistLocation;

    use super::*;

    struct TestCodec;

    impl DurableCacheCodec for TestCodec {
        type Key = String;
        type Val = String;
        type KeyCodec = String;
        type ValCodec = String;

        fn schemas() -> (
            <Self::KeyCodec as Codec>::Schema,
            <Self::ValCodec as Codec>::Schema,
        ) {
            (StringSchema, StringSchema)
        }

        fn encode_key(key: &Self::Key) -> Self::KeyCodec {
            key.clone()
        }
        fn encode_val(val: &Self::Val) -> Self::ValCodec {
            val.clone()
        }
        fn decode_key(key: Self::KeyCodec) -> Self::Key {
            key
        }
        fn decode_val(val: Self::ValCodec) -> Self::Val {
            val
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn durable_cache() {
        let persist = PersistClientCache::new_no_metrics();
        let persist = persist
            .open(PersistLocation::new_in_mem())
            .await
            .expect("location should be valid");
        let shard_id = ShardId::new();

        let mut cache0 = DurableCache::<TestCodec>::new(&persist, shard_id).await;
        assert_none!(cache0.get_local(&"foo".into()));
        assert_eq!(cache0.get(&"foo".into(), || "bar".into()).await, "bar");

        let mut cache1 = DurableCache::<TestCodec>::new(&persist, shard_id).await;
        assert_eq!(cache1.get(&"foo".into(), || panic!("boom")).await, "bar");
    }
}
