// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{AsCollection, Collection};
use mz_ore::cast::CastFrom;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::CriticalReaderId;
use mz_persist_client::operators::metadata::{
    CriticalReaderState, HollowBatch, LeasedReaderState, ShardDiffsStream, StateFieldDiff,
    StateFieldValDiff, WriterState,
};
use mz_persist_client::read::LeasedReaderId;
use mz_persist_client::write::WriterId;
use mz_persist_client::{PersistLocation, ShardId};
use mz_repr::{Datum, Diff, RelationDesc, Row, RowPacker, ScalarType, Timestamp};
use mz_timely_util::builder_async::OperatorBuilder;
use once_cell::sync::Lazy;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::Scope;
use timely::progress::Antichain;

pub static BATCHES_DESC: Lazy<RelationDesc> = Lazy::new(|| {
    RelationDesc::empty()
        .with_column("lower", ScalarType::UInt64.nullable(true))
        .with_column("upper", ScalarType::UInt64.nullable(true))
        .with_column("since", ScalarType::UInt64.nullable(true))
        .with_column("encoded_size_bytes", ScalarType::UInt64.nullable(false))
});

pub fn persist_metadata<G>(
    scope: &G,
    name: &str,
    clients: Arc<PersistClientCache>,
    location: PersistLocation,
    shard_id: ShardId,
) -> (PersistMetadata<G>, Rc<dyn Any>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let mut builder = OperatorBuilder::new(format!("shard_metadata({})", name), scope.clone());
    let (mut since_output, since_stream) = builder.new_output();
    let (mut batches_output, batches_stream) = builder.new_output();
    let (mut leased_readers_output, leased_readers_stream) = builder.new_output();
    let (mut critical_readers_output, critical_readers_stream) = builder.new_output();
    let (mut writers_output, writers_stream) = builder.new_output();

    let worker_idx = scope.index();
    let shutdown_button = builder.build(move |mut caps| async move {
        if worker_idx != 0 {
            return;
        }

        let client = clients
            .open(location)
            .await
            .expect("location should be valid");
        let mut diff_stream = ShardDiffsStream::<Timestamp>::new(client, shard_id)
            .await
            .expect("ts codec should match");
        while let Some(diff) = diff_stream.next().await {
            let ts = Timestamp::new(diff.walltime_ms);

            emit_diffs(&diff.since, since_to_row, |row, diff| {
                let mut output = since_output.activate();
                output.session(&caps[0].delayed(&ts)).give((row, ts, diff));
            });
            emit_diffs(&diff.spine, spine_to_row, |row, diff| {
                let mut output = batches_output.activate();
                output.session(&caps[1].delayed(&ts)).give((row, ts, diff));
            });
            emit_diffs(&diff.leased_readers, leased_reader_to_row, |row, diff| {
                let mut output = leased_readers_output.activate();
                output.session(&caps[2].delayed(&ts)).give((row, ts, diff));
            });
            emit_diffs(
                &diff.critical_readers,
                critical_reader_to_row,
                |row, diff| {
                    let mut output = critical_readers_output.activate();
                    output.session(&caps[3].delayed(&ts)).give((row, ts, diff));
                },
            );
            emit_diffs(&diff.writers, writer_to_row, |row, diff| {
                let mut output = writers_output.activate();
                output.session(&caps[4].delayed(&ts)).give((row, ts, diff));
            });

            for cap in caps.iter_mut() {
                cap.downgrade(&ts);
            }
        }
    });

    let metadata = PersistMetadata {
        since: since_stream.as_collection(),
        batches: batches_stream.as_collection(),
        leased_readers: leased_readers_stream.as_collection(),
        critical_readers: critical_readers_stream.as_collection(),
        writers: writers_stream.as_collection(),
    };
    (metadata, Rc::new(shutdown_button.press_on_drop()))
}

pub struct PersistMetadata<G: Scope> {
    // RelationDesc::empty()
    //   .with_column("ts", ScalarType::UInt64.nullable(true))
    pub since: Collection<G, Row, Diff>,
    /// [BATCHES_DESC]
    pub batches: Collection<G, Row, Diff>,
    // RelationDesc::empty()
    //   .with_column("id", ScalarType::String.nullable(false))
    //   .with_column("since", ScalarType::UInt64.nullable(true))
    //   .with_column("seqno", ScalarType::UInt64.nullable(false))
    //   .with_column("last_heartbeat_timestamp_ms", ScalarType::UInt64.nullable(false))
    //   .with_column("hostname", ScalarType::String.nullable(false))
    //   .with_column("purpose", ScalarType::String.nullable(false))
    pub leased_readers: Collection<G, Row, Diff>,
    // RelationDesc::empty()
    //   .with_column("id", ScalarType::String.nullable(false))
    //   .with_column("since", ScalarType::UInt64.nullable(true))
    //   .with_column("opaque", ScalarType::UInt64.nullable(true))
    //   .with_column("hostname", ScalarType::String.nullable(false))
    //   .with_column("purpose", ScalarType::String.nullable(false))
    pub critical_readers: Collection<G, Row, Diff>,
    // RelationDesc::empty()
    //   .with_column("id", ScalarType::String.nullable(false))
    //   .with_column("most_recent_write_upper", ScalarType::UInt64.nullable(false))
    //   .with_column("last_heartbeat_timestamp_ms", ScalarType::UInt64.nullable(false))
    //   .with_column("hostname", ScalarType::String.nullable(false))
    //   .with_column("purpose", ScalarType::String.nullable(false))
    pub writers: Collection<G, Row, Diff>,
}

fn since_to_row(_: &(), since: &Antichain<Timestamp>, row: &mut RowPacker) {
    row.push(Datum::from(since.as_option().copied()));
}

fn spine_to_row(batch: &HollowBatch<Timestamp>, _: &(), row: &mut RowPacker) {
    row.push(Datum::from(
        batch.desc.lower().as_option().map(|x| u64::from(x)),
    ));
    row.push(Datum::from(
        batch.desc.upper().as_option().map(|x| u64::from(x)),
    ));
    row.push(Datum::from(
        batch.desc.since().as_option().map(|x| u64::from(x)),
    ));
    row.push(Datum::from(u64::cast_from(
        batch
            .parts
            .iter()
            .map(|x| x.encoded_size_bytes)
            .sum::<usize>(),
    )));
}

fn leased_reader_to_row(
    id: &LeasedReaderId,
    meta: &LeasedReaderState<Timestamp>,
    row: &mut RowPacker,
) {
    row.push(Datum::from(id.to_string().as_str()));
    row.push(Datum::from(meta.since.as_option().copied()));
    row.push(Datum::from(meta.seqno.0));
    row.push(Datum::from(meta.last_heartbeat_timestamp_ms));
    row.push(Datum::from(meta.debug.hostname.as_str()));
    row.push(Datum::from(meta.debug.purpose.as_str()));
}

fn critical_reader_to_row(
    id: &CriticalReaderId,
    meta: &CriticalReaderState<Timestamp>,
    row: &mut RowPacker,
) {
    row.push(Datum::from(id.to_string().as_str()));
    row.push(Datum::from(meta.since.as_option().copied()));
    row.push(Datum::from(u64::from_le_bytes(meta.opaque.0)));
    row.push(Datum::from(meta.debug.hostname.as_str()));
    row.push(Datum::from(meta.debug.purpose.as_str()));
}

fn writer_to_row(id: &WriterId, meta: &WriterState<Timestamp>, row: &mut RowPacker) {
    row.push(Datum::from(id.to_string().as_str()));
    row.push(Datum::from(
        meta.most_recent_write_upper.as_option().copied(),
    ));
    row.push(Datum::from(meta.last_heartbeat_timestamp_ms));
    row.push(Datum::from(meta.debug.hostname.as_str()));
    row.push(Datum::from(meta.debug.purpose.as_str()));
}

fn emit_diffs<K, V, ToRow: FnMut(&K, &V, &mut RowPacker), Output: FnMut(Row, Diff)>(
    diffs: &[StateFieldDiff<K, V>],
    mut to_row: ToRow,
    mut output: Output,
) {
    for diff in diffs.iter() {
        match &diff.val {
            StateFieldValDiff::Insert(val) => {
                let mut row = Row::default();
                to_row(&diff.key, val, &mut row.packer());
                output(row, 1);
            }
            StateFieldValDiff::Delete(val) => {
                let mut row = Row::default();
                to_row(&diff.key, val, &mut row.packer());
                output(row, -1);
            }
            StateFieldValDiff::Update(from, to) => {
                let mut row = Row::default();
                to_row(&diff.key, from, &mut row.packer());
                output(row, -1);
                let mut row = Row::default();
                to_row(&diff.key, to, &mut row.packer());
                output(row, 1);
            }
        }
    }
}

impl<G: Scope> PersistMetadata<G>
where
    G::Timestamp: std::fmt::Debug,
{
    pub fn inspect(&self) -> ProbeHandle<G::Timestamp> {
        let mut probe = ProbeHandle::default();
        self.since
            .inspect(|(row, ts, diff)| eprintln!("since: {} {:?} {:?}", diff, ts, row))
            .probe_with(&mut probe);
        self.batches
            .inspect(|(row, ts, diff)| eprintln!("batches: {} {:?} {:?}", diff, ts, row))
            .probe_with(&mut probe);
        self.leased_readers
            .inspect(|(row, ts, diff)| eprintln!("leased_readers: {} {:?} {:?}", diff, ts, row))
            .probe_with(&mut probe);
        self.critical_readers
            .inspect(|(row, ts, diff)| eprintln!("critical_readers: {} {:?} {:?}", diff, ts, row))
            .probe_with(&mut probe);
        self.writers
            .inspect(|(row, ts, diff)| eprintln!("writers: {} {:?} {:?}", diff, ts, row))
            .probe_with(&mut probe);
        probe
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::{NowFn, SYSTEM_TIME};
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
    use timely::communication::Allocate;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
    use timely::worker::Worker;
    use timely::Config;
    use tokio::runtime::Handle as TokioHandle;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn persist_metadata() {
        mz_ore::test::init_logging();

        let handle = TokioHandle::current();
        let clock = SYSTEM_TIME.clone();
        let clients = Arc::new(PersistClientCache::new(
            PersistConfig::new(&DUMMY_BUILD_INFO, clock.clone()),
            &MetricsRegistry::new(),
        ));
        let location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        let shard_id = ShardId::new();
        let client = clients
            .open(location.clone())
            .await
            .expect("location should be valid");
        let read = client
            .open_leased_reader::<String, (), u64, i64>(
                shard_id,
                "test",
                Arc::new(StringSchema),
                Arc::new(UnitSchema),
            )
            .await
            .expect("codecs should match");

        let res = timely::execute(Config::process(1), move |worker| {
            let guard = handle.enter();

            let clients = Arc::clone(&clients);
            let location = location.clone();
            let client = client.clone();
            let (probe, token) = worker.dataflow(|scope| {
                let (metadata, token) =
                    super::persist_metadata(scope, "test", clients, location, shard_id);
                let probe = metadata.inspect();
                (probe, token)
            });

            struct RunAndWait<'a, A: Allocate>(
                &'a NowFn,
                &'a TokioHandle,
                &'a ProbeHandle<Timestamp>,
                &'a mut Worker<A>,
            );

            impl<A: Allocate> RunAndWait<'_, A> {
                fn call<T: Future, F: FnOnce() -> T>(&mut self, f: F) -> T::Output
                where
                    T::Output: std::fmt::Debug,
                {
                    let ts = self.0();
                    let ret = self.1.block_on(f());
                    let mut frontier = Antichain::from_elem(Timestamp::minimum());
                    self.3.step_while(|| {
                        self.2.with_frontier(|f| {
                            if frontier.borrow() != f {
                                frontier = f.to_owned();
                                tracing::info!("waiting for {} at {:?}", ts, frontier.elements());
                            }
                        });
                        self.2.less_than(&Timestamp::new(ts))
                    });
                    ret
                }
            }
            let mut run_and_wait = RunAndWait(&clock, &handle, &probe, worker);

            let mut write = run_and_wait
                .call(|| {
                    client.open_writer::<String, (), u64, i64>(
                        shard_id,
                        "test",
                        Arc::new(StringSchema),
                        Arc::new(UnitSchema),
                    )
                })
                .expect("codecs should match");

            for ts in 0..100 {
                run_and_wait.call(|| async {
                    let updates = vec![((ts.to_string(), ()), ts, 1)];
                    let () = write
                        .compare_and_append(
                            updates,
                            Antichain::from_elem(ts),
                            Antichain::from_elem(ts + 1),
                        )
                        .await
                        .expect("usage should be valid")
                        .expect("upper should match");
                    if ts % 2 == 0 {
                        let mut read = client
                            .open_leased_reader::<String, (), u64, i64>(
                                shard_id,
                                &format!("r{}", ts),
                                Arc::new(StringSchema),
                                Arc::new(UnitSchema),
                            )
                            .await
                            .expect("codecs should match");
                        read.downgrade_since(&Antichain::from_elem(ts / 2)).await;
                        read.expire().await;
                    }
                })
            }

            let () = run_and_wait.call(|| write.expire());
            drop(token);
            drop(guard);
        });
        let _ = res.unwrap();

        drop(read);
    }
}
