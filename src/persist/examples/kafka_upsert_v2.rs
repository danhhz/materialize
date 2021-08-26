// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;
use std::{cmp, env, process};

use ore::metrics::MetricsRegistry;
use ore::now::{system_time, NowFn};
use persist::file::{FileBlob, FileLog};
use persist::indexed::runtime::{self, RuntimeClient, StreamReadHandle, StreamWriteHandle};
use persist::storage::LockInfo;
use persist::{Codec, Data};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{operator, source, FrontieredInputHandle};
use timely::dataflow::operators::{Concat, Inspect, Map, Operator, ToStream};
use timely::dataflow::{Scope, Stream};

fn construct_persistent_kafka_upsert_source<G: Scope<Timestamp = u64>>(
    scope: &mut G,
    persist: RuntimeClient,
    name_base: &str,
) -> Result<
    (
        Stream<G, ((String, String), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ),
    Box<dyn Error>,
> {
    let (ts_write, ts_read) =
        persist.create_or_load::<KafkaOffset, AssignedTimestamp>(&format!("{}_ts", name_base))?;
    let (out_write, out_read) = persist
        .create_or_load::<(String, String), (KafkaOffset, AssignedTimestamp)>(&format!(
            "{}_out",
            name_base
        ))?;

    let epoch_interval_ms = 1000;
    let source_interval_ms = 2000;
    let timestamp_interval_ms = 5000;

    let start_epoch = cmp::min(sealed_ts(&ts_read)?, sealed_ts(&out_read)?);
    println!("Restored start epoch: {}", start_epoch);

    // Reload upsert state.
    // - TODO: Make this a third stream
    // - TODO: Do this as of start_ts
    // - TODO: Instead of the same one of these per worker, make them data
    //   parallel in the same way the computation is
    // - TODO: This needs to be respectful of ts and diff
    // - TODO: Don't use read_to_end_flattened
    let mut prev_value_by_key = HashMap::new();
    for ((k, v), ts, diff) in out_read.snapshot()?.read_to_end_flattened()? {
        if ts > start_epoch {
            continue;
        }
        prev_value_by_key.insert(k.clone(), ((k, v), ts, diff));
    }

    // Compute start offset.
    // - TODO: Don't use read_to_end_flattened
    // - TODO: Is this even actually how to find the right start offset?
    let mut start_offset = KafkaOffset(0);
    for ((offset, _), ts, _) in ts_read.snapshot()?.read_to_end_flattened()? {
        if ts > start_epoch {
            continue;
        }
        start_offset = cmp::max(start_offset, offset);
    }
    println!("Restored offset: {:?}", start_offset);
    // we don't want to repeat what we emitted last
    let start_offset = KafkaOffset(start_offset.0 + 1);
    println!("Start offset: {:?}", start_offset);

    println!("Restored output: {:?}", prev_value_by_key);

    // let out_ok_prev = prev_value_by_key
    prev_value_by_key
        .into_values()
        .to_stream(scope)
        .map(|((k, v), ts, diff)| ((k, format!("{:?} (restored)", v)), ts, diff))
        .inspect(|x| println!("{:?}", x));

    let epoch_source = epoch_source(scope, start_epoch, system_time, epoch_interval_ms);

    let raw_source = fake_kafka(
        scope,
        epoch_source,
        start_offset,
        system_time,
        source_interval_ms,
    );

    let (records, bindings) =
        assign_timestamps(scope, raw_source, system_time, timestamp_interval_ms);

    // TODO: wire up errors
    let out_ok_new = persist_records_and_bindings(scope, records, bindings, out_write, ts_write);
    let out_err_new = operator::empty(scope);

    // let ok_stream = out_ok_new.concat(&out_ok_prev);
    let ok_stream = out_ok_new;
    let err_stream = out_err_new;
    Ok((ok_stream, err_stream))
}

// HACK: This should be a method on StreamReadHandle that actually queries the
// runtime.
fn sealed_ts<K: Data, V: Data>(read: &StreamReadHandle<K, V>) -> Result<u64, Box<dyn Error>> {
    let mut sealed = 0;
    for (_, ts, _) in read.snapshot()?.read_to_end_flattened()? {
        sealed = cmp::max(sealed, ts);
    }
    Ok(sealed)
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default)]
struct KafkaOffset(u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default)]
struct AssignedTimestamp(u64);

fn epoch_source<G: Scope<Timestamp = u64>>(
    scope: &mut G,
    start_epoch: u64,
    now_fn: NowFn,
    interval_ms: u64,
) -> Stream<G, ()> {
    source(scope, "epoch_source", |mut cap, info| {
        let mut current_epoch = start_epoch;

        let activator = scope.activator_for(&info.address[..]);
        move |output| {
            let now = now_fn();
            let now_clamped = now - (now % interval_ms);

            if now_clamped != current_epoch {
                current_epoch = now_clamped;
                cap.downgrade(&current_epoch);
                let mut session = output.session(&cap);
                session.give(());
            }

            activator.activate_after(Duration::from_millis(100));
        }
    })
}

fn fake_kafka<G: Scope<Timestamp = u64>>(
    scope: &mut G,
    epoch_input: Stream<G, ()>,
    starting_offset: KafkaOffset,
    now_fn: NowFn,
    interval_ms: u64,
) -> Stream<G, ((String, String), KafkaOffset, isize)> {
    epoch_input.unary_frontier(Pipeline, "fake_kafka", |mut cap, info| {
        let mut offset = starting_offset.0;
        let current_ts = now_fn();
        let mut current_ts = current_ts - (current_ts % interval_ms);

        let activator = scope.activator_for(&info.address[..]);
        move |input, output| {
            input.for_each(|time, _data| {
                std::mem::swap(&mut cap, &mut time.retain());
            });

            let now = now_fn();
            let now_clamped = now - (now % interval_ms);
            if now_clamped != current_ts {
                current_ts = now_clamped;
                let kv = (format!("k{}", offset % 10), format!("v{}", offset));
                output.session(&cap).give((kv, KafkaOffset(offset), 1));
                offset += 1;
            }

            activator.activate_after(Duration::from_millis(100));
        }
    })
}

/// Mints a new timestamp binding every `update_interval_ms`. The current binding is assigned to
/// records that are coming through. When a new binding is minted the old binding is "closed" and
/// the current offset along with the binding is sent downstream.
///
/// Returns a stream of timestamped data and a stream of bindings.
fn assign_timestamps<G, D>(
    scope: &mut G,
    input: Stream<G, (D, KafkaOffset, isize)>,
    now_fn: NowFn,
    update_interval_ms: u64,
) -> (
    Stream<G, ((D, (KafkaOffset, AssignedTimestamp)), u64, isize)>,
    Stream<G, ((KafkaOffset, AssignedTimestamp), u64, isize)>,
)
where
    G: Scope<Timestamp = u64>,
    D: timely::Data,
{
    let mut assign_ts_op = OperatorBuilder::new("Assign Timestamps".to_string(), scope.clone());
    let mut assign_input = assign_ts_op.new_input(&input, Pipeline);

    let (mut records_out, records) = assign_ts_op.new_output();
    let (mut bindings_out, bindings) = assign_ts_op.new_output();

    let mut buffer = Vec::new();
    let mut current_ts = now_fn();
    let mut current_offset = 0;

    assign_ts_op.build(move |mut capabilities| {
        let mut records_capability = capabilities.remove(0);
        let mut bindings_capability = capabilities.remove(0);

        move |frontiers| {
            let mut assign_input = FrontieredInputHandle::new(&mut assign_input, &frontiers[0]);
            let mut records = records_out.activate();
            let mut bindings = bindings_out.activate();

            // TODO: Maybe we should have a "global" source of the current timestamp. Potentially,
            // a parallelism=1 operator that just mints and emits the current time periodically.
            let now = now_fn();
            let now_clamped = now - (now % update_interval_ms);
            if now_clamped != current_ts {
                let mut bindings_session = bindings.session(&bindings_capability);
                bindings_session.give((
                    (KafkaOffset(current_offset), AssignedTimestamp(current_ts)),
                    bindings_capability.time().clone(),
                    1,
                ));

                current_ts = now_clamped;
            }

            assign_input.for_each(|time, data| {
                let output_timestamp = time.clone();
                let mut new_cap = time.retain();
                if time.time() > records_capability.time() {
                    records_capability.downgrade(time);
                }
                if time.time() > bindings_capability.time() {
                    bindings_capability.downgrade(time);
                }
                data.swap(&mut buffer);

                let mut records_session = records.session(&records_capability);

                for (record, offset, diff) in buffer.drain(..) {
                    current_offset = std::cmp::max(offset.0, current_offset);
                    records_session.give((
                        (record, (offset, AssignedTimestamp(current_ts))),
                        output_timestamp,
                        diff,
                    ));
                }
            });
        }
    });

    (records, bindings)
}

/// Joins a stream of records to a stream of bindings. Records are stashed until we have a binding
/// that covers their offset/timestamp.
fn persist_records_and_bindings<G, K, V>(
    _scope: &mut G,
    records: Stream<G, (((K, V), (KafkaOffset, AssignedTimestamp)), u64, isize)>,
    bindings: Stream<G, ((KafkaOffset, AssignedTimestamp), u64, isize)>,
    records_write: StreamWriteHandle<(K, V), (KafkaOffset, AssignedTimestamp)>,
    bindings_write: StreamWriteHandle<KafkaOffset, AssignedTimestamp>,
) -> Stream<G, ((K, V), u64, isize)>
where
    G: Scope<Timestamp = u64>,
    K: timely::Data + Codec,
    V: timely::Data + Codec,
{
    let mut records_buffer = Vec::new();
    let mut bindings_buffer = Vec::new();

    let mut record_stash = HashMap::new();
    let mut bindings_stash = HashMap::new();

    let mut current_seal_ts = 0;

    records.binary_frontier(
        &bindings,
        Pipeline,
        Pipeline,
        "Await Bindings",
        move |_capability, _info| {
            move |records_input, bindings_input, output| {
                bindings_input.for_each(|time, data| {
                    data.swap(&mut bindings_buffer);

                    bindings_write
                        .write(bindings_buffer.iter().as_ref())
                        .recv()
                        .expect("writing bindings");

                    let stash_entry = bindings_stash
                        .entry(time.retain())
                        .or_insert_with(|| Vec::new());

                    for binding in bindings_buffer.drain(..) {
                        println!("Stashing binding: {:?}", binding);
                        stash_entry.push(binding);
                    }
                });

                records_input.for_each(|time, data| {
                    data.swap(&mut records_buffer);

                    records_write
                        .write(records_buffer.iter().as_ref())
                        .recv()
                        .expect("writing records");

                    let stash_entry = record_stash
                        .entry(time.retain())
                        .or_insert_with(|| Vec::new());

                    for record in records_buffer.drain(..) {
                        stash_entry.push(record);
                    }
                });

                // if there are more elements than first, we would have a problem
                let bindings_frontier = bindings_input.frontier().frontier().first().cloned();
                let records_frontier = records_input.frontier().frontier().first().cloned();

                match (bindings_frontier, records_frontier) {
                    (Some(bindings_frontier), Some(records_frontier)) => {
                        let combined_frontier = std::cmp::min(bindings_frontier, records_frontier);
                        if combined_frontier > current_seal_ts {
                            current_seal_ts = combined_frontier;
                            println!("sealing up to {}", current_seal_ts);

                            bindings_write
                                .seal(combined_frontier)
                                .recv()
                                .expect("sealing bindings");
                            records_write
                                .seal(combined_frontier)
                                .recv()
                                .expect("sealing records");

                            // TODO: Add real check for whether a given time/offset is covered. Or
                            // maybe not, because all we really need is that the read offsets are
                            // persisted. Which they are now.
                            bindings_stash
                                .retain(|time, _bindings| *time.time() >= combined_frontier);

                            let closed_ts: Vec<_> = record_stash
                                .keys()
                                .filter(|time| *time.time() <= combined_frontier)
                                .cloned()
                                .collect();

                            for closed_ts in closed_ts {
                                let mut records =
                                    record_stash.remove(&closed_ts).expect("missing records");
                                let mut output_session = output.session(&closed_ts);
                                for ((record, binding), _epoch_ts, diff) in records.drain(..) {
                                    output_session.give((record, binding.1 .0, diff));
                                }
                            }
                        }
                    }
                    _ => (),
                }
            }
        },
    )
}

fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    if args.len() != 2 {
        Err(format!("usage: {} <persist_dir>", &args[0]))?;
    }
    let base_dir = PathBuf::from(&args[1]);
    let persist = {
        let lock_info = LockInfo::new("kafka_upsert".into(), "nonce".into())?;
        let log = FileLog::new(base_dir.join("log"), lock_info.clone())?;
        let blob = FileBlob::new(base_dir.join("blob"), lock_info)?;
        runtime::start(log, blob, &MetricsRegistry::new())?
    };

    timely::execute_directly(|worker| {
        worker.dataflow(|scope| {
            let (ok_stream, err_stream) =
                construct_persistent_kafka_upsert_source(scope, persist, "persistent_kafka_1")
                    .unwrap_or_else(|err| {
                        let ok_stream = operator::empty(scope);
                        let err_stream = vec![(err.to_string(), 0, 1)].to_stream(scope);
                        (ok_stream, err_stream)
                    });
            ok_stream.inspect(|d| println!("ok: {:?}", d));
            err_stream.inspect(|d| println!("err: {:?}", d));
        })
    });

    Ok(())
}

fn main() {
    if let Err(err) = run(env::args().collect()) {
        eprintln!("error: {}", err);
        process::exit(1);
    }
}

mod kafka_offset_impls {
    use std::convert::TryFrom;

    use persist::Codec;

    use crate::AssignedTimestamp;
    use crate::KafkaOffset;

    impl Codec for KafkaOffset {
        fn codec_name() -> &'static str {
            "KafkaOffset"
        }

        fn size_hint(&self) -> usize {
            8
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(KafkaOffset(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }

    impl Codec for AssignedTimestamp {
        fn codec_name() -> &'static str {
            "AssignedTimestamp"
        }

        fn size_hint(&self) -> usize {
            8
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(AssignedTimestamp(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }
}
