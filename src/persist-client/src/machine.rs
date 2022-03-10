// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::HashMap;
use std::time::Instant;

use anyhow::anyhow;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::storage::StorageError;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::collection::Collection;
use crate::error::InvalidUsage;
use crate::metadata::{CollectionMeta, ReaderMeta, TraceBatchMeta, WriterMeta};
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::{Log, SeqNo};

#[derive(Clone, Debug)]
pub struct ReadCapability<T> {
    pub(crate) seqno: SeqNo,
    pub(crate) since: Antichain<T>,
}

#[derive(Clone, Debug)]
pub struct WriteCapability<T> {
    pub(crate) seqno: SeqNo,
    pub(crate) upper: Antichain<T>,
}

#[derive(Clone, Debug)]
pub struct State<T> {
    pub(crate) writers: HashMap<WriterId, WriteCapability<T>>,
    pub(crate) readers: HashMap<ReaderId, ReadCapability<T>>,
    pub(crate) trace: Vec<(String, Description<T>)>,
}

impl<T> Default for State<T> {
    fn default() -> Self {
        Self {
            writers: Default::default(),
            readers: Default::default(),
            trace: Default::default(),
        }
    }
}

impl<T: Timestamp + Lattice + Codec64> State<T> {
    pub fn from_meta(state: Option<&CollectionMeta>) -> Self {
        let state = match state {
            Some(x) => x,
            None => return Self::default(),
        };
        let writers = state
            .writers
            .iter()
            .map(|(id, meta)| {
                let upper = meta.upper.iter().map(|x| T::decode(*x)).collect::<Vec<_>>();
                let cap = WriteCapability {
                    seqno: meta.seqno,
                    upper: Antichain::from(upper),
                };
                (id.clone(), cap)
            })
            .collect();
        let readers = state
            .readers
            .iter()
            .map(|(id, meta)| {
                let since = meta.since.iter().map(|x| T::decode(*x)).collect::<Vec<_>>();
                let cap = ReadCapability {
                    seqno: meta.seqno,
                    since: Antichain::from(since),
                };
                (id.clone(), cap)
            })
            .collect();
        let trace = state
            .trace
            .iter()
            .map(|(key, desc)| {
                let lower = desc.lower.iter().map(|x| T::decode(*x)).collect::<Vec<_>>();
                let upper = desc.upper.iter().map(|x| T::decode(*x)).collect::<Vec<_>>();
                let since = desc.since.iter().map(|x| T::decode(*x)).collect::<Vec<_>>();
                let desc = Description::new(
                    Antichain::from(lower),
                    Antichain::from(upper),
                    Antichain::from(since),
                );
                (key.clone(), desc)
            })
            .collect();
        State {
            writers,
            readers,
            trace,
        }
    }

    pub fn to_meta(&self) -> CollectionMeta {
        CollectionMeta {
            writers: self
                .writers
                .iter()
                .map(|(id, cap)| {
                    let upper = cap.upper.elements().iter().map(|x| T::encode(x)).collect();
                    let meta = WriterMeta {
                        seqno: cap.seqno,
                        upper,
                    };
                    (id.clone(), meta)
                })
                .collect(),
            readers: self
                .readers
                .iter()
                .map(|(id, cap)| {
                    let since = cap.since.elements().iter().map(|x| T::encode(x)).collect();
                    let meta = ReaderMeta {
                        seqno: cap.seqno,
                        since,
                    };
                    (id.clone(), meta)
                })
                .collect(),
            trace: self
                .trace
                .iter()
                .map(|(key, desc)| {
                    let desc = TraceBatchMeta {
                        lower: desc.lower().iter().map(|x| T::encode(x)).collect(),
                        upper: desc.upper().iter().map(|x| T::encode(x)).collect(),
                        since: desc.since().iter().map(|x| T::encode(x)).collect(),
                    };
                    (key.clone(), desc)
                })
                .collect(),
            key_codec: "WIP".into(),
            val_codec: "WIP".into(),
            ts_codec: "WIP".into(),
            diff_codec: "WIP".into(),
        }
    }

    pub fn since(&self) -> Antichain<T> {
        //  WIP double check this initial and the meet_assign
        //
        // WIP what happens if all readers go away and then some come back?
        let mut ret = Antichain::from_elem(T::minimum());
        for (_, cap) in self.readers.iter() {
            ret.meet_assign(&cap.since);
        }
        ret
    }

    pub fn upper(&self) -> Antichain<T> {
        // WIP double check this initial and the join_assign
        //
        // WIP what happens if all writers go away and then some come back?
        let mut ret = Antichain::from_elem(T::minimum());
        for (_, cap) in self.writers.iter() {
            ret.join_assign(&cap.upper);
        }
        ret
    }

    pub fn seqno_since(&self) -> SeqNo {
        let mut ret = None;
        // WIP what happens if all writers go away and then some come back?
        for (_, cap) in self.readers.iter() {
            let prev = ret.get_or_insert(cap.seqno);
            *prev = cmp::min(*prev, cap.seqno);
        }
        ret.unwrap_or_default()
    }
}

// NB Clone for the interim while this is being run locally.
#[derive(Clone)]
pub struct Machine<T> {
    pub(crate) collection: Collection,

    pub(crate) state: State<T>,
    pub(crate) seqno: SeqNo,
}

impl<T: Timestamp + Lattice + Codec64> Machine<T> {
    pub fn new(collection: Collection) -> Self {
        Machine {
            collection,
            state: State::default(),
            seqno: SeqNo::default(),
        }
    }

    pub async fn register(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
        reader_id: &ReaderId,
    ) -> Result<(WriteCapability<T>, ReadCapability<T>), StorageError> {
        let (seqno, (write_cap, read_cap)) = self
            .apply_unbatched_cmd(deadline, |seqno, state| {
                let write_cap = WriteCapability {
                    seqno,
                    upper: state.upper(),
                };
                state.writers.insert(writer_id.clone(), write_cap.clone());
                let read_cap = ReadCapability {
                    seqno,
                    since: state.since(),
                };
                state.readers.insert(reader_id.clone(), read_cap.clone());
                (write_cap, read_cap)
            })
            .await?;
        debug_assert_eq!(seqno, write_cap.seqno);
        debug_assert_eq!(seqno, read_cap.seqno);
        Ok((write_cap, read_cap))
    }

    pub async fn write_batch(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
        key: &str,
        desc: &Description<T>,
    ) -> Result<Result<SeqNo, InvalidUsage>, StorageError> {
        let (seqno, res) = self
            .apply_unbatched_cmd(deadline, |_, state| {
                let write_cap = state
                    .writers
                    .get_mut(writer_id)
                    // WIP this is more likely that the lease expired
                    .ok_or_else(|| InvalidUsage(anyhow!("writer not registered")))?;
                if &write_cap.upper != desc.lower() {
                    return Err(InvalidUsage(anyhow!("WIP")));
                }
                write_cap.upper.clone_from(desc.upper());
                // WIP have to trim desc
                state.trace.push((key.to_owned(), desc.clone()));
                Ok(())
            })
            .await?;
        if let Err(err) = res {
            return Ok(Err(err));
        }
        Ok(Ok(seqno))
    }

    pub async fn downgrade_since(
        &mut self,
        deadline: Instant,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> Result<Result<SeqNo, InvalidUsage>, StorageError> {
        let (seqno, res) = self
            .apply_unbatched_cmd(deadline, |_, state| {
                let read_cap = state
                    .readers
                    .get_mut(reader_id)
                    // WIP this is more likely that the lease expired
                    .ok_or_else(|| InvalidUsage(anyhow!("reader not registered")))?;
                if !PartialOrder::less_equal(&read_cap.since, new_since) {
                    return Err(InvalidUsage(anyhow!("WIP")));
                }
                read_cap.since.clone_from(new_since);
                Ok(())
            })
            .await?;
        if let Err(err) = res {
            return Ok(Err(err));
        }
        self.maybe_compact_log(deadline).await?;
        Ok(Ok(seqno))
    }

    pub async fn deregister_writer(
        &mut self,
        deadline: Instant,
        writer_id: &WriterId,
    ) -> Result<SeqNo, StorageError> {
        let (seqno, ()) = self
            .apply_unbatched_cmd(deadline, |_, state| {
                state.writers.remove(writer_id);
            })
            .await?;
        Ok(seqno)
    }

    pub async fn deregister_reader(
        &mut self,
        deadline: Instant,
        reader_id: &ReaderId,
    ) -> Result<SeqNo, StorageError> {
        let (seqno, ()) = self
            .apply_unbatched_cmd(deadline, |_, state| {
                state.readers.remove(reader_id);
            })
            .await?;
        self.maybe_compact_log(deadline).await?;
        Ok(seqno)
    }

    pub async fn handle_compact_trace_res(
        &mut self,
        deadline: Instant,
        res: CompactTraceRes<T>,
    ) -> Result<SeqNo, StorageError> {
        let (seqno, ()) = self
            .apply_unbatched_cmd(deadline, |_, state| todo!("{:?} {:?}", state, res))
            .await?;
        Ok(seqno)
    }

    async fn apply_unbatched_cmd<R, WorkFn: FnMut(SeqNo, &mut State<T>) -> R>(
        &mut self,
        deadline: Instant,
        mut work_fn: WorkFn,
    ) -> Result<(SeqNo, R), StorageError> {
        let (_, _, seqno, ret) = self
            .collection
            .update_metadata(deadline, |durable_seqno, durable_state| {
                if self.seqno != durable_seqno {
                    self.seqno = durable_seqno;
                    self.state = State::from_meta(durable_state.as_ref());
                }
                let new_seqno = SeqNo(durable_seqno.0 + 1);
                let ret = work_fn(new_seqno, &mut self.state);
                let new_state = self.state.to_meta();
                (new_state, ret)
            })
            .await?;
        Ok((seqno, ret))
    }

    async fn maybe_compact_log(&mut self, deadline: Instant) -> Result<(), StorageError> {
        // TODO: GC old blobs here instead of leaking them.
        self.collection
            .log
            .compact(deadline, self.state.seqno_since())
            .await?;
        Ok(())
    }

    async fn maybe_compact_trace(&mut self, deadline: Instant) -> Result<(), StorageError> {
        let mut batches = self.state.trace.iter();
        // WIP not the compaction algorithm that we'll use
        let req = match (batches.next(), batches.next()) {
            (Some(b0), Some(b1)) => CompactTraceReq {
                b0: b0.clone(),
                b1: b1.clone(),
                since: self.state.since(),
            },
            _ => return Ok(()),
        };
        // WIP actually write out the batch.
        let merged = Description::new(
            req.b0.1.lower().clone(),
            req.b1.1.upper().clone(),
            req.since.clone(),
        );
        let res = CompactTraceRes {
            req,
            merged: ("WIP".into(), merged),
        };
        self.handle_compact_trace_res(deadline, res).await?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactTraceReq<T: Timestamp + Lattice + Codec64> {
    /// One of the batches to be merged.
    pub b0: (String, Description<T>),
    /// One of the batches to be merged.
    pub b1: (String, Description<T>),
    /// The since frontier to be used for the output batch. This must be at or
    /// in advance of the since frontier for both of the input batch.
    pub since: Antichain<T>,
}

/// A successful merge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactTraceRes<T: Timestamp + Lattice + Codec64> {
    /// The original request, so the caller doesn't have to do this matching.
    pub req: CompactTraceReq<T>,
    /// The compacted batch.
    pub merged: (String, Description<T>),
}
