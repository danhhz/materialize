// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::anyhow;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::collection::Collection;
use crate::metadata::{CollectionMeta, TraceBatchMeta};
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::SeqNo;

#[derive(Clone)]
pub struct State<T> {
    pub(crate) writers: HashMap<WriterId, Antichain<T>>,
    pub(crate) readers: HashMap<ReaderId, Antichain<T>>,
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
            .map(|(id, upper)| {
                let upper = upper.iter().map(|x| T::decode(*x)).collect::<Vec<_>>();
                (id.clone(), Antichain::from(upper))
            })
            .collect();
        let readers = state
            .readers
            .iter()
            .map(|(id, since)| {
                let since = since.iter().map(|x| T::decode(*x)).collect::<Vec<_>>();
                (id.clone(), Antichain::from(since))
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
                .map(|(id, upper)| {
                    let upper = upper.elements().iter().map(|x| T::encode(x)).collect();
                    (id.clone(), upper)
                })
                .collect(),
            readers: self
                .readers
                .iter()
                .map(|(id, upper)| {
                    let upper = upper.elements().iter().map(|x| T::encode(x)).collect();
                    (id.clone(), upper)
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

    fn since(&self) -> Antichain<T> {
        //  WIP double check this initial and the meet_assign
        let mut ret = Antichain::from_elem(T::minimum());
        for (_, since) in self.readers.iter() {
            ret.meet_assign(&since);
        }
        ret
    }

    fn upper(&self) -> Antichain<T> {
        // WIP double check this initial and the join_assign
        let mut ret = Antichain::from_elem(T::minimum());
        for (_, upper) in self.writers.iter() {
            ret.join_assign(&upper);
        }
        ret
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
        writer_id: &WriterId,
        reader_id: &ReaderId,
    ) -> (Antichain<T>, Antichain<T>) {
        self.apply_unbatched_cmd(|state| {
            let upper = state.upper();
            state.writers.insert(writer_id.clone(), upper.clone());
            let since = state.since();
            state.readers.insert(reader_id.clone(), since.clone());
            (since, upper)
        })
        .await
    }

    pub async fn write_batch(
        &mut self,
        writer_id: &WriterId,
        key: &str,
        desc: &Description<T>,
    ) -> Result<(), anyhow::Error> {
        self.apply_unbatched_cmd(|state| {
            let writer_upper = state
                .writers
                .get_mut(writer_id)
                .expect("writer not registered");
            if writer_upper != desc.lower() {
                return Err(anyhow!("WIP"));
            }
            *writer_upper = desc.upper().clone();
            // WIP have to trim desc
            state.trace.push((key.to_owned(), desc.clone()));
            Ok(())
        })
        .await
    }

    pub async fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> Result<(), anyhow::Error> {
        self.apply_unbatched_cmd(|state| {
            let reader_since = state
                .readers
                .get_mut(reader_id)
                .expect("reader not registered");
            if !PartialOrder::less_equal(reader_since, new_since) {
                return Err(anyhow!("WIP"));
            }
            *reader_since = new_since.clone();
            Ok(())
        })
        .await
    }

    async fn apply_unbatched_cmd<R, WorkFn: FnMut(&mut State<T>) -> R>(
        &mut self,
        mut work_fn: WorkFn,
    ) -> R {
        let (_, _, ret) = self
            .collection
            .update_metadata(|durable_seqno, durable_state| {
                if self.seqno != durable_seqno {
                    self.seqno = durable_seqno;
                    self.state = State::from_meta(durable_state.as_ref());
                }
                let ret = work_fn(&mut self.state);
                let new_state = self.state.to_meta();
                (new_state, ret)
            })
            .await
            .expect("WIP");
        ret
    }
}
