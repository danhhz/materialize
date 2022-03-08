// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::trace::Description;
use timely::progress::Timestamp;
use timely::PartialOrder;
use tracing::error;

#[derive(Debug)]
pub struct BatchDescs<T>(pub(crate) Vec<(String, Description<T>)>);

impl<T: Timestamp> BatchDescs<T> {
    pub fn cover(
        self,
        query: &Description<T>,
    ) -> Result<Vec<(String, Description<T>)>, anyhow::Error> {
        let mut prev_upper = query.lower().clone();
        let mut batches = Vec::new();
        for (key, desc) in self.0 {
            if desc.lower() == &prev_upper {
                prev_upper = desc.upper().clone();
                batches.push((key, desc));
            }
            // WIP: Is this right?
            if PartialOrder::less_equal(query.upper(), &prev_upper) {
                return Ok(batches);
            }
        }
        error!(
            "query={:?} prev_upper={:?} batches={:?}",
            query, prev_upper, batches
        );
        panic!("TODO: More resilient implementation of this");
    }
}
