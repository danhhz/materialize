// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// WIP: Define the semantics of this. In particular, we should use this
// opportunity to think through where we need to push retries into persist and
// which places need structured information back about errors we can't recover
// from.
#[derive(Debug)]
pub struct Error;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!("{:?}", f.width())
    }
}

impl std::error::Error for Error {}
