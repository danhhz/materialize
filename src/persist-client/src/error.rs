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
pub struct Error {
    // WIP switch to anyhow here too
    inner: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl std::error::Error for Error {}

impl From<mz_persist::error::Error> for Error {
    fn from(x: mz_persist::error::Error) -> Self {
        Error {
            inner: x.to_string(),
        }
    }
}

impl From<String> for Error {
    fn from(x: String) -> Self {
        Error { inner: x }
    }
}

impl From<&str> for Error {
    fn from(x: &str) -> Self {
        Error {
            inner: x.to_owned(),
        }
    }
}

#[derive(Debug)]
pub struct Permanent {
    inner: anyhow::Error,
}

// WIP doc that this means permanent _to the caller_. E.g. if some data is
// corrupted, but it exists redundantly elsewhere, then it's possible that
// retrying at a higher level of the stack could succeed.
impl Permanent {
    // NB: Intentionally not From to make it blindingly obvious at which points
    // we're calling an error permanent.
    pub fn new(inner: anyhow::Error) -> Self {
        Permanent { inner }
    }
}

impl std::fmt::Display for Permanent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl std::error::Error for Permanent {}
