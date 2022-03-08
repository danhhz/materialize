// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(Debug)]
pub struct StorageError {
    inner: String,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage: {}", self.inner)
    }
}

impl std::error::Error for StorageError {}

impl From<String> for StorageError {
    fn from(inner: String) -> Self {
        StorageError { inner }
    }
}

impl From<&str> for StorageError {
    fn from(x: &str) -> Self {
        StorageError {
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
        write!(f, "permanent: {}", self.inner)
    }
}

impl std::error::Error for Permanent {}
