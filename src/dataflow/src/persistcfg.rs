// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize-specific persistence configuration.

use std::path::PathBuf;

use dataflow_types::SourceDesc;
use expr::GlobalId;
use persist::file::{FileBlob, FileBuffer};
use persist::indexed::runtime::{self, RuntimeClient};

/// Configuration of the persistence runtime and features.
#[derive(Clone, Debug)]
pub struct PersistConfig {
    /// A directory under which un-indexed WAL-like writes are quickly stored.
    pub buffer_path: PathBuf,
    /// A directory under which larger batches of indexed data are stored. This
    /// will eventually be S3 for Cloud.
    pub blob_path: PathBuf,
    /// Whether to persist user tables. This is extremely experimental and
    /// should not even be tried by users. It's initially here for end-to-end
    /// testing.
    pub user_table_enabled: bool,
    /// Information stored in the "lock" files created by the buffer and blob to
    /// ensure that they are exclusive writers to those locations. This should
    /// contain whatever information might be useful to investigating an
    /// unexpected lock file (e.g. hostname and materialize version of the
    /// creating process).
    pub lock_info: String,
}

impl PersistConfig {
    /// Initializes the persistence runtime and returns a clone-able handle for
    /// interacting with it. Returns None and does not start the runtime if all
    /// persistence features are disabled.
    pub fn init(&self) -> Result<Option<PersisterWithConfig>, String> {
        if self.user_table_enabled {
            let buffer = FileBuffer::new(&self.buffer_path, &self.lock_info)
                .map_err(|err| err.to_string())?;
            let blob =
                FileBlob::new(&self.blob_path, &self.lock_info).map_err(|err| err.to_string())?;
            let persister = runtime::start(buffer, blob).map_err(|err| err.to_string())?;
            Ok(Some(PersisterWithConfig {
                config: self.clone(),
                persister,
            }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct PersisterWithConfig {
    pub config: PersistConfig,
    pub persister: RuntimeClient<Vec<u8>, ()>,
}

impl PersisterWithConfig {
    pub fn stream_name(&self, id: GlobalId, src: &SourceDesc) -> Option<String> {
        match id {
            GlobalId::User(id) if self.config.user_table_enabled => {
                // TODO: This needs to be written down somewhere in the catalog in case
                // we need to change the naming at some point.
                Some(format!("user-table-{:?}-{}", id, src.name))
            }
            _ => None,
        }
    }
}
