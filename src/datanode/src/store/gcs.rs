// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use object_store::services::Gcs;
use object_store::ObjectStore;
use snafu::prelude::*;

use crate::config::GcsConfig;
use crate::error::{self, Result};
use crate::store::build_http_client;

pub(crate) async fn new_gcs_object_store(gcs_config: &GcsConfig) -> Result<ObjectStore> {
    let client = build_http_client(&gcs_config.http_client)?;
    let builder = Gcs::from(&gcs_config.connection).http_client(client);

    Ok(ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish())
}
