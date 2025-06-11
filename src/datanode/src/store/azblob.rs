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

use object_store::services::Azblob;
use object_store::ObjectStore;
use snafu::prelude::*;

use crate::config::AzblobConfig;
use crate::error::{self, Result};
use crate::store::build_http_client;

pub(crate) async fn new_azblob_object_store(azblob_config: &AzblobConfig) -> Result<ObjectStore> {
    let client = build_http_client(&azblob_config.http_client)?;
    let builder = Azblob::from(&azblob_config.connection).http_client(client);

    Ok(ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish())
}
