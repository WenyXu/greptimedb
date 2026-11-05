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

use common_datasource::compression::CompressionType;
use common_telemetry::warn;
use object_store::{Entry, ObjectStore};
use snafu::ResultExt;
use store_api::ManifestVersion;

use crate::cache::manifest_cache::ManifestCache;
use crate::error::{CompressObjectSnafu, OpenDalSnafu, Result};
use crate::manifest::storage::file_compress_type;

const CHUNK_SIZE: usize = 64 * 1024 * 1024; // 64MB

/// Gets a manifest file from cache.
/// Returns the file data if found in cache, None otherwise.
pub(crate) async fn get_from_cache(cache: Option<&ManifestCache>, key: &str) -> Option<Vec<u8>> {
    let cache = cache?;
    let data = cache.get_file(key).await?;
    let compress_type = file_compress_type(key);
    // Older writers cached encoded bytes, while remote reads cached decoded bytes.
    // Manifest JSON cannot start with any of these compression headers.
    let encoded = match compress_type {
        CompressionType::Gzip => data.starts_with(&[0x1f, 0x8b]),
        CompressionType::Bzip2 => data.starts_with(b"BZh"),
        CompressionType::Xz => data.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]),
        CompressionType::Zstd => data.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]),
        CompressionType::Uncompressed => false,
    };
    if !encoded {
        return Some(data);
    }
    match compress_type.decode(data.as_slice()).await {
        Ok(decoded) => {
            put_to_cache(Some(cache), key.to_string(), &decoded).await;
            Some(decoded)
        }
        Err(error) => {
            warn!(error; "Failed to decode legacy cached manifest {}, retrying object storage", key);
            cache.remove(key).await;
            None
        }
    }
}

/// Puts a manifest file into cache.
pub(crate) async fn put_to_cache(cache: Option<&ManifestCache>, key: String, data: &[u8]) {
    let Some(cache) = cache else {
        return;
    };
    cache.put_file(key, data.to_vec()).await
}

/// Removes a manifest file from cache.
pub(crate) async fn remove_from_cache(cache: Option<&ManifestCache>, key: &str) {
    let Some(cache) = cache else {
        return;
    };
    cache.remove(key).await
}

/// Writes encoded bytes to object storage and caches the decoded content.
/// Returns the stored byte length.
pub(crate) async fn write_and_put_cache(
    object_store: &ObjectStore,
    cache: Option<&ManifestCache>,
    path: &str,
    bytes: &[u8],
    compress_type: CompressionType,
) -> Result<usize> {
    let data = compress_type
        .encode(bytes)
        .await
        .context(CompressObjectSnafu {
            compress_type,
            path,
        })?;
    let stored_size = data.len();
    object_store
        .write_with(path, data)
        .chunk(CHUNK_SIZE) // 64MB
        .await
        .context(OpenDalSnafu)?;
    put_to_cache(cache, path.to_string(), bytes).await;
    Ok(stored_size)
}

/// Sorts the manifest files.
pub(crate) fn sort_manifests(entries: &mut [(ManifestVersion, Entry)]) {
    entries.sort_unstable_by_key(|(version, _)| *version);
}
