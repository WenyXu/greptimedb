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

use api::v1::meta::reconcile_request::Target;
use api::v1::meta::{ReconcileCatalog, ReconcileRequest};
use common_macro::admin_fn;
use common_query::error::{MissingProcedureServiceHandlerSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::*;
use session::context::QueryContextRef;

use crate::handlers::ProcedureServiceHandlerRef;

/// A function to reconcile a catalog.
/// Returns the number of affected rows if success.
///
/// - `reconcile_catalog()`.
#[admin_fn(
    name = ReconcileCatalogFunction,
    display_name = reconcile_catalog,
    sig_fn = signature,
    ret = uint64
)]
pub(crate) async fn reconcile_catalog(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    _params: &[ValueRef<'_>],
) -> Result<Value> {
    procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileCatalog(ReconcileCatalog {
                catalog_name: query_ctx.current_catalog().to_string(),
            })),
            ..Default::default()
        })
        .await?;

    Ok(Value::from(0u64))
}

fn signature() -> Signature {
    Signature::nullary(Volatility::Immutable)
}
