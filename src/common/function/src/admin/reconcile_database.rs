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
use api::v1::meta::{ReconcileDatabase, ReconcileRequest};
use common_macro::admin_fn;
use common_query::error::{
    InvalidFuncArgsSnafu, MissingProcedureServiceHandlerSnafu, Result,
    UnsupportedInputDataTypeSnafu,
};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::*;
use session::context::QueryContextRef;
use snafu::ensure;

use crate::handlers::ProcedureServiceHandlerRef;

/// A function to reconcile a table.
/// Returns the number of affected rows if success.
///
/// - `reconcile_database(database_name)`.
///
/// The parameters:
/// - `database_name`:  the database name
#[admin_fn(
    name = ReconcileDatabaseFunction,
    display_name = reconcile_database,
    sig_fn = signature,
    ret = uint64
)]
pub(crate) async fn reconcile_database(
    procedure_service_handler: &ProcedureServiceHandlerRef,
    query_ctx: &QueryContextRef,
    params: &[ValueRef<'_>],
) -> Result<Value> {
    ensure!(
        params.len() == 1,
        InvalidFuncArgsSnafu {
            err_msg: format!(
                "The length of the args is not correct, expect 1, have: {}",
                params.len()
            ),
        }
    );

    let ValueRef::String(database_name) = params[0] else {
        return UnsupportedInputDataTypeSnafu {
            function: "reconcile_database",
            datatypes: params.iter().map(|v| v.data_type()).collect::<Vec<_>>(),
        }
        .fail();
    };

    procedure_service_handler
        .reconcile(ReconcileRequest {
            target: Some(Target::ReconcileDatabase(ReconcileDatabase {
                catalog_name: query_ctx.current_catalog().to_string(),
                database_name: database_name.to_string(),
            })),
            ..Default::default()
        })
        .await?;

    Ok(Value::from(0u64))
}

fn signature() -> Signature {
    Signature::uniform(
        1,
        vec![ConcreteDataType::string_datatype()],
        Volatility::Immutable,
    )
}
