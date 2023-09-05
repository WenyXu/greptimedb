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

pub mod deleter;
pub(crate) mod inserter;

use std::sync::Arc;

use api::v1::greptime_request::Request;
use api::v1::region::{region_request, RegionResponse};
use api::v1::{CreateDatabaseExpr, DeleteRequests};
use async_trait::async_trait;
use catalog::CatalogManager;
use common_error::ext::BoxedError;
use common_meta::key::schema_name::{SchemaNameKey, SchemaNameValue};
use common_meta::table_name::TableName;
use common_query::Output;
use datanode::instance::sql::table_idents_to_full_name;
use partition::manager::PartitionInfo;
use partition::partition::PartitionBound;
use query::error::QueryExecutionSnafu;
use query::query_engine::SqlStatementExecutor;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{Ident, Value as SqlValue};
use sql::statements::create::{PartitionEntry, Partitions};
use sql::statements::statement::Statement;
use sql::statements::{self};
use table::TableRef;

use super::region_handler::RegionRequestHandler;
use crate::catalog::FrontendCatalogManager;
use crate::error::{
    self, CatalogSnafu, NotSupportedSnafu, Result, SchemaExistsSnafu, TableNotFoundSnafu,
};
use crate::inserter::req_convert::StatementToRegion;
use crate::instance::distributed::deleter::DistDeleter;
use crate::instance::distributed::inserter::DistInserter;
use crate::MAX_VALUE;

#[derive(Clone)]
pub struct DistInstance {
    pub(crate) catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistInstance {
    pub fn new(catalog_manager: Arc<FrontendCatalogManager>) -> Self {
        Self { catalog_manager }
    }

    async fn handle_statement(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        match stmt {
            Statement::CreateDatabase(stmt) => {
                let expr = CreateDatabaseExpr {
                    database_name: stmt.name.to_string(),
                    create_if_not_exists: stmt.if_not_exists,
                    options: Default::default(),
                };
                self.handle_create_database(expr, query_ctx).await
            }
            Statement::Insert(insert) => {
                let request = StatementToRegion::new(self.catalog_manager.as_ref(), &query_ctx)
                    .convert(&insert)
                    .await?;
                let inserter = DistInserter::new(&self.catalog_manager);
                let affected_rows = inserter.insert(request).await?;
                Ok(Output::AffectedRows(affected_rows as usize))
            }
            Statement::ShowCreateTable(show) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(&show.table_name, query_ctx.clone())
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;

                let table_ref = self
                    .catalog_manager
                    .table(&catalog, &schema, &table)
                    .await
                    .context(CatalogSnafu)?
                    .context(TableNotFoundSnafu { table_name: &table })?;
                let table_name = TableName::new(catalog, schema, table);

                self.show_create_table(table_name, table_ref, query_ctx.clone())
                    .await
            }
            _ => NotSupportedSnafu {
                feat: format!("{stmt:?}"),
            }
            .fail(),
        }
    }

    /// Handles distributed database creation
    async fn handle_create_database(
        &self,
        expr: CreateDatabaseExpr,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog = query_ctx.current_catalog();
        if self
            .catalog_manager
            .schema_exist(catalog, &expr.database_name)
            .await
            .context(CatalogSnafu)?
        {
            return if expr.create_if_not_exists {
                Ok(Output::AffectedRows(1))
            } else {
                SchemaExistsSnafu {
                    name: &expr.database_name,
                }
                .fail()
            };
        }

        let schema = SchemaNameKey::new(catalog, &expr.database_name);
        let exist = self
            .catalog_manager
            .table_metadata_manager_ref()
            .schema_manager()
            .exist(schema)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        ensure!(
            !exist,
            SchemaExistsSnafu {
                name: schema.to_string(),
            }
        );

        let schema_value =
            SchemaNameValue::try_from(&expr.options).context(error::TableMetadataManagerSnafu)?;
        self.catalog_manager
            .table_metadata_manager_ref()
            .schema_manager()
            .create(schema, Some(schema_value))
            .await
            .context(error::TableMetadataManagerSnafu)?;

        // Since the database created on meta does not go through KvBackend, so we manually
        // invalidate the cache here.
        //
        // TODO(fys): when the meta invalidation cache mechanism is established, remove it.
        self.catalog_manager()
            .invalidate_schema(catalog, &expr.database_name)
            .await;

        Ok(Output::AffectedRows(1))
    }

    async fn show_create_table(
        &self,
        table_name: TableName,
        table: TableRef,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let partitions = self
            .catalog_manager
            .partition_manager()
            .find_table_partitions(table.table_info().table_id())
            .await
            .context(error::FindTablePartitionRuleSnafu {
                table_name: &table_name.table_name,
            })?;

        let partitions = create_partitions_stmt(partitions)?;

        query::sql::show_create_table(table, partitions, query_ctx)
            .context(error::ExecuteStatementSnafu)
    }

    async fn handle_dist_delete(
        &self,
        request: DeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let deleter = DistDeleter::new(
            ctx.current_catalog().to_string(),
            ctx.current_schema().to_string(),
            self.catalog_manager(),
        );
        let affected_rows = deleter.grpc_delete(request).await?;
        Ok(Output::AffectedRows(affected_rows))
    }

    pub fn catalog_manager(&self) -> Arc<FrontendCatalogManager> {
        self.catalog_manager.clone()
    }
}

#[async_trait]
impl SqlStatementExecutor for DistInstance {
    async fn execute_sql(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> query::error::Result<Output> {
        self.handle_statement(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(QueryExecutionSnafu)
    }
}

#[async_trait]
impl GrpcQueryHandler for DistInstance {
    type Error = error::Error;

    async fn do_query(&self, request: Request, ctx: QueryContextRef) -> Result<Output> {
        match request {
            Request::Inserts(_) => NotSupportedSnafu { feat: "inserts" }.fail(),
            Request::RowInserts(_) => NotSupportedSnafu {
                feat: "row inserts",
            }
            .fail(),
            Request::RowDeletes(_) => NotSupportedSnafu {
                feat: "row deletes",
            }
            .fail(),
            Request::Deletes(requests) => self.handle_dist_delete(requests, ctx).await,
            Request::Query(_) => {
                unreachable!("Query should have been handled directly in Frontend Instance!")
            }
            Request::Ddl(_) => NotSupportedSnafu { feat: "ddl" }.fail(),
        }
    }
}

pub(crate) struct DistRegionRequestHandler {
    catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistRegionRequestHandler {
    pub fn arc(catalog_manager: Arc<FrontendCatalogManager>) -> Arc<Self> {
        Arc::new(Self { catalog_manager })
    }
}

#[async_trait]
impl RegionRequestHandler for DistRegionRequestHandler {
    async fn handle(
        &self,
        request: region_request::Body,
        ctx: QueryContextRef,
    ) -> Result<RegionResponse> {
        match request {
            region_request::Body::Inserts(inserts) => {
                let inserter =
                    DistInserter::new(&self.catalog_manager).with_trace_id(ctx.trace_id());
                let affected_rows = inserter.insert(inserts).await? as _;
                Ok(RegionResponse {
                    header: Some(Default::default()),
                    affected_rows,
                })
            }
            region_request::Body::Deletes(_) => NotSupportedSnafu {
                feat: "region deletes",
            }
            .fail(),
            region_request::Body::Create(_) => NotSupportedSnafu {
                feat: "region create",
            }
            .fail(),
            region_request::Body::Drop(_) => NotSupportedSnafu {
                feat: "region drop",
            }
            .fail(),
            region_request::Body::Open(_) => NotSupportedSnafu {
                feat: "region open",
            }
            .fail(),
            region_request::Body::Close(_) => NotSupportedSnafu {
                feat: "region close",
            }
            .fail(),
            region_request::Body::Alter(_) => NotSupportedSnafu {
                feat: "region alter",
            }
            .fail(),
            region_request::Body::Flush(_) => NotSupportedSnafu {
                feat: "region flush",
            }
            .fail(),
            region_request::Body::Compact(_) => NotSupportedSnafu {
                feat: "region compact",
            }
            .fail(),
        }
    }
}

fn create_partitions_stmt(partitions: Vec<PartitionInfo>) -> Result<Option<Partitions>> {
    if partitions.is_empty() {
        return Ok(None);
    }

    let column_list: Vec<Ident> = partitions[0]
        .partition
        .partition_columns()
        .iter()
        .map(|name| name[..].into())
        .collect();

    let entries = partitions
        .into_iter()
        .map(|info| {
            // Generated the partition name from id
            let name = &format!("r{}", info.id.region_number());
            let bounds = info.partition.partition_bounds();
            let value_list = bounds
                .iter()
                .map(|b| match b {
                    PartitionBound::Value(v) => statements::value_to_sql_value(v)
                        .with_context(|_| error::ConvertSqlValueSnafu { value: v.clone() }),
                    PartitionBound::MaxValue => Ok(SqlValue::Number(MAX_VALUE.to_string(), false)),
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(PartitionEntry {
                name: name[..].into(),
                value_list,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(Partitions {
        column_list,
        entries,
    }))
}
