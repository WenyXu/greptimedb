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
use api::v1::region::{region_request, QueryRequest, RegionResponse};
use api::v1::DeleteRequests;
use arrow_flight::Ticket;
use async_trait::async_trait;
use client::error::{HandleRequestSnafu, Result as ClientResult};
use client::region::RegionRequester;
use client::region_handler::RegionRequestHandler;
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;
use prost::Message;
use query::error::QueryExecutionSnafu;
use query::query_engine::SqlStatementExecutor;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::statements::statement::Statement;
use store_api::storage::RegionId;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    self, FindDatanodeSnafu, FindTableRouteSnafu, NotSupportedSnafu, RequestDatanodeSnafu, Result,
};
use crate::inserter::req_convert::StatementToRegion;
use crate::instance::distributed::deleter::DistDeleter;
use crate::instance::distributed::inserter::DistInserter;

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
            Statement::Insert(insert) => {
                let request = StatementToRegion::new(self.catalog_manager.as_ref(), &query_ctx)
                    .convert(&insert)
                    .await?;
                let inserter = DistInserter::new(&self.catalog_manager);
                let affected_rows = inserter.insert(request).await?;
                Ok(Output::AffectedRows(affected_rows as usize))
            }
            _ => NotSupportedSnafu {
                feat: format!("{stmt:?}"),
            }
            .fail(),
        }
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
    ) -> ClientResult<RegionResponse> {
        self.handle_inner(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(HandleRequestSnafu)
    }

    async fn do_get(&self, request: QueryRequest) -> ClientResult<SendableRecordBatchStream> {
        self.do_get_inner(request)
            .await
            .map_err(BoxedError::new)
            .context(HandleRequestSnafu)
    }
}

impl DistRegionRequestHandler {
    async fn handle_inner(
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

    async fn do_get_inner(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let region_id = RegionId::from_u64(request.region_id);

        let table_route = self
            .catalog_manager
            .partition_manager()
            .find_table_route(region_id.table_id())
            .await
            .context(FindTableRouteSnafu {
                table_id: region_id.table_id(),
            })?;
        let peer = table_route
            .find_region_leader(region_id.region_number())
            .context(FindDatanodeSnafu {
                region: region_id.region_number(),
            })?;

        let client = self
            .catalog_manager
            .datanode_clients()
            .get_client(peer)
            .await;

        let ticket = Ticket {
            ticket: request.encode_to_vec().into(),
        };
        let region_requester = RegionRequester::new(client);
        region_requester
            .do_get(ticket)
            .await
            .context(RequestDatanodeSnafu)
    }
}
