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

use std::sync::Arc;

use common_procedure::{watcher, ProcedureManagerRef, ProcedureWithId};
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::{self, Result, SubmitProcedureSnafu, TableNotFoundSnafu, WaitProcedureSnafu};
use crate::key::table_name::TableNameKey;
use crate::key::TableMetadataManagerRef;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::reconcile_database::ReconcileDatabaseProcedure;
use crate::reconciliation::reconcile_logical_tables::ReconcileLogicalTablesProcedure;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::reconcile_table::ReconcileTableProcedure;
use crate::reconciliation::utils::Context;

pub type ReconciliationManagerRef = Arc<ReconciliationManager>;

pub struct ReconciliationManager {
    procedure_manager: ProcedureManagerRef,
    context: Context,
}

impl ReconciliationManager {
    pub fn new(
        node_manager: NodeManagerRef,
        table_metadata_manager: TableMetadataManagerRef,
        cache_invalidator: CacheInvalidatorRef,
        procedure_manager: ProcedureManagerRef,
    ) -> Self {
        Self {
            procedure_manager,
            context: Context {
                node_manager,
                table_metadata_manager,
                cache_invalidator,
            },
        }
    }

    pub fn try_start(&self) -> Result<()> {
        let context = self.context.clone();
        self.procedure_manager
            .register_loader(
                ReconcileLogicalTablesProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    let procedure = ReconcileLogicalTablesProcedure::from_json(context, json)?;
                    Ok(Box::new(procedure))
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: ReconcileLogicalTablesProcedure::TYPE_NAME,
            })?;

        let context = self.context.clone();
        self.procedure_manager
            .register_loader(
                ReconcileTableProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    let procedure = ReconcileTableProcedure::from_json(context, json)?;
                    Ok(Box::new(procedure))
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: ReconcileTableProcedure::TYPE_NAME,
            })?;

        Ok(())
    }

    pub async fn reconcile_table(&self, table_ref: TableReference<'_>) -> Result<()> {
        let table_name_key =
            TableNameKey::new(table_ref.catalog, table_ref.schema, table_ref.table);
        let table_metadata_manager = &self.context.table_metadata_manager;
        let table_id = table_metadata_manager
            .table_name_manager()
            .get(table_name_key)
            .await?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?
            .table_id();

        let (physical_table_id, _) = table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await?;

        if physical_table_id == table_id {
            self.reconcile_physical_table(table_id, table_ref.into())
                .await?;
        } else {
            let physical_table_info = table_metadata_manager
                .table_info_manager()
                .get(physical_table_id)
                .await?
                .with_context(|| TableNotFoundSnafu {
                    table_name: format!("table_id: {}", physical_table_id),
                })?;

            self.reconcile_logical_tables(
                physical_table_id,
                physical_table_info.table_name(),
                vec![(table_id, table_ref.into())],
            )
            .await?;
        }

        Ok(())
    }

    pub async fn reconcile_database(&self, catalog: String, schema: String) -> Result<()> {
        let procedure = ReconcileDatabaseProcedure::new(
            self.context.clone(),
            catalog,
            schema,
            false,
            64,
            ResolveStrategy::UseLatest,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(SubmitProcedureSnafu)?;
        watcher::wait(&mut watcher)
            .await
            .context(WaitProcedureSnafu)?;

        Ok(())
    }

    async fn reconcile_physical_table(
        &self,
        table_id: TableId,
        table_name: TableName,
    ) -> Result<()> {
        let procedure = ReconcileTableProcedure::new(
            self.context.clone(),
            table_id,
            table_name,
            ResolveStrategy::UseLatest,
            false,
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(SubmitProcedureSnafu)?;
        watcher::wait(&mut watcher)
            .await
            .context(WaitProcedureSnafu)?;

        Ok(())
    }

    async fn reconcile_logical_tables(
        &self,
        physical_table_id: TableId,
        physical_table_name: TableName,
        logical_tables: Vec<(TableId, TableName)>,
    ) -> Result<()> {
        let procedure = ReconcileLogicalTablesProcedure::new(
            self.context.clone(),
            physical_table_id,
            physical_table_name,
            logical_tables,
            false,
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(SubmitProcedureSnafu)?;
        watcher::wait(&mut watcher)
            .await
            .context(WaitProcedureSnafu)?;

        Ok(())
    }
}
