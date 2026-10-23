// Copyright 2026 Greptime Team
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

use common_procedure::{ProcedureContext, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use common_telemetry::info;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::{
    Context as DrainDatanodeContext, DrainDatanodeProcedure, DrainDatanodeProcedureTask,
    MAX_BATCH_SIZE, PersistentContext,
};

pub type DrainDatanodeManagerRef = Arc<DrainDatanodeManager>;

/// Manages the [`DrainDatanodeProcedure`]s.
///
/// It registers the procedure loader and submits drain tasks. Procedures of the same
/// datanode are serialized by the procedure lock declared in
/// [`DrainDatanodeProcedure::lock_key`].
pub struct DrainDatanodeManager {
    procedure_manager: ProcedureManagerRef,
    drain_datanode_context: DrainDatanodeContext,
}

impl DrainDatanodeManager {
    /// Returns a new [`DrainDatanodeManager`].
    pub fn new(
        procedure_manager: ProcedureManagerRef,
        drain_datanode_context: DrainDatanodeContext,
    ) -> Self {
        Self {
            procedure_manager,
            drain_datanode_context,
        }
    }

    /// Submits a [`DrainDatanodeProcedure`] and returns its id.
    pub async fn submit_procedure(
        &self,
        procedure_context: ProcedureContext,
        task: DrainDatanodeProcedureTask,
    ) -> Result<ProcedureId> {
        if task.target_node_id == Some(task.node_id) {
            return error::InvalidArgumentsSnafu {
                err_msg: "the target datanode must differ from the drained datanode",
            }
            .fail();
        }
        if task.batch_size == 0 || task.batch_size > MAX_BATCH_SIZE {
            return error::InvalidArgumentsSnafu {
                err_msg: format!("the batch size must be in 1..={}", MAX_BATCH_SIZE),
            }
            .fail();
        }

        let persistent_ctx = PersistentContext {
            node_id: task.node_id,
            target_node_id: task.target_node_id,
            batch_size: task.batch_size,
            strategy: task.strategy,
            leader_regions: vec![],
            inflight_procedures: vec![],
            migrated: 0,
            failed_regions: vec![],
            follower_region_count: 0,
            rescan_round: 0,
        };
        let procedure =
            DrainDatanodeProcedure::new(persistent_ctx, self.drain_datanode_context.clone());
        let procedure_with_id =
            ProcedureWithId::with_random_id(Box::new(procedure)).with_context(procedure_context);
        let procedure_id = procedure_with_id.id;
        info!(
            "Starting drain datanode procedure {procedure_id} for datanode {}",
            task.node_id
        );
        self.procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;

        Ok(procedure_id)
    }

    /// Registers the loader of [`DrainDatanodeProcedure`].
    pub(crate) fn try_start(&self) -> Result<()> {
        let context = self.drain_datanode_context.clone();
        self.procedure_manager
            .register_loader(
                DrainDatanodeProcedure::TYPE_NAME,
                Box::new(move |json| {
                    DrainDatanodeProcedure::from_json(json, context.clone())
                        .map(|procedure| Box::new(procedure) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: DrainDatanodeProcedure::TYPE_NAME,
            })
    }
}
