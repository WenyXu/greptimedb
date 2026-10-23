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

use std::any::Any;

use common_error::ext::BoxedError;
use common_meta::key::table_route::TableRouteValue;
use common_meta::peer::Peer;
use common_procedure::{
    Context as ProcedureContext, ProcedureContext as SubmissionContext, ProcedureWithId, Status,
};
use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{self, Result};
use crate::procedure::drain_datanode::check_drained::CheckDrained;
use crate::procedure::drain_datanode::collect::Collect;
use crate::procedure::drain_datanode::utils;
use crate::procedure::drain_datanode::{
    Context, DrainDatanodeStrategy, PersistentContext, ProcedureMeta, State,
};
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};

/// Dispatches a batch of region migration sub-procedures.
///
/// A region leaves [`PersistentContext::leader_regions`] only after its sub-procedure
/// succeeded, so an aborted procedure can resume from the persisted list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Dispatch;

#[async_trait::async_trait]
#[typetag::serde]
impl State for Dispatch {
    async fn next(
        &mut self,
        ctx: &mut Context,
        persistent_ctx: &mut PersistentContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let node_id = persistent_ctx.node_id;
        if persistent_ctx.leader_regions.is_empty() {
            return Ok((Box::new(CheckDrained), Status::executing(true)));
        }

        let regions: Vec<RegionId> = persistent_ctx
            .leader_regions
            .iter()
            .filter(|region_id| {
                !persistent_ctx
                    .inflight_procedures
                    .iter()
                    .any(|meta| meta.region_id == **region_id)
            })
            .copied()
            .take(persistent_ctx.batch_size)
            .collect();

        let mut children = Vec::with_capacity(regions.len());
        for region_id in regions {
            match build_migration_procedure(ctx, persistent_ctx, procedure_ctx, region_id).await {
                Ok(Some(child)) => {
                    info!(
                        "Dispatching migration of region {} off datanode {}, procedure id: {}",
                        region_id, node_id, child.id
                    );
                    persistent_ctx.inflight_procedures.push(ProcedureMeta {
                        region_id,
                        procedure_id: child.id,
                    });
                    children.push(child);
                }
                Ok(None) => {
                    // The region is not led by `node_id` anymore.
                    info!(
                        "Skipping region {}, it is not led by datanode {} anymore",
                        region_id, node_id
                    );
                    persistent_ctx
                        .leader_regions
                        .retain(|region| *region != region_id);
                }
                Err(e) => match persistent_ctx.strategy {
                    DrainDatanodeStrategy::FailFast => return Err(e),
                    DrainDatanodeStrategy::TryAll => {
                        error!(
                            e;
                            "Gave up draining region {} off datanode {}",
                            region_id,
                            node_id
                        );
                        persistent_ctx
                            .leader_regions
                            .retain(|region| *region != region_id);
                        persistent_ctx.failed_regions.push(region_id);
                    }
                },
            }
        }

        if children.is_empty() {
            return Ok((Box::new(CheckDrained), Status::executing(true)));
        }

        Ok((Box::new(Collect), Status::suspended(children, true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Builds the migration sub-procedure of `region_id`.
///
/// Returns `Ok(None)` when the region is not led by `node_id` anymore.
async fn build_migration_procedure(
    ctx: &Context,
    persistent_ctx: &PersistentContext,
    procedure_ctx: &ProcedureContext,
    region_id: RegionId,
) -> Result<Option<ProcedureWithId>> {
    let node_id = persistent_ctx.node_id;
    let Some(table_route) = ctx
        .table_metadata_manager
        .table_route_manager()
        .table_route_storage()
        .get(region_id.table_id())
        .await
        .map_err(BoxedError::new)
        .context(error::OtherSnafu)?
    else {
        return Ok(None);
    };
    let TableRouteValue::Physical(table_route) = table_route else {
        return Ok(None);
    };
    let Some(region_route) = table_route
        .region_routes
        .iter()
        .find(|region_route| region_route.region.id == region_id)
    else {
        return Ok(None);
    };
    if region_route.leader_peer.as_ref().map(|peer| peer.id) != Some(node_id) {
        return Ok(None);
    }

    let to_peer = utils::select_migration_target(
        ctx,
        node_id,
        region_id,
        persistent_ctx.target_node_id,
        &region_route.follower_peers,
    )
    .await?;

    let submission_context = match procedure_ctx.event_context.clone() {
        Some(event_context) => SubmissionContext::from_event_context(event_context),
        None => SubmissionContext::default(),
    };
    let child = ctx
        .region_migration_manager
        .build_subprocedure(
            submission_context,
            RegionMigrationProcedureTask::new(
                region_id,
                Peer::empty(node_id),
                to_peer,
                ctx.timeout,
                RegionMigrationTriggerReason::Manual,
            ),
        )
        .await?;

    Ok(Some(child))
}
