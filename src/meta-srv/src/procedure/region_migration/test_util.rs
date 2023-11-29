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

use std::assert_matches::assert_matches;
use std::sync::Arc;

use api::v1::meta::mailbox_message::Payload;
use api::v1::meta::{HeartbeatResponse, MailboxMessage, RequestHeader};
use common_meta::instruction::{
    DowngradeRegionReply, InstructionReply, SimpleReply, UpgradeRegionReply,
};
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use common_meta::rpc::router::RegionRoute;
use common_meta::sequence::Sequence;
use common_meta::DatanodeId;
use common_procedure::{Context as ProcedureContext, ProcedureId, Status};
use common_procedure_test::MockContextProvider;
use common_telemetry::debug;
use common_time::util::current_time_millis;
use futures::future::BoxFuture;
use store_api::storage::RegionId;
use table::metadata::RawTableInfo;
use tokio::sync::mpsc::{Receiver, Sender};

use super::upgrade_candidate_region::UpgradeCandidateRegion;
use super::{Context, ContextFactory, ContextFactoryImpl, State};
use crate::error::Result;
use crate::handler::{HeartbeatMailbox, Pusher, Pushers};
use crate::procedure::region_migration::downgrade_leader_region::DowngradeLeaderRegion;
use crate::procedure::region_migration::migration_end::RegionMigrationEnd;
use crate::procedure::region_migration::update_metadata::UpdateMetadata;
use crate::procedure::region_migration::PersistentContext;
use crate::region::lease_keeper::{OpeningRegionKeeper, OpeningRegionKeeperRef};
use crate::service::mailbox::{Channel, MailboxRef};

pub type MockHeartbeatReceiver = Receiver<std::result::Result<HeartbeatResponse, tonic::Status>>;

/// The context of mailbox.
pub struct MailboxContext {
    mailbox: MailboxRef,
    // The pusher is used in the mailbox.
    pushers: Pushers,
}

impl MailboxContext {
    pub fn new(sequence: Sequence) -> Self {
        let pushers = Pushers::default();
        let mailbox = HeartbeatMailbox::create(pushers.clone(), sequence);

        Self { mailbox, pushers }
    }

    /// Inserts a pusher for `datanode_id`
    pub async fn insert_heartbeat_response_receiver(
        &mut self,
        channel: Channel,
        tx: Sender<std::result::Result<HeartbeatResponse, tonic::Status>>,
    ) {
        let pusher_id = channel.pusher_id();
        let pusher = Pusher::new(tx, &RequestHeader::default());
        let _ = self.pushers.insert(pusher_id, pusher).await;
    }

    pub fn mailbox(&self) -> &MailboxRef {
        &self.mailbox
    }
}

/// `TestingEnv` provides components during the tests.
pub struct TestingEnv {
    table_metadata_manager: TableMetadataManagerRef,
    mailbox_ctx: MailboxContext,
    opening_region_keeper: OpeningRegionKeeperRef,
    server_addr: String,
}

impl TestingEnv {
    /// Returns an empty [TestingEnv].
    pub fn new() -> Self {
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

        let mailbox_sequence = Sequence::new("test_heartbeat_mailbox", 0, 1, kv_backend.clone());

        let mailbox_ctx = MailboxContext::new(mailbox_sequence);
        let opening_region_keeper = Arc::new(OpeningRegionKeeper::default());

        Self {
            table_metadata_manager,
            opening_region_keeper,
            mailbox_ctx,
            server_addr: "localhost".to_string(),
        }
    }

    /// Returns a context of region migration procedure.
    pub fn context_factory(&self) -> ContextFactoryImpl {
        ContextFactoryImpl {
            table_metadata_manager: self.table_metadata_manager.clone(),
            opening_region_keeper: self.opening_region_keeper.clone(),
            volatile_ctx: Default::default(),
            mailbox: self.mailbox_ctx.mailbox().clone(),
            server_addr: self.server_addr.to_string(),
        }
    }

    /// Returns the mutable [MailboxContext].
    pub fn mailbox_context(&mut self) -> &mut MailboxContext {
        &mut self.mailbox_ctx
    }

    /// Returns the [TableMetadataManagerRef]
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }

    /// Returns the [OpeningRegionKeeperRef]
    pub fn opening_region_keeper(&self) -> &OpeningRegionKeeperRef {
        &self.opening_region_keeper
    }

    /// Returns a [ProcedureContext] with a random [ProcedureId] and a [MockContextProvider].
    pub fn procedure_context() -> ProcedureContext {
        ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        }
    }
}

/// Generates a [InstructionReply::CloseRegion] reply.
pub fn new_close_region_reply(id: u64) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::CloseRegion(SimpleReply {
                result: false,
                error: None,
            }))
            .unwrap(),
        )),
    }
}

/// Generates a [InstructionReply::DowngradeRegion] reply.
pub fn new_downgrade_region_reply(
    id: u64,
    last_entry_id: Option<u64>,
    exist: bool,
    error: Option<String>,
) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::DowngradeRegion(DowngradeRegionReply {
                last_entry_id,
                exists: exist,
                error,
            }))
            .unwrap(),
        )),
    }
}

/// Generates a [InstructionReply::UpgradeRegion] reply.
pub fn new_upgrade_region_reply(
    id: u64,
    ready: bool,
    exists: bool,
    error: Option<String>,
) -> MailboxMessage {
    MailboxMessage {
        id,
        subject: "mock".to_string(),
        from: "datanode".to_string(),
        to: "meta".to_string(),
        timestamp_millis: current_time_millis(),
        payload: Some(Payload::Json(
            serde_json::to_string(&InstructionReply::UpgradeRegion(UpgradeRegionReply {
                ready,
                exists,
                error,
            }))
            .unwrap(),
        )),
    }
}

/// Sends a mock reply.
pub fn send_mock_reply(
    mailbox: MailboxRef,
    mut rx: MockHeartbeatReceiver,
    msg: impl FnOnce(u64) -> Result<MailboxMessage> + Send + 'static,
) {
    common_runtime::spawn_bg(async move {
        let resp = rx.recv().await.unwrap().unwrap();
        let reply_id = resp.mailbox_message.unwrap().id;
        mailbox.on_recv(reply_id, msg(reply_id)).await.unwrap();
    });
}

pub fn new_persistent_context(from: u64, to: u64, region_id: RegionId) -> PersistentContext {
    PersistentContext {
        from_peer: Peer::empty(from),
        to_peer: Peer::empty(to),
        region_id,
        cluster_id: 0,
    }
}

/// The test suite for region migration procedure.
pub(crate) struct ProcedureMigrationTestSuite {
    pub(crate) env: TestingEnv,
    context: Context,
    state: Box<dyn State>,
}

/// The hook is called before the test starts.
pub(crate) type BeforeTest =
    Box<dyn FnOnce(&mut ProcedureMigrationTestSuite) -> BoxFuture<'_, ()> + Send>;

/// Custom assertion.
pub(crate) type CustomAssertion = Box<
    dyn FnOnce(
            &mut ProcedureMigrationTestSuite,
            Result<(Box<dyn State>, Status)>,
        ) -> BoxFuture<'_, Result<()>>
        + Send,
>;

/// State assertion function.
pub(crate) type StateAssertion = Box<dyn FnOnce(&Box<dyn State>) + Send>;

/// Status assertion function.
pub(crate) type StatusAssertion = Box<dyn FnOnce(Status) + Send>;

/// The type of assertion.
pub(crate) enum Assertion {
    Simple(StateAssertion, StatusAssertion),
    Custom(CustomAssertion),
}

impl Assertion {
    /// Returns an [Assertion::Simple].
    pub(crate) fn simple<
        T: FnOnce(&Box<dyn State>) + Send + 'static,
        U: FnOnce(Status) + Send + 'static,
    >(
        state: T,
        status: U,
    ) -> Self {
        Self::Simple(Box::new(state), Box::new(status))
    }
}

impl ProcedureMigrationTestSuite {
    /// Returns a [ProcedureMigrationTestSuite].
    pub(crate) fn new(persistent_ctx: PersistentContext, start: Box<dyn State>) -> Self {
        let env = TestingEnv::new();
        let context = env.context_factory().new_context(persistent_ctx);

        Self {
            env,
            context,
            state: start,
        }
    }

    /// Mocks the `next` of [State] is called.
    pub(crate) async fn next(
        &mut self,
        name: &str,
        before: Option<BeforeTest>,
        assertion: Assertion,
    ) -> Result<()> {
        debug!("suite test: {name}");

        if let Some(before) = before {
            before(self).await;
        }

        debug!("suite test: {name} invoking next");
        let result = self.state.next(&mut self.context).await;

        match assertion {
            Assertion::Simple(state_assert, status_assert) => {
                let (next, status) = result?;
                state_assert(&next);
                status_assert(status);
                self.state = next;
            }
            Assertion::Custom(assert_fn) => {
                assert_fn(self, result);
            }
        }

        Ok(())
    }

    /// Initializes table metadata.
    pub(crate) async fn init_table_metadata(
        &self,
        table_info: RawTableInfo,
        region_routes: Vec<RegionRoute>,
    ) {
        self.env
            .table_metadata_manager()
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();
    }

    /// Verifies table metadata after region migration.
    pub(crate) async fn verify_table_metadata(&self) {
        let region_id = self.context.persistent_ctx.region_id;
        let region_routes = self
            .env
            .table_metadata_manager
            .table_route_manager()
            .get(region_id.table_id())
            .await
            .unwrap()
            .unwrap()
            .into_inner()
            .region_routes;

        let expected_leader_id = self.context.persistent_ctx.to_peer.id;
        let removed_follower_id = self.context.persistent_ctx.from_peer.id;

        let region_route = region_routes
            .into_iter()
            .find(|route| route.region.id == region_id)
            .unwrap();

        assert_eq!(region_route.leader_peer.unwrap().id, expected_leader_id);
        assert!(!region_route
            .follower_peers
            .into_iter()
            .any(|route| route.id == removed_follower_id))
    }
}

/// The test runner of [ProcedureMigrationTestSuite].
pub(crate) struct ProcedureMigrationSuiteRunner {
    pub(crate) suite: ProcedureMigrationTestSuite,
    steps: Vec<(String, Option<BeforeTest>, Assertion)>,
}

impl ProcedureMigrationSuiteRunner {
    /// Returns the [ProcedureMigrationSuiteRunner]
    pub(crate) fn new(
        suite: ProcedureMigrationTestSuite,
        steps: Vec<(&str, Option<BeforeTest>, Assertion)>,
    ) -> Self {
        let steps = steps
            .into_iter()
            .map(|(name, before, assertion)| (name.to_string(), before, assertion))
            .collect();
        Self { suite, steps }
    }

    pub(crate) async fn run_once(mut self) -> Self {
        for (name, before, assertion) in self.steps.drain(..) {
            self.suite.next(&name, before, assertion).await.unwrap();
        }

        self
    }
}

/// Asserts the [Status] needs to be persistent.
pub(crate) fn assert_need_persist(status: Status) {
    assert!(status.need_persist());
}

/// Asserts the [Status] doesn't need to be persistent.
pub(crate) fn assert_no_persist(status: Status) {
    assert!(!status.need_persist());
}

/// Asserts the [Status] should be [Status::Done].
pub(crate) fn assert_done(status: Status) {
    assert_matches!(status, Status::Done)
}

/// Asserts the [State] should be [UpdateMetadata::Downgrade].
pub(crate) fn assert_update_metadata_downgrade(next: &Box<dyn State>) {
    let state = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
    assert_matches!(state, UpdateMetadata::Downgrade);
}

/// Asserts the [State] should be [UpdateMetadata::Upgrade].
pub(crate) fn assert_update_metadata_upgrade(next: &Box<dyn State>) {
    let state = next.as_any().downcast_ref::<UpdateMetadata>().unwrap();
    assert_matches!(state, UpdateMetadata::Upgrade);
}

/// Asserts the [State] should be [RegionMigrationEnd].
pub(crate) fn assert_region_migration_end(next: &Box<dyn State>) {
    let _ = next.as_any().downcast_ref::<RegionMigrationEnd>().unwrap();
}

/// Asserts the [State] should be [DowngradeLeaderRegion].
pub(crate) fn assert_downgrade_leader_region(next: &Box<dyn State>) {
    let _ = next
        .as_any()
        .downcast_ref::<DowngradeLeaderRegion>()
        .unwrap();
}

/// Asserts the [State] should be [UpgradeCandidateRegion].
pub(crate) fn assert_upgrade_candidate_region(next: &Box<dyn State>) {
    let _ = next
        .as_any()
        .downcast_ref::<UpgradeCandidateRegion>()
        .unwrap();
}

pub(crate) fn mock_datanode_reply(
    peer_id: DatanodeId,
    msg: Box<dyn FnOnce(u64) -> Result<MailboxMessage> + Send>,
) -> BeforeTest {
    Box::new(move |suite| {
        Box::pin(async move {
            let mailbox_ctx = suite.env.mailbox_context();
            let mailbox = mailbox_ctx.mailbox().clone();
            let (tx, rx) = tokio::sync::mpsc::channel(1);

            mailbox_ctx
                .insert_heartbeat_response_receiver(Channel::Datanode(peer_id), tx)
                .await;

            send_mock_reply(mailbox, rx, msg);

            ()
        })
    })
}
