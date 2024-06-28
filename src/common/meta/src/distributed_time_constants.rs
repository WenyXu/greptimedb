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

use std::sync::{Mutex, Once};
use std::time::Duration;

use once_cell::sync::Lazy;

/// Heartbeat interval time (is the basic unit of various time).
pub const HEARTBEAT_INTERVAL_MILLIS: u64 = 3000;

/// The frontend will also send heartbeats to Metasrv, sending an empty
/// heartbeat every HEARTBEAT_INTERVAL_MILLIS * 6 seconds.
pub const FRONTEND_HEARTBEAT_INTERVAL_MILLIS: u64 = HEARTBEAT_INTERVAL_MILLIS * 6;

/// The lease seconds of a region. It's set by 3 heartbeat intervals
/// (HEARTBEAT_INTERVAL_MILLIS × 3), plus some extra buffer (1 second).
pub const REGION_LEASE_SECS: u64 =
    Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS * 3).as_secs() + 1;

/// When creating table or region failover, a target node needs to be selected.
/// If the node's lease has expired, the `Selector` will not select it.
pub const DATANODE_LEASE_SECS: u64 = REGION_LEASE_SECS;

pub const FLOWNODE_LEASE_SECS: u64 = DATANODE_LEASE_SECS;

/// The lease seconds of metasrv leader.
pub const META_LEASE_SECS: u64 = 3;

/// In a lease, there are two opportunities for renewal.
pub const META_KEEP_ALIVE_INTERVAL_SECS: u64 = META_LEASE_SECS / 2;

/// The default mailbox round-trip timeout.
pub const MAILBOX_RTT_SECS: u64 = 1;

/// A global instance of `TimeConstants` initialized lazily.
pub static GLOBAL_TIME_CONSTANTS: Lazy<TimeConstants> =
    Lazy::new(|| *CONFIG_TIME_CONSTANTS.lock().unwrap());

static CONFIG_TIME_CONSTANTS: Lazy<Mutex<TimeConstants>> =
    Lazy::new(|| Mutex::new(TimeConstants::default()));

/// Constants representing various time durations used in the system.
#[derive(Debug, Clone, Copy)]
pub struct TimeConstants {
    /// Heartbeat interval time for datanodes, used as the basic unit for various time calculations.
    pub datanode_heartbeat_interval: Duration,
    /// Heartbeat interval time for the frontend.
    pub frontend_heartbeat_interval: Duration,
    /// The lease timeout for the metasrv leader.
    pub meta_leader_lease_timeout: Duration,
    /// The timeout duration for mailbox message round-trips.
    pub mailbox_message_round_trip_timeout: Duration,
}

impl TimeConstants {
    /// Returns the meta leader keep-alive interval, which is half of the meta leader lease duration.
    pub fn meta_leader_keep_alive_interval(&self) -> Duration {
        self.meta_leader_lease_timeout / 2
    }

    /// Returns the lease duration for flownodes.
    ///
    /// The lease seconds of a region. It's set by 3 heartbeat intervals (`datanode_heartbeat_interval` × 3),
    /// plus some extra buffer (1 second).
    pub fn flownode_lease(&self) -> Duration {
        self.datanode_heartbeat_interval * 3 + Duration::from_secs(1)
    }

    /// Returns the lease duration for datanodes.
    ///
    /// The lease seconds of a region. It's set by 3 heartbeat intervals (`datanode_heartbeat_interval` × 3),
    /// plus some extra buffer (1 second).
    pub fn datanode_lease(&self) -> Duration {
        self.datanode_heartbeat_interval * 3 + Duration::from_secs(1)
    }
}

impl Default for TimeConstants {
    fn default() -> Self {
        TimeConstants {
            datanode_heartbeat_interval: Duration::from_secs(3),
            frontend_heartbeat_interval: Duration::from_secs(18),
            meta_leader_lease_timeout: Duration::from_secs(3),
            mailbox_message_round_trip_timeout: Duration::from_secs(1),
        }
    }
}

/// Initialize the global [TimeConstants]
pub fn init_global_time_constants(constants: TimeConstants) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_TIME_CONSTANTS.lock().unwrap();
        *c = constants;
    });
}
