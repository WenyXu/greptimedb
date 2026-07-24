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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex, RwLock};

use common_event_recorder::{Event, EventRecorder};
use snafu::ensure;

use super::*;
use crate::store::poison_store::PoisonStore;
use crate::{EventTrigger, ProcedureEvent, error};

/// An event recorder that stores captured procedure events for assertions.
#[derive(Debug, Default)]
pub struct CapturingEventRecorder {
    events: Mutex<Vec<Box<dyn Event>>>,
}

impl CapturingEventRecorder {
    /// Returns the triggers of captured procedure events.
    pub fn triggers(&self) -> Vec<EventTrigger> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|event| {
                event
                    .as_any()
                    .downcast_ref::<ProcedureEvent>()
                    .unwrap()
                    .trigger
                    .clone()
            })
            .collect()
    }
}

impl EventRecorder for CapturingEventRecorder {
    fn record(&self, event: Box<dyn Event>) {
        self.events.lock().unwrap().push(event);
    }

    fn close(&self) {}
}

/// A minimal procedure event for lifecycle tests.
#[derive(Debug)]
pub struct TestProcedureEvent;

impl Event for TestProcedureEvent {
    fn event_type(&self) -> &str {
        "test_procedure"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A poison store that uses an in-memory map to store the poison state.
#[derive(Debug, Default)]
pub struct InMemoryPoisonStore {
    map: Arc<RwLock<HashMap<String, String>>>,
}

impl InMemoryPoisonStore {
    /// Create a new in-memory poison manager.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl PoisonStore for InMemoryPoisonStore {
    async fn try_put_poison(&self, key: String, token: String) -> Result<()> {
        let mut map = self.map.write().unwrap();
        match map.entry(key) {
            Entry::Vacant(v) => {
                v.insert(token.clone());
            }
            Entry::Occupied(o) => {
                let value = o.get();
                ensure!(
                    value == &token,
                    error::UnexpectedSnafu {
                        err_msg: format!("The poison is already set by other token {}", value)
                    }
                );
            }
        }
        Ok(())
    }

    async fn delete_poison(&self, key: String, token: String) -> Result<()> {
        let mut map = self.map.write().unwrap();
        match map.entry(key) {
            Entry::Vacant(_) => {
                // do nothing
            }
            Entry::Occupied(o) => {
                let value = o.get();
                ensure!(
                    value == &token,
                    error::UnexpectedSnafu {
                        err_msg: format!("The poison is not set by the token {}", value)
                    }
                );

                o.remove();
            }
        }
        Ok(())
    }

    async fn get_poison(&self, key: &str) -> Result<Option<String>> {
        let map = self.map.read().unwrap();
        let key = key.to_string();
        Ok(map.get(&key).cloned())
    }
}
