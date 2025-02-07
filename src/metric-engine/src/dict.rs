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

use std::borrow::Borrow;
use std::fmt::{self, Debug, Display};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use ahash::{HashSet, HashSetExt};

#[derive(Clone, PartialEq, Eq)]
pub struct ArcString(Arc<String>);

impl From<&str> for ArcString {
    fn from(s: &str) -> Self {
        ArcString(Arc::from(s.to_string()))
    }
}

impl ArcString {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for ArcString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for ArcString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Default, Debug, Clone)]
pub struct Dict {
    inner: Arc<RwLock<HashSet<ArcString>>>,
}

impl Borrow<str> for ArcString {
    fn borrow(&self) -> &str {
        self.0.as_str()
    }
}

impl Hash for ArcString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq<str> for ArcString {
    fn eq(&self, other: &str) -> bool {
        self.0.as_str() == other
    }
}

impl Dict {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new dictionary with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashSet::with_capacity(capacity))),
        }
    }

    /// Gets or inserts a string into the dictionary.
    ///
    /// If the string already exists, return the existing string.
    /// Otherwise, insert the string into the dictionary and return the new string.
    pub fn get_or_insert(&self, s: &str) -> ArcString {
        let mut inner = self.inner.write().unwrap();
        if inner.contains(s) {
            inner.get(s).unwrap().clone()
        } else {
            let str = ArcString::from(s);
            inner.insert(str.clone());
            str
        }
    }

    /// Tries to release a string from the dictionary.
    ///
    /// If the string is not shared by other, remove it from the dictionary and return true.
    /// Otherwise, return false.
    pub fn try_release(&self, s: &str) -> bool {
        let mut inner = self.inner.write().unwrap();

        let mut released = false;
        if let Some(s) = inner.get(s) {
            // If the string is not shared by other, remove it from the dictionary.
            let count = Arc::strong_count(&s.0);
            if count == 1 {
                released = true;
            }
        }

        if released {
            inner.remove(s);
        }

        released
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_insert() {
        let dict = Dict::new();
        let s1 = "test_string";
        let result = dict.get_or_insert(s1);
        assert_eq!(result.as_str(), s1);

        // Inserting the same string again should return the existing string
        let result2 = dict.get_or_insert(s1);
        assert_eq!(Arc::strong_count(&result.0), 3); // Check reference count
        assert_eq!(result2.as_str(), s1);
    }

    #[test]
    fn test_try_release() {
        common_telemetry::init_default_ut_logging();
        let dict = Dict::default();
        let s1 = "release_string";
        {
            let _result = dict.get_or_insert(s1);
            assert!(!dict.try_release(s1));
        }

        assert!(dict.try_release(s1));
    }
}
