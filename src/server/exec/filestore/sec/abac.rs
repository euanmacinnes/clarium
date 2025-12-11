//! ABAC predicate interface (stub).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Predicate {
    #[serde(default)]
    pub json: Option<String>,
}

impl Predicate {
    pub fn evaluate(&self) -> bool {
        // Placeholder: always true until wired to real context
        true
    }
}
