use std::fmt;
use uuid::Uuid;

/// Lightweight correlation ID helper based on UUID v4.
/// Kept as a thin wrapper to ease formatting and optional propagation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CorrelationId(String);

impl CorrelationId {
    /// Generate a new random correlation id.
    pub fn new() -> Self { Self(Uuid::new_v4().to_string()) }

    /// Create from optional string; if None or invalid, generate a new one.
    pub fn from_opt_str(s: Option<&str>) -> Self {
        if let Some(v) = s {
            if let Ok(u) = Uuid::parse_str(v) {
                return Self(u.to_string());
            }
            // Accept any non-empty string without UUID validation to avoid dropping upstream IDs
            if !v.trim().is_empty() {
                return Self(v.to_string());
            }
        }
        Self::new()
    }

    /// Access the inner string.
    pub fn as_str(&self) -> &str { &self.0 }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

/// Convenience to turn an optional string into a `CorrelationId`.
pub fn correlation_id_opt_str(s: Option<&str>) -> CorrelationId { CorrelationId::from_opt_str(s) }
