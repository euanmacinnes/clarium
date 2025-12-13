//! Centralized internal/reserved column names and prefixes used across executor stages.
//! Using these constants avoids magic strings and reduces coupling between stages.

/// Prefix for all internal/temporary executor columns.
pub const INTERNAL_PREFIX: &str = "__";

/// Stable internal row identifier (tiebreakers, joins, etc.).
pub const ROW_ID: &str = "__row_id";

/// Temporary column name for boolean masks (WHERE, HAVING, etc.).
pub const MASK: &str = "__mask";

/// Temporary column name used during ANN scoring/sorting.
pub const ANN_SCORE: &str = "__ann_score";

/// Generic temporary name base (callers should append unique suffixes when needed).
pub const TMP: &str = "__tmp__";

/// Prefix for synthesized argument/placeholder columns.
pub const ARG_PREFIX: &str = "__arg"; // e.g., __arg0, __arg1

/// Sentinel group key for global aggregates (GROUP BY ALL / no-key grouping).
pub const ALL_GROUP_KEY: &str = "__ALL__";

/// Internal single-row unit column used when there is no FROM source.
pub const UNIT: &str = "__unit";

/// Internal temporary column for tracking left row ids in manual LEFT joins.
pub const LEFT_ROW_ID: &str = "__left_row_id";

/// Prefix for temporary window-order columns built during window evaluation.
pub const WINDOW_ORDER_PREFIX: &str = "__window_order_";

/// Temporary boolean alias used in where-subquery mask materialization (lazy selects).
pub const TMP_BOOL_ALIAS: &str = "__m__";

/// Temporary alias for left-side values in ANY/ALL evaluation (lazy selects).
pub const TMP_LEFT_ALIAS: &str = "__l__";

/// Returns true if a name is considered an internal/executor-managed column.
#[inline]
pub fn is_internal(name: &str) -> bool {
    name.starts_with(INTERNAL_PREFIX)
}
