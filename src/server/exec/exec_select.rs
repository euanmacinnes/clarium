use anyhow::Result;
use polars::prelude::*;
use tracing::debug;

use crate::{storage::{SharedStore}};

use crate::server::data_context::{DataContext};
use crate::server::exec::select_stages::having::apply_having_with_validation;
use crate::server::exec::select_stages::from_where::from_where as stage_from_where;
use crate::server::exec::select_stages::by_or_groupby::by_or_groupby as stage_by_or_groupby;
use crate::server::exec::select_stages::rolling::rolling as stage_rolling;
use crate::server::exec::select_stages::project_select::project_select as stage_project_select;
use crate::server::exec::select_stages::order_limit::order_limit as stage_order_limit;
use crate::scripts::get_script_registry;



pub fn run_select(store: &SharedStore, q: &crate::query::Query) -> Result<DataFrame> {
    run_select_with_context(store, q, None)
}

// Expose for subquery execution within WHERE/HAVING evaluation and FROM subqueries
pub(crate) fn run_select_with_context(store: &SharedStore, q: &crate::query::Query, parent_ctx: Option<&DataContext>) -> Result<DataFrame> {
    // When debug logging is enabled, print the entire parsed Query for leak diagnostics
    // tprintln!("run_select: full Query AST = {:#?}", q);

    // Initialize stage context with defaults derived from the query
    let (def_db, def_schema) = derive_defaults_from_ident(q.base_table.as_ref().and_then(|t| t.table_name()).unwrap_or(""));
    
    // Capture a snapshot of the current global registry for this query.
    // This provides isolation: the query sees a stable registry throughout execution,
    // immune to concurrent modifications by other threads/tests.
    let registry_snapshot = get_script_registry()
        .and_then(|r| r.snapshot().ok());
    
    let mut ctx = DataContext::with_defaults(def_db, def_schema);
    if let Some(reg) = registry_snapshot {
        ctx.script_registry = Some(reg);
    }
    
    // Inherit CTE tables and parent source context for nested subquery correlation
    if let Some(parent) = parent_ctx {
        ctx.cte_tables = parent.cte_tables.clone();
        // Accumulate all parent-level sources (parent's own sources + its parents) into parent_sources
        // This allows nested subqueries to identify which aliases belong to outer query levels
        ctx.parent_sources.extend(parent.parent_sources.iter().cloned());
        ctx.parent_sources.extend(parent.sources.iter().cloned());
    }
    
    debug!(target: "clarium::exec", "run_select (staged): base_table_present={} joins_present={} by_window_ms={:?} group_by_cols={:?} rolling_window_ms={:?} select_len={} where_present={} order_by_present={:?} limit={:?} into_table_present={}",
            q.base_table.is_some(), q.joins.is_some(), q.by_window_ms, q.group_by_cols, q.rolling_window_ms, q.select.len(), q.where_clause.is_some(), q.order_by.as_ref().map(|v| !v.is_empty()), q.limit, q.into_table.is_some());

    // Execute CTEs (Common Table Expressions) if present
    if let Some(ctes) = &q.with_ctes {
        for cte in ctes {
            debug!(target: "clarium::exec", "Executing CTE: {}", cte.name);
            // Recursively execute each CTE query, passing current ctx so nested CTEs can reference earlier CTEs
            let cte_df = run_select_with_context(store, &cte.query, Some(&ctx))?;
            // Store the result in the context for later reference
            ctx.cte_tables.insert(cte.name.clone(), cte_df);
        }
    }

    // Execute stages in mandated order
    let df_from = stage_from_where(store, q, &mut ctx)?;

    // If there is no FROM source, skip dependent clauses (WHERE/JOIN already skipped inside from_where)
    if q.base_table.is_none() {
        // Skip BY/GROUP BY, ROLLING, ORDER BY/LIMIT, HAVING
        let df_proj = stage_project_select(df_from, q, &mut ctx)?;
        return Ok(df_proj);
    }

    let df_by = stage_by_or_groupby(store, df_from, q, &mut ctx)?;
    let df_roll = if q.rolling_window_ms.is_some() { stage_rolling(df_by, q, &mut ctx)? } else { df_by };
    let df_proj = stage_project_select(df_roll, q, &mut ctx)?;
    let df_order = stage_order_limit(df_proj, q, &mut ctx)?;
    let df_final = if let Some(h) = &q.having_clause { apply_having_with_validation(df_order, h, &ctx)? } else { df_order };
    Ok(df_final)
}



// Helper: derive (db, schema) defaults from an identifier that may be fully-qualified
fn derive_defaults_from_ident(ident: &str) -> (String, String) {
    // Try path-like db/schema/table(.time)
    if ident.contains('/') || ident.contains('\\') {
        let norm = ident.replace('\\', "/");
        let parts: Vec<&str> = norm.split('/').collect();
        if parts.len() >= 2 { return (parts[0].to_string(), parts[1].to_string()); }
    }
    // Try dotted db.schema.table
    let parts: Vec<&str> = ident.split('.').collect();
    if parts.len() >= 3 { return (parts[0].to_string(), parts[1].to_string()); }
    ("clarium".into(), "public".into())
}