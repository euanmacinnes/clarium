use crate::server::data_context::DataContext;

#[test]
fn resolve_prefers_alias_then_fq_then_unqualified() {
    let mut ctx = DataContext::with_defaults("clarium", "public");
    // Register two tables with same short name in different schemas
    ctx.register_table_names(Some("p".to_string()), Some("clarium/public/people".to_string()), Some("people".to_string()));
    ctx.register_table_names(None, Some("clarium/alt/people".to_string()), Some("people".to_string()));

    // Alias match
    let r1 = ctx.resolve_table_name("p");
    assert_eq!(r1, "clarium/public/people");

    // Fully qualified input should be canonicalized and returned
    let r2 = ctx.resolve_table_name("clarium.public.people");
    assert_eq!(r2, "clarium/public/people");

    // Unqualified should scope to current defaults (public)
    let r3 = ctx.resolve_table_name("people");
    assert_eq!(r3, "clarium/public/people");
}

#[test]
fn resolve_respects_current_scope_on_ambiguity() {
    let mut ctx = DataContext::with_defaults("clarium", "alt");
    ctx.register_table_names(None, Some("clarium/public/items".to_string()), Some("items".to_string()));
    ctx.register_table_names(None, Some("clarium/alt/items".to_string()), Some("items".to_string()));

    // With schema=alt, unqualified should prefer alt/items
    let r = ctx.resolve_table_name("items");
    assert_eq!(r, "clarium/alt/items");
}

#[test]
fn resolve_cte_like_alias_without_fq() {
    let mut ctx = DataContext::with_defaults("clarium", "public");
    // Simulate CTE/subquery registration: alias only
    ctx.register_table_names(Some("cte1".to_string()), None, None);
    let r = ctx.resolve_table_name("cte1");
    // When fq is unknown, we keep alias as effective name
    assert_eq!(r, "cte1");
}
