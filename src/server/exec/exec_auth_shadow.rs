//! SQL authorization using Security v2 (RBAC/ABAC).
//!
//! Provides helper functions to map parsed SQL `Command` to a coarse
//! `Action` and `ResourceId`, evaluate via `filestore::sec::authorize`,
//! and either log (shadow) or enforce.

use crate::identity::RequestContext;
use crate::tprintln;
use crate::server::query::Command;

use crate::server::exec::filestore::sec as sec;

fn map_cmd_to_action(cmd: &Command) -> sec::model::Action {
    use sec::model::Action as A;
    match cmd {
        Command::Select(_) => A::Read,
        Command::Explain { .. } => A::Read,
        Command::Insert { .. } => A::Write,
        Command::Update { .. } => A::Write,
        Command::DeleteRows { .. } | Command::DeleteColumns { .. } => A::Delete,
        Command::CreateTable { .. }
        | Command::AlterTable { .. }
        | Command::DropTable { .. }
        | Command::CreateView { .. }
        | Command::DropView { .. }
        | Command::CreateDatabase { .. }
        | Command::DropDatabase { .. }
        | Command::RenameDatabase { .. }
        | Command::DatabaseAdd { .. }
        | Command::DatabaseDelete { .. }
        | Command::CreateSchema { .. }
        | Command::DropSchema { .. }
        | Command::RenameSchema { .. }
        | Command::SchemaAdd { .. }
        | Command::CreateTimeTable { .. }
        | Command::DropTimeTable { .. }
        | Command::RenameTimeTable { .. }
        | Command::CreateStore { .. }
        | Command::DropStore { .. }
        | Command::RenameStore { .. }
        | Command::WriteKey { .. }
        | Command::DropKey { .. }
        | Command::RenameKey { .. }
        | Command::UserAdd { .. }
        | Command::UserDelete { .. }
        => A::Write,
        Command::SchemaShow { .. }
        | Command::ListStores { .. }
        | Command::ListKeys { .. }
        | Command::DescribeKey { .. }
        | Command::ReadKey { .. }
        | Command::ShowView { .. }
        => A::Read,
        _ => A::Read,
    }
}

// Map a SQL Command to a resource id. Keep the function small; refine per-object helpers later.
fn map_cmd_to_resource(ctx: &RequestContext, cmd: &Command) -> sec::model::ResourceId {
    // Small per-object helpers; avoid large matches by delegating.
    #[inline]
    fn split_db_schema_table(ctx: &RequestContext, path: &str) -> (String, String, String) {
        // Accept forms: db/schema/table, schema.table, table; fall back to ctx defaults
        let def_db = ctx.database.as_deref().unwrap_or(crate::ident::DEFAULT_DB);
        let def_schema = crate::ident::DEFAULT_SCHEMA;
        let p = path.replace('\\', "/");
        let parts: Vec<&str> = p.split('/').collect();
        match parts.len() {
            3 => (parts[0].to_string(), parts[1].to_string(), parts[2].to_string()),
            2 => (def_db.to_string(), parts[0].to_string(), parts[1].to_string()),
            1 => (def_db.to_string(), def_schema.to_string(), parts[0].to_string()),
            _ => (def_db.to_string(), def_schema.to_string(), path.to_string()),
        }
    }
    #[inline]
    fn split_db_schema(ctx: &RequestContext, path: &str) -> (String, String) {
        // Accept forms: db/schema or schema; default db from ctx
        let def_db = ctx.database.as_deref().unwrap_or(crate::ident::DEFAULT_DB);
        let p = path.replace('\\', "/");
        let parts: Vec<&str> = p.split('/').collect();
        match parts.len() {
            2 => (parts[0].to_string(), parts[1].to_string()),
            1 => (def_db.to_string(), parts[0].to_string()),
            _ => (def_db.to_string(), path.to_string()),
        }
    }

    use sec::resources as R;
    let db_default = ctx.database.as_deref().unwrap_or(crate::ident::DEFAULT_DB);
    match cmd {
        // Data access
        Command::Select(_q) => {
            // For now, enforce at database scope to avoid parser-specific table refs here
            R::res_database(db_default)
        }
        Command::Update { table, .. }
        | Command::CreateTimeTable { table }
        | Command::DropTimeTable { table }
        | Command::RenameTimeTable { from: table, .. }
        | Command::CreateTable { table, .. }
        | Command::DropTable { table, .. }
        | Command::RenameTable { from: table, .. }
        | Command::AlterTable { table, .. } => {
            let (db, schema, t) = split_db_schema_table(ctx, table);
            R::res_table(&db, &schema, &t)
        }
        Command::DeleteRows { database, .. }
        | Command::DeleteColumns { database, .. }
        | Command::SchemaShow { database }
        | Command::CreateStore { database, .. }
        | Command::DropStore { database, .. }
        | Command::RenameStore { database, .. }
        | Command::ListStores { database }
        | Command::ListKeys { database, .. }
        | Command::DescribeKey { database, .. }
        | Command::WriteKey { database, .. }
        | Command::ReadKey { database, .. }
        | Command::DropKey { database, .. }
        | Command::RenameKey { database, .. } => {
            // Keys and stores are scoped to a database
            R::res_database(database)
        }
        Command::CreateSchema { path }
        | Command::DropSchema { path }
        | Command::RenameSchema { from: path, .. } => {
            let (db, schema) = split_db_schema(ctx, path);
            R::res_schema(&db, &schema)
        }
        Command::CreateDatabase { name }
        | Command::DropDatabase { name }
        | Command::RenameDatabase { from: name, .. }
        | Command::DatabaseAdd { database: name }
        | Command::DatabaseDelete { database: name } => R::res_database(name),
        // View and misc default to database scope
        Command::CreateView { .. }
        | Command::DropView { .. }
        | Command::ShowView { .. }
        | Command::ClearScriptCache { .. }
        | Command::UserAdd { .. }
        | Command::UserDelete { .. }
        | Command::Calculate { .. }
        | Command::SelectUnion { .. }
        | Command::Explain { .. }
        | _ => R::res_database(db_default),
    }
}

pub fn shadow_authorize_sql(ctx: &RequestContext, cmd: &Command) {
    let Some(pr) = ctx.principal.as_ref() else { return; };
    let user = sec::model::User { id: pr.user_id.clone(), roles: pr.roles.clone(), ip: pr.attrs.ip.clone() };
    let action = map_cmd_to_action(cmd);
    let res = map_cmd_to_resource(ctx, cmd);
    let c = sec::model::Context { request_id: ctx.request_id.clone(), ..Default::default() };
    let dec = sec::authorize(&user, action, &res, &c);
    tprintln!(
        "sec.shadow sql user={} action={:?} resource={} allow={} reason={:?}",
        user.id, action, res.0, dec.allow, dec.reason
    );
}

pub fn enforce_authorize_sql(ctx: &RequestContext, cmd: &Command) -> anyhow::Result<()> {
    use anyhow::anyhow;
    // If no principal, deny by default
    let Some(pr) = ctx.principal.as_ref() else { return Err(anyhow!("unauthorized: no principal")); };
    let user = sec::model::User { id: pr.user_id.clone(), roles: pr.roles.clone(), ip: pr.attrs.ip.clone() };
    let action = map_cmd_to_action(cmd);
    let res = map_cmd_to_resource(ctx, cmd);
    let c = sec::model::Context { request_id: ctx.request_id.clone(), ..Default::default() };
    let dec = sec::authorize(&user, action, &res, &c);
    // Emit post-auth hook for auditing
    let ev = sec::hooks::HookEvent { user: user.clone(), action, resource: res.clone(), ctx: c.clone(), decision: Some(dec.clone()) };
    sec::hooks::emit_post_auth(&ev);
    if dec.allow { Ok(()) } else { Err(anyhow!(format!("unauthorized: user={} action={:?} resource={} reason={}", user.id, action, res.0, dec.reason.unwrap_or_else(|| "deny".into())))) }
}
