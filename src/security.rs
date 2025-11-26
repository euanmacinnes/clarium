use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use polars::prelude::*;
use argon2::{Argon2, PasswordHasher, PasswordVerifier};
use password_hash::{SaltString, PasswordHash};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope<'a> { Global, Database(&'a str) }

#[derive(Debug, Clone, Default)]
pub struct Perms {
    pub is_admin: bool,
    pub select: bool,
    pub insert: bool,
    pub calculate: bool,
    pub delete: bool,
}

fn global_user_path(db_root: &str) -> PathBuf { Path::new(db_root).join("user.parquet") }
fn db_user_path(db_root: &str, db: &str) -> PathBuf { Path::new(db_root).join(db).join("user.parquet") }

fn mk_schema_df() -> DataFrame {
    let usernames: Series = Series::new("username".into(), Vec::<String>::new());
    let hashes: Series = Series::new("password_hash".into(), Vec::<String>::new());
    let is_admin: Series = Series::new("is_admin".into(), Vec::<bool>::new());
    let perm_select: Series = Series::new("perm_select".into(), Vec::<bool>::new());
    let perm_insert: Series = Series::new("perm_insert".into(), Vec::<bool>::new());
    let perm_calculate: Series = Series::new("perm_calculate".into(), Vec::<bool>::new());
    let perm_delete: Series = Series::new("perm_delete".into(), Vec::<bool>::new());
    DataFrame::new(vec![usernames.into(), hashes.into(), is_admin.into(), perm_select.into(), perm_insert.into(), perm_calculate.into(), perm_delete.into()]).unwrap()
}

fn hash_password(password: &str) -> Result<String> {
    let mut salt_bytes = [0u8; 16];
    getrandom::getrandom(&mut salt_bytes).map_err(|e| anyhow!(e.to_string()))?;
    let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| anyhow!(e.to_string()))?;
    let argon2 = Argon2::default();
    let phc = argon2.hash_password(password.as_bytes(), &salt).map_err(|e| anyhow!(e.to_string()))?.to_string();
    Ok(phc)
}

fn verify_password(hash: &str, password: &str) -> bool {
    if let Ok(parsed) = PasswordHash::new(hash) {
        let argon2 = Argon2::default();
        argon2.verify_password(password.as_bytes(), &parsed).is_ok()
    } else { false }
}

fn read_users(path: &Path) -> Result<DataFrame> {
    if !path.exists() { return Ok(mk_schema_df()); }
    let file = std::fs::File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    Ok(df)
}

fn write_users(path: &Path, mut df: DataFrame) -> Result<()> {
    if let Some(dir) = path.parent() { std::fs::create_dir_all(dir).ok(); }
    // Ensure columns exist with correct dtypes
    let expected = mk_schema_df();
    for name in expected.get_column_names() {
        if !df.get_column_names().iter().any(|n| n.as_str() == name.as_str()) {
            // add empty default column
            let s = expected.column(name.as_str()).unwrap().clone();
            df.with_column(s)?;
        }
    }
    let mut f = std::fs::File::create(path)?;
    ParquetWriter::new(&mut f).finish(&mut df)?;
    Ok(())
}

pub fn ensure_default_admin(db_root: &str) -> Result<()> {
    let p = global_user_path(db_root);
    if p.exists() { return Ok(()); }
    let mut df = mk_schema_df();
    let hash = hash_password("timeline")?;
    let usernames = Series::new("username".into(), vec!["timeline".to_string()]);
    let hashes = Series::new("password_hash".into(), vec![hash]);
    let is_admin = Series::new("is_admin".into(), vec![true]);
    let perm_select = Series::new("perm_select".into(), vec![true]);
    let perm_insert = Series::new("perm_insert".into(), vec![true]);
    let perm_calculate = Series::new("perm_calculate".into(), vec![true]);
    let perm_delete = Series::new("perm_delete".into(), vec![true]);
    df = DataFrame::new(vec![usernames.into(), hashes.into(), is_admin.into(), perm_select.into(), perm_insert.into(), perm_calculate.into(), perm_delete.into()])?;
    write_users(&p, df)
}

pub fn add_user(db_root: &str, scope: Scope, username: &str, password: &str, perms: Perms) -> Result<()> {
    use polars::prelude::{AnyValue, BooleanType, ChunkedArray};
    let p = match scope { Scope::Global => global_user_path(db_root), Scope::Database(db) => db_user_path(db_root, db) };
    let mut df = read_users(&p)?;
    // Filter out any existing row(s) for this username
    if df.height() > 0 && df.get_column_names().iter().any(|n| n.as_str() == "username") {
        let user_s = df.column("username")?.clone();
        if let Some(series) = user_s.as_series() {
            let mask: ChunkedArray<BooleanType> = series.iter().map(|av| match av {
                AnyValue::String(s) => s != username,
                AnyValue::StringOwned(s) => s.as_str() != username,
                _ => true,
            }).collect();
            df = df.filter(&mask)?;
        }
    }
    let hash = hash_password(password)?;
    // Append row
    let new = DataFrame::new(vec![
        Series::new("username".into(), vec![username.to_string()]).into(),
        Series::new("password_hash".into(), vec![hash]).into(),
        Series::new("is_admin".into(), vec![perms.is_admin]).into(),
        Series::new("perm_select".into(), vec![perms.select]).into(),
        Series::new("perm_insert".into(), vec![perms.insert]).into(),
        Series::new("perm_calculate".into(), vec![perms.calculate]).into(),
        Series::new("perm_delete".into(), vec![perms.delete]).into(),
    ])?;
    if df.height() == 0 { write_users(&p, new) } else { let stacked = df.vstack(&new)?; write_users(&p, stacked) }
}

pub fn delete_user(db_root: &str, scope: Scope, username: &str) -> Result<()> {
    use polars::prelude::{AnyValue, BooleanType, ChunkedArray};
    let p = match scope { Scope::Global => global_user_path(db_root), Scope::Database(db) => db_user_path(db_root, db) };
    let mut df = read_users(&p)?;
    if df.height() == 0 { return Ok(()); }
    // Build mask of rows to keep
    let user_s = df.column("username")?.clone();
    if let Some(series) = user_s.as_series() {
        let mask: ChunkedArray<BooleanType> = series.iter().map(|av| match av {
            AnyValue::String(s) => s != username,
            AnyValue::StringOwned(s) => s.as_str() != username,
            _ => true,
        }).collect();
        df = df.filter(&mask)?;
    }
    write_users(&p, df)
}

pub fn alter_user(db_root: &str, scope: Scope, username: &str, new_password: Option<&str>, new_admin: Option<bool>, new_perms: Option<Perms>) -> Result<()> {
    use polars::prelude::{AnyValue, BooleanType, ChunkedArray};
    let p = match scope { Scope::Global => global_user_path(db_root), Scope::Database(db) => db_user_path(db_root, db) };
    let mut df = read_users(&p)?;
    if df.height() == 0 { return Err(anyhow!("user not found")); }
    // Capture current row values by scanning
    let mut found = false;
    let mut cur_hash = String::new();
    let mut cur_admin = false;
    let mut cur_sel = false;
    let mut cur_ins = false;
    let mut cur_calc = false;
    let mut cur_del = false;
    for i in 0..df.height() {
        let uname = df.column("username")?.get(i)?;
        let name_matches = match uname {
            AnyValue::String(s) => s == username,
            AnyValue::StringOwned(ref s) => s.as_str() == username,
            _ => false,
        };
        if name_matches {
            found = true;
            cur_hash = match df.column("password_hash")?.get(i)? {
                AnyValue::String(s) => s.to_string(),
                AnyValue::StringOwned(s) => s.to_string(),
                _ => String::new(),
            };
            cur_admin = df.column("is_admin")?.bool()?.get(i).unwrap_or(false);
            cur_sel = df.column("perm_select")?.bool()?.get(i).unwrap_or(false);
            cur_ins = df.column("perm_insert")?.bool()?.get(i).unwrap_or(false);
            cur_calc = df.column("perm_calculate")?.bool()?.get(i).unwrap_or(false);
            cur_del = df.column("perm_delete")?.bool()?.get(i).unwrap_or(false);
            break;
        }
    }
    if !found { return Err(anyhow!("user not found")); }

    let new_hash = if let Some(pw) = new_password { hash_password(pw)? } else { cur_hash };
    let new_admin2 = new_admin.unwrap_or(cur_admin);
    let mut sel = cur_sel; let mut ins = cur_ins; let mut calc = cur_calc; let mut del = cur_del;
    if let Some(p) = new_perms { sel = p.select; ins = p.insert; calc = p.calculate; del = p.delete; }

    // Remove all existing rows for this username
    let user_s = df.column("username")?.clone();
    if let Some(series) = user_s.as_series() {
        let keep_mask: ChunkedArray<BooleanType> = series.iter().map(|av| match av {
            AnyValue::String(s) => s != username,
            AnyValue::StringOwned(s) => s.as_str() != username,
            _ => true,
        }).collect();
        df = df.filter(&keep_mask)?;
    }

    // Append updated row
    let updated = DataFrame::new(vec![
        Series::new("username".into(), vec![username.to_string()]).into(),
        Series::new("password_hash".into(), vec![new_hash]).into(),
        Series::new("is_admin".into(), vec![new_admin2]).into(),
        Series::new("perm_select".into(), vec![sel]).into(),
        Series::new("perm_insert".into(), vec![ins]).into(),
        Series::new("perm_calculate".into(), vec![calc]).into(),
        Series::new("perm_delete".into(), vec![del]).into(),
    ])?;
    if df.height() == 0 { write_users(&p, updated) } else { let stacked = df.vstack(&updated)?; write_users(&p, stacked) }
}

pub fn authenticate(db_root: &str, username: &str, password: &str) -> Result<bool> {
    use polars::prelude::AnyValue;
    let p = global_user_path(db_root);
    let df = read_users(&p)?;
    if df.height() == 0 { return Ok(false); }
    for i in 0..df.height() {
        let uname = df.column("username")?.get(i)?;
        let matches = match uname {
            AnyValue::String(s) => s == username,
            AnyValue::StringOwned(ref s) => s.as_str() == username,
            _ => false,
        };
        if matches {
            let hv = df.column("password_hash")?.get(i)?;
            let hash = match hv {
                AnyValue::String(s) => s,
                AnyValue::StringOwned(ref s) => s.as_str(),
                _ => "",
            };
            return Ok(verify_password(hash, password));
        }
    }
    Ok(false)
}

fn load_perms_from_df(df: &DataFrame, username: &str) -> Option<Perms> {
    for i in 0..df.height() {
        let uname = df.column("username").ok()?.get(i).ok()?;
        let matches = match uname {
            polars::prelude::AnyValue::String(s) => s == username,
            polars::prelude::AnyValue::StringOwned(ref s) => s.as_str() == username,
            _ => false,
        };
        if matches {
            let is_admin = df.column("is_admin").ok()?.bool().ok()?.get(i).unwrap_or(false);
            let psel = df.column("perm_select").ok()?.bool().ok()?.get(i).unwrap_or(false);
            let pins = df.column("perm_insert").ok()?.bool().ok()?.get(i).unwrap_or(false);
            let pcalc = df.column("perm_calculate").ok()?.bool().ok()?.get(i).unwrap_or(false);
            let pdel = df.column("perm_delete").ok()?.bool().ok()?.get(i).unwrap_or(false);
            return Some(Perms { is_admin, select: psel, insert: pins, calculate: pcalc, delete: pdel });
        }
    }
    None
}

pub enum CommandKind { Select, Insert, Calculate, DeleteRows, DeleteColumns, Schema, Database, Other }

pub fn authorize(db_root: &str, username: &str, cmd: CommandKind, db: Option<&str>) -> Result<bool> {
    // 1) Global admin override
    let global = read_users(&global_user_path(db_root))?;
    if let Some(p) = load_perms_from_df(&global, username) { if p.is_admin { return Ok(true); } }

    // Helper for mapping perms to command kinds
    fn allow(cmd: CommandKind, perms: &Perms) -> bool {
        match cmd {
            CommandKind::Select => perms.select,
            CommandKind::Insert => perms.insert,
            CommandKind::Calculate => perms.calculate,
            CommandKind::DeleteRows | CommandKind::DeleteColumns => perms.delete,
            // DDL and other commands are restricted to admins (handled above)
            CommandKind::Schema | CommandKind::Database | CommandKind::Other => false,
        }
    }

    // 2) Hierarchical lookup: table → schema → database
    if let Some(raw) = db {
        // Normalize separators to '/'
        let mut cur = raw.replace('\\', "/");
        // Walk from most specific to least by stripping trailing path segments
        loop {
            let p = db_user_path(db_root, &cur);
            if p.exists() {
                let df = read_users(&p)?;
                if let Some(perms) = load_perms_from_df(&df, username) {
                    return Ok(allow(cmd, &perms));
                }
            }
            // Move to parent (strip last segment). Stop when no parent remains.
            if let Some((parent, _last)) = cur.rsplit_once('/') { cur = parent.to_string(); } else { break; }
            if cur.is_empty() { break; }
        }
    }

    // 3) Fall back to global perms
    if let Some(p) = load_perms_from_df(&global, username) { return Ok(allow(cmd, &p)); }

    Ok(false)
}
