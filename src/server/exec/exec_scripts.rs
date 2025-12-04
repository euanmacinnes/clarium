use anyhow::Result;
use serde_json::Value;
use std::path::Path;

use crate::server::query::Command;
use crate::scripts::{get_script_registry, scripts_dir_for};
use crate::storage::SharedStore;

pub fn execute_scripts(store: &SharedStore, cmd: Command) -> Result<Value> {
    match cmd {
        Command::CreateScript { path, code } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            // Expect path in form db/schema/name[.lua]
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(Path::new(&root), parts[0], parts[1]);
            fs::create_dir_all(&dir)?;
            let mut fname = parts[2].to_string();
            if !fname.ends_with(".lua") { fname.push_str(".lua"); }
            let fpath = dir.join(&fname);
            fs::write(&fpath, code.as_bytes())?;
            if let Some(reg) = get_script_registry() {
                let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]);
                let text = code;
                let _ = reg.load_script_text(name_no_ext, &text);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::DropScript { path } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(Path::new(&root), parts[0], parts[1]);
            let mut fname = parts[2].to_string();
            if !fname.ends_with(".lua") { fname.push_str(".lua"); }
            let fpath = dir.join(&fname);
            if fpath.exists() { fs::remove_file(&fpath)?; }
            if let Some(reg) = get_script_registry() {
                let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]);
                reg.unload_function(name_no_ext);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::RenameScript { from, to } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            let fparts: Vec<&str> = from.split('/').collect();
            if fparts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
            let dir = scripts_dir_for(Path::new(&root), fparts[0], fparts[1]);
            fs::create_dir_all(&dir)?;
            let mut from_name = fparts[2].to_string(); if !from_name.ends_with(".lua") { from_name.push_str(".lua"); }
            let mut to_name = {
                let tparts: Vec<&str> = to.split('/').collect();
                if tparts.len() == 1 { tparts[0].to_string() } else if tparts.len() == 3 { if tparts[0]!=fparts[0] || tparts[1]!=fparts[1] { anyhow::bail!("Cannot move scripts across schemas"); } tparts[2].to_string() } else { anyhow::bail!("Invalid RENAME SCRIPT target"); }
            };
            if !to_name.ends_with(".lua") { to_name.push_str(".lua"); }
            let fp_from = dir.join(&from_name);
            let fp_to = dir.join(&to_name);
            fs::rename(&fp_from, &fp_to)?;
            if let Some(reg) = get_script_registry() {
                let oldn = fparts[2].split('.').next().unwrap_or(fparts[2]);
                let newn = to_name.trim_end_matches(".lua");
                let _ = reg.rename_function(oldn, newn);
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        Command::LoadScript { path } => {
            use std::fs;
            let root = { let g = store.0.lock(); g.root_path().clone() };
            if let Some(p) = path {
                let parts: Vec<&str> = p.split('/').collect();
                if parts.len() != 3 { anyhow::bail!("SCRIPT path must be <db>/<schema>/<name>"); }
                let dir = scripts_dir_for(Path::new(&root), parts[0], parts[1]);
                let mut fname = parts[2].to_string(); if !fname.ends_with(".lua") { fname.push_str(".lua"); }
                let fpath = dir.join(&fname);
                let code = fs::read_to_string(&fpath)?;
                if let Some(reg) = get_script_registry() { let name_no_ext = parts[2].split('.').next().unwrap_or(parts[2]); let _ = reg.load_script_text(name_no_ext, &code); }
            } else {
                // Load all scripts from all schemas
                for dbent in fs::read_dir(&root)? {
                    let dbent = dbent?; if !dbent.file_type()?.is_dir() { continue; }
                    for schent in fs::read_dir(dbent.path())? { let schent = schent?; if !schent.file_type()?.is_dir() { continue; }
                        let sdir = scripts_dir_for(Path::new(&root), &dbent.file_name().to_string_lossy(), &schent.file_name().to_string_lossy());
                        if sdir.exists() {
                            for sf in fs::read_dir(&sdir)? { let sf = sf?; let pth = sf.path(); if pth.extension().and_then(|e| e.to_str()).unwrap_or("").eq_ignore_ascii_case("lua") { let name = pth.file_stem().and_then(|s| s.to_str()).unwrap_or(""); let code = fs::read_to_string(&pth)?; if let Some(reg) = get_script_registry() { let _ = reg.load_script_text(name, &code); } } }
                        }
                    }
                }
            }
            Ok(serde_json::json!({"status":"ok"}))
        }
        other => anyhow::bail!(format!("unsupported SCRIPT command: {:?}", other)),
    }
}
