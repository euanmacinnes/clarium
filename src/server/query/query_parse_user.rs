use crate::server::query::*;

pub fn parse_user(s: &str) -> Result<Command> {
    // USER ADD <username> PASSWORD '<pw>' [ADMIN] [PERMISSIONS (<list>)] [GLOBAL | (IN|FROM|TO) <db>]
    // USER DELETE <username> [GLOBAL | (IN|FROM|TO) <db>]
    let rest = s[4..].trim();
    let up = rest.to_uppercase();
    if up.starts_with("ADD ") {
        let mut tail = &rest[4..];
        // username up to space
        let mut parts = tail.trim().splitn(2, ' ');
        let username = parts.next().unwrap_or("").trim();
        if username.is_empty() { anyhow::bail!("USER ADD: missing username"); }
        tail = parts.next().unwrap_or("").trim_start();
        let tail_up = tail.to_uppercase();
        if !tail_up.starts_with("PASSWORD ") { anyhow::bail!("USER ADD: expected PASSWORD"); }
        let after_pw = &tail[9..].trim();
        // password token: quoted string until next space or end; accept single quotes
        let pw = if after_pw.starts_with('\'') {
            if let Some(idx) = after_pw[1..].find('\'') { &after_pw[1..1+idx] } else { anyhow::bail!("USER ADD: unterminated password"); }
        } else { // allow unquoted for convenience
            let mut it = after_pw.split_whitespace(); it.next().unwrap_or("")
        };
        let mut is_admin = false;
        let mut perms: Vec<String> = Vec::new();
        let mut scope_db: Option<String> = None;
        // Remaining tail after password
        let after_pw_tail = if after_pw.starts_with('\'') { &after_pw[pw.len()+2..] } else { &after_pw[pw.len()..] };
        let mut t = after_pw_tail.trim();
        loop {
            if t.is_empty() { break; }
            let t_up = t.to_uppercase();
            if t_up.starts_with("ADMIN") {
                is_admin = true; t = t[5..].trim_start(); continue;
            }
            if t_up.starts_with("PERMISSIONS ") {
                let inner = &t[12..].trim();
                if inner.starts_with('(') {
                    if let Some(end) = inner.find(')') {
                        let list = &inner[1..end];
                        perms = list.split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect();
                        t = inner[end+1..].trim_start();
                        continue;
                    } else { anyhow::bail!("USER ADD: PERMISSIONS missing )"); }
                } else { anyhow::bail!("USER ADD: PERMISSIONS expects (..)"); }
            }
            if t_up.starts_with("GLOBAL") { scope_db = None; t = t[6..].trim_start(); continue; }
            if t_up.starts_with("IN ") || t_up.starts_with("FROM ") || t_up.starts_with("TO ") {
                let db = t[3..].trim(); scope_db = Some(db.to_string()); t = ""; continue;
            }
            break;
        }
        return Ok(Command::UserAdd { username: username.to_string(), password: pw.to_string(), is_admin, perms, scope_db });
    } else if up.starts_with("ALTER ") {
        // USER ALTER <username> [PASSWORD '<pw>'] [ADMIN true|false] [PERMISSIONS (<list>)] [GLOBAL | (IN|FROM|TO) <db>]
        let mut tail = &rest[6..];
        // username up to space or end
        let mut parts = tail.trim().splitn(2, ' ');
        let username = parts.next().unwrap_or("").trim();
        if username.is_empty() { anyhow::bail!("USER ALTER: missing username"); }
        tail = parts.next().unwrap_or("").trim_start();
        let mut new_password: Option<String> = None;
        let mut is_admin: Option<bool> = None;
        let mut perms: Option<Vec<String>> = None;
        let mut scope_db: Option<String> = None;
        let mut t = tail;
        loop {
            if t.is_empty() { break; }
            let t_up = t.to_uppercase();
            if t_up.starts_with("PASSWORD ") {
                let after_pw = &t[9..].trim();
                let pw = if after_pw.starts_with('\'') {
                    if let Some(idx) = after_pw[1..].find('\'') { &after_pw[1..1+idx] } else { anyhow::bail!("USER ALTER: unterminated password"); }
                } else {
                    let mut it = after_pw.split_whitespace(); it.next().unwrap_or("")
                };
                new_password = Some(pw.to_string());
                t = if after_pw.starts_with('\'') { &after_pw[pw.len()+2..] } else { &after_pw[pw.len()..] };
                t = t.trim_start();
                continue;
            }
            if t_up.starts_with("ADMIN ") {
                let val = t[6..].trim();
                let (word, rest): (&str, &str) = if let Some(i) = val.find(' ') { (&val[..i], val[i+1..].trim()) } else { (val, "") };
                let b = match word.to_uppercase().as_str() { "TRUE" | "T" | "YES" | "Y" | "1" => true, "FALSE" | "F" | "NO" | "N" | "0" => false, _ => anyhow::bail!("USER ALTER: ADMIN expects true|false") };
                is_admin = Some(b);
                t = rest.trim_start();
                continue;
            }
            if t_up.starts_with("PERMISSIONS ") {
                let inner = &t[12..].trim();
                if inner.starts_with('(') {
                    if let Some(end) = inner.find(')') {
                        let list = &inner[1..end];
                        let p: Vec<String> = list.split(',').map(|s| s.trim().to_uppercase()).filter(|s| !s.is_empty()).collect();
                        perms = Some(p);
                        t = inner[end+1..].trim_start();
                        continue;
                    } else { anyhow::bail!("USER ALTER: PERMISSIONS missing )"); }
                } else { anyhow::bail!("USER ALTER: PERMISSIONS expects (..)"); }
            }
            if t_up.starts_with("GLOBAL") { scope_db = None; t = t[6..].trim_start(); continue; }
            if t_up.starts_with("IN ") || t_up.starts_with("FROM ") || t_up.starts_with("TO ") {
                let db = t[3..].trim(); scope_db = Some(db.to_string()); t = ""; continue;
            }
            break;
        }
        return Ok(Command::UserAlter { username: username.to_string(), new_password, is_admin, perms, scope_db });
    } else if up.starts_with("DELETE ") {
        let tail = &rest[7..].trim();
        let mut scope_db: Option<String> = None;
        let up_tail = tail.to_uppercase();
        let username_str: String;
        if let Some(i) = up_tail.find(" IN ") {
            username_str = tail[..i].trim().to_string();
            scope_db = Some(tail[i+4..].trim().to_string());
        } else if let Some(i) = up_tail.find(" FROM ") {
            username_str = tail[..i].trim().to_string();
            scope_db = Some(tail[i+6..].trim().to_string());
        } else if let Some(i) = up_tail.find(" TO ") {
            username_str = tail[..i].trim().to_string();
            scope_db = Some(tail[i+4..].trim().to_string());
        } else if up_tail.ends_with(" GLOBAL") {
            username_str = tail[..tail.len()-7].trim().to_string();
            scope_db = None;
        } else {
            username_str = tail.trim().to_string();
        }
        if username_str.is_empty() { anyhow::bail!("USER DELETE: missing username"); }
        return Ok(Command::UserDelete { username: username_str, scope_db });
    }
    anyhow::bail!("Invalid USER syntax")
}
