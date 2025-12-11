use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use base64::Engine;
use crate::tprintln;

use super::principal::Principal;

pub type SessionToken = String;

#[derive(Debug, Clone)]
pub struct Session {
    pub session_id: String,
    pub token: SessionToken,
    pub principal: Principal,
    pub issued_at: Instant,
    pub expires_at: Instant,
}

#[derive(Debug)]
struct SessionEntry {
    session: Session,
}

static SESSIONS: Lazy<RwLock<HashMap<String, SessionEntry>>> = Lazy::new(|| RwLock::new(HashMap::new()));
static USER_INDEX: Lazy<RwLock<HashMap<String, HashSet<String>>>> = Lazy::new(|| RwLock::new(HashMap::new()));
static REVOKED: Lazy<RwLock<HashSet<String>>> = Lazy::new(|| RwLock::new(HashSet::new()));

fn gen_id() -> String {
    // 128-bit random token base64url without padding
    let mut buf = [0u8; 32];
    let _ = getrandom::getrandom(&mut buf);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

pub struct SessionManager {
    pub ttl: Duration,
}

impl Default for SessionManager {
    fn default() -> Self { Self { ttl: Duration::from_secs(60 * 60) } }
}

impl SessionManager {
    pub fn issue(&self, principal: Principal) -> Session {
        let now = Instant::now();
        let sid = gen_id();
        let token = gen_id();
        let sess = Session {
            session_id: sid.clone(),
            token: token.clone(),
            principal: principal.clone(),
            issued_at: now,
            expires_at: now + self.ttl,
        };
        let entry = SessionEntry { session: sess.clone() };
        {
            let mut m = SESSIONS.write();
            m.insert(token.clone(), entry);
        }
        {
            let mut uidx = USER_INDEX.write();
            let set = uidx.entry(principal.user_id.clone()).or_insert_with(HashSet::new);
            set.insert(token.clone());
        }
        tprintln!("session.issue user={} sid={} ttl_secs={}", principal.user_id, sid, self.ttl.as_secs());
        sess
    }

    pub fn validate(&self, token: &str) -> Option<Principal> {
        // prune revoked
        if REVOKED.read().contains(token) { return None; }
        let now = Instant::now();
        let mut drop_key: Option<String> = None;
        let out = {
            let map = SESSIONS.read();
            if let Some(ent) = map.get(token) {
                if ent.session.expires_at > now {
                    Some(ent.session.principal.clone())
                } else {
                    drop_key = Some(token.to_string());
                    None
                }
            } else { None }
        };
        if let Some(k) = drop_key {
            SESSIONS.write().remove(&k);
        }
        out
    }

    pub fn logout(&self, token: &str) -> bool {
        let mut removed = false;
        if let Some(ent) = SESSIONS.write().remove(token) {
            removed = true;
            let uid = ent.session.principal.user_id;
            let mut idx = USER_INDEX.write();
            if let Some(set) = idx.get_mut(&uid) { set.remove(token); }
            REVOKED.write().insert(token.to_string());
        }
        removed
    }

    pub fn revoke_user(&self, user_id: &str) -> usize {
        let mut count = 0usize;
        if let Some(tokens) = USER_INDEX.read().get(user_id).cloned() {
            let mut s = SESSIONS.write();
            let mut r = REVOKED.write();
            for t in tokens.iter() {
                if s.remove(t).is_some() { count += 1; }
                r.insert(t.clone());
            }
        }
        tprintln!("session.revoke user={} count={}", user_id, count);
        count
    }
}
