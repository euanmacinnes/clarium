use polars::prelude::{DataFrame, Series, NamedFrom};

// Shared builder for role rows used by pg_roles and pg_authid
pub struct RoleRows {
    pub oid: Vec<i32>,
    pub rolname: Vec<String>,
    pub rolsuper: Vec<bool>,
    pub rolinherit: Vec<bool>,
    pub rolcreaterole: Vec<bool>,
    pub rolcreatedb: Vec<bool>,
    pub rolcanlogin: Vec<bool>,
    pub rolreplication: Vec<bool>,
    pub rolbypassrls: Vec<bool>,
    pub rolconnlimit: Vec<i32>,
    pub rolpassword: Vec<String>,
    pub rolvaliduntil: Vec<String>,
}

impl RoleRows {
    pub fn to_df(&self) -> DataFrame {
        DataFrame::new(vec![
            Series::new("oid".into(), self.oid.clone()).into(),
            Series::new("rolname".into(), self.rolname.clone()).into(),
            Series::new("rolsuper".into(), self.rolsuper.clone()).into(),
            Series::new("rolinherit".into(), self.rolinherit.clone()).into(),
            Series::new("rolcreaterole".into(), self.rolcreaterole.clone()).into(),
            Series::new("rolcreatedb".into(), self.rolcreatedb.clone()).into(),
            Series::new("rolcanlogin".into(), self.rolcanlogin.clone()).into(),
            Series::new("rolreplication".into(), self.rolreplication.clone()).into(),
            Series::new("rolconnlimit".into(), self.rolconnlimit.clone()).into(),
            Series::new("rolpassword".into(), self.rolpassword.clone()).into(),
            Series::new("rolvaliduntil".into(), self.rolvaliduntil.clone()).into(),
            Series::new("rolbypassrls".into(), self.rolbypassrls.clone()).into(),
        ]).unwrap()
    }
}

// For now, synthesize two core roles and map RBAC-like options onto rol* flags.
// Future: extend from persisted RBAC once available.
pub fn synthesize_core_roles() -> RoleRows {
    // Stable OIDs for built-ins within this engine's lifetime
    let mut oid: Vec<i32> = Vec::new();
    let mut rolname: Vec<String> = Vec::new();
    let mut rolsuper: Vec<bool> = Vec::new();
    let mut rolinherit: Vec<bool> = Vec::new();
    let mut rolcreaterole: Vec<bool> = Vec::new();
    let mut rolcreatedb: Vec<bool> = Vec::new();
    let mut rolcanlogin: Vec<bool> = Vec::new();
    let mut rolreplication: Vec<bool> = Vec::new();
    let mut rolbypassrls: Vec<bool> = Vec::new();
    let mut rolconnlimit: Vec<i32> = Vec::new();
    let mut rolpassword: Vec<String> = Vec::new();
    let mut rolvaliduntil: Vec<String> = Vec::new();

    // postgres: admin with full capabilities
    oid.push(10);
    rolname.push("postgres".to_string());
    rolsuper.push(true);
    rolinherit.push(true);
    rolcreaterole.push(true);
    rolcreatedb.push(true);
    rolcanlogin.push(true);
    rolreplication.push(false);
    rolbypassrls.push(true);
    rolconnlimit.push(-1);
    rolpassword.push(String::new());
    rolvaliduntil.push(String::new());

    // public: implicit, cannot login
    oid.push(11);
    rolname.push("public".to_string());
    rolsuper.push(false);
    rolinherit.push(true);
    rolcreaterole.push(false);
    rolcreatedb.push(false);
    rolcanlogin.push(false);
    rolreplication.push(false);
    rolbypassrls.push(false);
    rolconnlimit.push(-1);
    rolpassword.push(String::new());
    rolvaliduntil.push(String::new());

    RoleRows {
        oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, rolvaliduntil,
    }
}
