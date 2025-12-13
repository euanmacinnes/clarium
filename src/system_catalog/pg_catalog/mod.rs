
// (legacy try_build removed; registry-based dispatch is now the single path)

// ---- Registration ----

// Column definitions for known pg_catalog tables (subset used by our engine)
const COLS_PG_AM: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "amname", coltype: ColType::Text },
    ColumnDef { name: "amhandler", coltype: ColType::Integer },
    ColumnDef { name: "amtype", coltype: ColType::Text },
];
const COLS_PG_AMOP: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "amopfamily", coltype: ColType::Integer },
    ColumnDef { name: "amoplefttype", coltype: ColType::Integer },
    ColumnDef { name: "amoprighttype", coltype: ColType::Integer },
    ColumnDef { name: "amopstrategy", coltype: ColType::Integer },
    ColumnDef { name: "amoppurpose", coltype: ColType::Text },
    ColumnDef { name: "amopopr", coltype: ColType::Integer },
    ColumnDef { name: "amopmethod", coltype: ColType::Integer },
    ColumnDef { name: "amopsortfamily", coltype: ColType::Integer },
];
const COLS_PG_AMPROC: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "amprocfamily", coltype: ColType::Integer },
    ColumnDef { name: "amproclefttype", coltype: ColType::Integer },
    ColumnDef { name: "amprocrighttype", coltype: ColType::Integer },
    ColumnDef { name: "amprocnum", coltype: ColType::Integer },
    ColumnDef { name: "amproc", coltype: ColType::Integer },
];
const COLS_PG_OPERATOR: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "oprname", coltype: ColType::Text },
    ColumnDef { name: "oprnamespace", coltype: ColType::Integer },
    ColumnDef { name: "oprleft", coltype: ColType::Integer },
    ColumnDef { name: "oprright", coltype: ColType::Integer },
    ColumnDef { name: "oprresult", coltype: ColType::Integer },
    ColumnDef { name: "oprcom", coltype: ColType::Integer },
    ColumnDef { name: "oprnegate", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "oprowner", coltype: ColType::Integer },
    ColumnDef { name: "oprkind", coltype: ColType::Text },
    ColumnDef { name: "oprcanmerge", coltype: ColType::Boolean },
    ColumnDef { name: "oprcanhash", coltype: ColType::Boolean },
    ColumnDef { name: "oprcode", coltype: ColType::Integer },
    ColumnDef { name: "oprrest", coltype: ColType::Integer },
    ColumnDef { name: "oprjoin", coltype: ColType::Integer },
];
const COLS_PG_OPCLASS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "opcname", coltype: ColType::Text },
    ColumnDef { name: "opcnamespace", coltype: ColType::Integer },
    ColumnDef { name: "opcmethod", coltype: ColType::Integer },
    ColumnDef { name: "opcintype", coltype: ColType::Integer },
    ColumnDef { name: "opckeytype", coltype: ColType::Integer },
    ColumnDef { name: "opcdefault", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "opcowner", coltype: ColType::Integer },
    ColumnDef { name: "opcfamily", coltype: ColType::Integer },
];
const COLS_PG_OPFAMILY: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "opfname", coltype: ColType::Text },
    ColumnDef { name: "opfnamespace", coltype: ColType::Integer },
    ColumnDef { name: "opfmethod", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "opfowner", coltype: ColType::Integer },
];
const COLS_PG_COLLATION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "collname", coltype: ColType::Text },
    ColumnDef { name: "collnamespace", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "collowner", coltype: ColType::Integer },
    ColumnDef { name: "collprovider", coltype: ColType::Text },
    ColumnDef { name: "collisdeterministic", coltype: ColType::Boolean },
    ColumnDef { name: "collencoding", coltype: ColType::Integer },
    ColumnDef { name: "collcollate", coltype: ColType::Text },
    ColumnDef { name: "collctype", coltype: ColType::Text },
    ColumnDef { name: "collversion", coltype: ColType::Text },
];
const COLS_PG_CONVERSION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "conname", coltype: ColType::Text },
    ColumnDef { name: "connamespace", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "conowner", coltype: ColType::Integer },
    ColumnDef { name: "conforencoding", coltype: ColType::Integer },
    ColumnDef { name: "contoencoding", coltype: ColType::Integer },
    ColumnDef { name: "conproc", coltype: ColType::Integer },
    ColumnDef { name: "condefault", coltype: ColType::Boolean },
];
const COLS_PG_LANGUAGE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "lanname", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "lanowner", coltype: ColType::Integer },
    ColumnDef { name: "lanispl", coltype: ColType::Boolean },
    ColumnDef { name: "lanpltrusted", coltype: ColType::Boolean },
    ColumnDef { name: "lanplcallfoid", coltype: ColType::Integer },
    ColumnDef { name: "laninline", coltype: ColType::Integer },
    ColumnDef { name: "lanvalidator", coltype: ColType::Integer },
    ColumnDef { name: "lanacl", coltype: ColType::Text },
];
const COLS_PG_INDEX: &[ColumnDef] = &[
    ColumnDef { name: "indexrelid", coltype: ColType::Integer },
    ColumnDef { name: "indrelid", coltype: ColType::Integer },
    ColumnDef { name: "indisunique", coltype: ColType::Boolean },
    ColumnDef { name: "indisprimary", coltype: ColType::Boolean },
    // missing columns from reconciliation
    ColumnDef { name: "indnatts", coltype: ColType::Integer },
    ColumnDef { name: "indnkeyatts", coltype: ColType::Integer },
    ColumnDef { name: "indisexclusion", coltype: ColType::Boolean },
    ColumnDef { name: "indimmediate", coltype: ColType::Boolean },
    ColumnDef { name: "indisclustered", coltype: ColType::Boolean },
    ColumnDef { name: "indisvalid", coltype: ColType::Boolean },
    ColumnDef { name: "indcheckxmin", coltype: ColType::Boolean },
    ColumnDef { name: "indisready", coltype: ColType::Boolean },
    ColumnDef { name: "indislive", coltype: ColType::Boolean },
    ColumnDef { name: "indisreplident", coltype: ColType::Boolean },
    ColumnDef { name: "indkey", coltype: ColType::Text },
    ColumnDef { name: "indcollation", coltype: ColType::Text },
    ColumnDef { name: "indclass", coltype: ColType::Text },
    ColumnDef { name: "indoption", coltype: ColType::Text },
    ColumnDef { name: "indexprs", coltype: ColType::Text },
    ColumnDef { name: "indpred", coltype: ColType::Text },
];
const COLS_PG_INHERITS: &[ColumnDef] = &[
    ColumnDef { name: "inhrelid", coltype: ColType::Integer },
    ColumnDef { name: "inhparent", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "inhseqno", coltype: ColType::Integer },
    ColumnDef { name: "inhdetachpending", coltype: ColType::Boolean },
];
const COLS_PG_REWRITE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "ev_class", coltype: ColType::Integer },
    ColumnDef { name: "rulename", coltype: ColType::Text },
    ColumnDef { name: "ev_type", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "ev_enabled", coltype: ColType::Text },
    ColumnDef { name: "is_instead", coltype: ColType::Boolean },
    ColumnDef { name: "ev_qual", coltype: ColType::Text },
    ColumnDef { name: "ev_action", coltype: ColType::Text },
];
const COLS_PG_TRIGGER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "tgrelid", coltype: ColType::Integer },
    ColumnDef { name: "tgname", coltype: ColType::Text },
    ColumnDef { name: "tgenabled", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "tgparentid", coltype: ColType::Integer },
    ColumnDef { name: "tgfoid", coltype: ColType::Integer },
    ColumnDef { name: "tgtype", coltype: ColType::Integer },
    ColumnDef { name: "tgisinternal", coltype: ColType::Boolean },
    ColumnDef { name: "tgconstrrelid", coltype: ColType::Integer },
    ColumnDef { name: "tgconstrindid", coltype: ColType::Integer },
    ColumnDef { name: "tgconstraint", coltype: ColType::Integer },
    ColumnDef { name: "tgdeferrable", coltype: ColType::Boolean },
    ColumnDef { name: "tginitdeferred", coltype: ColType::Boolean },
    ColumnDef { name: "tgnargs", coltype: ColType::Integer },
    ColumnDef { name: "tgattr", coltype: ColType::Text },
    ColumnDef { name: "tgargs", coltype: ColType::Text },
    ColumnDef { name: "tgqual", coltype: ColType::Text },
    ColumnDef { name: "tgoldtable", coltype: ColType::Text },
    ColumnDef { name: "tgnewtable", coltype: ColType::Text },
];
const COLS_PG_TABLESPACE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "spcname", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "spcowner", coltype: ColType::Integer },
    ColumnDef { name: "spcacl", coltype: ColType::Text },
    ColumnDef { name: "spcoptions", coltype: ColType::Text },
];
const COLS_PG_CAST: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "castsource", coltype: ColType::Integer },
    ColumnDef { name: "casttarget", coltype: ColType::Integer },
    ColumnDef { name: "castfunc", coltype: ColType::Integer },
    ColumnDef { name: "castcontext", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "castmethod", coltype: ColType::Text },
];
const COLS_PG_ENUM: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "enumtypid", coltype: ColType::Integer },
    ColumnDef { name: "enumlabel", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "enumsortorder", coltype: ColType::Text },
];

// ---- New NoOp tables from reconciliation ----
const COLS_PG_PUBLICATION_REL: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "prpubid", coltype: ColType::Integer },
    ColumnDef { name: "prrelid", coltype: ColType::Integer },
];
const COLS_PG_DEFAULT_ACL: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "defaclrole", coltype: ColType::Integer },
    ColumnDef { name: "defaclnamespace", coltype: ColType::Integer },
    ColumnDef { name: "defaclobjtype", coltype: ColType::Text },
    ColumnDef { name: "defaclacl", coltype: ColType::Text },
];
const COLS_PG_PUBLICATION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "pubname", coltype: ColType::Text },
    ColumnDef { name: "pubowner", coltype: ColType::Integer },
    ColumnDef { name: "puballtables", coltype: ColType::Boolean },
    ColumnDef { name: "pubinsert", coltype: ColType::Boolean },
    ColumnDef { name: "pubupdate", coltype: ColType::Boolean },
    ColumnDef { name: "pubdelete", coltype: ColType::Boolean },
    ColumnDef { name: "pubtruncate", coltype: ColType::Boolean },
    ColumnDef { name: "pubviaroot", coltype: ColType::Boolean },
];
const COLS_PG_SEQUENCE: &[ColumnDef] = &[
    ColumnDef { name: "seqrelid", coltype: ColType::Integer },
    ColumnDef { name: "seqtypid", coltype: ColType::Integer },
    ColumnDef { name: "seqstart", coltype: ColType::BigInt },
    ColumnDef { name: "seqincrement", coltype: ColType::BigInt },
    ColumnDef { name: "seqmax", coltype: ColType::BigInt },
    ColumnDef { name: "seqmin", coltype: ColType::BigInt },
    ColumnDef { name: "seqcache", coltype: ColType::BigInt },
    ColumnDef { name: "seqcycle", coltype: ColType::Boolean },
];
const COLS_PG_POLICY: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "polname", coltype: ColType::Text },
    ColumnDef { name: "polrelid", coltype: ColType::Integer },
    ColumnDef { name: "polcmd", coltype: ColType::Text },
    ColumnDef { name: "polpermissive", coltype: ColType::Boolean },
    ColumnDef { name: "polroles", coltype: ColType::Text },
    ColumnDef { name: "polqual", coltype: ColType::Text },
    ColumnDef { name: "polwithcheck", coltype: ColType::Text },
];
const COLS_PG_SECLABEL: &[ColumnDef] = &[
    ColumnDef { name: "objoid", coltype: ColType::Integer },
    ColumnDef { name: "classoid", coltype: ColType::Integer },
    ColumnDef { name: "objsubid", coltype: ColType::Integer },
    ColumnDef { name: "provider", coltype: ColType::Text },
    ColumnDef { name: "label", coltype: ColType::Text },
];
const COLS_PG_LARGEOBJECT_METADATA: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "lomowner", coltype: ColType::Integer },
    ColumnDef { name: "lomacl", coltype: ColType::Text },
];
const COLS_PG_LARGEOBJECT: &[ColumnDef] = &[
    ColumnDef { name: "loid", coltype: ColType::Integer },
    ColumnDef { name: "pageno", coltype: ColType::Integer },
    ColumnDef { name: "data", coltype: ColType::Text },
];
const COLS_PG_DB_ROLE_SETTING: &[ColumnDef] = &[
    ColumnDef { name: "setdatabase", coltype: ColType::Integer },
    ColumnDef { name: "setrole", coltype: ColType::Integer },
    ColumnDef { name: "setconfig", coltype: ColType::Text },
];
const COLS_PG_TS_CONFIG_MAP: &[ColumnDef] = &[
    ColumnDef { name: "mapcfg", coltype: ColType::Integer },
    ColumnDef { name: "maptokentype", coltype: ColType::Integer },
    ColumnDef { name: "mapseqno", coltype: ColType::Integer },
    ColumnDef { name: "mapdict", coltype: ColType::Integer },
];
const COLS_PG_TRANSFORM: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "trftype", coltype: ColType::Integer },
    ColumnDef { name: "trflang", coltype: ColType::Integer },
    ColumnDef { name: "trffromsql", coltype: ColType::Integer },
    ColumnDef { name: "trftosql", coltype: ColType::Integer },
];
const COLS_PG_AUTHID: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "rolname", coltype: ColType::Text },
    ColumnDef { name: "rolsuper", coltype: ColType::Boolean },
    ColumnDef { name: "rolinherit", coltype: ColType::Boolean },
    ColumnDef { name: "rolcreaterole", coltype: ColType::Boolean },
    ColumnDef { name: "rolcreatedb", coltype: ColType::Boolean },
    ColumnDef { name: "rolcanlogin", coltype: ColType::Boolean },
    ColumnDef { name: "rolreplication", coltype: ColType::Boolean },
    ColumnDef { name: "rolbypassrls", coltype: ColType::Boolean },
    ColumnDef { name: "rolconnlimit", coltype: ColType::Integer },
    ColumnDef { name: "rolpassword", coltype: ColType::Text },
    ColumnDef { name: "rolvaliduntil", coltype: ColType::Text },
];
const COLS_PG_AUTH_MEMBERS: &[ColumnDef] = &[
    ColumnDef { name: "roleid", coltype: ColType::Integer },
    ColumnDef { name: "member", coltype: ColType::Integer },
    ColumnDef { name: "grantor", coltype: ColType::Integer },
    ColumnDef { name: "admin_option", coltype: ColType::Boolean },
];
const COLS_PG_STATISTIC_EXT: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "stxrelid", coltype: ColType::Integer },
    ColumnDef { name: "stxname", coltype: ColType::Text },
    ColumnDef { name: "stxnamespace", coltype: ColType::Integer },
    ColumnDef { name: "stxowner", coltype: ColType::Integer },
    ColumnDef { name: "stxstattarget", coltype: ColType::Integer },
    ColumnDef { name: "stxkeys", coltype: ColType::Text },
    ColumnDef { name: "stxkind", coltype: ColType::Text },
    ColumnDef { name: "stxexprs", coltype: ColType::Text },
];
const COLS_PG_STATISTIC_EXT_DATA: &[ColumnDef] = &[
    ColumnDef { name: "stxoid", coltype: ColType::Integer },
    ColumnDef { name: "stxdndistinct", coltype: ColType::Text },
    ColumnDef { name: "stxddependencies", coltype: ColType::Text },
    ColumnDef { name: "stxdmcv", coltype: ColType::Text },
    ColumnDef { name: "stxdexpr", coltype: ColType::Text },
];
const COLS_PG_STATISTIC: &[ColumnDef] = &[
    ColumnDef { name: "starelid", coltype: ColType::Integer },
    ColumnDef { name: "staattnum", coltype: ColType::Integer },
    ColumnDef { name: "stainherit", coltype: ColType::Boolean },
    ColumnDef { name: "stanullfrac", coltype: ColType::Text },
    ColumnDef { name: "stawidth", coltype: ColType::Integer },
    ColumnDef { name: "stadistinct", coltype: ColType::Text },
    ColumnDef { name: "stakind1", coltype: ColType::Integer },
    ColumnDef { name: "stakind2", coltype: ColType::Integer },
    ColumnDef { name: "stakind3", coltype: ColType::Integer },
    ColumnDef { name: "stakind4", coltype: ColType::Integer },
    ColumnDef { name: "stakind5", coltype: ColType::Integer },
    ColumnDef { name: "staop1", coltype: ColType::Integer },
    ColumnDef { name: "staop2", coltype: ColType::Integer },
    ColumnDef { name: "staop3", coltype: ColType::Integer },
    ColumnDef { name: "staop4", coltype: ColType::Integer },
    ColumnDef { name: "staop5", coltype: ColType::Integer },
    ColumnDef { name: "stacoll1", coltype: ColType::Integer },
    ColumnDef { name: "stacoll2", coltype: ColType::Integer },
    ColumnDef { name: "stacoll3", coltype: ColType::Integer },
    ColumnDef { name: "stacoll4", coltype: ColType::Integer },
    ColumnDef { name: "stacoll5", coltype: ColType::Integer },
    ColumnDef { name: "stanumbers1", coltype: ColType::Text },
    ColumnDef { name: "stanumbers2", coltype: ColType::Text },
    ColumnDef { name: "stanumbers3", coltype: ColType::Text },
    ColumnDef { name: "stanumbers4", coltype: ColType::Text },
    ColumnDef { name: "stanumbers5", coltype: ColType::Text },
    ColumnDef { name: "stavalues1", coltype: ColType::Text },
    ColumnDef { name: "stavalues2", coltype: ColType::Text },
    ColumnDef { name: "stavalues3", coltype: ColType::Text },
    ColumnDef { name: "stavalues4", coltype: ColType::Text },
    ColumnDef { name: "stavalues5", coltype: ColType::Text },
];
const COLS_PG_USER_MAPPING: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "umuser", coltype: ColType::Integer },
    ColumnDef { name: "umserver", coltype: ColType::Integer },
    ColumnDef { name: "umoptions", coltype: ColType::Text },
];
const COLS_PG_SHSECLABEL: &[ColumnDef] = &[
    ColumnDef { name: "objoid", coltype: ColType::Integer },
    ColumnDef { name: "classoid", coltype: ColType::Integer },
    ColumnDef { name: "provider", coltype: ColType::Text },
    ColumnDef { name: "label", coltype: ColType::Text },
];
const COLS_PG_INIT_PRIVS: &[ColumnDef] = &[
    ColumnDef { name: "objoid", coltype: ColType::Integer },
    ColumnDef { name: "classoid", coltype: ColType::Integer },
    ColumnDef { name: "objsubid", coltype: ColType::Integer },
    ColumnDef { name: "privtype", coltype: ColType::Text },
    ColumnDef { name: "initprivs", coltype: ColType::Text },
];
const COLS_PG_SHDEPEND: &[ColumnDef] = &[
    ColumnDef { name: "dbid", coltype: ColType::Integer },
    ColumnDef { name: "classid", coltype: ColType::Integer },
    ColumnDef { name: "objid", coltype: ColType::Integer },
    ColumnDef { name: "objsubid", coltype: ColType::Integer },
    ColumnDef { name: "refclassid", coltype: ColType::Integer },
    ColumnDef { name: "refobjid", coltype: ColType::Integer },
    ColumnDef { name: "deptype", coltype: ColType::Text },
];
const COLS_PG_PARTITIONED_TABLE: &[ColumnDef] = &[
    ColumnDef { name: "partrelid", coltype: ColType::Integer },
    ColumnDef { name: "partstrat", coltype: ColType::Text },
    ColumnDef { name: "partnatts", coltype: ColType::Integer },
    ColumnDef { name: "partdefid", coltype: ColType::Integer },
    ColumnDef { name: "partattrs", coltype: ColType::Text },
    ColumnDef { name: "partclass", coltype: ColType::Text },
    ColumnDef { name: "partcollation", coltype: ColType::Text },
    ColumnDef { name: "partexprs", coltype: ColType::Text },
];
const COLS_PG_SUBSCRIPTION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "subdbid", coltype: ColType::Integer },
    ColumnDef { name: "subname", coltype: ColType::Text },
    ColumnDef { name: "subowner", coltype: ColType::Integer },
    ColumnDef { name: "subenabled", coltype: ColType::Boolean },
    ColumnDef { name: "subbinary", coltype: ColType::Boolean },
    ColumnDef { name: "substream", coltype: ColType::Boolean },
    ColumnDef { name: "subconninfo", coltype: ColType::Text },
    ColumnDef { name: "subslotname", coltype: ColType::Text },
    ColumnDef { name: "subsynccommit", coltype: ColType::Text },
    ColumnDef { name: "subpublications", coltype: ColType::Text },
];
const COLS_PG_SUBSCRIPTION_REL: &[ColumnDef] = &[
    ColumnDef { name: "srsubid", coltype: ColType::Integer },
    ColumnDef { name: "srrelid", coltype: ColType::Integer },
    ColumnDef { name: "srsubstate", coltype: ColType::Text },
    ColumnDef { name: "srsublsn", coltype: ColType::Text },
];
const COLS_PG_REPLICATION_ORIGIN: &[ColumnDef] = &[
    ColumnDef { name: "roident", coltype: ColType::Integer },
    ColumnDef { name: "roname", coltype: ColType::Text },
];
const COLS_PG_EVENT_TRIGGER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "evtname", coltype: ColType::Text },
    ColumnDef { name: "evtevent", coltype: ColType::Text },
    ColumnDef { name: "evtowner", coltype: ColType::Integer },
    ColumnDef { name: "evtfoid", coltype: ColType::Integer },
    ColumnDef { name: "evtenabled", coltype: ColType::Text },
    ColumnDef { name: "evttags", coltype: ColType::Text },
];
const COLS_PG_RANGE: &[ColumnDef] = &[
    ColumnDef { name: "rngtypid", coltype: ColType::Integer },
    ColumnDef { name: "rngsubtype", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "rngmultitypid", coltype: ColType::Integer },
    ColumnDef { name: "rngcollation", coltype: ColType::Integer },
    ColumnDef { name: "rngsubopc", coltype: ColType::Integer },
    ColumnDef { name: "rngcanonical", coltype: ColType::Integer },
    ColumnDef { name: "rngsubdiff", coltype: ColType::Integer },
];
const COLS_PG_EXTENSION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "extname", coltype: ColType::Text },
    ColumnDef { name: "extnamespace", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "extowner", coltype: ColType::Integer },
    ColumnDef { name: "extrelocatable", coltype: ColType::Boolean },
    ColumnDef { name: "extversion", coltype: ColType::Text },
    ColumnDef { name: "extconfig", coltype: ColType::Text },
    ColumnDef { name: "extcondition", coltype: ColType::Text },
];
const COLS_PG_FOREIGN_DATA_WRAPPER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "fdwname", coltype: ColType::Text },
    // missing columns from reconciliation
    ColumnDef { name: "fdwowner", coltype: ColType::Integer },
    ColumnDef { name: "fdwhandler", coltype: ColType::Integer },
    ColumnDef { name: "fdwvalidator", coltype: ColType::Integer },
    ColumnDef { name: "fdwacl", coltype: ColType::Text },
    ColumnDef { name: "fdwoptions", coltype: ColType::Text },
];
const COLS_PG_FOREIGN_SERVER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "srvname", coltype: ColType::Text },
    ColumnDef { name: "srvfdw", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "srvowner", coltype: ColType::Integer },
    ColumnDef { name: "srvtype", coltype: ColType::Text },
    ColumnDef { name: "srvversion", coltype: ColType::Text },
    ColumnDef { name: "srvacl", coltype: ColType::Text },
    ColumnDef { name: "srvoptions", coltype: ColType::Text },
];
const COLS_PG_FOREIGN_TABLE: &[ColumnDef] = &[
    ColumnDef { name: "ftrelid", coltype: ColType::Integer },
    ColumnDef { name: "ftserver", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "ftoptions", coltype: ColType::Text },
];
const COLS_PG_TS_CONFIG: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "cfgname", coltype: ColType::Text },
    ColumnDef { name: "cfgnamespace", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "cfgowner", coltype: ColType::Integer },
    ColumnDef { name: "cfgparser", coltype: ColType::Integer },
];
const COLS_PG_TS_DICT: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "dictname", coltype: ColType::Text },
    ColumnDef { name: "dictnamespace", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "dictowner", coltype: ColType::Integer },
    ColumnDef { name: "dicttemplate", coltype: ColType::Integer },
    ColumnDef { name: "dictinitoption", coltype: ColType::Text },
];
const COLS_PG_TS_PARSER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "prsname", coltype: ColType::Text },
    ColumnDef { name: "prsnamespace", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "prsstart", coltype: ColType::Integer },
    ColumnDef { name: "prstoken", coltype: ColType::Integer },
    ColumnDef { name: "prsend", coltype: ColType::Integer },
    ColumnDef { name: "prsheadline", coltype: ColType::Integer },
    ColumnDef { name: "prslextype", coltype: ColType::Integer },
];
const COLS_PG_TS_TEMPLATE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "tmplname", coltype: ColType::Text },
    ColumnDef { name: "tmplnamespace", coltype: ColType::Integer },
    // missing columns from reconciliation
    ColumnDef { name: "tmplinit", coltype: ColType::Integer },
    ColumnDef { name: "tmpllexize", coltype: ColType::Integer },
];

pub fn register_defaults() {
    // Register concrete implementations first, so they take precedence
    pg_namespace::register();
    pg_database::register();
    pg_roles::register();
    pg_authid::register();
    pg_auth_members::register();
    pg_attribute::register();
    pg_attrdef::register();
    pg_class::register();
    pg_type::register();
    pg_proc::register();
    pg_aggregate::register();
    pg_constraint::register();
    pg_constraint_columns::register();
    pg_views::register();

    // Register NoOp system tables for pg_catalog coverage
    let regs: &[(&str, &[ColumnDef])] = &[
        ("pg_am", COLS_PG_AM),
        ("pg_amop", COLS_PG_AMOP),
        ("pg_amproc", COLS_PG_AMPROC),
        ("pg_operator", COLS_PG_OPERATOR),
        ("pg_opclass", COLS_PG_OPCLASS),
        ("pg_opfamily", COLS_PG_OPFAMILY),
        ("pg_collation", COLS_PG_COLLATION),
        ("pg_conversion", COLS_PG_CONVERSION),
        ("pg_language", COLS_PG_LANGUAGE),
        ("pg_index", COLS_PG_INDEX),
        ("pg_inherits", COLS_PG_INHERITS),
        ("pg_rewrite", COLS_PG_REWRITE),
        ("pg_trigger", COLS_PG_TRIGGER),
        ("pg_tablespace", COLS_PG_TABLESPACE),
        ("pg_cast", COLS_PG_CAST),
        ("pg_enum", COLS_PG_ENUM),
        ("pg_range", COLS_PG_RANGE),
        ("pg_extension", COLS_PG_EXTENSION),
        ("pg_foreign_data_wrapper", COLS_PG_FOREIGN_DATA_WRAPPER),
        ("pg_foreign_server", COLS_PG_FOREIGN_SERVER),
        ("pg_foreign_table", COLS_PG_FOREIGN_TABLE),
        ("pg_ts_config", COLS_PG_TS_CONFIG),
        ("pg_ts_dict", COLS_PG_TS_DICT),
        ("pg_ts_parser", COLS_PG_TS_PARSER),
        ("pg_ts_template", COLS_PG_TS_TEMPLATE),
        // Newly covered as NoOp to replace legacy builders
        ("pg_description", COLS_PG_DESCRIPTION),
        ("pg_depend", COLS_PG_DEPEND),
        ("pg_shdescription", COLS_PG_SHDESCRIPTION),
        // New NoOp tables from reconciliation
        ("pg_publication_rel", COLS_PG_PUBLICATION_REL),
        ("pg_default_acl", COLS_PG_DEFAULT_ACL),
        ("pg_publication", COLS_PG_PUBLICATION),
        ("pg_sequence", COLS_PG_SEQUENCE),
        ("pg_policy", COLS_PG_POLICY),
        ("pg_seclabel", COLS_PG_SECLABEL),
        ("pg_largeobject_metadata", COLS_PG_LARGEOBJECT_METADATA),
        ("pg_largeobject", COLS_PG_LARGEOBJECT),
        ("pg_db_role_setting", COLS_PG_DB_ROLE_SETTING),
        ("pg_ts_config_map", COLS_PG_TS_CONFIG_MAP),
        ("pg_transform", COLS_PG_TRANSFORM),
        ("pg_statistic_ext", COLS_PG_STATISTIC_EXT),
        ("pg_statistic_ext_data", COLS_PG_STATISTIC_EXT_DATA),
        ("pg_statistic", COLS_PG_STATISTIC),
        ("pg_user_mapping", COLS_PG_USER_MAPPING),
        ("pg_shseclabel", COLS_PG_SHSECLABEL),
        ("pg_init_privs", COLS_PG_INIT_PRIVS),
        ("pg_shdepend", COLS_PG_SHDEPEND),
        ("pg_partitioned_table", COLS_PG_PARTITIONED_TABLE),
        ("pg_subscription", COLS_PG_SUBSCRIPTION),
        ("pg_subscription_rel", COLS_PG_SUBSCRIPTION_REL),
        ("pg_replication_origin", COLS_PG_REPLICATION_ORIGIN),
        ("pg_event_trigger", COLS_PG_EVENT_TRIGGER),
    ];

    for (name, cols) in regs.iter() {
        reg::register(Box::new(NoOpSystemTable::new("pg_catalog", name, cols)));
    }
}

// ---- Additional column definitions for newly registered NoOp tables ----
const COLS_PG_DESCRIPTION: &[ColumnDef] = &[
    ColumnDef { name: "objoid", coltype: ColType::Integer },
    ColumnDef { name: "classoid", coltype: ColType::Integer },
    ColumnDef { name: "objsubid", coltype: ColType::Integer },
    ColumnDef { name: "description", coltype: ColType::Text },
];
const COLS_PG_DEPEND: &[ColumnDef] = &[
    ColumnDef { name: "classid", coltype: ColType::Integer },
    ColumnDef { name: "objid", coltype: ColType::Integer },
    ColumnDef { name: "objsubid", coltype: ColType::Integer },
    ColumnDef { name: "refclassid", coltype: ColType::Integer },
    ColumnDef { name: "refobjid", coltype: ColType::Integer },
    ColumnDef { name: "refobjsubid", coltype: ColType::Integer },
    ColumnDef { name: "deptype", coltype: ColType::Text },
];
const COLS_PG_SHDESCRIPTION: &[ColumnDef] = &[
    ColumnDef { name: "objoid", coltype: ColType::Integer },
    ColumnDef { name: "classoid", coltype: ColType::Integer },
    ColumnDef { name: "description", coltype: ColType::Text },
];

use crate::system_catalog::registry::{self as reg, ColumnDef, ColType, NoOpSystemTable};

pub mod pg_namespace;
pub mod pg_database;
pub mod pg_roles;
pub mod pg_authid;
pub mod pg_auth_members;
pub mod role_common;
pub mod pg_attribute;
pub mod pg_attrdef;
pub mod pg_type;
pub mod pg_proc;
pub mod pg_aggregate;
pub mod pg_class;
pub mod pg_constraint;
pub mod pg_constraint_columns;
pub mod pg_views;