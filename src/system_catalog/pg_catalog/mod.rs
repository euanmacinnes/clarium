
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
];
const COLS_PG_OPCLASS: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "opcname", coltype: ColType::Text },
    ColumnDef { name: "opcnamespace", coltype: ColType::Integer },
    ColumnDef { name: "opcmethod", coltype: ColType::Integer },
    ColumnDef { name: "opcintype", coltype: ColType::Integer },
    ColumnDef { name: "opckeytype", coltype: ColType::Integer },
    ColumnDef { name: "opcdefault", coltype: ColType::Text },
];
const COLS_PG_OPFAMILY: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "opfname", coltype: ColType::Text },
    ColumnDef { name: "opfnamespace", coltype: ColType::Integer },
    ColumnDef { name: "opfmethod", coltype: ColType::Integer },
];
const COLS_PG_COLLATION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "collname", coltype: ColType::Text },
    ColumnDef { name: "collnamespace", coltype: ColType::Integer },
];
const COLS_PG_CONVERSION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "conname", coltype: ColType::Text },
    ColumnDef { name: "connamespace", coltype: ColType::Integer },
];
const COLS_PG_LANGUAGE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "lanname", coltype: ColType::Text },
];
const COLS_PG_INDEX: &[ColumnDef] = &[
    ColumnDef { name: "indexrelid", coltype: ColType::Integer },
    ColumnDef { name: "indrelid", coltype: ColType::Integer },
    ColumnDef { name: "indisunique", coltype: ColType::Boolean },
    ColumnDef { name: "indisprimary", coltype: ColType::Boolean },
];
const COLS_PG_INHERITS: &[ColumnDef] = &[
    ColumnDef { name: "inhrelid", coltype: ColType::Integer },
    ColumnDef { name: "inhparent", coltype: ColType::Integer },
];
const COLS_PG_REWRITE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "ev_class", coltype: ColType::Integer },
    ColumnDef { name: "rulename", coltype: ColType::Text },
    ColumnDef { name: "ev_type", coltype: ColType::Text },
];
const COLS_PG_TRIGGER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "tgrelid", coltype: ColType::Integer },
    ColumnDef { name: "tgname", coltype: ColType::Text },
    ColumnDef { name: "tgenabled", coltype: ColType::Text },
];
const COLS_PG_TABLESPACE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "spcname", coltype: ColType::Text },
];
const COLS_PG_CAST: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "castsource", coltype: ColType::Integer },
    ColumnDef { name: "casttarget", coltype: ColType::Integer },
    ColumnDef { name: "castfunc", coltype: ColType::Integer },
    ColumnDef { name: "castcontext", coltype: ColType::Text },
];
const COLS_PG_ENUM: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "enumtypid", coltype: ColType::Integer },
    ColumnDef { name: "enumlabel", coltype: ColType::Text },
];
const COLS_PG_RANGE: &[ColumnDef] = &[
    ColumnDef { name: "rngtypid", coltype: ColType::Integer },
    ColumnDef { name: "rngsubtype", coltype: ColType::Integer },
];
const COLS_PG_EXTENSION: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "extname", coltype: ColType::Text },
    ColumnDef { name: "extnamespace", coltype: ColType::Integer },
];
const COLS_PG_FOREIGN_DATA_WRAPPER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "fdwname", coltype: ColType::Text },
];
const COLS_PG_FOREIGN_SERVER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "srvname", coltype: ColType::Text },
    ColumnDef { name: "srvfdw", coltype: ColType::Integer },
];
const COLS_PG_FOREIGN_TABLE: &[ColumnDef] = &[
    ColumnDef { name: "ftrelid", coltype: ColType::Integer },
    ColumnDef { name: "ftserver", coltype: ColType::Integer },
];
const COLS_PG_TS_CONFIG: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "cfgname", coltype: ColType::Text },
    ColumnDef { name: "cfgnamespace", coltype: ColType::Integer },
];
const COLS_PG_TS_DICT: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "dictname", coltype: ColType::Text },
    ColumnDef { name: "dictnamespace", coltype: ColType::Integer },
];
const COLS_PG_TS_PARSER: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "prsname", coltype: ColType::Text },
    ColumnDef { name: "prsnamespace", coltype: ColType::Integer },
];
const COLS_PG_TS_TEMPLATE: &[ColumnDef] = &[
    ColumnDef { name: "oid", coltype: ColType::Integer },
    ColumnDef { name: "tmplname", coltype: ColType::Text },
    ColumnDef { name: "tmplnamespace", coltype: ColType::Integer },
];

pub fn register_defaults() {
    // Register concrete implementations first, so they take precedence
    pg_namespace::register();
    pg_database::register();
    pg_roles::register();
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
    ColumnDef { name: "refobjid", coltype: ColType::Integer },
    ColumnDef { name: "refobjsubid", coltype: ColType::Integer },
    ColumnDef { name: "classid", coltype: ColType::Integer },
    ColumnDef { name: "refclassid", coltype: ColType::Integer },
    ColumnDef { name: "objid", coltype: ColType::Integer },
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
pub mod pg_attribute;
pub mod pg_attrdef;
pub mod pg_type;
pub mod pg_proc;
pub mod pg_aggregate;
pub mod pg_class;
pub mod pg_constraint;
pub mod pg_constraint_columns;
pub mod pg_views;