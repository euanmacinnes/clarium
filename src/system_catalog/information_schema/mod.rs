pub mod schemata;
pub mod tables;
pub mod columns;
pub mod views;

use crate::system_catalog::registry::{self as reg, ColumnDef, ColType, NoOpSystemTable};

// NoOp tables to cover missing information_schema entries referenced in reconciliation
const COLS_SQL_FEATURES: &[ColumnDef] = &[
    ColumnDef { name: "feature_id", coltype: ColType::Text },
    ColumnDef { name: "feature_name", coltype: ColType::Text },
    ColumnDef { name: "sub_feature_id", coltype: ColType::Text },
    ColumnDef { name: "sub_feature_name", coltype: ColType::Text },
    ColumnDef { name: "is_supported", coltype: ColType::Text },
    ColumnDef { name: "is_verified_by", coltype: ColType::Text },
    ColumnDef { name: "comments", coltype: ColType::Text },
];

const COLS_SQL_IMPLEMENTATION_INFO: &[ColumnDef] = &[
    ColumnDef { name: "implementation_info_id", coltype: ColType::Text },
    ColumnDef { name: "implementation_info_name", coltype: ColType::Text },
    ColumnDef { name: "integer_value", coltype: ColType::Integer },
    ColumnDef { name: "character_value", coltype: ColType::Text },
    ColumnDef { name: "comments", coltype: ColType::Text },
];

const COLS_SQL_PARTS: &[ColumnDef] = &[
    ColumnDef { name: "feature_id", coltype: ColType::Text },
    ColumnDef { name: "feature_name", coltype: ColType::Text },
    ColumnDef { name: "is_supported", coltype: ColType::Text },
    ColumnDef { name: "is_verified_by", coltype: ColType::Text },
    ColumnDef { name: "comments", coltype: ColType::Text },
];

const COLS_SQL_SIZING: &[ColumnDef] = &[
    ColumnDef { name: "sizing_id", coltype: ColType::Integer },
    ColumnDef { name: "sizing_name", coltype: ColType::Text },
    ColumnDef { name: "supported_value", coltype: ColType::Integer },
    ColumnDef { name: "comments", coltype: ColType::Text },
];

pub fn register_defaults() {
    schemata::register();
    tables::register();
    columns::register();
    views::register();

    // Register NoOp information_schema tables
    let regs: &[(&str, &[ColumnDef])] = &[
        ("sql_features", COLS_SQL_FEATURES),
        ("sql_implementation_info", COLS_SQL_IMPLEMENTATION_INFO),
        ("sql_parts", COLS_SQL_PARTS),
        ("sql_sizing", COLS_SQL_SIZING),
    ];
    for (name, cols) in regs.iter() {
        reg::register(Box::new(NoOpSystemTable::new("information_schema", name, cols)));
    }
}
