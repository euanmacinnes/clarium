// The pg_aggregate table is a PostgreSQL system catalog that stores metadata about aggregate functions available in the database, such as SUM, COUNT, and MAX.
// Key Details

// Purpose: The primary function of pg_aggregate is to store the internal details necessary for the database engine to execute aggregate functions.
// Relationship to pg_proc: Each entry in pg_aggregate is an extension of an entry in the pg_proc system catalog, which stores general information about all functions (or procedures). The pg_proc entry carries the aggregate's name, input/output data types, and other general function information.
// Internal Mechanics: The table contains crucial information for how an aggregate function works, including:
// Transition Function (aggtransfn): The function used to update the internal "state" as it processes each input row.
// Final Function (aggfinalfn): An optional function that is called once after all rows have been processed to compute the final result from the internal state (e.g., calculating the average from the sum and count).
// Initial Value (agginitval): The starting value for the transition state.
// Transition State Type (aggtranstype): The data type of the aggregate function's internal state.

use polars::prelude::{DataFrame, Series, NamedFrom};
use crate::system_catalog::registry::{SystemTable, ColumnDef, ColType};
use crate::system_catalog::registry;
use crate::storage::SharedStore;

pub struct PgAggregate;

const COLS: &[ColumnDef] = &[
    ColumnDef { name: "aggfnoid", coltype: ColType::Integer },
    ColumnDef { name: "aggkind", coltype: ColType::Text },
    ColumnDef { name: "aggsortop", coltype: ColType::Integer },
    ColumnDef { name: "aggtranstype", coltype: ColType::Integer },
    ColumnDef { name: "agginitval", coltype: ColType::Text },
    // added per reconciliation
    ColumnDef { name: "aggnumdirectargs", coltype: ColType::Integer },
    ColumnDef { name: "aggtransfn", coltype: ColType::Integer },
    ColumnDef { name: "aggfinalfn", coltype: ColType::Integer },
    ColumnDef { name: "aggcombinefn", coltype: ColType::Integer },
    ColumnDef { name: "aggserialfn", coltype: ColType::Integer },
    ColumnDef { name: "aggdeserialfn", coltype: ColType::Integer },
    ColumnDef { name: "aggmtransfn", coltype: ColType::Integer },
    ColumnDef { name: "aggminvtransfn", coltype: ColType::Integer },
    ColumnDef { name: "aggmfinalfn", coltype: ColType::Integer },
    ColumnDef { name: "aggfinalextra", coltype: ColType::Boolean },
    ColumnDef { name: "aggmfinalextra", coltype: ColType::Boolean },
    ColumnDef { name: "aggfinalmodify", coltype: ColType::Text },
    ColumnDef { name: "aggmfinalmodify", coltype: ColType::Text },
    ColumnDef { name: "aggtransspace", coltype: ColType::Integer },
    ColumnDef { name: "aggmtranstype", coltype: ColType::Integer },
    ColumnDef { name: "aggmtransspace", coltype: ColType::Integer },
    ColumnDef { name: "aggminitval", coltype: ColType::Text },
];

impl SystemTable for PgAggregate {
    fn schema(&self) -> &'static str { "pg_catalog" }
    fn name(&self) -> &'static str { "pg_aggregate" }
    fn columns(&self) -> &'static [ColumnDef] { COLS }
    fn build(&self, _store: &SharedStore) -> Option<DataFrame> {
        DataFrame::new(vec![
            Series::new("aggfnoid".into(), Vec::<i32>::new()).into(),
            Series::new("aggkind".into(), Vec::<String>::new()).into(),
            Series::new("aggsortop".into(), Vec::<i32>::new()).into(),
            Series::new("aggtranstype".into(), Vec::<i32>::new()).into(),
            Series::new("agginitval".into(), Vec::<String>::new()).into(),
            Series::new("aggnumdirectargs".into(), Vec::<i32>::new()).into(),
            Series::new("aggtransfn".into(), Vec::<i32>::new()).into(),
            Series::new("aggfinalfn".into(), Vec::<i32>::new()).into(),
            Series::new("aggcombinefn".into(), Vec::<i32>::new()).into(),
            Series::new("aggserialfn".into(), Vec::<i32>::new()).into(),
            Series::new("aggdeserialfn".into(), Vec::<i32>::new()).into(),
            Series::new("aggmtransfn".into(), Vec::<i32>::new()).into(),
            Series::new("aggminvtransfn".into(), Vec::<i32>::new()).into(),
            Series::new("aggmfinalfn".into(), Vec::<i32>::new()).into(),
            Series::new("aggfinalextra".into(), Vec::<bool>::new()).into(),
            Series::new("aggmfinalextra".into(), Vec::<bool>::new()).into(),
            Series::new("aggfinalmodify".into(), Vec::<String>::new()).into(),
            Series::new("aggmfinalmodify".into(), Vec::<String>::new()).into(),
            Series::new("aggtransspace".into(), Vec::<i32>::new()).into(),
            Series::new("aggmtranstype".into(), Vec::<i32>::new()).into(),
            Series::new("aggmtransspace".into(), Vec::<i32>::new()).into(),
            Series::new("aggminitval".into(), Vec::<String>::new()).into(),
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgAggregate)); }
