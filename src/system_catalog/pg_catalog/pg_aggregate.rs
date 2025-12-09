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
        ]).ok()
    }
}

pub fn register() { registry::register(Box::new(PgAggregate)); }
