use polars::prelude::DataType;
use crate::pgwire_server::misc::PG_TYPE_TEXT;

pub fn map_polars_dtype_to_pg_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean => 16,
        DataType::Int8 | DataType::Int16 => 21,
        DataType::Int32 => 23,
        DataType::Int64 => 20,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::Binary => 17, // bytea
        DataType::String | DataType::Categorical(_, _) => 25,
        DataType::Date => 1082, // date
        DataType::Datetime(_, tz) => if tz.is_some() { 1184 } else { 1114 }, // timestamptz or timestamp
        DataType::Time => 1083, // time without tz
        DataType::Duration(_) => 1186, // interval
        DataType::Decimal(_, _) => 1700, // numeric/decimal
        DataType::List(inner) => map_pg_array_oid(inner),
        DataType::Struct(_) => 2249, // record
        _ => PG_TYPE_TEXT,
    }
}

pub fn map_pg_array_oid(inner: &DataType) -> i32 {
    match inner.as_ref() {
        DataType::Boolean => 1000, // bool[]
        DataType::Int16 | DataType::Int8 => 1005, // int2[]
        DataType::Int32 => 1007, // int4[]
        DataType::Int64 | DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => 1016, // int8[]
        DataType::Float32 => 1021, // float4[]
        DataType::Float64 => 1022, // float8[]
        DataType::String | DataType::Categorical(_, _) => 1009, // text[]
        DataType::Binary => 1001, // bytea[]
        DataType::Date => 1182, // date[]
        DataType::Datetime(_, tz) => if tz.is_some() { 1185 } else { 1115 }, // timestamptz[] or timestamp[]
        DataType::Time => 1183, // time[]
        DataType::Decimal(_, _) => 1231, // numeric[]
        _ => 1009,
    }
}

pub fn infer_literal_oid_from_value(s: &str) -> i32 {
    // Very small heuristic for constant SELECTs in Describe
    if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false") { return 16; }
    if s.parse::<i32>().is_ok() { return 23; }
    if s.parse::<i64>().is_ok() { return 20; }
    if s.parse::<f64>().is_ok() { return 701; }
    25
}