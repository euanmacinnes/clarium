use polars::prelude::AnyValue;
use anyhow::Result;
use crate::pgwire_server::{send::send_ready_with_status, structs::ConnState};

#[inline]
pub fn is_array_oid(oid: i32) -> bool {
    matches!(oid, 1000|1005|1007|1016|1021|1022|1009|1001|1182|1115|1185|1183)
}

#[inline]
pub fn array_elem_oid(array_oid: i32) -> i32 {
    match array_oid {
        1000 => 16,   // bool[] -> bool
        1005 => 21,   // int2[] -> int2
        1007 => 23,   // int4[] -> int4
        1016 => 20,   // int8[] -> int8
        1021 => 700,  // float4[] -> float4
        1022 => 701,  // float8[] -> float8
        1009 => 25,   // text[] -> text
        1001 => 17,   // bytea[] -> bytea
        1182 => 1082, // date[] -> date
        1115 => 1114, // timestamp[] -> timestamp
        1185 => 1184, // timestamptz[] -> timestamptz
        1183 => 1083, // time[] -> time
        _ => 25,
    }
}

#[inline]
pub fn anyvalue_to_opt_string(av: &AnyValue) -> Option<String> {
    match av {
        AnyValue::Null => None,
        AnyValue::String(s) => Some(s.to_string()),
        AnyValue::StringOwned(s) => Some(s.to_string()),
        AnyValue::Int8(v) => Some(v.to_string()),
        AnyValue::Int16(v) => Some(v.to_string()),
        AnyValue::Int32(v) => Some(v.to_string()),
        AnyValue::Int64(v) => Some(v.to_string()),
        AnyValue::UInt8(v) => Some(v.to_string()),
        AnyValue::UInt16(v) => Some(v.to_string()),
        AnyValue::UInt32(v) => Some(v.to_string()),
        AnyValue::UInt64(v) => Some(v.to_string()),
        AnyValue::Float32(v) => Some(v.to_string()),
        AnyValue::Float64(v) => Some(v.to_string()),
        AnyValue::Boolean(v) => Some(v.to_string()),
        other => Some(format!("{}", other)),
    }
}

#[inline]
pub async fn send_ready(socket: &mut tokio::net::TcpStream, state: &ConnState) -> Result<()> {
    let status = if state.in_tx {
        if state.in_error { b'E' } else { b'T' }
    } else { b'I' };
    send_ready_with_status(socket, status).await
}





