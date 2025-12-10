use polars::prelude::AnyValue;
use polars::datatypes::TimeUnit;

use crate::pgwire_server::inline::*;
use crate::pgwire_server::oids::*;

pub fn encode_element_binary(buf: &mut Vec<u8>, inner_oid: i32, av: &AnyValue<'_>) {
    match (inner_oid, av) {
        (16, AnyValue::Boolean(b)) => { buf.extend_from_slice(&1i32.to_be_bytes()); buf.push(if *b {1} else {0}); }
        (21, AnyValue::Int16(v)) => { buf.extend_from_slice(&2i32.to_be_bytes()); buf.extend_from_slice(&v.to_be_bytes()); }
        (23, AnyValue::Int32(v)) => { buf.extend_from_slice(&4i32.to_be_bytes()); buf.extend_from_slice(&v.to_be_bytes()); }
        (20, AnyValue::Int64(v)) => { buf.extend_from_slice(&8i32.to_be_bytes()); buf.extend_from_slice(&v.to_be_bytes()); }
        (700, AnyValue::Float32(f)) => { let bits=f.to_bits(); buf.extend_from_slice(&4i32.to_be_bytes()); buf.extend_from_slice(&bits.to_be_bytes()); }
        (701, AnyValue::Float64(f)) => { let bits=f.to_bits(); buf.extend_from_slice(&8i32.to_be_bytes()); buf.extend_from_slice(&bits.to_be_bytes()); }
        (17, AnyValue::Binary(b)) => { buf.extend_from_slice(&(b.len() as i32).to_be_bytes()); buf.extend_from_slice(b); }
        (25, _) => { let s = format!("{}", av); let bytes=s.as_bytes(); buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes()); buf.extend_from_slice(bytes); }
        (1082, AnyValue::Date(days_since_unix)) => {
            const DAYS_BETWEEN_UNIX_AND_PG_EPOCH: i32 = 10957;
            let pg_days: i32 = days_since_unix - DAYS_BETWEEN_UNIX_AND_PG_EPOCH;
            buf.extend_from_slice(&4i32.to_be_bytes()); buf.extend_from_slice(&pg_days.to_be_bytes());
        }
        (1083, AnyValue::Time(nanos_since_midnight)) => {
            let micros: i64 = nanos_since_midnight / 1000; buf.extend_from_slice(&8i32.to_be_bytes()); buf.extend_from_slice(&micros.to_be_bytes());
        }
        (1114, AnyValue::Datetime(val, unit, _)) | (1184, AnyValue::Datetime(val, unit, _)) => {
            let micros_since_unix: i64 = match unit { TimeUnit::Nanoseconds => (*val)/1000, TimeUnit::Microseconds => *val, TimeUnit::Milliseconds => (*val)*1000 };
            const MICROS_BETWEEN_UNIX_AND_PG_EPOCH: i64 = 946_684_800_i64 * 1_000_000;
            let pg_micros: i64 = micros_since_unix - MICROS_BETWEEN_UNIX_AND_PG_EPOCH; buf.extend_from_slice(&8i32.to_be_bytes()); buf.extend_from_slice(&pg_micros.to_be_bytes());
        }
        // Fallback: encode as text
        _ => { let s = format!("{}", av); let bytes=s.as_bytes(); buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes()); buf.extend_from_slice(bytes); }
    }
}



// Encode a decimal string into PostgreSQL NUMERIC binary format.
// Format: int16 ndigits, int16 weight, int16 sign, int16 dscale, then ndigits * int16 base-10000 digits.
// Returns None if parsing fails.
pub fn encode_pg_numeric_from_str(s: &str) -> Option<Vec<u8>> {
    let st = s.trim();
    if st.eq_ignore_ascii_case("nan") { return Some({
        let mut v = Vec::new();
        v.extend_from_slice(&0i16.to_be_bytes()); // ndigits
        v.extend_from_slice(&0i16.to_be_bytes()); // weight
        v.extend_from_slice(&(0xC000u16 as i16).to_be_bytes()); // sign = NaN
        v.extend_from_slice(&0i16.to_be_bytes()); // dscale
        v
    }); }
    let mut sign = 0i16; // 0=positive, 0x4000=negative
    let mut p = st;
    if let Some(stripped) = p.strip_prefix('-') { sign = 0x4000u16 as i16; p = stripped; } else if let Some(stripped) = p.strip_prefix('+') { p = stripped; }
    // Split into integer and fraction
    let parts: Vec<&str> = p.split('.').collect();
    if parts.len() > 2 { return None; }
    let int_part = parts.get(0).copied().unwrap_or("");
    let frac_part = if parts.len() == 2 { parts[1] } else { "" };
    if int_part.is_empty() && frac_part.is_empty() { return None; }
    // Remove leading zeros in integer and trailing zeros in fraction for compactness (scale preserved)
    let int_part_trim = int_part.trim_start_matches('0');
    let frac_part_trim = frac_part.trim_end_matches('0');
    let dscale: i16 = frac_part.len() as i16; // decimal digits after decimal point as provided
    // Prepare a contiguous digit string without the decimal point
    let mut digits = String::new();
    digits.push_str(if int_part_trim.is_empty() { "0" } else { int_part_trim });
    digits.push_str(frac_part_trim);
    // Validate digits
    if !digits.chars().all(|c| c.is_ascii_digit()) { return None; }
    // Compute weight: index of the first base-10000 group left of decimal
    // Weight is the number of base-10000 digits before the decimal - 1
    let int_len = if int_part_trim.is_empty() { 1 } else { int_part_trim.len() } as i32;
    let leading_group_digits = (int_len as i32 + 3) / 4; // ceil(int_len/4)
    let mut weight: i16 = (leading_group_digits - 1) as i16;
    // Left-pad digits to multiple of 4
    let rem = digits.len() % 4;
    let mut padded = String::new();
    if rem != 0 { padded.push_str(&"0".repeat(4 - rem)); }
    padded.push_str(&digits);
    // Build base-10000 digit array
    let mut base_digits: Vec<i16> = Vec::new();
    for chunk in padded.as_bytes().chunks(4) {
        let val = ((chunk[0]-b'0') as i32)*1000 + ((chunk[1]-b'0') as i32)*100 + ((chunk[2]-b'0') as i32)*10 + ((chunk[3]-b'0') as i32);
        base_digits.push(val as i16);
    }
    // If overall is zero, normalize sign/weight/scale
    let is_zero = base_digits.iter().all(|&d| d==0);
    if is_zero { sign = 0; weight = 0; }
    // Compose binary
    let mut out = Vec::new();
    out.extend_from_slice(&(base_digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for d in base_digits { out.extend_from_slice(&d.to_be_bytes()); }
    Some(out)
}

// Decode PostgreSQL NUMERIC binary into a canonical decimal string.
// Returns None if bytes malformed.
pub fn decode_pg_numeric_to_string(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 8 { return None; }
    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as i32;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]) as i32;
    let sign = i16::from_be_bytes([bytes[4], bytes[5]]) as i32;
    let dscale = i16::from_be_bytes([bytes[6], bytes[7]]) as i32;
    if sign == 0xC000 { return Some("NaN".to_string()); }
    if ndigits < 0 { return None; }
    if bytes.len() < 8 + (ndigits as usize) * 2 { return None; }
    let mut groups: Vec<i32> = Vec::with_capacity(ndigits as usize);
    let mut off = 8usize;
    for _ in 0..ndigits {
        let g = i16::from_be_bytes([bytes[off], bytes[off+1]]) as i32;
        groups.push(g);
        off += 2;
    }
    // Build contiguous decimal digits from base-10000 groups
    let mut digits = String::new();
    for (i, g) in groups.iter().enumerate() {
        if i == 0 { digits.push_str(&format!("{}", g)); }
        else { digits.push_str(&format!("{:04}", g)); }
    }
    if digits.is_empty() { digits.push('0'); }
    // Position of decimal point in digits string
    let dp = (weight + 1) * 4;
    let mut out = String::new();
    if sign == 0x4000 { out.push('-'); }
    if dp <= 0 {
        out.push_str("0.");
        for _ in 0..(-dp) { out.push('0'); }
        out.push_str(&digits);
    } else if (dp as usize) >= digits.len() {
        out.push_str(&digits);
        for _ in 0..(dp as usize - digits.len()) { out.push('0'); }
        if dscale > 0 {
            out.push('.');
            for _ in 0..dscale { out.push('0'); }
        }
    } else {
        let (intp, fracp) = digits.split_at(dp as usize);
        out.push_str(intp);
        out.push('.');
        out.push_str(fracp);
        // Ensure at least dscale fractional digits by padding zeros
        let cur_scale = fracp.len() as i32;
        if dscale > cur_scale {
            for _ in 0..(dscale - cur_scale) { out.push('0'); }
        }
    }
    Some(out)
}

// Decode a 1-D PostgreSQL array binary into a Postgres array literal string "{...}".
pub fn decode_pg_array_to_literal(bytes: &[u8], array_oid: i32) -> Option<String> {
    if bytes.len() < 12 { return None; }
    let ndims = i32::from_be_bytes([bytes[0],bytes[1],bytes[2],bytes[3]]);
    let _hasnull = i32::from_be_bytes([bytes[4],bytes[5],bytes[6],bytes[7]]);
    let elemtype = i32::from_be_bytes([bytes[8],bytes[9],bytes[10],bytes[11]]);
    if ndims != 1 { return None; }
    if bytes.len() < 20 { return None; }
    let len = i32::from_be_bytes([bytes[12],bytes[13],bytes[14],bytes[15]]) as usize;
    let _lbound = i32::from_be_bytes([bytes[16],bytes[17],bytes[18],bytes[19]]);
    let mut pos = 20usize;
    let elem_oid = if is_array_oid(array_oid) { array_elem_oid(array_oid) } else { elemtype };
    let mut elems: Vec<String> = Vec::with_capacity(len);
    for _ in 0..len {
        if pos + 4 > bytes.len() { return None; }
        let l = i32::from_be_bytes([bytes[pos],bytes[pos+1],bytes[pos+2],bytes[pos+3]]);
        pos += 4;
        if l < 0 { elems.push("NULL".to_string()); continue; }
        let l = l as usize;
        if pos + l > bytes.len() { return None; }
        let cell = &bytes[pos..pos+l];
        pos += l;
        // Decode element value to literal
        let val = match elem_oid {
            16 => if l==1 { if cell[0]!=0 { "true".into() } else { "false".into() } } else { return None },
            21 => if l==2 { i16::from_be_bytes(cell.try_into().ok()?).to_string() } else { return None },
            23 => if l==4 { i32::from_be_bytes(cell.try_into().ok()?).to_string() } else { return None },
            20 => if l==8 { i64::from_be_bytes(cell.try_into().ok()?).to_string() } else { return None },
            700 => if l==4 { f32::from_bits(u32::from_be_bytes(cell.try_into().ok()?)).to_string() } else { return None },
            701 => if l==8 { f64::from_bits(u64::from_be_bytes(cell.try_into().ok()?)).to_string() } else { return None },
            25 => {
                // quote and escape for array literal
                let s = String::from_utf8_lossy(cell).into_owned();
                let esc = s.replace("\\", "\\\\").replace("\"", "\\\"");
                format!("\"{}\"", esc)
            }
            17 => {
                // bytea: represent as hex string inside quotes
                let mut h = String::with_capacity(l*2+2);
                h.push_str("\\x");
                for b in cell { h.push_str(&format!("{:02x}", b)); }
                format!("\"{}\"", h)
            }
            1082 => { // date int32 days from PG epoch
                if l!=4 { return None; }
                let pg_days = i32::from_be_bytes(cell.try_into().ok()?);
                const DAYS_BETWEEN_UNIX_AND_PG_EPOCH: i32 = 10957;
                let unix_days = pg_days + DAYS_BETWEEN_UNIX_AND_PG_EPOCH;
                // format yyyy-mm-dd approximately (not precise timezone handling)
                // Fallback to integer days
                unix_days.to_string()
            }
            1114|1184|1083 => {
                // For complex temporal element types, fallback to binary length placeholder text
                // The engine will accept textual casts when echoed; we return quoted raw bytes
                let s = String::from_utf8_lossy(cell).into_owned();
                if s.is_empty() { "\"\"".into() } else { format!("\"{}\"", s) }
            }
            1700 => {
                decode_pg_numeric_to_string(cell).unwrap_or_else(|| "0".into())
            }
            _ => {
                // default: utf8
                let s = String::from_utf8_lossy(cell).into_owned();
                let esc = s.replace("\\", "\\\\").replace("\"", "\\\"");
                format!("\"{}\"", esc)
            }
        };
        elems.push(val);
    }
    Some(format!("{{{}}}", elems.join(",")))
}
