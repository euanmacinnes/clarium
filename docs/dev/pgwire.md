PGWire codecs and OIDs
======================

This document describes Clarium's experimental PostgreSQL wire (pgwire) codecs, supported OIDs, and parameter/result formats.

Overview
--------
- The server announces accurate PostgreSQL OIDs per column in `RowDescription`.
- Result formats (text=0, binary=1) are honored per-column as specified by the portal at `Bind` time.
- Binary encoders are implemented for:
  - Scalars: `bool(16)`, `int2(21)`, `int4(23)`, `int8(20)`, `float4(700)`, `float8(701)`, `bytea(17)`
  - Temporal: `date(1082)`, `time(1083)`, `timestamp(1114)`, `timestamptz(1184)`
  - Interval: `interval(1186)` — encoded as 16-byte tuple `(microseconds i64, days i32, months i32)`
  - Numeric/Decimal: `numeric(1700)` — NUMERIC binary (base-10000 digits); text fallback if out-of-range
  - Arrays (1-D) of common types: `bool[]`, `int2[]`, `int4[]`, `int8[]`, `float4[]`, `float8[]`, `text[]`, `bytea[]`, `date[]`, `timestamp[]`, `timestamptz[]`, `time[]`
- Composite/record: `record(2249)` currently uses text payloads with correct OID.

Parameter decoding
------------------
- Text parameters are accepted for all types; explicit casts (e.g., `$1::int4`) are recommended for fidelity.
- Binary parameter decoding is supported for:
  - `bool`, `int2/4/8`, `float4/8`, `text/varchar/bpchar`
  - `interval(1186)` — 16-byte tuple
  - `numeric(1700)` — NUMERIC binary format → canonical decimal string
  - 1-D arrays of the common element types listed above → Postgres array literal (e.g., `"{1,2,3}"`)

Temporal encodings
------------------
- `date(1082)`: `i32` days since PostgreSQL epoch (2000-01-01). We map Polars dates from UNIX epoch by subtracting 10957 days.
- `time(1083)`: `i64` microseconds since midnight.
- `timestamp(1114)` / `timestamptz(1184)`: `i64` microseconds since PostgreSQL epoch (2000-01-01 00:00:00 UTC)
  - Conversion uses UNIX epoch delta: `946_684_800_000_000` microseconds.

NUMERIC (1700)
--------------
- Binary format: `int16 ndigits, int16 weight, int16 sign, int16 dscale, digits...` where digits are base-10000 groups.
- The server provides:
  - Encoder from canonical decimal strings/ints/floats → NUMERIC binary; text fallback if parsing fails.
  - Decoder for binary parameters → canonical decimal string with preserved scale and sign.

Arrays
------
- We support 1-D arrays only. Binary layout: `ndims(i32)=1, hasnull(i32), elemtype(i32), len(i32), lbound(i32), [per-element length(i32) + payload]`.
- Element payloads use each element type’s binary encoding if supported; NULL elements use length `-1`.

Result format selection
-----------------------
- At `Bind`, result format codes are interpreted per protocol:
  - `0 formats`: all text
  - `1 format`: applies to all columns
  - `N formats == N columns`: per-column codes
- For unsupported combinations, we fall back to text while keeping accurate OIDs.

Graceful error handling
-----------------------
- Decoding/encoding errors do not panic. The server responds with an error frame or falls back to text/NULL where appropriate.

Debugging
---------
- Set `CLARIUM_PGWIRE_TRACE=1` to enable detailed `tprintln!` diagnostics of frames and codec paths.
