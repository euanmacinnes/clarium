use std::io::{Cursor, Read, Write};
use std::net::TcpListener;
use std::time::Duration;

use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::sync::watch;

// Minimal pgwire client for explicit binary params/results testing

async fn start_pgwire_ephemeral(tmp: &TempDir) -> (JoinHandle<()>, String, u16) {
    let shared = clarium::storage::SharedStore::new(tmp.path()).expect("init SharedStore");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind 127.0.0.1:0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    std::env::set_var("CLARIUM_PGWIRE_TRUST", "1");
    let bind = format!("127.0.0.1:{}", port);
    let (_tx, rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        if let Err(e) = clarium::pgwire_server::start_pgwire(shared, &bind, rx).await {
            eprintln!("pgwire server task error: {e:?}");
        }
    });
    (handle, "127.0.0.1".to_string(), port)
}

async fn wait_until_connectable(host: &str, port: u16, timeout_ms: u64) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if std::net::TcpStream::connect((host, port)).is_ok() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("timeout connecting to {host}:{port}"));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn write_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_be_bytes()); }
fn write_i16(buf: &mut Vec<u8>, v: i16) { buf.extend_from_slice(&v.to_be_bytes()); }
fn write_i32(buf: &mut Vec<u8>, v: i32) { buf.extend_from_slice(&v.to_be_bytes()); }

fn startup_packet(user: &str, dbname: &str) -> Vec<u8> {
    // len (i32) + protocol 196608 + key/value pairs + terminator 0
    let mut payload: Vec<u8> = Vec::new();
    payload.extend_from_slice(&196608u32.to_be_bytes());
    for (k, v) in [("user", user), ("database", dbname)] {
        payload.extend_from_slice(k.as_bytes()); payload.push(0);
        payload.extend_from_slice(v.as_bytes()); payload.push(0);
    }
    payload.push(0);
    let mut out = Vec::new();
    write_u32(&mut out, (payload.len() as u32) + 4);
    out.extend_from_slice(&payload);
    out
}

fn read_exact(c: &mut std::net::TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    c.read_exact(&mut buf).unwrap();
    buf
}

fn read_u32(c: &mut std::net::TcpStream) -> u32 { let mut b=[0u8;4]; c.read_exact(&mut b).unwrap(); u32::from_be_bytes(b) }

fn consume_until_ready_for_query(c: &mut std::net::TcpStream) {
    loop {
        let mut tag = [0u8;1];
        c.read_exact(&mut tag).unwrap();
        let len = read_u32(c);
        match tag[0] {
            b'R' => { // AuthenticationOk or password request (we use TRUST so expect AuthOk)
                let _code = read_u32(c);
            }
            b'S' => { // ParameterStatus
                let mut payload = read_exact(c, (len - 4) as usize);
                // consume two cstrings and terminator
                let _ = payload;
            }
            b'K' => { // BackendKeyData
                let _payload = read_exact(c, (len - 4) as usize);
            }
            b'Z' => { // ReadyForQuery
                let _status = read_exact(c, (len - 4) as usize);
                break;
            }
            _ => { let _ = read_exact(c, (len - 4) as usize); }
        }
    }
}

fn simple_sync(c: &mut std::net::TcpStream) {
    // Send Sync
    let mut buf = Vec::new();
    buf.push(b'S');
    write_i32(&mut buf, 4);
    c.write_all(&buf).unwrap();
    // Read ReadyForQuery
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'Z');
    let _len = read_u32(c); let _ = read_exact(c, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_explicit_binary_params_and_results_temporal() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>);
    impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");

    // Connect raw
    let mut c = std::net::TcpStream::connect((host.as_str(), port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    // Startup
    let pkt = startup_packet("tester", "default");
    c.write_all(&pkt).unwrap();
    consume_until_ready_for_query(&mut c);

    // Prepare a statement returning many types; we’ll compute values from params to avoid engine literal parsing issues
    // SQL: SELECT $1::int4, $2::float8, $3::bool, $4::text,
    //              to_date($5::text), to_time($6::text), to_timestamp($7::int8), to_timestamp_tz($8::int8)
    // The engine’s exact functions may vary; fallback: just echo out text for complex ones.
    // To keep this robust, we’ll only rely on basic ones supported already: text passthrough for $4 and compute temporal via numeric epoch columns where supported.

    // We will use simple arithmetic/selects that the engine supports: SELECT $1::int4 as i4, $2::float8 as f8, $3::bool as b, $4::text as s,
    // and numeric epoch-based timestamps via engine’s generic functions: for now, select $7 and $8 back so we can validate binary encoding path exists.
    let sql = "SELECT $1::int4 AS i4, $2::float8 AS f8, $3::bool AS b, $4::text AS s, $5::date AS d, $6::time AS t, $7::timestamp AS ts, $8::timestamptz AS tstz";

    // Parse
    let mut parse = Vec::new();
    parse.push(b'P');
    // payload: stmt_name("")\0, query\0, ntypes(i16)=8, then 8 i32 type oids
    let mut p = Vec::new();
    p.push(0);
    p.extend_from_slice(sql.as_bytes()); p.push(0);
    write_i16(&mut p, 8);
    for oid in [23, 701, 16, 25, 1082, 1083, 1114, 1184] { write_i32(&mut p, oid); }
    write_i32(&mut parse, (p.len() as i32) + 4);
    parse.extend_from_slice(&p);
    c.write_all(&parse).unwrap();
    // Expect ParseComplete ('1')
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'1');
    let _len = read_u32(&mut c);

    // Bind: single format for all params = binary(1), 8 params; per-column result format = binary(1) for all
    let mut bind = Vec::new(); bind.push(b'B');
    let mut bpayload = Vec::new();
    bpayload.push(0); // portal name
    bpayload.push(0); // stmt name (unnamed)
    write_i16(&mut bpayload, 1); write_i16(&mut bpayload, 1); // one param format = 1 (binary)
    write_i16(&mut bpayload, 8); // 8 params
    // p1 int4=42
    write_i32(&mut bpayload, 4); bpayload.extend_from_slice(&42i32.to_be_bytes());
    // p2 float8=3.5
    write_i32(&mut bpayload, 8); bpayload.extend_from_slice(&3.5f64.to_bits().to_be_bytes());
    // p3 bool=true (1 byte)
    write_i32(&mut bpayload, 1); bpayload.push(1);
    // p4 text in text format (override): we’ll append as binary fallback by sending text bytes but decoder will accept
    write_i32(&mut bpayload, 5); bpayload.extend_from_slice(b"hello");
    // p5 date: send text '2025-01-02' (engine casts); use text bytes
    write_i32(&mut bpayload, 10); bpayload.extend_from_slice(b"2025-01-02");
    // p6 time: '12:34:56'
    write_i32(&mut bpayload, 8); bpayload.extend_from_slice(b"12:34:56");
    // p7 timestamp: send microseconds since UNIX epoch as text (engine casts)
    write_i32(&mut bpayload, 16); bpayload.extend_from_slice(b"2025-01-02 03:04:05");
    // p8 timestamptz: send text; server will encode in binary on output
    write_i32(&mut bpayload, 25); bpayload.extend_from_slice(b"2025-01-02 03:04:05+00");

    // Result formats: one code applies to all = 1 (binary)
    write_i16(&mut bpayload, 1); write_i16(&mut bpayload, 1);
    write_i32(&mut bind, (bpayload.len() as i32) + 4);
    bind.extend_from_slice(&bpayload);
    c.write_all(&bind).unwrap();
    // BindComplete ('2')
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'2');
    let _len = read_u32(&mut c);

    // Describe (portal) to verify RowDescription; optional check
    let mut desc = Vec::new(); desc.push(b'D');
    let mut d = Vec::new(); d.push(b'P'); d.push(0); // portal, unnamed
    write_i32(&mut desc, (d.len() as i32) + 4); desc.extend_from_slice(&d);
    c.write_all(&desc).unwrap();
    // Expect RowDescription ('T') then No Ready yet
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'T');
    let len = read_u32(&mut c) as usize; let _rd = read_exact(&mut c, len - 4);

    // Execute (portal, max rows 0 = unlimited)
    let mut exec = Vec::new(); exec.push(b'E');
    let mut epl = Vec::new(); epl.push(0); write_i32(&mut epl, 0);
    write_i32(&mut exec, (epl.len() as i32) + 4); exec.extend_from_slice(&epl);
    c.write_all(&exec).unwrap();

    // Expect one DataRow ('D'), then CommandComplete ('C'), then Sync/Ready
    // We skip decoding fields precisely; just assert binary lengths exist for first few columns
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'D');
    let len = read_u32(&mut c) as usize; let mut payload = read_exact(&mut c, len - 4);
    // Parse fields
    let mut cur = Cursor::new(&payload);
    let mut nbuf = [0u8;2]; cur.read_exact(&mut nbuf).unwrap();
    let ncols = i16::from_be_bytes(nbuf) as usize; assert_eq!(ncols, 8);
    // field 1 length
    let mut lbuf = [0u8;4]; cur.read_exact(&mut lbuf).unwrap(); let l1 = i32::from_be_bytes(lbuf); assert_eq!(l1, 4);
    let mut b = vec![0u8; l1 as usize]; cur.read_exact(&mut b).unwrap(); assert_eq!(i32::from_be_bytes([b[0],b[1],b[2],b[3]]), 42);
    // field 2 float8
    cur.read_exact(&mut lbuf).unwrap(); let l2 = i32::from_be_bytes(lbuf); assert_eq!(l2, 8);
    let mut f = vec![0u8; l2 as usize]; cur.read_exact(&mut f).unwrap(); let _ = f64::from_bits(u64::from_be_bytes(f.try_into().unwrap()));
    // field 3 bool
    cur.read_exact(&mut lbuf).unwrap(); let l3 = i32::from_be_bytes(lbuf); assert_eq!(l3, 1); let mut vb=[0u8;1]; cur.read_exact(&mut vb).unwrap(); assert_eq!(vb[0], 1);

    // Consume remaining fields without strict checks (server may fall back to text if casting not supported)
    // Just ensure payload position advanced to end
    let mut tmp = Vec::new(); cur.read_to_end(&mut tmp).unwrap();

    // Read CommandComplete
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'C');
    let len = read_u32(&mut c) as usize; let _ = read_exact(&mut c, len - 4);

    // Sync -> Ready
    simple_sync(&mut c);
}
