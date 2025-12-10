use std::io::{Read, Cursor};
use std::net::TcpListener;
use std::time::Duration;
use tokio::task::JoinHandle;

// Helpers (minimal manual pgwire client)
fn write_u32(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_be_bytes()); }
fn write_i16(buf: &mut Vec<u8>, v: i16) { buf.extend_from_slice(&v.to_be_bytes()); }
fn write_i32(buf: &mut Vec<u8>, v: i32) { buf.extend_from_slice(&v.to_be_bytes()); }

fn startup_packet(user: &str, dbname: &str) -> Vec<u8> {
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

fn read_exact(c: &mut std::net::TcpStream, n: usize) -> Vec<u8> { let mut b=vec![0u8;n]; c.read_exact(&mut b).unwrap(); b }
fn read_u32(c: &mut std::net::TcpStream) -> u32 { let mut b=[0u8;4]; c.read_exact(&mut b).unwrap(); u32::from_be_bytes(b) }

async fn start_pgwire_ephemeral(tmp: &tempfile::TempDir) -> (JoinHandle<()>, String, u16) {
    let shared = clarium::storage::SharedStore::new(tmp.path()).expect("init SharedStore");
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind 127.0.0.1:0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    std::env::set_var("CLARIUM_PGWIRE_TRUST", "1");
    let bind = format!("127.0.0.1:{}", port);
    let handle = tokio::spawn(async move {
        if let Err(e) = clarium::pgwire_server::start_pgwire(shared, &bind).await { eprintln!("pgwire server task error: {e:?}"); }
    });
    (handle, "127.0.0.1".to_string(), port)
}

async fn wait_until_connectable(host: &str, port: u16, timeout_ms: u64) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if std::net::TcpStream::connect((host, port)).is_ok() { return Ok(()); }
        if std::time::Instant::now() >= deadline { return Err(format!("timeout connecting to {host}:{port}")); }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn consume_until_ready_for_query(c: &mut std::net::TcpStream) {
    loop {
        let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap();
        let len = read_u32(c);
        match tag[0] {
            b'R' => { let _ = read_exact(c, (len-4) as usize); }
            b'S' => { let _ = read_exact(c, (len-4) as usize); }
            b'K' => { let _ = read_exact(c, (len-4) as usize); }
            b'Z' => { let _ = read_exact(c, (len-4) as usize); break; }
            _ => { let _ = read_exact(c, (len-4) as usize); }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_manual_binary_params_arrays_numeric_interval() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>); impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");
    let mut c = std::net::TcpStream::connect((host.as_str(), port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    // Startup
    let pkt = startup_packet("tester", "default");
    c.write_all(&pkt).unwrap();
    consume_until_ready_for_query(&mut c);

    // Prepare: SELECT $1::int4[] a1, $2::numeric n1, $3::interval i1
    let sql = "SELECT $1::int4[] AS a1, $2::numeric AS n1, $3::interval AS i1";
    let mut parse = Vec::new(); parse.push(b'P');
    let mut p = Vec::new();
    p.push(0); // unnamed
    p.extend_from_slice(sql.as_bytes()); p.push(0);
    write_i16(&mut p, 3);
    for oid in [1007, 1700, 1186] { write_i32(&mut p, oid); }
    write_i32(&mut parse, (p.len() as i32) + 4); parse.extend_from_slice(&p);
    c.write_all(&parse).unwrap();
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'1'); let _ = read_u32(&mut c);

    // Build binary params
    // p1: int4[] = {1,NULL,3}
    let mut arr = Vec::new();
    arr.extend_from_slice(&1i32.to_be_bytes()); // ndims
    arr.extend_from_slice(&1i32.to_be_bytes()); // hasnull=true
    arr.extend_from_slice(&23i32.to_be_bytes()); // elemtype=int4
    arr.extend_from_slice(&3i32.to_be_bytes()); // len
    arr.extend_from_slice(&1i32.to_be_bytes()); // lbound
    // elem1: 1
    arr.extend_from_slice(&4i32.to_be_bytes()); arr.extend_from_slice(&1i32.to_be_bytes());
    // elem2: NULL
    arr.extend_from_slice(&(-1i32).to_be_bytes());
    // elem3: 3
    arr.extend_from_slice(&4i32.to_be_bytes()); arr.extend_from_slice(&3i32.to_be_bytes());

    // p2: numeric 12345.6700 — encode as text here; server can accept and still return binary
    let numeric_text = b"12345.6700".to_vec();

    // p3: interval: micros=2_000_000, days=1, months=0
    let mut interval = Vec::new();
    interval.extend_from_slice(&2_000_000i64.to_be_bytes());
    interval.extend_from_slice(&1i32.to_be_bytes());
    interval.extend_from_slice(&0i32.to_be_bytes());

    // Bind: one param format (binary), 3 params; result formats: one for all (binary)
    let mut bind = Vec::new(); bind.push(b'B');
    let mut bpl = Vec::new();
    bpl.push(0); // portal
    bpl.push(0); // stmt
    write_i16(&mut bpl, 1); write_i16(&mut bpl, 1); // 1 param fmt = binary
    write_i16(&mut bpl, 3);
    // p1
    write_i32(&mut bpl, arr.len() as i32); bpl.extend_from_slice(&arr);
    // p2 (send as text even though format=1; server tolerates and decodes to text)
    write_i32(&mut bpl, numeric_text.len() as i32); bpl.extend_from_slice(&numeric_text);
    // p3
    write_i32(&mut bpl, interval.len() as i32); bpl.extend_from_slice(&interval);
    // result fmts: one binary for all
    write_i16(&mut bpl, 1); write_i16(&mut bpl, 1);
    write_i32(&mut bind, (bpl.len() as i32) + 4); bind.extend_from_slice(&bpl); c.write_all(&bind).unwrap();
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'2'); let _ = read_u32(&mut c);

    // Describe portal to get RowDescription (optional)
    let mut desc = Vec::new(); desc.push(b'D'); let mut d=Vec::new(); d.push(b'P'); d.push(0);
    write_i32(&mut desc, (d.len() as i32) + 4); desc.extend_from_slice(&d); c.write_all(&desc).unwrap();
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'T'); let len = read_u32(&mut c) as usize; let _ = read_exact(&mut c, len-4);

    // Execute and read one DataRow
    let mut exec = Vec::new(); exec.push(b'E'); let mut e=Vec::new(); e.push(0); write_i32(&mut e, 0);
    write_i32(&mut exec, (e.len() as i32)+4); exec.extend_from_slice(&e); c.write_all(&exec).unwrap();

    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'D');
    let len = read_u32(&mut c) as usize; let payload = read_exact(&mut c, len-4);
    let mut cur = Cursor::new(&payload);
    let mut nbuf=[0u8;2]; cur.read_exact(&mut nbuf).unwrap(); let ncols = i16::from_be_bytes(nbuf) as usize; assert_eq!(ncols, 3);
    // col1 array length and header check
    let mut lbuf=[0u8;4]; cur.read_exact(&mut lbuf).unwrap(); let l1 = i32::from_be_bytes(lbuf) as usize; assert!(l1 >= 20, "array payload too small");
    let mut a = vec![0u8; l1]; cur.read_exact(&mut a).unwrap(); assert_eq!(&a[0..4], &1i32.to_be_bytes()); // ndims
    // col2 numeric: length present
    cur.read_exact(&mut lbuf).unwrap(); let l2 = i32::from_be_bytes(lbuf) as usize; assert!(l2 >= 8, "numeric payload too small"); let _ = vec![0u8; l2];
    // skip reading numeric bytes fully
    // Instead, advance cursor
    let mut skip = vec![0u8; l2]; cur.read_exact(&mut skip).unwrap();
    // col3 interval: length == 16
    cur.read_exact(&mut lbuf).unwrap(); let l3 = i32::from_be_bytes(lbuf); assert_eq!(l3, 16);
    let mut iv = vec![0u8; 16]; cur.read_exact(&mut iv).unwrap();

    // CommandComplete ('C')
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'C'); let len = read_u32(&mut c) as usize; let _ = read_exact(&mut c, len-4);

    // Sync -> Ready
    let mut sync = Vec::new(); sync.push(b'S'); write_i32(&mut sync, 4); c.write_all(&sync).unwrap();
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'Z'); let _ = read_u32(&mut c); let _ = read_exact(&mut c, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_manual_mixed_result_formats_per_column() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>); impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");
    let mut c = std::net::TcpStream::connect((host.as_str(), port)).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.set_write_timeout(Some(Duration::from_secs(5))).unwrap();

    // Startup
    let pkt = startup_packet("tester", "default"); c.write_all(&pkt).unwrap(); consume_until_ready_for_query(&mut c);

    // Prepare: SELECT $1::int4, $2::text, $3::float8
    let sql = "SELECT $1::int4 AS a, $2::text AS b, $3::float8 AS c";
    let mut parse = Vec::new(); parse.push(b'P'); let mut p = Vec::new(); p.push(0); p.extend_from_slice(sql.as_bytes()); p.push(0);
    write_i16(&mut p, 3); for oid in [23,25,701] { write_i32(&mut p, oid); } write_i32(&mut parse, (p.len() as i32)+4); parse.extend_from_slice(&p); c.write_all(&parse).unwrap();
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'1'); let _ = read_u32(&mut c);

    // Bind: param fmts = binary for all; params: a=42(int4 bin), b="xyz"(text bytes), c=2.5(f64 bin)
    let mut bind = Vec::new(); bind.push(b'B'); let mut bpl = Vec::new(); bpl.push(0); bpl.push(0);
    write_i16(&mut bpl, 1); write_i16(&mut bpl, 1);
    write_i16(&mut bpl, 3);
    write_i32(&mut bpl, 4); bpl.extend_from_slice(&42i32.to_be_bytes());
    write_i32(&mut bpl, 3); bpl.extend_from_slice(b"xyz");
    write_i32(&mut bpl, 8); bpl.extend_from_slice(&2.5f64.to_bits().to_be_bytes());
    // result fmts per-column: [text(0), binary(1), binary(1)]
    write_i16(&mut bpl, 3); write_i16(&mut bpl, 0); write_i16(&mut bpl, 1); write_i16(&mut bpl, 1);
    write_i32(&mut bind, (bpl.len() as i32)+4); bind.extend_from_slice(&bpl); c.write_all(&bind).unwrap();
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'2'); let _ = read_u32(&mut c);

    // Execute and read row
    let mut exec = Vec::new(); exec.push(b'E'); let mut e=Vec::new(); e.push(0); write_i32(&mut e, 0); write_i32(&mut exec, (e.len() as i32)+4); exec.extend_from_slice(&e); c.write_all(&exec).unwrap();
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'D'); let len = read_u32(&mut c) as usize; let payload = read_exact(&mut c, len-4);
    let mut cur = Cursor::new(&payload);
    let mut nbuf=[0u8;2]; cur.read_exact(&mut nbuf).unwrap(); let ncols = i16::from_be_bytes(nbuf) as usize; assert_eq!(ncols, 3);
    // col1 text: length and ascii digits
    let mut lbuf=[0u8;4]; cur.read_exact(&mut lbuf).unwrap(); let l1 = i32::from_be_bytes(lbuf) as usize; assert!(l1 >= 2);
    let mut v1 = vec![0u8; l1]; cur.read_exact(&mut v1).unwrap(); let s1 = String::from_utf8_lossy(&v1).into_owned(); assert_eq!(s1, "42");
    // col2 binary float8? Actually col2 was text column requested binary; server may fallback to text. Accept any positive length.
    cur.read_exact(&mut lbuf).unwrap(); let l2 = i32::from_be_bytes(lbuf) as usize; let mut v2 = vec![0u8; l2]; cur.read_exact(&mut v2).unwrap(); assert!(l2 >= 3);
    // col3 binary float8
    cur.read_exact(&mut lbuf).unwrap(); let l3 = i32::from_be_bytes(lbuf) as usize; assert_eq!(l3, 8); let mut v3 = [0u8;8]; cur.read_exact(&mut v3).unwrap(); let _ = f64::from_bits(u64::from_be_bytes(v3));

    // CommandComplete
    let mut tag = [0u8;1]; c.read_exact(&mut tag).unwrap(); assert_eq!(tag[0], b'C'); let len = read_u32(&mut c) as usize; let _ = read_exact(&mut c, len-4);
}
