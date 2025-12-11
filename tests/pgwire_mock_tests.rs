use std::net::{TcpListener};
use std::time::Duration;

use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::sync::watch;

use clarium::storage::{Store, SharedStore};

// Start the in-process pgwire server bound to an ephemeral localhost port.
// Returns (join_handle, host, port). Caller should abort the handle to stop the server.
async fn start_pgwire_ephemeral(tmp: &TempDir) -> (JoinHandle<()>, String, u16) {
    // Create store in the temp dir
    let _store = Store::new(tmp.path()).expect("init Store");
    let shared = SharedStore::new(tmp.path()).expect("init SharedStore");

    // Reserve an ephemeral port
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind 127.0.0.1:0");
    let port = listener.local_addr().unwrap().port();
    drop(listener); // free it; tiny race window but acceptable for tests

    // TRUST mode: skip password auth for tests
    std::env::set_var("CLARIUM_PGWIRE_TRUST", "1");

    let bind = format!("127.0.0.1:{}", port);

    // Shutdown channel required by start_pgwire; we abort the task in tests, but provide a receiver
    let (_tx, rx) = watch::channel(false);

    let handle = tokio::spawn(async move {
        // start_pgwire runs an accept loop forever; we abort the task on drop
        if let Err(e) = clarium::pgwire_server::start_pgwire(shared, &bind, rx).await {
            eprintln!("pgwire server task error: {e:?}");
        }
    });

    (handle, "127.0.0.1".to_string(), port)
}

async fn wait_until_connectable(host: &str, port: u16, timeout_ms: u64) -> Result<(), String> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let mut cfg = tokio_postgres::Config::new();
        cfg.host(host)
            .port(port)
            .user("tester")
            .dbname("default");
        match cfg.connect(tokio_postgres::NoTls).await {
            Ok((_client, connection)) => {
                // Spawn the connection task and then immediately drop; this confirms reachability
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return Ok(());
            }
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(format!("timeout connecting to {host}:{port}: {e}"));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_simple_query_select_1() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;

    // Ensure cleanup no matter what
    struct Guard(JoinHandle<()>);
    impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");

    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host).port(port).user("tester").dbname("default");
    let (client, connection) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = connection.await; });

    // Simple Query flow is used by Client::simple_query
    let rows = client
        .simple_query("SELECT 1 AS one").await
        .expect("simple_query");

    // tokio-postgres returns a mix of SimpleQueryMessage; we verify one row with column value "1"
    let mut saw_row = false;
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
            let v = r.get(0).unwrap_or("");
            assert_eq!(v, "1");
            saw_row = true;
        }
    }
    assert!(saw_row, "expected at least one DataRow");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_extended_protocol_prepared_select_add() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>);
    impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");

    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host).port(port).user("tester").dbname("default");
    let (client, connection) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = connection.await; });

    // Prepare/execute without parameters still exercises extended protocol
    let stmt = client.prepare("SELECT 3 + 4 AS s").await.expect("prepare");
    // Verify server announces a numeric OID for the column
    let col_oid = stmt.columns()[0].type_().oid();
    assert!(col_oid == 23 || col_oid == 20 || col_oid == 701, "unexpected OID {}", col_oid);
    // tokio-postgres should be able to deserialize to i32 when OID indicates numeric
    let row = client.query_one(&stmt, &[]).await.expect("execute");
    let sum: i32 = row.get(0);
    assert_eq!(sum, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_extended_protocol_parameter_passing() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>);
    impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");

    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host).port(port).user("tester").dbname("default");
    let (client, connection) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = connection.await; });

    // Numeric parameters
    let stmt = client.prepare("SELECT $1::int4 + $2::int4 AS s").await.expect("prepare");
    let row = client.query_one(&stmt, &[&10i32, &32i32]).await.expect("execute");
    let sum: i32 = row.get(0);
    assert_eq!(sum, 42);

    // Text parameters
    let stmt2 = client.prepare("SELECT $1::text || '-' || $2::text AS s").await.expect("prepare");
    let row2 = client.query_one(&stmt2, &[&"alpha", &"beta"]).await.expect("execute");
    let s: String = row2.get(0);
    assert_eq!(s, "alpha-beta");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_sqlalchemy_like_inspection_queries() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>);
    impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");

    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host).port(port).user("tester").dbname("default");
    let (client, connection) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = connection.await; });

    // A few introspection queries inspired by SQLAlchemy ORM
    // 1) version()
    let ver_row = client.query_one("SELECT pg_catalog.version()", &[]).await.expect("version()");
    let ver: String = ver_row.get(0);
    assert!(!ver.is_empty());

    // 2) types and namespaces join
    let rows = client
        .query(
            "SELECT t.typname, n.nspname \
             FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid \
             ORDER BY t.typname LIMIT 5",
            &[],
        )
        .await
        .expect("pg_type join pg_namespace");
    assert!(!rows.is_empty());

    // 3) available extensions TVF backed by scripts (may be empty but should succeed)
    let _ = client
        .simple_query("SELECT name, default_version, comment FROM pg_available_extensions()")
        .await
        .expect("pg_available_extensions()");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_simple_protocol_ddl_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>);
    impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");

    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host).port(port).user("tester").dbname("default");
    let (client, connection) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = connection.await; });

    // Create table
    let msgs = client.simple_query("CREATE TABLE t_ddlt(a int, b text)").await.expect("create table");
    assert!(msgs.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))), "expected CommandComplete for CREATE TABLE");

    // Insert rows
    let msgs = client.simple_query("INSERT INTO t_ddlt VALUES (1,'x'), (2,'y'), (3,'z')").await.expect("insert");
    assert!(msgs.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))), "expected CommandComplete for INSERT");

    // Update
    let msgs = client.simple_query("UPDATE t_ddlt SET b = 'yy' WHERE a = 2").await.expect("update");
    assert!(msgs.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))), "expected CommandComplete for UPDATE");

    // Select count
    let rows = client.simple_query("SELECT COUNT(*) FROM t_ddlt").await.expect("count");
    let mut cnt = None;
    for m in rows { if let tokio_postgres::SimpleQueryMessage::Row(r) = m { cnt = r.get(0).map(|s| s.parse::<i64>().unwrap()); } }
    assert_eq!(cnt, Some(3));

    // Create view
    let msgs = client.simple_query("CREATE VIEW v_ddlt AS SELECT * FROM t_ddlt WHERE a > 1").await.expect("create view");
    assert!(msgs.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))), "expected CommandComplete for CREATE VIEW");

    // Show schemas
    let _ = client.simple_query("SHOW SCHEMAS").await.expect("show schemas");

    // Drop table
    let msgs = client.simple_query("DROP TABLE t_ddlt").await.expect("drop table");
    assert!(msgs.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))), "expected CommandComplete for DROP TABLE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_binary_results_and_types() {
    let tmp = tempfile::tempdir().unwrap();
    let (srv, host, port) = start_pgwire_ephemeral(&tmp).await;
    struct Guard(JoinHandle<()>);
    impl Drop for Guard { fn drop(&mut self) { self.0.abort(); } }
    let _g = Guard(srv);

    wait_until_connectable(&host, port, 3_000).await.expect("server reachable");

    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&host).port(port).user("tester").dbname("default");
    let (client, connection) = cfg.connect(tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(async move { let _ = connection.await; });

    // Build a table with multiple types to ensure proper schema typing and allow binary results
    client.simple_query("CREATE TABLE t_types(i4 int4, i8 int8, f8 double precision, b boolean, s text)").await.expect("create table");
    client.simple_query("INSERT INTO t_types VALUES (42, 42000000000, 3.14159, true, 'hello')").await.expect("insert");

    // Prepare a select; tokio-postgres will typically request binary for supported types.
    let stmt = client
        .prepare("SELECT i4, i8, f8, b, s FROM t_types WHERE i4 = $1::int4")
        .await
        .expect("prepare");

    // OIDs should match (int4=23, int8=20, float8=701, bool=16, text=25)
    let oids: Vec<u32> = stmt.columns().iter().map(|c| c.type_().oid()).collect();
    assert_eq!(oids, vec![23, 20, 701, 16, 25], "unexpected OIDs: {:?}", oids);

    let row = client.query_one(&stmt, &[&42i32]).await.expect("execute");
    // If binary results engaged, tokio-postgres decodes to native types
    let v_i4: i32 = row.get(0);
    let v_i8: i64 = row.get(1);
    let v_f8: f64 = row.get(2);
    let v_b: bool = row.get(3);
    let v_s: String = row.get(4);
    assert_eq!(v_i4, 42);
    assert_eq!(v_i8, 42000000000);
    assert!((v_f8 - 3.14159).abs() < 1e-6);
    assert!(v_b);
    assert_eq!(v_s, "hello");

    // Cleanup
    let _ = client.simple_query("DROP TABLE t_types").await;
}
