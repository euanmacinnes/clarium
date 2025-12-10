use std::net::{TcpListener};
use std::time::Duration;

use tempfile::TempDir;
use tokio::task::JoinHandle;

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

    let handle = tokio::spawn(async move {
        // start_pgwire runs an accept loop forever; we abort the task on drop
        if let Err(e) = clarium::pgwire_server::start_pgwire(shared, &bind).await {
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
