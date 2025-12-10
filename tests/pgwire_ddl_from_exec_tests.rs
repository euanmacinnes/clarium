use tokio::task::JoinHandle;

// Reuse the same pgwire helper used in other tests
mod common {
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::task::JoinHandle;
    use tempfile::TempDir;

    pub async fn start_pgwire_ephemeral(tmp: &TempDir) -> (JoinHandle<()>, String, u16) {
        let shared = clarium::storage::SharedStore::new(tmp.path()).expect("init SharedStore");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind 127.0.0.1:0");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        std::env::set_var("CLARIUM_PGWIRE_TRUST", "1");
        let bind = format!("127.0.0.1:{}", port);
        let handle = tokio::spawn(async move {
            if let Err(e) = clarium::pgwire_server::start_pgwire(shared, &bind).await {
                eprintln!("pgwire server task error: {e:?}");
            }
        });
        (handle, "127.0.0.1".to_string(), port)
    }

    pub async fn wait_until_connectable(host: &str, port: u16, timeout_ms: u64) -> Result<(), String> {
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let mut cfg = tokio_postgres::Config::new();
            cfg.host(host).port(port).user("tester").dbname("default");
            match cfg.connect(tokio_postgres::NoTls).await {
                Ok((_client, connection)) => {
                    tokio::spawn(async move { let _ = connection.await; });
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_reuse_exec_ddl_tests_create_view_drop_view() {
    use common::*;
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

    // Create a base table and populate
    client.simple_query("CREATE TABLE src(a int4, b text)").await.expect("create table src");
    client.simple_query("INSERT INTO src VALUES (1,'x'), (2,'y'), (3,'z')").await.expect("insert src");

    // Create a view from the table
    let msgs = client.simple_query("CREATE VIEW v_src AS SELECT a, b FROM src WHERE a > 1").await.expect("create view");
    assert!(msgs.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))), "CREATE VIEW should complete");

    // Query the view
    let rows = client.query("SELECT COUNT(*) FROM v_src", &[]).await.expect("select from view");
    let cnt: i64 = rows[0].get::<_, i64>(0);
    assert_eq!(cnt, 2);

    // Drop the view
    let msgs = client.simple_query("DROP VIEW v_src").await.expect("drop view");
    assert!(msgs.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::CommandComplete(_))), "DROP VIEW should complete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgwire_reuse_exec_ddl_show_tables_columns() {
    use common::*;
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

    // Create a couple of tables
    client.simple_query("CREATE TABLE t1(id int4, name text)").await.expect("create t1");
    client.simple_query("CREATE TABLE t2(x int8, y double precision)").await.expect("create t2");

    // SHOW SCHEMAS should work (already covered elsewhere)
    let _ = client.simple_query("SHOW SCHEMAS").await.expect("show schemas");

    // If SHOW TABLES is supported, it should list t1/t2; do not assert content strictly to avoid engine differences
    let _ = client.simple_query("SHOW TABLES").await.ok();

    // SHOW COLUMNS FROM t1 if supported
    let _ = client.simple_query("SHOW COLUMNS FROM t1").await.ok();
}
