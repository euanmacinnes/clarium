use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

mod bench_support;
use bench_support::BenchCtx;

fn load_graph_tables(ctx: &BenchCtx, nodes_tab: &str, edges_tab: &str, n: usize, avg_degree: usize, seed: u64) {
    // Create tables
    let _ = ctx.exec_ok(&format!("CREATE TABLE {}", nodes_tab)).unwrap();
    let _ = ctx.exec_ok(&format!("CREATE TABLE {}", edges_tab)).unwrap();
    // Insert nodes
    let batch = 1000usize;
    let mut i = 0usize;
    while i < n {
        let end = (i + batch).min(n);
        let mut sql = String::with_capacity(32 * (end - i) + 64);
        sql.push_str(&format!("INSERT INTO {} (id) VALUES ", nodes_tab));
        for r in i..end {
            if r > i { sql.push_str(", "); }
            sql.push_str(&format!("({})", r));
        }
        let _ = ctx.exec_ok(&sql).unwrap();
        i = end;
    }
    // Insert edges (synthetic random graph, directed; approximate avg_degree)
    let mut rng = StdRng::seed_from_u64(seed);
    let mut inserted: usize = 0;
    i = 0;
    while i < n {
        let end = (i + batch).min(n);
        let mut sql = String::with_capacity(48 * avg_degree * (end - i) + 64);
        let mut first = true;
        sql.push_str(&format!("INSERT INTO {} (src, dst) VALUES ", edges_tab));
        for u in i..end {
            for _ in 0..avg_degree {
                let mut v = (rng.gen::<usize>() % n) as usize;
                if v == u { v = (v + 1) % n; }
                if !first { sql.push_str(", "); }
                first = false;
                sql.push_str(&format!("({},{})", u, v));
                inserted += 1;
            }
        }
        if !first { let _ = ctx.exec_ok(&sql).unwrap(); }
        i = end;
    }
    // Optionally add some reverse edges to simulate undirected-ness (skip to keep load light)
}

fn create_graph(ctx: &BenchCtx, graph_name: &str, nodes_tab: &str, edges_tab: &str) {
    // Minimal logical schema: Node(id) and edge type E from Node to Node; bind via USING TABLES
    let sql = format!(
        "CREATE GRAPH {} NODES (Node KEY(id)) EDGES (E FROM Node TO Node) USING TABLES (nodes={}, edges={})",
        graph_name, nodes_tab, edges_tab
    );
    let _ = ctx.exec_ok(&sql).unwrap();
}

fn bench_sql_graph(c: &mut Criterion) {
    let ns = [50_000usize, 200_000usize];
    let degree = 8usize;
    let mut group = c.benchmark_group("sql_graph");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(12);

    for &n in &ns {
        let ctx = BenchCtx::new().expect("bench ctx");
        let nodes_tab = format!("clarium/public/bench_nodes_n{}", n);
        let edges_tab = format!("clarium/public/bench_edges_n{}", n);
        let graph_name = format!("clarium/public/bench_graph_n{}", n);
        load_graph_tables(&ctx, &nodes_tab, &edges_tab, n, degree, 0xA11C_EC70 ^ (n as u64));
        create_graph(&ctx, &graph_name, &nodes_tab, &edges_tab);

        // Choose a fixed start node for repeatability
        let start = 0usize;

        // Depth-2
        group.throughput(Throughput::Elements((degree * degree) as u64));
        let sql2 = format!(
            "SELECT COUNT(*) AS cnt FROM graph_neighbors('{}','{}','E',2)",
            graph_name, start
        );
        group.bench_with_input(BenchmarkId::new("neighbors_depth2", n.to_string()), &n, |b, _| {
            let _ = ctx.exec(&sql2).unwrap();
            b.iter(|| {
                let _ = ctx.exec(&sql2).unwrap();
            });
        });

        // Depth-3
        let sql3 = format!(
            "SELECT COUNT(*) AS cnt FROM graph_neighbors('{}','{}','E',3)",
            graph_name, start
        );
        group.bench_with_input(BenchmarkId::new("neighbors_depth3", n.to_string()), &n, |b, _| {
            let _ = ctx.exec(&sql3).unwrap();
            b.iter(|| {
                let _ = ctx.exec(&sql3).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sql_graph);
criterion_main!(benches);
