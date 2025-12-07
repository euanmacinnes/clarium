use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

mod bench_support;
use bench_support::BenchCtx;

fn load_vectors(ctx: &BenchCtx, table: &str, n: usize, dim: usize, seed: u64) {
    let _ = ctx.exec_ok(&format!("CREATE TABLE {}", table)).unwrap();
    let mut rng = StdRng::seed_from_u64(seed);
    let batch = 500usize;
    let mut i = 0usize;
    while i < n {
        let end = (i + batch).min(n);
        let mut sql = String::with_capacity(dim * 16 * (end - i) + 128);
        sql.push_str(&format!("INSERT INTO {} (id, vec) VALUES ", table));
        for r in i..end {
            let id = r as i64;
            if r > i { sql.push_str(", "); }
            // emit vector literal as JSON-like list: [x0, x1, ...]
            sql.push_str(&format!("({},'[");
            for d in 0..dim {
                let x: f32 = rng.gen();
                if d > 0 { sql.push_str(","); }
                sql.push_str(&format!("{:.6}", x));
            }
            sql.push_str("]')");
        }
        let _ = ctx.exec_ok(&sql).unwrap();
        i = end;
    }
}

fn build_index(ctx: &BenchCtx, index_name: &str, table: &str, metric: &str, m: usize, ef_build: usize) {
    // Use HNSW when available; engine will gracefully ignore if feature is disabled.
    let sql = format!(
        "CREATE VECTOR INDEX {} ON {}(vec) USING hnsw WITH (metric='{}', M={}, ef_build={})",
    index_name, table, metric, m, ef_build);
    let _ = ctx.exec_ok(&sql).unwrap();
}

fn bench_sql_vector(c: &mut Criterion) {
    let ns = [10_000usize, 100_000usize];
    let dims = [64usize, 384usize, 768usize];
    let ks = [10usize, 100usize];

    let mut group = c.benchmark_group("sql_vector");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(12);

    for &n in &ns {
        for &dim in &dims {
            // Two separate tables: one flat-only (no index), one ANN (with index)
            let ctx = BenchCtx::new().expect("bench ctx");
            let tbl_flat = format!("clarium/public/v_flat_n{}_d{}", n, dim);
            let tbl_ann = format!("clarium/public/v_ann_n{}_d{}", n, dim);
            load_vectors(&ctx, &tbl_flat, n, dim, 0xC1A2_45F0 ^ (dim as u64));
            // clone data by simple SELECT INTO if supported; otherwise load again deterministically
            load_vectors(&ctx, &tbl_ann, n, dim, 0xC1A2_45F0 ^ (dim as u64));
            // Build index for ANN table (L2). If ANN feature is disabled, engine will keep flat path.
            build_index(&ctx, &format!("idx_v_ann_n{}_d{}", n, dim), &tbl_ann, "l2", 32, 200);

            // Prepare a deterministic query vector of given dim
            let mut rng = StdRng::seed_from_u64(0xC1A2_77FF ^ (dim as u64));
            let mut q = String::with_capacity(dim * 12);
            q.push('[');
            for d in 0..dim {
                if d > 0 { q.push(','); }
                let x: f32 = rng.gen();
                q.push_str(&format!("{:.6}", x));
            }
            q.push(']');

            for &k in &ks {
                // Exact via ORDER BY L2_DISTANCE or via TVF on the flat table
                group.throughput(Throughput::Elements(k as u64));
                // Prefer TVF to exercise the same runtime; it will do flat scan on a non-indexed table
                let sql_flat = format!(
                    "SELECT COUNT(*) AS cnt FROM nearest_neighbors('{}','vec','{}', {}, 'l2')",
                    tbl_flat, q, k
                );
                group.bench_with_input(
                    BenchmarkId::new("flat_l2", format!("n{}_d{}_k{}", n, dim, k)),
                    &k,
                    |b, _| {
                        let _ = ctx.exec(&sql_flat).unwrap(); // warm-up
                        b.iter(|| {
                            let _ = ctx.exec(&sql_flat).unwrap();
                        });
                    },
                );

                // ANN via TVF on the indexed table. Planner/runtime should take ANN path.
                let sql_ann = format!(
                    "SELECT COUNT(*) AS cnt FROM nearest_neighbors('{}','vec','{}', {}, 'l2', 96)",
                    tbl_ann, q, k
                );
                group.bench_with_input(
                    BenchmarkId::new("ann_hnsw_l2", format!("n{}_d{}_k{}", n, dim, k)),
                    &k,
                    |b, _| {
                        let _ = ctx.exec(&sql_ann).unwrap();
                        b.iter(|| {
                            let _ = ctx.exec(&sql_ann).unwrap();
                        });
                    },
                );
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_sql_vector);
criterion_main!(benches);
