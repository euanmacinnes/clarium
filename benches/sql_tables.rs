use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

mod bench_support;
use bench_support::BenchCtx;

fn load_table(ctx: &BenchCtx, table: &str, n: usize, seed: u64) {
    // Create table (no schema decl; columns provided on INSERT)
    let _ = ctx.exec_ok(&format!("CREATE TABLE {}", table)).unwrap();
    // Deterministic data
    let mut rng = StdRng::seed_from_u64(seed);
    // Insert in batches to avoid huge statements
    let batch = 1000usize;
    let mut i = 0usize;
    while i < n {
        let end = (i + batch).min(n);
        let mut sql = String::with_capacity(128 * (end - i) + 128);
        sql.push_str(&format!("INSERT INTO {} (id, cat, val, ts) VALUES ", table));
        for r in i..end {
            let id = r as i64;
            let cat = (rng.gen::<u32>() % 16) as i32;
            let val = rng.gen::<f64>() * 1000.0;
            let ts = 1_700_000_000i64 + (r as i64) + ((rng.gen::<u32>() % 10) as i64);
            if r > i { sql.push_str(", "); }
            sql.push_str(&format!("({},{},{:.6},{})", id, cat, val, ts));
        }
        let _ = ctx.exec_ok(&sql).unwrap();
        i = end;
    }
}

fn bench_sql_tables(c: &mut Criterion) {
    let ns = [100_000usize, 1_000_000usize];
    let mut group = c.benchmark_group("sql_tables");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(15);

    for &n in &ns {
        let ctx = BenchCtx::new().expect("bench ctx");
        let table = "clarium/public/bench_t";
        load_table(&ctx, table, n, 0xABCD_5678);

        // Filter benchmark: measure query engine path; keep output small
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("filter_val_gt", n.to_string()), &n, |b, _| {
            // Warm-up once
            let _ = ctx.exec(&format!("SELECT COUNT(*) AS cnt FROM {} WHERE val > 500.0", table)).unwrap();
            b.iter(|| {
                let _ = ctx.exec(&format!("SELECT COUNT(*) AS cnt FROM {} WHERE val > 500.0", table)).unwrap();
            });
        });

        // Group-by aggregation benchmark
        group.bench_with_input(BenchmarkId::new("groupby_cat_sum_val", n.to_string()), &n, |b, _| {
            let _ = ctx.exec(&format!("SELECT cat, SUM(val) AS s FROM {} GROUP BY cat", table)).unwrap();
            b.iter(|| {
                let _ = ctx.exec(&format!("SELECT cat, SUM(val) AS s FROM {} GROUP BY cat", table)).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sql_tables);
criterion_main!(benches);
