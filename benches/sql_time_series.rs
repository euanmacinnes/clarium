use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

mod bench_support;
use bench_support::BenchCtx;

fn load_ts(ctx: &BenchCtx, table: &str, n: usize, groups: usize, seed: u64) {
    let _ = ctx.exec_ok(&format!("CREATE TABLE {}", table)).unwrap();
    let mut rng = StdRng::seed_from_u64(seed);
    let batch = 1000usize;
    let mut i = 0usize;
    let mut t: i64 = 1_700_000_000;
    while i < n {
        let end = (i + batch).min(n);
        let mut sql = String::with_capacity(96 * (end - i) + 64);
        sql.push_str(&format!("INSERT INTO {} (key, ts, val) VALUES ", table));
        for r in i..end {
            let key = (r % groups) as i32;
            t += 1 + (rng.gen::<u32>() % 3) as i64;
            let val = rng.gen::<f64>() * 10.0;
            if r > i { sql.push_str(", "); }
            sql.push_str(&format!("({},{},{:.6})", key, t, val));
        }
        let _ = ctx.exec_ok(&sql).unwrap();
        i = end;
    }
}

fn bench_sql_time_series(c: &mut Criterion) {
    let ns = [100_000usize, 1_000_000usize];
    let mut group = c.benchmark_group("sql_time_series");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(15);

    for &n in &ns {
        let ctx = BenchCtx::new().expect("bench ctx");
        let table = "clarium/public/bench_ts";
        load_ts(&ctx, table, n, 64, 0x5151_7171);

        // Range filter by timestamp (count to minimize materialization)
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("range_filter", n.to_string()), &n, |b, _| {
            // choose a middle third window
            let t0 = 1_700_000_000i64 + (n as i64) / 3;
            let t1 = t0 + (n as i64) / 3;
            let warm = format!("SELECT COUNT(*) AS cnt FROM {} WHERE ts BETWEEN {} AND {}", table, t0, t1);
            let _ = ctx.exec(&warm).unwrap();
            b.iter(|| {
                let _ = ctx.exec(&warm).unwrap();
            });
        });

        // Group-by average per key (surrogate for rolling workload)
        group.bench_with_input(BenchmarkId::new("groupby_key_avg_val", n.to_string()), &n, |b, _| {
            let sql = format!("SELECT key, AVG(val) AS avg_val FROM {} GROUP BY key", table);
            let _ = ctx.exec(&sql).unwrap();
            b.iter(|| {
                let _ = ctx.exec(&sql).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sql_time_series);
criterion_main!(benches);
