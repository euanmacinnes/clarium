use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

mod bench_support;
use bench_support::BenchCtx;

fn gen_keys(n: usize, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n).map(|_| rng.gen::<u64>()).collect()
}

fn bench_sql_kv(c: &mut Criterion) {
    let ns = [100_000usize, 1_000_000usize];
    let mut group = c.benchmark_group("sql_kv");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(12);

    for &n in &ns {
        let ctx = BenchCtx::new().expect("bench ctx");
        // Use default database 'clarium' and a fixed store name
        let db = "clarium";
        let store = format!("kv_bench_{}", n);

        // Write sequential keys
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("put_seq", n.to_string()), &n, |b, &n| {
            b.iter(|| {
                for i in 0..n as u64 {
                    let sql = format!(
                        "WRITE KEY {} IN {}.store.{} = {} NO RESET",
                        i, db, store, (i ^ 0xDEAD_BEEFu64)
                    );
                    let _ = ctx.exec(&sql).unwrap();
                }
            });
        });

        // Random write keys
        group.bench_with_input(BenchmarkId::new("put_rand", n.to_string()), &n, |b, &n| {
            let keys = gen_keys(n, 0xBEEF_CAFE);
            b.iter(|| {
                for &k in &keys {
                    let sql = format!(
                        "WRITE KEY {} IN {}.store.{} = {} NO RESET",
                        k, db, store, k.rotate_left(17)
                    );
                    let _ = ctx.exec(&sql).unwrap();
                }
            });
        });

        // Prepare: ensure store has values for read/delete
        {
            let keys = gen_keys(n, 0xDEAD_BEEF);
            for &k in &keys {
                let sql = format!(
                    "WRITE KEY {} IN {}.store.{} = {} NO RESET",
                    k, db, store, (k ^ 0x9E37_79B9u64)
                );
                let _ = ctx.exec(&sql).unwrap();
            }
        }

        // Random reads
        group.bench_with_input(BenchmarkId::new("get_rand", n.to_string()), &n, |b, &n| {
            let keys = gen_keys(n, 0xFACE_FEED);
            b.iter(|| {
                let mut acc: u64 = 0;
                for &i in &keys {
                    let k = i; // pre-generated random
                    let sql = format!("READ KEY {} IN {}.store.{}", k, db, store);
                    if let Ok(v) = ctx.exec(&sql) {
                        if let Some(n) = v.get("value").and_then(|x| x.as_u64()) { acc ^= n; }
                    }
                }
                criterion::black_box(acc);
            });
        });

        // Delete-all (random order)
        group.bench_with_input(BenchmarkId::new("delete_rand", n.to_string()), &n, |b, &n| {
            let keys = gen_keys(n, 0xDE1E_7EAD);
            b.iter(|| {
                for &k in &keys {
                    let sql = format!("DROP KEY {} IN {}.store.{}", k, db, store);
                    let _ = ctx.exec(&sql).unwrap();
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sql_kv);
criterion_main!(benches);
