use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashMap;

fn gen_keys(n: usize, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n).map(|_| rng.gen::<u64>()).collect()
}

fn bench_kv(c: &mut Criterion) {
    let ns = [100_000usize, 1_000_000usize];
    let mut group = c.benchmark_group("kv_hashmap");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);

    for &n in &ns {
        // Sequential put
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("put_seq", n.to_string()), &n, |b, &n| {
            b.iter(|| {
                let mut m: HashMap<u64, u64> = HashMap::with_capacity(n * 2);
                for i in 0..n as u64 { m.insert(i, i ^ 0xDEAD_BEEF); }
                criterion::black_box(&m);
            });
        });

        // Random put
        group.bench_with_input(BenchmarkId::new("put_rand", n.to_string()), &n, |b, &n| {
            let keys = gen_keys(n, 0xBEEF_CAFE);
            b.iter(|| {
                let mut m: HashMap<u64, u64> = HashMap::with_capacity(n * 2);
                for &k in &keys { let _ = m.insert(k, k.rotate_left(17)); }
                criterion::black_box(&m);
            });
        });

        // Build once for get/delete benchmarks
        let keys = gen_keys(n, 0xDEAD_BEEF);
        let mut base: HashMap<u64, u64> = HashMap::with_capacity(n * 2);
        for &k in &keys { base.insert(k, k ^ 0x9E37_79B9); }

        // Random get
        group.bench_with_input(BenchmarkId::new("get_rand", n.to_string()), &n, |b, _| {
            let idxs = gen_keys(n, 0xFACE_FEED);
            b.iter(|| {
                let mut sum = 0u64;
                for &i in &idxs {
                    let k = keys[(i as usize) % keys.len()];
                    if let Some(v) = base.get(&k) { sum ^= *v; }
                }
                criterion::black_box(sum);
            });
        });

        // Delete-all
        group.bench_with_input(BenchmarkId::new("delete_all", n.to_string()), &n, |b, &n| {
            b.iter(|| {
                let mut m = base.clone();
                for &k in &keys { let _ = m.remove(&k); }
                criterion::black_box(&m);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_kv);
criterion_main!(benches);
