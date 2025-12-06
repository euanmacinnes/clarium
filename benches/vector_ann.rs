use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::cmp::Ordering;

#[cfg(feature = "ann_hnsw")]
use hnsw_rs::prelude::*;

#[derive(Clone, Copy)]
struct Config {
    n: usize,
    dim: usize,
    k: usize,
}

fn gen_data(n: usize, dim: usize, seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut v = vec![0f32; n * dim];
    for x in v.iter_mut() {
        *x = rng.gen::<f32>();
    }
    v
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

fn topk_flat(data: &[f32], dim: usize, q: &[f32], k: usize) -> Vec<(u32, f32)> {
    use std::collections::BinaryHeap;
    #[derive(PartialEq)]
    struct Item(u32, f32); // (id, dist)
    impl Eq for Item {}
    impl PartialOrd for Item {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.1.partial_cmp(&other.1)
        }
    }
    impl Ord for Item {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap_or(Ordering::Equal)
        }
    }
    let mut heap = BinaryHeap::with_capacity(k + 1);
    let rows = data.len() / dim;
    for r in 0..rows {
        let off = r * dim;
        let d = l2(&data[off..off + dim], q);
        if heap.len() < k {
            heap.push(Item(r as u32, -d)); // max-heap by negative distance → acts as min-heap
        } else if let Some(top) = heap.peek() {
            if -d > top.1 {
                let _ = heap.pop();
                heap.push(Item(r as u32, -d));
            }
        }
    }
    // drain and invert back
    let mut out: Vec<(u32, f32)> = heap.into_iter().map(|Item(id, nd)| (id, -nd)).collect();
    out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    out
}

#[cfg(feature = "ann_hnsw")]
fn build_hnsw(data: &[f32], dim: usize, m: usize, ef: usize) -> Hnsw<f32, DistL2> {
    let rows = data.len() / dim;
    let mut h = HnswBuilder::default().m(m).ef_construct(ef).num_elements(rows).build();
    for r in 0..rows {
        let off = r * dim;
        let v = &data[off..off + dim];
        h.insert((v, r)).expect("hnsw insert");
    }
    h.build();
    h
}

fn bench_vector_ann(c: &mut Criterion) {
    // Config matrix
    let ns = [10_000usize, 100_000usize];
    let dims = [64usize, 384usize, 768usize];
    let ks = [10usize, 100usize];

    let mut group = c.benchmark_group("vector_ann_l2");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);

    for &n in &ns {
        for &dim in &dims {
            // Generate base data once per (n, dim)
            let data = gen_data(n, dim, 0xC1A2_10FF);
            let q = gen_data(1, dim, 0xC1A2_20FF); // single query

            // Build HNSW once and reuse for different k (typical)
            #[cfg(feature = "ann_hnsw")]
            let hnsw = build_hnsw(&data, dim, 32, 200);

            for &k in &ks {
                let cfg = Config { n, dim, k };
                // Flat exact search benchmark
                group.throughput(Throughput::Elements(k as u64));
                group.bench_with_input(BenchmarkId::new("flat_l2", format!("n{}_d{}_k{}", n, dim, k)), &cfg, |b, cfg| {
                    b.iter(|| {
                        let _res = topk_flat(&data, cfg.dim, &q[..cfg.dim], cfg.k);
                    });
                });

                // HNSW ANN benchmark (if feature enabled)
                #[cfg(feature = "ann_hnsw")]
                {
                    group.bench_with_input(BenchmarkId::new("hnsw_l2", format!("n{}_d{}_k{}", n, dim, k)), &cfg, |b, cfg| {
                        b.iter(|| {
                            let _res = hnsw.search(&q[..cfg.dim], cfg.k);
                        });
                    });
                }
            }

            // Also include build-time benchmark for HNSW
            #[cfg(feature = "ann_hnsw")]
            group.bench_with_input(BenchmarkId::new("build_hnsw", format!("n{}_d{}", n, dim)), &dim, |b, &d| {
                b.iter(|| {
                    let _ = build_hnsw(&data, d, 32, 200);
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_vector_ann);
criterion_main!(benches);
