use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use polars::prelude::*;

fn gen_table(n: usize, seed: u64) -> DataFrame {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut id: Vec<i64> = Vec::with_capacity(n);
    let mut cat: Vec<u8> = Vec::with_capacity(n);
    let mut val: Vec<f64> = Vec::with_capacity(n);
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut t: i64 = 1_700_000_000; // epoch base
    for i in 0..n {
        id.push(i as i64);
        cat.push((rng.gen::<u32>() % 16) as u8);
        val.push(rng.gen::<f64>() * 1000.0);
        t += (rng.gen::<u32>() % 10) as i64; // small increments
        ts.push(t);
    }
    DataFrame::new(vec![
        Series::new("id", id).into(),
        Series::new("cat", cat).into(),
        Series::new("val", val).into(),
        Series::new("ts", ts).into(),
    ]).expect("df build")
}

fn bench_tables(c: &mut Criterion) {
    let ns = [100_000usize, 1_000_000usize];
    let mut group = c.benchmark_group("tables");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);

    for &n in &ns {
        let df = gen_table(n, 0xABCD_1234);

        // Scan + filter benchmark
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("filter_val_gt", n.to_string()), &n, |b, _| {
            b.iter(|| {
                let ser = df.column("val").unwrap();
                let mut mask: Vec<bool> = Vec::with_capacity(ser.len());
                for i in 0..ser.len() {
                    let keep = ser
                        .get(i)
                        .ok()
                        .and_then(|v| v.try_extract::<f64>().ok())
                        .map(|x| x > 500.0)
                        .unwrap_or(false);
                    mask.push(keep);
                }
                let mask_series = Series::new("__mask", mask);
                let _df2 = df.filter(mask_series.bool().unwrap()).unwrap();
            });
        });

        // Group-by aggregation benchmark
        group.bench_with_input(BenchmarkId::new("groupby_cat_sum_val", n.to_string()), &n, |b, _| {
            b.iter(|| {
                let lf = df.lazy();
                let out = lf
                    .group_by([col("cat")])
                    .agg([col("val").sum()])
                    .collect();
                let _ = out.unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_tables);
criterion_main!(benches);
