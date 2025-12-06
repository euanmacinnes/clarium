use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};
use polars::prelude::*;

fn gen_ts(n: usize, groups: usize, seed: u64) -> DataFrame {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut key: Vec<i32> = Vec::with_capacity(n);
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut val: Vec<f64> = Vec::with_capacity(n);
    let mut t = 1_700_000_000i64;
    for i in 0..n {
        key.push((i % groups) as i32);
        t += 1 + (rng.gen::<u32>() % 3) as i64; // mostly increasing
        ts.push(t);
        val.push(rng.gen::<f64>() * 10.0);
    }
    DataFrame::new(vec![
        Series::new("key", key).into(),
        Series::new("ts", ts).into(),
        Series::new("val", val).into(),
    ]).expect("df build")
}

fn bench_time_series(c: &mut Criterion) {
    let ns = [100_000usize, 1_000_000usize];
    let mut group = c.benchmark_group("time_series");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);

    for &n in &ns {
        let df = gen_ts(n, 64, 0x5151_5151);

        // Range filter by timestamp
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("range_filter", n.to_string()), &n, |b, _| {
            b.iter(|| {
                let ts = df.column("ts").unwrap();
                let t0 = 1_700_000_000i64 + (n as i64) / 3;
                let t1 = t0 + (n as i64) / 3;
                let mut mask: Vec<bool> = Vec::with_capacity(ts.len());
                for i in 0..ts.len() {
                    let keep = ts
                        .get(i)
                        .ok()
                        .and_then(|v| v.try_extract::<i64>().ok())
                        .map(|x| x >= t0 && x <= t1)
                        .unwrap_or(false);
                    mask.push(keep);
                }
                let mask_series = Series::new("__mask", mask);
                let _df2 = df.filter(mask_series.bool().unwrap()).unwrap();
            });
        });

        // Rolling mean per key (window size ~ 32)
        group.bench_with_input(BenchmarkId::new("rolling_mean", n.to_string()), &n, |b, _| {
            b.iter(|| {
                let lf = df
                    .clone()
                    .lazy()
                    .group_by_stable([col("key")])
                    .agg([col("val").rolling_mean(RollingOptionsImpl {
                        window_size: 32,
                        min_periods: 1,
                        weights: None,
                        center: false,
                        by: None,
                        closed_window: ClosedWindow::Both,
                        fn_params: None,
                    })]);
                let _ = lf.collect().unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_time_series);
criterion_main!(benches);
