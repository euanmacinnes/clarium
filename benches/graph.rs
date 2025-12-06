use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Clone)]
struct Graph {
    adj: Vec<Vec<u32>>, // adjacency list
}

fn gen_graph(n: usize, avg_degree: usize, seed: u64) -> Graph {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut adj: Vec<Vec<u32>> = vec![Vec::with_capacity(avg_degree); n];
    for u in 0..n {
        for _ in 0..avg_degree {
            let mut v = rng.gen::<usize>() % n;
            if v == u { v = (v + 1) % n; }
            adj[u].push(v as u32);
        }
        // optionally add reverse edges to simulate undirected
        for &v in adj[u].clone().iter() {
            let v = v as usize;
            if adj[v].len() < avg_degree * 2 {
                adj[v].push(u as u32);
            }
        }
    }
    Graph { adj }
}

fn bfs_k_hop(g: &Graph, src: u32, max_depth: u8) -> usize {
    use std::collections::VecDeque;
    let n = g.adj.len();
    let mut seen = vec![false; n];
    let mut q = VecDeque::new();
    q.push_back((src, 0u8));
    seen[src as usize] = true;
    let mut visited = 0usize;
    while let Some((u, d)) = q.pop_front() {
        visited += 1;
        if d >= max_depth { continue; }
        for &v in &g.adj[u as usize] {
            let vi = v as usize;
            if !seen[vi] {
                seen[vi] = true;
                q.push_back((v, d + 1));
            }
        }
    }
    visited
}

fn bench_graph(c: &mut Criterion) {
    let ns = [50_000usize, 200_000usize];
    let degree = 8usize;
    let mut group = c.benchmark_group("graph_bfs");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);

    for &n in &ns {
        let g = gen_graph(n, degree, 0xA11C_EC70);
        // BFS depth-2 and depth-3 from a fixed set of sources
        let sources: Vec<u32> = (0..100).map(|i| (i * (n / 100)) as u32).collect();
        group.throughput(Throughput::Elements((sources.len() as u64) * degree as u64));
        group.bench_with_input(BenchmarkId::new("bfs_depth2", n.to_string()), &n, |b, _| {
            b.iter(|| {
                let mut total = 0usize;
                for &s in &sources {
                    total += bfs_k_hop(&g, s, 2);
                }
                criterion::black_box(total);
            });
        });
        group.bench_with_input(BenchmarkId::new("bfs_depth3", n.to_string()), &n, |b, _| {
            b.iter(|| {
                let mut total = 0usize;
                for &s in &sources {
                    total += bfs_k_hop(&g, s, 3);
                }
                criterion::black_box(total);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_graph);
criterion_main!(benches);
