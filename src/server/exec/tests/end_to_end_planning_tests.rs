use crate::query::{self, Command};
use crate::server::exec::exec_select::run_select;
use crate::server::exec::tests::fixtures::*;

// End-to-end hybrid: ANN top-k docs + graph expansion + combined score
// Score = 0.7*semantic + 0.3*(1.0/cost_sum) with safe defaults when cost missing.
#[test]
fn hybrid_ann_plus_graph_expansion_with_fallbacks() {
    let tmp = tempfile::tempdir().unwrap();
    let store = new_store(&tmp);

    // Seed docs with embeddings and a vector index
    seed_docs_with_embeddings(&store, "clarium/public/docs");
    write_vindex_sidecar(&store, "clarium/public/idx_docs_body", "clarium/public/docs", "body_embed", "l2", 3);

    // Seed graph nodes/edges + sidecar (weighted)
    seed_tools_graph(&store, "clarium/public/know_nodes", "clarium/public/know_edges");
    write_graph_sidecar(&store, "clarium/public/know", "clarium/public/know_nodes", "clarium/public/know_edges");

    // ANN top-2 docs then join a 2-hop tool chain and compute combined score
    let sql = "WITH q AS (SELECT to_vec('[0.25,0,0]') v), \
                    top_docs AS ( \
                        SELECT id, body_embed \
                        FROM clarium/public/docs \
                        ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING ANN \
                        LIMIT 2 \
                    ), \
                    chains AS ( \
                        SELECT g.node_id, g.hop \
                        FROM graph_neighbors('clarium/public/know','planner','Calls',2) g \
                    ) \
               SELECT d.id, \
                      cosine_sim(d.body_embed, (SELECT v FROM q)) AS sem, \
                      1.0 as inv_cost, \
                      (0.7*cosine_sim(d.body_embed, (SELECT v FROM q)) + 0.3*1.0) AS score \
               FROM top_docs d \
               CROSS JOIN chains \
               ORDER BY score DESC, id ASC \
               LIMIT 2";

    let q = match query::parse(sql).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df = run_select(&store, &q).unwrap();
    // Should return 2 rows
    assert_eq!(df.height(), 2);
    // Columns present
    assert!(df.column("id").is_ok());
    assert!(df.column("score").is_ok());

    // Now force EXACT and ensure query still succeeds (fallback behavior)
    let sql_exact = "WITH q AS (SELECT to_vec('[0.25,0,0]') v), \
                    top_docs AS ( \
                        SELECT id, body_embed \
                        FROM clarium/public/docs \
                        ORDER BY vec_l2(clarium/public/docs.body_embed, (SELECT v FROM q)) USING EXACT \
                        LIMIT 2 \
                    ), \
                    chains AS ( \
                        SELECT g.node_id, g.hop \
                        FROM graph_neighbors('clarium/public/know','planner','Calls',2) g \
                    ) \
               SELECT d.id, \
                      cosine_sim(d.body_embed, (SELECT v FROM q)) AS sem, \
                      1.0 as inv_cost, \
                      (0.7*cosine_sim(d.body_embed, (SELECT v FROM q)) + 0.3*1.0) AS score \
               FROM top_docs d \
               CROSS JOIN chains \
               ORDER BY score DESC, id ASC \
               LIMIT 2";
    let q2 = match query::parse(sql_exact).unwrap() { Command::Select(q) => q, _ => unreachable!() };
    let df2 = run_select(&store, &q2).unwrap();
    assert_eq!(df2.height(), 2);
}
