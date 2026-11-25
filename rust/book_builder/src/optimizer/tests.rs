use super::*;
use std::collections::HashSet;
fn limits() -> exact::Limits {
    exact::Limits {
        milliseconds: 10000,
        states: 1_000_000,
        max_nodes: 100,
        max_variables: 1000,
    }
}
#[test]
fn diamond_unique_budget_and_mip_oracle_agree() {
    let g = fixtures::diamond().core().unwrap();
    let v = View::new(&g).unwrap();
    for (b, want) in [(0, 0.2), (1, 0.2), (2, 0.55), (3, 0.9)] {
        let o = exact::enumerate(&v, b, &limits()).unwrap();
        assert_eq!(o.status, "oracle-optimal");
        assert!((o.value - want).abs() < 1e-10);
        let m = exact::mip(&v, b, &Book::new(), None, &limits()).unwrap();
        assert_eq!(m.status, "mip-ProvenOptimal");
        assert!((m.value - want).abs() < 1e-7);
    }
    assert!(exact::tree_values(&v, 3, 1000).is_err());
}
#[test]
fn full_tree_tables_keep_delayed_payoff() {
    let g = fixtures::chain(12).core().unwrap();
    let v = View::new(&g).unwrap();
    let t = exact::tree_values(&v, 13, 1000).unwrap();
    assert!((t[12] - 0.55).abs() < 1e-10);
    assert!((t[13] - 0.95).abs() < 1e-10);
    let start = Book::from([(0, 1)]);
    let free = (0..g.n_our as u32).collect::<HashSet<_>>();
    let m = exact::mip(&v, 13, &start, Some(&free), &limits()).unwrap();
    assert!((m.value - 0.95).abs() < 1e-7);
}
#[test]
fn large_chain_and_real_256_package_cap_are_exercised() {
    let g = fixtures::chain(24).core().unwrap();
    let v = View::new(&g).unwrap();
    let t = exact::tree_values(&v, 25, 2000).unwrap();
    assert!((t[24] - 0.55).abs() < 1e-10);
    assert!((t[25] - 0.95).abs() < 1e-10);
    let m = exact::mip(&v, 25, &Book::new(), None, &limits()).unwrap();
    assert_eq!(m.status, "mip-ProvenOptimal");
    assert!((m.value - t[25]).abs() < 1e-7);
    let g = fixtures::wide_tree(320).core().unwrap();
    let v = View::new(&g).unwrap();
    let exact = exact::tree_values(&v, 300, 200_000).unwrap();
    let (a, pa) = search::dp_baseline(&v, 300, 256).unwrap();
    let (b, pb) = search::dp_baseline(&v, 300, 512).unwrap();
    assert!(pa.binding > 0 && pa.merged_packages > 0 && pa.largest_input > 256);
    assert_eq!(pb.binding, 0);
    assert!(a.value <= exact[300] + 1e-9 && b.value <= exact[300] + 1e-9);
}
#[test]
fn rare_positive_reach_cannot_drop_a_fixed_prescription() {
    let s = fixtures::snapshot(
        vec![
            fixtures::node(
                0,
                false,
                0.,
                vec![
                    fixtures::edge(0, Some(1), 0., 1e-12),
                    fixtures::edge(0, None, 0.6, 1. - 1e-12),
                ],
            ),
            fixtures::node(1, true, 0.2, vec![fixtures::edge(1, None, 0.9, 1.)]),
        ],
        0,
        "tiny-positive-fixed-reach",
    );
    let g = s.core().unwrap();
    let v = View::new(&g).unwrap();
    let base = Book::from([(0, 1)]);
    let m = exact::mip(&v, 1, &base, Some(&HashSet::new()), &limits()).unwrap();
    assert_eq!(m.book, base);
}
#[test]
fn native_random_dags_against_independent_oracle() {
    for seed in 0..24 {
        let g = fixtures::random(seed, 10).core().unwrap();
        let v = View::new(&g).unwrap();
        for b in [1, 3, 5] {
            let o = exact::enumerate(&v, b, &limits()).unwrap();
            assert_eq!(o.status, "oracle-optimal");
            let m = exact::mip(&v, b, &Book::new(), None, &limits()).unwrap();
            assert_eq!(m.status, "mip-ProvenOptimal", "seed {seed}");
            assert!(
                (m.value - o.value).abs() < 1e-7,
                "seed {seed}, budget {b}: MIP {} oracle {}",
                m.value,
                o.value
            );
            let p = search::price_baseline(&v, b).unwrap();
            let (d, _) = search::dp_baseline(&v, b, 256).unwrap();
            let portfolio = search::polish_starts(&v, &[p.book, d.book], b).unwrap();
            assert!(portfolio.value + 1e-10 >= p.value.max(d.value));
            assert!(portfolio.value <= o.value + 1e-9);
        }
    }
}
#[test]
fn fixed_branch_can_release_unreachable_prescriptions() {
    let g = fixtures::chain(3).core().unwrap();
    let v = View::new(&g).unwrap();
    // Fixed descendants need not be learned when root changes to a terminal move.
    let base = Book::from([(0, 2), (1, 3), (2, 3)]);
    let free = HashSet::from([0]);
    let m = exact::mip(&v, 3, &base, Some(&free), &limits()).unwrap();
    assert_eq!(m.book, Book::from([(0, 1)]));
}
#[test]
fn released_branch_budget_funds_a_new_continuation() {
    let s = fixtures::snapshot(
        vec![
            fixtures::node(
                0,
                true,
                0.1,
                vec![
                    fixtures::edge(1, Some(1), 0., 1.),
                    fixtures::edge(2, Some(4), 0., 1.),
                ],
            ),
            fixtures::node(1, true, 0.1, vec![fixtures::edge(1, Some(2), 0., 1.)]),
            fixtures::node(2, true, 0.1, vec![fixtures::edge(1, Some(3), 0., 1.)]),
            fixtures::node(3, true, 0.1, vec![fixtures::edge(1, None, 0.6, 1.)]),
            fixtures::node(4, true, 0.1, vec![fixtures::edge(1, Some(5), 0., 1.)]),
            fixtures::node(5, true, 0.1, vec![fixtures::edge(1, None, 0.9, 1.)]),
        ],
        0,
        "released-budget",
    );
    let g = s.core().unwrap();
    let v = View::new(&g).unwrap();
    let base = Book::from([(0, 1), (1, 1), (2, 1), (3, 1)]);
    let m = exact::mip(&v, 4, &base, Some(&HashSet::from([0, 4, 5])), &limits()).unwrap();
    assert!((m.value - 0.9).abs() < 1e-7);
    assert_eq!(m.book, Book::from([(0, 2), (4, 1), (5, 1)]));
}
#[test]
fn full_keys_snapshot_and_tamper_rejection() {
    let s = fixtures::diamond();
    let g = s.core().unwrap();
    let copy = snapshot::Snapshot::from_core(&g, s.provenance.clone()).unwrap();
    let g2 = copy.core().unwrap();
    assert_eq!(g.keys, g2.keys);
    assert!(g.keys.iter().all(|&k| k > u64::MAX as u128));
    let mut bad = s.clone();
    bad.nodes[1].key = bad.nodes[0].key.clone();
    assert!(bad.core().is_err());
    let mut bad = s.clone();
    bad.nodes[0].edges[0].probability = f64::NAN;
    assert!(bad.core().is_err());
    let mut bad = s;
    bad.nodes[0].edges[0].probability = 1.;
    assert!(bad.core().is_err());
}
#[test]
fn cycle_convention_matches_and_is_not_tree_exactness() {
    let s = fixtures::snapshot(
        vec![
            fixtures::node(0, true, 0.3, vec![fixtures::edge(1, Some(1), 0., 1.)]),
            fixtures::node(1, false, 0., vec![fixtures::edge(0, Some(0), 0., 1.)]),
        ],
        0,
        "cycle",
    );
    let g = s.core().unwrap();
    let v = View::new(&g).unwrap();
    assert!(v.cuts > 0);
    let o = exact::enumerate(&v, 1, &limits()).unwrap();
    let m = exact::mip(&v, 1, &Book::new(), None, &limits()).unwrap();
    assert!((m.value - o.value).abs() < 1e-7);
    assert!(exact::tree_values(&v, 1, 100).is_err());
}
#[test]
fn limited_oracle_does_not_claim_optimality() {
    let g = fixtures::chain(8).core().unwrap();
    let v = View::new(&g).unwrap();
    let mut l = limits();
    l.states = 1;
    let o = exact::enumerate(&v, 9, &l).unwrap();
    assert_eq!(o.status, "oracle-limited");
    assert!(o.bound.unwrap() >= 0.95);
}
#[test]
fn cap_instrumentation_is_observational() {
    let c = crate::dp::Curve::new(0., vec![1; 5], vec![0.5, 0.4, 0.3, 0.2, 0.1]);
    let a = crate::dp::cap_curve(c.clone(), 5, 2);
    let (b, p) = search::cap_profile(|| crate::dp::cap_curve(c, 5, 2));
    assert_eq!(a.gains, b.gains);
    assert_eq!(p.binding, 1);
    assert_eq!(p.merged_packages, 3);
}
#[test]
fn duplicate_replies_and_zero_mass_are_not_extra_budget() {
    let s = fixtures::snapshot(
        vec![
            fixtures::node(
                0,
                false,
                0.,
                vec![
                    fixtures::edge(0, Some(1), 0., 0.4),
                    fixtures::edge(0, Some(1), 0., 0.6),
                    fixtures::edge(0, Some(2), 0., 0.),
                ],
            ),
            fixtures::node(1, true, 0.2, vec![fixtures::edge(1, None, 0.8, 1.)]),
            fixtures::node(2, true, 0.3, vec![fixtures::edge(1, None, 1., 1.)]),
        ],
        0,
        "duplicate-replies",
    );
    let g = s.core().unwrap();
    let v = View::new(&g).unwrap();
    let m = exact::mip(&v, 1, &Book::new(), None, &limits()).unwrap();
    assert!((m.value - 0.8).abs() < 1e-7);
    assert_eq!(m.book.len(), 1);
}
#[test]
fn cone_snapshot_records_boundary_and_remains_evaluable() {
    let g = fixtures::chain(12).core().unwrap();
    let v = View::new(&g).unwrap();
    let cone = snapshot::extract(&v, 0, &Book::new(), 4, 3).unwrap();
    assert_eq!(cone.nodes.len(), 4);
    assert!(cone.provenance["boundary"].contains("local cone"));
    let c = cone.core().unwrap();
    let cv = View::new(&c).unwrap();
    let m = exact::mip(&cv, 4, &Book::new(), None, &limits()).unwrap();
    assert!((m.value - 0.55).abs() < 1e-7);
}
#[test]
fn resource_limits_and_invalid_inputs_fail_closed() {
    let g = fixtures::diamond().core().unwrap();
    let v = View::new(&g).unwrap();
    let mut l = limits();
    l.max_nodes = 1;
    assert!(exact::mip(&v, 3, &Book::new(), None, &l)
        .err()
        .unwrap()
        .contains("resource limit"));
    assert!(search::dp_baseline(&v, 3, 0).is_err());
    assert!(v.evaluate(&Book::from([(0, 999)]), 3).is_err());
    l.milliseconds = 0;
    assert!(l.validate().is_err());
    let mut broken = g.clone();
    broken.c_node[0] = 100000;
    assert!(View::new(&broken).is_err());
}
#[test]
fn native_plan_covers_both_colors_gates_and_budgets() {
    let p = benchmark::study_plan();
    let runs = p["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 8);
    for color in ["white", "black"] {
        for gate in ["stricter", "permissive"] {
            for budget in [500, 2000] {
                assert_eq!(
                    runs.iter()
                        .filter(|r| r["color"] == color
                            && r["gate"] == gate
                            && r["budget"] == budget)
                        .count(),
                    1
                );
            }
        }
    }
    assert_eq!(p["changes_main_defaults"], false);
}
#[test]
fn neighborhood_sees_unselected_long_alternative() {
    let g = fixtures::chain(12).core().unwrap();
    let v = View::new(&g).unwrap();
    let cfg = search::NeighborhoodConfig {
        width: 20,
        rounds: 1,
        discovery_nodes: 100,
        limits: limits(),
    };
    let mut n = 0;
    let o = search::improve(&v, 13, &Book::from([(0, 1)]), &cfg, |_| {
        n += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(n, 1);
    assert!((o.value - 0.95).abs() < 1e-7);
}
#[test]
fn interrupted_neighborhood_resumes_at_completed_boundary() {
    let g = fixtures::chain(12).core().unwrap();
    let v = View::new(&g).unwrap();
    let cfg = search::NeighborhoodConfig {
        width: 8,
        rounds: 4,
        discovery_nodes: 100,
        limits: limits(),
    };
    let base = Book::from([(0, 1)]);
    let mut saved = Vec::new();
    let interrupted = search::improve(&v, 13, &base, &cfg, |step| {
        saved.push(step.clone());
        Err("test interruption after durable step".into())
    });
    assert!(interrupted.is_err());
    let prior = &saved[0];
    let seed = if prior.accepted {
        &prior.outcome.book
    } else {
        &base
    };
    let resumed = search::improve_from(&v, 13, seed, &cfg, 1, |_| Ok(())).unwrap();
    let full = search::improve(&v, 13, &base, &cfg, |_| Ok(())).unwrap();
    assert_eq!(resumed.book, full.book);
    assert_eq!(resumed.value, full.value);
}
#[test]
fn checkpoints_resume_and_reject_recipe_drift() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("optimizer-test-{}", std::process::id()));
    // Unique test-owned directory; leave failures for inspection.
    assert!(!dir.exists());
    let mut cfg = benchmark::Config::default();
    cfg.budget = 3;
    cfg.caps = vec![256];
    cfg.neighborhood.rounds = 1;
    let snap = fixtures::diamond();
    let a = benchmark::run(&snap, &cfg, &dir, "test-code").unwrap();
    let b = benchmark::run(&snap, &cfg, &dir, "test-code").unwrap();
    assert_eq!(snapshot::digest(&a).unwrap(), snapshot::digest(&b).unwrap());
    // A torn or altered completed cell must fail, never be silently recomputed.
    let cell = dir.join("price.json");
    let original = std::fs::read(&cell).unwrap();
    let mut corrupt: serde_json::Value = serde_json::from_slice(&original).unwrap();
    corrupt["payload"]["outcome"]["value"] = serde_json::json!(0.123);
    std::fs::write(&cell, serde_json::to_vec(&corrupt).unwrap()).unwrap();
    assert!(benchmark::run(&snap, &cfg, &dir, "test-code")
        .err()
        .unwrap()
        .contains("digest mismatch"));
    std::fs::write(&cell, original).unwrap();
    cfg.budget = 2;
    assert!(benchmark::run(&snap, &cfg, &dir, "test-code")
        .err()
        .unwrap()
        .contains("identity drift"));
    assert!(dir.canonicalize().unwrap().starts_with(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .canonicalize()
            .unwrap()
    ));
    std::fs::remove_dir_all(&dir).unwrap();
}
