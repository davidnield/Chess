use super::snapshot::{hex, Edge, Node, Snapshot};
use std::collections::BTreeMap;
fn key(i: usize) -> String {
    hex((1u128 << 96) + (i as u128 + 1))
}
pub fn edge(san: i64, child: Option<usize>, leaf: f64, p: f64) -> Edge {
    Edge {
        san,
        child: child.map(key),
        leaf,
        probability: p,
    }
}
pub fn node(i: usize, ours: bool, stop: f64, edges: Vec<Edge>) -> Node {
    Node {
        key: key(i),
        ours,
        stop,
        constant: 0.,
        reach: 1.,
        edges,
    }
}
pub fn snapshot(nodes: Vec<Node>, root: usize, label: &str) -> Snapshot {
    Snapshot {
        schema: 1,
        root: key(root),
        set_order: nodes.iter().map(|n| n.key.clone()).collect(),
        nodes,
        price_source: BTreeMap::new(),
        provenance: BTreeMap::from([
            ("fixture".into(), label.into()),
            (
                "boundary".into(),
                "explicit-leaves-fixed-topological-cuts".into(),
            ),
        ]),
    }
}
/// Two opponent branches share a prescription, which must cost once.
pub fn diamond() -> Snapshot {
    snapshot(
        vec![
            node(
                0,
                false,
                0.,
                vec![edge(0, Some(1), 0., 0.5), edge(0, Some(2), 0., 0.5)],
            ),
            node(1, true, 0.2, vec![edge(1, Some(3), 0., 1.)]),
            node(2, true, 0.2, vec![edge(1, Some(3), 0., 1.)]),
            node(3, false, 0., vec![edge(0, Some(4), 0., 1.)]),
            node(4, true, 0.2, vec![edge(1, None, 0.9, 1.)]),
        ],
        0,
        "shared-diamond",
    )
}
/// An early alternative only wins when its entire chain is prepared.
pub fn chain(length: usize) -> Snapshot {
    let mut nodes = vec![node(
        0,
        true,
        0.2,
        vec![edge(1, None, 0.55, 1.), edge(2, Some(1), 0.2, 1.)],
    )];
    for i in 1..=length {
        nodes.push(node(
            i,
            true,
            0.2,
            vec![edge(
                3,
                if i == length { None } else { Some(i + 1) },
                0.95,
                1.,
            )],
        ));
    }
    snapshot(nodes, 0, "delayed-payoff")
}
/// Independent one-decision branches with distinct slopes: >256 packages at root.
pub fn wide_tree(width: usize) -> Snapshot {
    let mut nodes = vec![node(
        0,
        false,
        0.,
        (1..=width)
            .map(|i| edge(0, Some(i), 0., 1. / width as f64))
            .collect(),
    )];
    for i in 1..=width {
        nodes.push(node(
            i,
            true,
            0.2,
            vec![edge(1, None, 0.4 + 0.5 * i as f64 / width as f64, 1.)],
        ));
    }
    snapshot(nodes, 0, "wide-tree-cap-binding")
}
/// Deterministic small DAGs, alternating sides, with same-side terminal edges allowed.
pub fn random(seed: u64, n: usize) -> Snapshot {
    let mut state = seed;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let mut nodes = Vec::new();
    for i in 0..n {
        let ours = (i + seed as usize) % 2 == 0;
        let stop = 0.1 + (next() % 800) as f64 / 1000.;
        let count = 1 + (next() % 3) as usize;
        let mut edges = Vec::new();
        for j in 0..count {
            let child = if i + 1 < n && next() % 4 != 0 {
                Some(i + 1 + (next() as usize % (n - i - 1)))
            } else {
                None
            };
            let leaf = 0.1 + (next() % 800) as f64 / 1000.;
            edges.push(edge(
                j as i64,
                child,
                leaf,
                if ours { 1. } else { 1. / count as f64 },
            ));
        }
        nodes.push(node(i, ours, stop, edges));
    }
    snapshot(nodes, 0, &format!("random-{seed}-{n}"))
}
