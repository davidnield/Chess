use book_builder_rs::core::{key_order, Core};
use book_builder_rs::dp::{build_curves, extract_book};
use std::collections::HashMap;

fn key(lo: i64, hi: i64) -> u128 {
    (u128::from(hi as u64) << 64) | u128::from(lo as u64)
}

#[test]
fn colliding_low_halves_get_independent_prescriptions_and_values() {
    // Black's two decisions after different first moves: e5 is best on the
    // first branch and d5 on the second. Each leaf also has a low-half twin.
    let a = key(-17, -3);
    let b = key(-17, 4);
    let root = key(71, 2);
    let leaves = [key(8, -1), key(9, -1), key(8, 1), key(9, 1)];
    let mut graph = Core {
        keys: vec![a, b, root], id: HashMap::from([(a, 0), (b, 1), (root, 2)]),
        n_our: 2, root, l_node: vec![0.25, 0.25], crack: vec![None; 2],
        c_ptr: vec![0, 2, 4], c_san: vec![0, 1, 0, 1], c_bare: vec![0, 1, 0, 1],
        c_key: leaves.iter().copied().map(Some).collect(), c_in: vec![false; 4],
        c_lv: vec![0.25; 4], const_base: vec![0.0], k_ptr: vec![0, 2],
        k_p: vec![0.75, 0.25], k_key: vec![Some(a), Some(b)], k_in: vec![true; 2],
        k_lv: vec![0.25; 2], f_ptr: vec![0, 0], node_reach: vec![0.75, 0.25, 1.0],
        set_order: vec![2, 1, 0], ..Core::default()
    };
    graph.link();
    let values: HashMap<_, _> = leaves.into_iter().zip([0.875, 0.125, 0.125, 0.625]).collect();
    graph.apply_stop_values(&HashMap::new(), &values);
    assert!(key_order(a) < key_order(b));
    assert_eq!(graph.stop_positions(), (vec![a, b], vec![leaves[0], leaves[2], leaves[1], leaves[3]]));
    let (curves, _) = build_curves(&graph, 2, 8);
    let order = graph.topo_down();
    let one = extract_book(&graph, &curves, 1, &order);
    assert_eq!(one.spent_distinct(), 1);
    assert_eq!(one.root_value_realized, Some(0.71875));
    let two = extract_book(&graph, &curves, 2, &order);
    let prescriptions: HashMap<_, _> = two.booked.iter()
        .map(|&(node, candidate)| (graph.keys[node as usize], graph.c_san[candidate])).collect();
    assert_eq!(prescriptions, HashMap::from([(a, 0), (b, 1)]));
    assert_eq!(two.root_value_realized, Some(0.8125));
}
