//! Portable bounded graph snapshots. Full keys are hex strings, never JSON floats.
use super::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub schema: u32,
    pub provenance: BTreeMap<String, String>,
    pub root: String,
    pub set_order: Vec<String>,
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub price_source: BTreeMap<String, i64>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    pub key: String,
    pub ours: bool,
    pub stop: f64,
    pub constant: f64,
    pub reach: f64,
    pub edges: Vec<Edge>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub san: i64,
    pub probability: f64,
    pub child: Option<String>,
    pub leaf: f64,
}
pub fn hex(k: u128) -> String {
    format!("{k:032x}")
}
fn key(s: &str) -> Result<u128> {
    if s.len() != 32 {
        return Err("keys must have 32 hex digits".into());
    }
    u128::from_str_radix(s, 16).map_err(|e| e.to_string())
}
pub fn digest<T: Serialize>(x: &T) -> Result<String> {
    let canonical = serde_json::to_value(x).map_err(|e| e.to_string())?;
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&canonical).map_err(|e| e.to_string())?)
    ))
}
/// Streaming semantic fingerprint: no second graph-sized JSON/dataframe copy.
/// The decoder is mandatory at the Python boundary, whose keys are run-local IDs.
pub fn core_digest(g: &Core, mut decode: impl FnMut(u128) -> Result<u128>) -> Result<String> {
    validate(g)?;
    let mut h = Sha256::new();
    h.update(b"optimizer-core-v1");
    h.update((g.n() as u64).to_le_bytes());
    h.update((g.n_our as u64).to_le_bytes());
    h.update(g.root_id().to_le_bytes());
    for &k in &g.keys {
        h.update(decode(k)?.to_le_bytes());
    }
    for &u in &g.set_order {
        h.update(u.to_le_bytes());
    }
    for xs in [
        &g.l_node,
        &g.const_base,
        &g.node_reach,
        &g.c_lv,
        &g.k_p,
        &g.k_lv,
    ] {
        h.update((xs.len() as u64).to_le_bytes());
        for &x in xs {
            h.update(x.to_bits().to_le_bytes());
        }
    }
    for xs in [&g.c_ptr, &g.k_ptr] {
        h.update((xs.len() as u64).to_le_bytes());
        for &x in xs {
            h.update((x as u64).to_le_bytes());
        }
    }
    for xs in [&g.c_san, &g.c_bare] {
        h.update((xs.len() as u64).to_le_bytes());
        for &x in xs {
            h.update(x.to_le_bytes());
        }
    }
    for xs in [&g.c_node, &g.k_node] {
        h.update((xs.len() as u64).to_le_bytes());
        for &x in xs {
            h.update(x.to_le_bytes());
        }
    }
    Ok(format!("{:x}", h.finalize()))
}
impl Snapshot {
    pub fn from_core(g: &Core, provenance: BTreeMap<String, String>) -> Result<Self> {
        if !g.sort_aliases.is_empty() {
            return Err("snapshot requires decoded full keys, not run-local aliases".into());
        }
        validate(g)?;
        Ok(Self {
            schema: 1,
            provenance,
            root: hex(g.root),
            price_source: BTreeMap::new(),
            set_order: g
                .set_order
                .iter()
                .map(|&u| hex(g.keys[u as usize]))
                .collect(),
            nodes: (0..g.n())
                .map(|u| Node {
                    key: hex(g.keys[u]),
                    ours: u < g.n_our,
                    stop: if u < g.n_our { g.l_node[u] } else { 0. },
                    constant: if u >= g.n_our {
                        g.const_base[u - g.n_our]
                    } else {
                        0.
                    },
                    reach: g.node_reach[u],
                    edges: if u < g.n_our {
                        g.cands(u)
                            .map(|j| Edge {
                                san: g.c_san[j],
                                probability: 1.,
                                child: if g.c_node[j] != NONE {
                                    Some(hex(g.keys[g.c_node[j] as usize]))
                                } else {
                                    None
                                },
                                leaf: g.c_lv[j],
                            })
                            .collect()
                    } else {
                        g.kids(u)
                            .map(|j| Edge {
                                san: 0,
                                probability: g.k_p[j],
                                child: if g.k_node[j] != NONE {
                                    Some(hex(g.keys[g.k_node[j] as usize]))
                                } else {
                                    None
                                },
                                leaf: g.k_lv[j],
                            })
                            .collect()
                    },
                })
                .collect(),
        })
    }
    pub fn core(&self) -> Result<Core> {
        if self.schema != 1 {
            return Err("unsupported snapshot schema".into());
        }
        let mut nodes: Vec<_> = self.nodes.iter().collect();
        nodes.sort_by_key(|n| !n.ours); // stable: preserve each side's order
        let keys: Vec<_> = nodes.iter().map(|n| key(&n.key)).collect::<Result<_>>()?;
        if keys.len() > u32::MAX as usize {
            return Err("too many nodes".into());
        }
        let id: HashMap<_, _> = keys
            .iter()
            .enumerate()
            .map(|(i, &k)| (k, i as u32))
            .collect();
        if id.len() != keys.len() {
            return Err("duplicate full position key".into());
        }
        let order = self
            .set_order
            .iter()
            .map(|k| {
                id.get(&key(k)?)
                    .copied()
                    .ok_or_else(|| "unknown order key".into())
            })
            .collect::<Result<Vec<_>>>()?;
        let n_our = nodes.iter().filter(|n| n.ours).count();
        let mut g = Core {
            root: key(&self.root)?,
            keys,
            id,
            set_order: order,
            n_our,
            c_ptr: vec![0],
            k_ptr: vec![0],
            f_ptr: vec![0],
            ..Core::default()
        };
        for n in nodes {
            g.node_reach.push(n.reach);
            if n.ours {
                g.l_node.push(n.stop);
                g.crack.push(None);
                g.inc_bare.push(-1);
            } else {
                g.const_base.push(n.constant);
            }
            for e in &n.edges {
                let k = e.child.as_deref().map(key).transpose()?;
                if k.is_some_and(|k| !g.id.contains_key(&k)) {
                    return Err("child absent from snapshot; use explicit boundary value".into());
                }
                if n.ours {
                    g.c_san.push(e.san);
                    g.c_bare.push(e.san);
                    g.c_key.push(k);
                    g.c_in.push(k.is_some());
                    g.c_lv.push(e.leaf);
                } else {
                    g.k_p.push(e.probability);
                    g.k_key.push(k);
                    g.k_in.push(k.is_some());
                    g.k_lv.push(e.leaf);
                }
            }
            if n.ours {
                g.c_ptr.push(g.c_san.len());
            } else {
                g.k_ptr.push(g.k_p.len());
                g.f_ptr.push(0);
            }
        }
        g.link();
        validate(&g)?;
        Ok(g)
    }
    pub fn source_book(&self, g: &Core) -> Result<Book> {
        self.price_source
            .iter()
            .map(|(k, &s)| {
                let u =
                    g.id.get(&key(k)?)
                        .copied()
                        .ok_or_else(|| "source position absent".to_string())?;
                if !g.is_our(u) || !g.cands(u as usize).any(|j| g.c_san[j] == s) {
                    return Err("source move not admitted".into());
                }
                Ok((u, s))
            })
            .collect()
    }
}

/// Bounded calibration cone. Outside values are fixed to the supplied policy, so
/// its optimum/bound is LOCAL to this boundary, never a full-repertoire bound.
pub fn extract(
    v: &View<'_>,
    root: u32,
    base: &Book,
    max_nodes: usize,
    max_depth: usize,
) -> Result<Snapshot> {
    if !v.g.sort_aliases.is_empty() {
        return Err("decode run-local aliases before exporting a calibration cone".into());
    }
    extract_decoded(v, root, base, max_nodes, max_depth, Ok)
}

/// Export only the bounded cone, decoding identities through the original registry.
/// The parent graph stays borrowed; no full-key copy of it is constructed.
pub fn extract_decoded(
    v: &View<'_>,
    root: u32,
    base: &Book,
    max_nodes: usize,
    max_depth: usize,
    mut decode: impl FnMut(u128) -> Result<u128>,
) -> Result<Snapshot> {
    use std::collections::VecDeque;
    if root as usize >= v.g.n() || max_nodes == 0 || max_depth == 0 {
        return Err("invalid calibration cone limits/root".into());
    }
    let parent_sha = core_digest(v.g, &mut decode)?;
    v.evaluate(base, usize::MAX)?;
    let boundary = v.independent_values(base);
    let mut selected = HashSet::from([root]);
    let mut queue = VecDeque::from([(root, 0)]);
    while let Some((u, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        let children: Vec<_> = if v.g.is_our(u) {
            v.g.cands(u as usize).map(|j| v.candidate(u, j)).collect()
        } else {
            v.g.kids(u as usize).map(|j| v.reply(u, j)).collect()
        };
        for c in children {
            if let Child::Node(c) = c {
                if !selected.contains(&c) && selected.len() < max_nodes {
                    selected.insert(c);
                    queue.push_back((c, depth + 1));
                }
            }
        }
    }
    let full_keys: BTreeMap<u32, String> = selected
        .iter()
        .map(|&u| Ok((u, hex(decode(v.g.keys[u as usize])?))))
        .collect::<Result<_>>()?;
    let edge = |san, p, c| match c {
        Child::Node(c) if selected.contains(&c) => Edge {
            san,
            probability: p,
            child: Some(full_keys[&c].clone()),
            leaf: boundary[c as usize],
        },
        Child::Node(c) => Edge {
            san,
            probability: p,
            child: None,
            leaf: boundary[c as usize],
        },
        Child::Terminal(x) => Edge {
            san,
            probability: p,
            child: None,
            leaf: x,
        },
    };
    let nodes = selected
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|u| {
            let ours = v.g.is_our(u);
            Node {
                key: full_keys[&u].clone(),
                ours,
                stop: if ours { v.g.l_node[u as usize] } else { 0. },
                constant: if ours {
                    0.
                } else {
                    v.g.const_base[u as usize - v.g.n_our]
                },
                reach: v.g.node_reach[u as usize],
                edges: if ours {
                    v.g.cands(u as usize)
                        .map(|j| edge(v.g.c_san[j], 1., v.candidate(u, j)))
                        .collect()
                } else {
                    v.g.kids(u as usize)
                        .map(|j| edge(0, v.g.k_p[j], v.reply(u, j)))
                        .collect()
                },
            }
        })
        .collect();
    Ok(Snapshot {
        schema: 1,
        root: full_keys[&root].clone(),
        nodes,
        price_source: BTreeMap::new(),
        set_order: v
            .g
            .set_order
            .iter()
            .filter(|u| selected.contains(u))
            .map(|&u| full_keys[&u].clone())
            .collect(),
        provenance: BTreeMap::from([
            ("parent_graph".into(), parent_sha),
            ("boundary_policy".into(), digest(base)?),
            (
                "boundary".into(),
                "frozen-policy-values; local cone optimum only; original cycle cuts".into(),
            ),
            ("max_nodes".into(), max_nodes.to_string()),
            ("max_depth".into(), max_depth.to_string()),
        ]),
    })
}

pub fn validate(g: &Core) -> Result<()> {
    let n = g.n();
    let no = g.n_our;
    if n == 0 || no > n || g.root_id() == NONE || g.id.len() != n {
        return Err("invalid graph/root".into());
    }
    if g.stick {
        return Err("optimizer study requires stickiness off".into());
    }
    if g.keys
        .iter()
        .enumerate()
        .any(|(i, k)| g.id.get(k) != Some(&(i as u32)))
    {
        return Err("key index mismatch".into());
    }
    if g.set_order.len() != n
        || g.set_order.iter().copied().collect::<HashSet<_>>().len() != n
        || g.set_order.iter().any(|&x| x as usize >= n)
    {
        return Err("order is not a node permutation".into());
    }
    let ptr = |p: &[usize], rows: usize, len: usize| {
        p.len() == rows + 1
            && p.first() == Some(&0)
            && p.last() == Some(&len)
            && p.windows(2).all(|w| w[0] <= w[1])
    };
    if !ptr(&g.c_ptr, no, g.c_san.len()) || !ptr(&g.k_ptr, n - no, g.k_p.len()) {
        return Err("invalid CSR".into());
    }
    if g.l_node.len() != no
        || g.const_base.len() != n - no
        || g.node_reach.len() != n
        || g.crack.len() != no
        || g.inc_bare.len() != no
    {
        return Err("node column length mismatch".into());
    }
    let nc = g.c_san.len();
    let nk = g.k_p.len();
    if [
        g.c_node.len(),
        g.c_lv.len(),
        g.c_key.len(),
        g.c_in.len(),
        g.c_bare.len(),
    ]
    .iter()
    .any(|&x| x != nc)
        || [g.k_node.len(), g.k_lv.len(), g.k_key.len(), g.k_in.len()]
            .iter()
            .any(|&x| x != nk)
    {
        return Err("edge column length mismatch".into());
    }
    if g.c_node
        .iter()
        .chain(&g.k_node)
        .any(|&x| x != NONE && x as usize >= n)
    {
        return Err("invalid child id".into());
    }
    for (keys, inside, ids) in [
        (&g.c_key, &g.c_in, &g.c_node),
        (&g.k_key, &g.k_in, &g.k_node),
    ] {
        for ((k, ins), &u) in keys.iter().zip(inside).zip(ids) {
            let expected = if *ins {
                k.and_then(|k| g.id.get(&k).copied()).unwrap_or(NONE)
            } else {
                NONE
            };
            if expected != u || (*ins && u == NONE) {
                return Err("inconsistent child link".into());
            }
        }
    }
    if g.l_node
        .iter()
        .chain(&g.const_base)
        .chain(&g.c_lv)
        .chain(&g.k_lv)
        .chain(&g.k_p)
        .any(|x| !x.is_finite() || *x < 0. || *x > 1.)
        || g.node_reach.iter().any(|x| !x.is_finite() || *x < 0.)
    {
        return Err("invalid probability/value".into());
    }
    for u in 0..no {
        let s: HashSet<_> = g.cands(u).map(|j| g.c_san[j]).collect();
        if s.len() != g.cands(u).len() {
            return Err("duplicate candidate move".into());
        }
    }
    for u in no..n {
        if g.const_base[u - no] + g.kids(u).map(|j| g.k_p[j]).sum::<f64>() > 1. + 1e-9 {
            return Err("opponent mass exceeds one".into());
        }
    }
    Ok(())
}
