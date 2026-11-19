"""Bounded floor propagation on the native builder's admitted menus and followed replies."""
import hashlib
import json
from pathlib import Path

import numpy as np
import polars as pl


def sha256(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def evidence(out, sans, eval_keys, eval_values):
    """Snapshot the pre-veto graph. Empty rows preserve nodes with no usable move or reply."""
    nodes = out["node_keys"]
    own = nodes[out["node_our"]]
    opp = nodes[~out["node_our"]]
    sizes = np.diff(out["c_ptr"])
    codes = out["c_san"]
    children = out["c_child"]
    order = np.argsort(eval_keys)
    keys, vals = eval_keys[order], eval_values[order]
    scores = np.full(len(codes), np.nan)
    if len(keys):
        for start in range(0, len(codes), 1_000_000):
            block = slice(start, start + 1_000_000)
            ix = np.minimum(np.searchsorted(keys, children[block]), len(keys)-1)
            hits = (keys[ix] == children[block]) & out["c_has_child"][block]
            scores[block] = np.where(hits, vals[ix], np.nan)
    ours = pl.DataFrame({"parent": np.repeat(own, sizes), "san": pl.Series(sans, dtype=pl.String).gather(codes),
                         "child": children, "score": scores, "expanded": out["c_in_sub"],
                         "_child": out["c_has_child"]}).with_columns(
        pl.when(pl.col("_child")).then(pl.col("child")).alias("child"), pl.col("score").fill_nan(None),
        pl.lit(True).alias("ours")).drop("_child")
    nreplies = np.diff(out["k_ptr"])
    replies = pl.DataFrame({"parent": np.repeat(opp, nreplies), "child": out["k_child"],
                            "expanded": out["k_in_sub"] & (out["k_p"] > 0), "reply_p": out["k_p"]}).with_columns(pl.lit(False).alias("ours"))
    empty = pl.DataFrame({"parent": np.concatenate([own[sizes==0], opp[nreplies==0]]),
                          "ours": np.repeat([True,False], [(sizes==0).sum(), (nreplies==0).sum()])}).with_columns(
                              pl.lit(False).alias("expanded"))
    edges = pl.concat([ours, replies, empty], how="diagonal")
    reach = pl.DataFrame({"parent": out["reach_keys"], "pre_reach": out["reach_vals"]})
    ply = pl.DataFrame({"parent":out["ply_keys"], "pre_ply":out["ply_vals"]})
    return edges.join(reach.join(ply, on="parent"), on="parent", how="left", validate="m:1", maintain_order="left")


def propagate(edges, white, prior, floor, depth, score_gate, max_ply):
    """Record the first losing depth. A missing score remains unknown; an empty admitted menu
    has no bookable defense. An unresolved frontier gives no evidence for a downstream veto."""
    ours = edges.filter(pl.col("ours")).select("parent", "san", "child", "expanded", "score", "pre_ply")
    side = pl.col("score") if white else 1.0-pl.col("score")
    bar = (prior if white else 1.0-prior) - floor
    passes = (side + 1e-12 >= bar) if score_gate else (
        (pl.col("score") >= prior-floor) if white else (pl.col("score") <= prior+floor))
    ours = ours.with_columns(passes.alias("passes"))
    replies = edges.filter(~pl.col("ours") & pl.col("expanded")).select("parent","child")
    def dead(bad):
        viable = pl.col("san").is_not_null() & pl.col("passes").fill_null(True) & ~bad
        return ours.group_by("parent").agg(viable.any().alias("alive")).filter(~pl.col("alive")).select("parent")

    # Keep the first forcing distance. A transposed child can have a much
    # earlier arrival elsewhere; this particular parent still needs two real
    # plies per step to reach the failing decision from its own earliest route.
    blocked = pl.col("blocked")
    ours = ours.with_columns(pl.lit(False).alias("blocked"))
    frontier = dead(blocked)
    ranks = frontier.with_columns(pl.lit(0, dtype=pl.Int32).alias("dead_at"))
    initial = len(frontier)
    counts = []
    fixed = False
    for step in range(1, depth + 1):
        forcing = replies.join(frontier, left_on="child", right_on="parent",
                               how="semi")["parent"].unique()
        newly_blocked = (pl.col("expanded")
                         & pl.col("child").is_in(forcing.implode()).fill_null(False)
                         & (pl.col("pre_ply") + 2 * step <= max_ply))
        ours = ours.with_columns((blocked | newly_blocked).alias("blocked"))
        frontier = dead(blocked).join(ranks, on="parent", how="anti")
        counts.append(ours.select(blocked.sum()).item())
        if frontier.is_empty():
            fixed = True
            break
        ranks = pl.concat([ranks, frontier.with_columns(pl.lit(step, dtype=pl.Int32).alias("dead_at"))])
    flagged = (blocked | ~pl.col("passes").fill_null(True)) & pl.col("san").is_not_null()
    ours = ours.with_columns(flagged.alias("flagged"), (pl.col("passes").fill_null(False) & ~blocked).any()
                           .over("parent").alias("alternative"))
    veto = ours.filter(pl.col("flagged") & pl.col("alternative")).select("parent","san")
    void = ours.filter(pl.col("flagged") & ~pl.col("alternative"))["parent"].n_unique()
    return veto, ranks, dict(depth=depth, dead_nodes=initial, dead_by_depth=len(ranks),
                             bad_moves_by_depth=counts, fixed_point=fixed, void_nodes=void,
                             vetoed_nodes=veto["parent"].n_unique(), vetoed_moves=len(veto))


def prepare(out, sans, ek, ev, *, white, prior, floor, depth, score_gate, max_ply, path, keys=None):
    frame = evidence(out, sans, ek, ev)
    veto, ranks, stats = propagate(frame, white, prior, floor, depth, score_gate, max_ply)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    proof = frame.join(ranks, on="parent", how="left", maintain_order="left")
    if keys is not None:
        proof = keys.restore(proof, "parent", "child")
    proof.write_parquet(tmp, compression="zstd")
    tmp.replace(path)
    stats.update(schema=2, evidence_file=path.name, evidence_sha256=sha256(path))
    grouped = veto.group_by("parent").agg(pl.col("san").unique().sort())
    by_node = dict(grouped.iter_rows())
    code = {san:i for i,san in enumerate(sans)}
    reached = pl.DataFrame({"parent": out["reach_keys"], "reach": out["reach_vals"]})
    veto_reach = dict(reached.join(grouped, on="parent", how="semi").iter_rows())
    coded = {h: [code[s] for s in moves] for h, moves in by_node.items()}
    return coded, by_node, veto_reach, stats


def write_veto(book, by_node, reach, keys=None):
    path = Path(str(book) + ".lookahead.json")
    rows = [{"position_hash":h, "reach":reach[h], "moves":sorted({s.rstrip("+#") for s in moves})}
            for h,moves in sorted(by_node.items())]
    if keys is not None:
        rows = keys.records(rows, ["position_hash"])
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(rows), encoding="utf-8")
    tmp.replace(path)
    return sha256(path)
