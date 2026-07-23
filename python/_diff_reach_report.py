"""One-off: rank move-decision differences between two white reps by reach
(% of games the position is seen when playing a reference rep), and reconstruct
a readable line to each. Reused for request-2 (single vs two-pass) and request-3
(eval-weight variants).

Usage:
  _diff_reach_report.py <repA.parquet> <repB.parquet> <reach.parquet> <labelA> <labelB> [topN]
    A = reference/baseline rep (its move shown first), B = variant.
    reach = reach export whose rep the % is measured against.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, r"C:\Users\David\Documents\Chess\python")
import numpy as np, polars as pl, chess
from zobrist import zobrist_int64

repA_path, repB_path, reach_path, labelA, labelB = sys.argv[1:6]
topN = int(sys.argv[6]) if len(sys.argv) > 6 else 10

def load(path):
    r = pl.read_parquet(path, columns=["event","elo_band","position_hash","side_to_move",
                                        "best_move","value","eval_score","crush_potential"])
    return r.filter((pl.col("event")=="Pooled") & (pl.col("elo_band")==0) &
                    (pl.col("side_to_move")=="white"))

A = load(repA_path); B = load(repB_path)
j = A.join(B, on="position_hash", how="inner", suffix="_B")
diff = j.filter(pl.col("best_move") != pl.col("best_move_B"))
reach = pl.read_parquet(reach_path).group_by("position_hash").agg(pl.col("reach").sum())
d = diff.join(reach, on="position_hash", how="inner").sort("reach", descending=True)
total_reachable_diffs = d.height
top = d.head(max(topN*3, 40))  # extra headroom for labeling
targets = set(top["position_hash"].to_list())

# ---- reconstruct a line to each target: DFS from start, follow rep-A moves at our
#      nodes, branch opponent empirical replies (share > 2%), epsilon/depth pruned ----
amove = {h:m for h,m in zip(A["position_hash"], A["best_move"])}
st = pl.read_parquet("E:/chess/position-stats/position_stats_pooled_ge1800_2019_2025_brc.parquet",
                     columns=["parent_hash","move_san","total"]).sort("parent_hash")
ph = st["parent_hash"].to_numpy(); mv = st["move_san"].to_list(); tot = st["total"].to_numpy()
def children(h):
    lo = np.searchsorted(ph, h, "left"); hi = np.searchsorted(ph, h, "right")
    if lo==hi: return []
    t = tot[lo:hi]; s = t.sum()
    return [(mv[lo+i], t[i]/s) for i in range(hi-lo)] if s>0 else []

label = {}
def dfs(board, w, path, depth):
    if len(label) == len(targets) or depth > 18 or w < 1e-5:
        return
    h = zobrist_int64(board)
    if board.turn == chess.WHITE:
        if h in targets and h not in label:
            label[h] = list(path)
        m = amove.get(h)
        if m is None: return
        try: board.push_san(m)
        except Exception: return
        dfs(board, w, path+[m], depth+1); board.pop()
    else:
        for san, share in children(h):
            if w*share < 1e-5: continue
            try: board.push_san(san)
            except Exception: continue
            dfs(board, w*share, path+[san], depth+1); board.pop()

def render_line(sans):  # replay plain SANs into numbered movetext "1.e4 c5 2.d4 ..."
    b = chess.Board(); out = []
    for s in sans:
        out.append(f"{b.fullmove_number}.{s}" if b.turn == chess.WHITE else s)
        try: b.push_san(s)
        except Exception: break
    return " ".join(out)
dfs(chess.Board(), 1.0, [], 0)

print(f"# reference rep (A) = {labelA}   variant (B) = {labelB}")
print(f"# reach-carrying positions where move differs: {total_reachable_diffs:,}")
print(f"# (of all {diff.height:,} differing nodes; reach measured on {reach_path.split('/')[-1]})\n")
print(f"{'reach%':>7} {'line':52} {labelA+'→'+labelB:14} {'Δvalue':>7} {'Δeval':>6}")
print("-"*100)
for row in top.head(topN).iter_rows(named=True):
    h = row["position_hash"]
    ln = render_line(label[h]) if h in label else "(deep/rare - not on common tree)"
    if len(ln) > 50: ln = ln[:49]+"…"
    dv = row["value_B"]-row["value"]; de = row["eval_score_B"]-row["eval_score"]
    print(f"{row['reach']*100:6.2f}% {ln:52} {row['best_move']+'→'+row['best_move_B']:14} "
          f"{dv:+7.4f} {de:+6.3f}")
