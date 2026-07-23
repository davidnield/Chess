"""
Repertoire explorer — walk the built repertoires interactively in a browser.

Loads a WHITE and/or BLACK Stage 3 repertoire parquet plus the shared Stage 2
position-stats, and serves a chessboard UI at http://127.0.0.1:8765/ where you can:

  - Toggle **time control** (event) and **elo band** — every slice in the files.
  - Toggle **Play as White / Black** — switches which repertoire drives "our move".
  - At our turn: see our recommended move and its propagated metrics
    (value, value-vs-best-defense, crush rate, decisiveness, opponent-error, eval).
  - At the opponent's turn: see their top empirical replies with game counts and
    white-score; replies that stay inside our prepared tree are marked "in book".

This generalises the older single-file `repertoire_browser.py`: it holds the
parquets as Polars frames and filters per slice on demand (fast), so switching
event/elo/colour is near-instant and no giant per-slice dicts are pre-built.

Usage:
    .venv/Scripts/python.exe python/repertoire_explorer.py \\
        --white-e4 E:/chess/repertoire/repertoire_..._white_e4_crush.parquet \\
        --white-d4 E:/chess/repertoire/repertoire_..._white_d4_crush.parquet \\
        --black    E:/chess/repertoire/repertoire_..._black_crush.parquet
    # Each repertoire is a labeled entry in the dropdown. --stats defaults to the
    # combined 2024+2025 position-stats.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import webbrowser
from collections import OrderedDict
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import chess
import chess.polyglot
import numpy as np
import polars as pl

from zobrist import zobrist_int64

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


# Per-move metric columns we surface if present in the repertoire parquet.
METRIC_COLS = ["value", "value_worst", "value_robust", "crush_rate", "crush_potential",
               "memo_cost", "cover_eff", "decisiveness", "opponent_error", "forcingness",
               "eval_score"]


def _meta_crush_weight(rep_path) -> float | None:
    """Recover the crush selection weight from a rep's provenance sidecar
    ('<rep>.parquet.meta.json', written by build_sharp_reps.py). Returns None when the
    sidecar is absent or unreadable, so the caller can fall back to a default."""
    side = Path(rep_path).with_name(Path(rep_path).name + ".meta.json")
    if not side.exists():
        return None
    try:
        cw = json.loads(side.read_text(encoding="utf-8")).get("crush_weight")
        return float(cw) if cw is not None else None
    except (ValueError, OSError):
        return None


# ── Data layer ────────────────────────────────────────────────────────────

class Explorer:
    def __init__(self, rep_specs, stats_path, cache_slices=6, crush_weight=25.0,
                 crush_totals=None):
        # rep_specs: list of (label, perspective, path). label is the dropdown entry.
        # crush_weight: the Stage-3 crush selection weight, used to reconstruct the
        # selection key (sign*value + crush_weight*crush_potential) for ranking the
        # alternative moves at our turn (gold/silver/bronze).
        # crush_totals: optional per-edge winpos totals parquet (parent_hash, move_san,
        # n, {white,black}_we[5]) — the UNSHRUNK win-event rate + sample size, shown
        # next to the prior-shrunk crush so thin-line shrinkage is visible at a glance.
        self.crush_weight = crush_weight
        self._ct = None
        if crush_totals and Path(crush_totals).exists():
            ct = pl.read_parquet(crush_totals).sort("parent_hash")
            self._ct_ph = ct["parent_hash"].to_numpy()
            self._ct_san = ct["move_san"].to_list()
            self._ct = {c: ct[c].to_numpy()
                        for c in ("n", "white_we", "white_we5", "black_we", "black_we5")}
            print(f"Loaded crush edge totals: {ct.height:,} edges from {Path(crush_totals).name}")
        elif crush_totals:
            print(f"NOTE: crush totals not found ({crush_totals}) — raw-crush column off.")
        self.reps: dict[str, pl.DataFrame] = {}
        self.persp: dict[str, str] = {}
        self.labels: list[str] = []
        for label, persp, p in rep_specs:
            if p and Path(p).exists():
                df = pl.read_parquet(p)
                keep = ["event", "elo_band", "position_hash", "best_move"]
                keep += [c for c in METRIC_COLS if c in df.columns]
                self.reps[label] = df.select(keep)
                self.persp[label] = persp
                self.labels.append(label)
                print(f"Loaded '{label}' ({persp}): {df.height:,} rows from {Path(p).name}")
        if not self.reps:
            sys.exit("No repertoire loaded.")

        scols = ["event", "elo_band", "parent_hash", "move_san", "total", "white_score_avg"]
        sdf = pl.read_parquet(stats_path)
        if "child_hash" in sdf.columns:
            scols.append("child_hash")
        self.stats = sdf.select(scols)
        print(f"Loaded stats: {self.stats.height:,} rows from {Path(stats_path).name}")

        # Guard: every rep's (event, elo_band) slice must exist in the stats file.
        # If not, opponent-move lookups return empty and the UI silently shows
        # "No opponent moves recorded" for every position — the exact pooled-rep
        # vs default-2024_2025-stats mismatch that previously cost a debugging
        # cycle. Fail loudly instead.
        stat_slices = set(map(tuple,
            self.stats.select(["event", "elo_band"]).unique().iter_rows()))
        problems = []
        for lb in self.labels:
            rep_slices = set(map(tuple,
                self.reps[lb].select(["event", "elo_band"]).unique().iter_rows()))
            missing = sorted(rep_slices - stat_slices)
            if missing:
                problems.append(f"  '{lb}': slice(s) {missing} absent from stats")
        if problems:
            sys.exit(
                f"FATAL: repertoire slice(s) missing from --stats "
                f"({Path(stats_path).name}); opponent moves would be empty.\n"
                + "\n".join(problems)
                + "\n  Pass the matching --stats — e.g. the canonical "
                  "position_stats_pooled_ge1800_2019_2025_brc.parquet for pooled reps.")

        # Slices available = union of (event, elo_band) across the loaded reps.
        sl = (pl.concat([r.select(["event", "elo_band"]) for r in self.reps.values()])
              .unique().sort(["event", "elo_band"]))
        self.slice_keys = [(r["event"], r["elo_band"]) for r in sl.iter_rows(named=True)]
        self.start_hash = zobrist_int64(chess.Board())
        self._cache: OrderedDict[tuple, dict] = OrderedDict()
        self._cache_max = cache_slices

    # -- per-slice filtered views, cached (LRU) --------------------------------
    # Built from column lists (fast) rather than iter_rows over millions of rows.
    def _slice(self, label: str, ev: str, eb: int) -> dict | None:
        if label not in self.reps:
            return None
        key = (label, ev, eb)
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached

        rep = self.reps[label].filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
        st = self.stats.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))

        rep_ph = rep["position_hash"].to_list()
        rep_cols = {"best_move": rep["best_move"].to_list()}
        for c in METRIC_COLS:
            if c in rep.columns:
                rep_cols[c] = rep[c].to_list()
        rep_index = {ph: i for i, ph in enumerate(rep_ph)}
        pos_set = set(rep_ph)

        st_parent = st["parent_hash"].to_list()
        st_cols = {"move_san": st["move_san"].to_list(),
                   "total": st["total"].to_list(),
                   "white_score_avg": st["white_score_avg"].to_list()}
        st_cols["child_hash"] = (st["child_hash"].to_list()
                                 if "child_hash" in st.columns else [None] * len(st_parent))
        groups: dict[int, list[int]] = {}
        for i, p in enumerate(st_parent):
            groups.setdefault(p, []).append(i)
        totals = st_cols["total"]
        for p in groups:
            groups[p].sort(key=lambda i: -totals[i])
        n_games = sum(totals[i] for i in groups.get(self.start_hash, []))

        entry = {"rep_index": rep_index, "rep_cols": rep_cols, "pos_set": pos_set,
                 "groups": groups, "st_cols": st_cols, "n_games": n_games}
        self._cache[key] = entry
        if len(self._cache) > self._cache_max:
            self._cache.popitem(last=False)
        return entry

    def _raw_crush(self, ph: int, san: str, persp: str, ev: str, eb: int) -> dict | None:
        """Unshrunk winpos win-event rate + histogram n for edge (ph, san), from the
        offline per-edge totals. Pooled-slice only (the totals were aggregated there)."""
        if self._ct is None or ev != "Pooled" or eb != 0:
            return None
        lo = int(np.searchsorted(self._ct_ph, ph, "left"))
        hi = int(np.searchsorted(self._ct_ph, ph, "right"))
        side = "white" if persp == "white" else "black"
        for i in range(lo, hi):
            if self._ct_san[i] == san:
                n = int(self._ct["n"][i])
                if n == 0:
                    return None
                return {"n": n, "raw": int(self._ct[f"{side}_we"][i]) / n,
                        "raw5": int(self._ct[f"{side}_we5"][i]) / n}
        return None

    def list_slices(self) -> dict:
        return {
            "reps": [{"label": lb, "perspective": self.persp[lb]} for lb in self.labels],
            "slices": [{"event": ev, "elo_band": eb} for (ev, eb) in self.slice_keys],
        }

    def slice_info(self, label: str, ev: str, eb: int) -> dict:
        sl = self._slice(label, ev, eb)
        return {"n_games": sl["n_games"] if sl else 0}

    def get_position(self, label: str, ev: str, eb: int, epd: str) -> dict | None:
        sl = self._slice(label, ev, eb)
        if sl is None:
            return None
        try:
            board = chess.Board(epd)
        except Exception:
            return None

        persp = self.persp[label]
        ph = zobrist_int64(board)
        ri = sl["rep_index"].get(ph)
        group = sl["groups"].get(ph, [])
        is_our_turn = ((board.turn == chess.WHITE and persp == "white")
                       or (board.turn == chess.BLACK and persp == "black"))

        out = {
            "epd": epd, "position_hash": ph,
            "side_to_move": "white" if board.turn == chess.WHITE else "black",
            "is_our_turn": is_our_turn,
            "our_best_move": None, "metrics": {},
            "opp_top_moves": [], "our_moves": [], "in_repertoire": ri is not None,
        }
        if ri is not None:
            rc = sl["rep_cols"]
            out["our_best_move"] = rc["best_move"][ri]
            out["metrics"] = {c: rc[c][ri] for c in METRIC_COLS if c in rc}

        if not is_our_turn and group:
            sc = sl["st_cols"]
            rc_val = sl["rep_cols"].get("value")  # propagated value column, if present
            rc_eval = sl["rep_cols"].get("eval_score")        # engine eval of child
            rc_crush = sl["rep_cols"].get("crush_potential")  # crush potential of child
            total_all = sum(sc["total"][i] for i in group) or 1
            for i in group[:12]:
                child = sc["child_hash"][i]
                # Propagated repertoire value of the position AFTER this opponent
                # reply: the backwards-induction expected white-score assuming WE
                # then follow the repertoire. None when the reply leaves our book.
                child_ri = sl["rep_index"].get(child) if child is not None else None

                def _child(col):
                    return (float(col[child_ri])
                            if (col is not None and child_ri is not None
                                and col[child_ri] is not None) else None)

                out["opp_top_moves"].append({
                    "san": sc["move_san"][i],
                    "total": int(sc["total"][i]),
                    "share": sc["total"][i] / total_all,
                    "white_score": float(sc["white_score_avg"][i]),
                    "rep_value": _child(rc_val),
                    # Engine eval + crush potential of the position THIS reply reaches:
                    # surfaces which opponent replies are mistakes and how crushable.
                    "eval_score": _child(rc_eval),
                    "crush_potential": _child(rc_crush),
                    "crush_raw": self._raw_crush(ph, sc["move_san"][i], persp, ev, eb),
                    "in_book": (child in sl["pos_set"]) if child is not None else None,
                })

        if is_our_turn and group:
            # ALL of our candidate moves with the metrics of the position each
            # reaches, ranked by the Stage-3 selection key. This shows WHY the
            # alternatives weren't chosen (lower key, or a worse value-vs-best-
            # defense that the refutation gate would drop). Gold = the repertoire's
            # actual pick (best_move); silver/bronze = the next two by key.
            sc = sl["st_cols"]
            rc_val   = sl["rep_cols"].get("value")           # propagated mean value
            rc_worst = sl["rep_cols"].get("value_worst")     # OUR book vs best defense (<= rep value)
            rc_rob   = sl["rep_cols"].get("value_robust")    # objective gate metric (best play both sides)
            rc_eval  = sl["rep_cols"].get("eval_score")      # engine eval of child
            rc_crush = sl["rep_cols"].get("crush_potential") # propagated crush of child
            rc_memo  = sl["rep_cols"].get("memo_cost")       # propagated memo cost of child
            sign = 1.0 if persp == "white" else -1.0
            cw = self.crush_weight
            total_all = sum(sc["total"][i] for i in group) or 1
            best = out["our_best_move"]
            moves = []
            for i in group:
                child = sc["child_hash"][i]
                cri = sl["rep_index"].get(child) if child is not None else None

                def _c(col, cri=cri):
                    return (float(col[cri]) if (col is not None and cri is not None
                            and col[cri] is not None) else None)

                val = _c(rc_val); rob = _c(rc_rob); ev = _c(rc_eval); cr = _c(rc_crush)
                mc = _c(rc_memo); worst = _c(rc_worst)
                if val is not None and cr is not None:
                    key = sign * val + cw * cr
                elif val is not None:
                    key = sign * val
                else:
                    key = None
                moves.append({
                    "san": sc["move_san"][i],
                    "total": int(sc["total"][i]),
                    "share": sc["total"][i] / total_all,
                    "white_score": float(sc["white_score_avg"][i]),
                    "value": val, "value_worst": worst, "value_robust": rob,
                    "eval_score": ev, "crush_potential": cr, "memo_cost": mc,
                    "crush_raw": self._raw_crush(ph, sc["move_san"][i], persp, ev, eb),
                    "key": key,
                    "in_book": (child in sl["pos_set"]) if child is not None else None,
                    "is_recommended": (sc["move_san"][i] == best),
                    "rank": None,
                })
            # Gold = the actual repertoire pick; silver/bronze = next two by key.
            by_key = sorted(moves, key=lambda m: (m["key"] is not None, m["key"]),
                            reverse=True)
            gold = next((m for m in moves if best is not None and m["san"] == best), None)
            if gold is None and best is not None:
                # ENGINE-AUGMENTED pick: best_move has no stats edge (it was never
                # played from here — stage3's rescue admitted it via the eval DB at
                # a forced-losing node). Without this synthesized row the gold badge
                # silently fell on the top RECORDED candidate, displaying a gated
                # (often losing) move as the recommendation.
                try:
                    child_board = board.copy()
                    child_board.push_san(best)
                    ch = zobrist_int64(child_board)
                except Exception:
                    ch = None
                cri = sl["rep_index"].get(ch) if ch is not None else None

                def _c(col, cri=cri):
                    return (float(col[cri]) if (col is not None and cri is not None
                            and col[cri] is not None) else None)

                val = _c(rc_val); cr = _c(rc_crush)
                worst, rob, ev = _c(rc_worst), _c(rc_rob), _c(rc_eval)
                if val is None and out["metrics"]:
                    # Terminal rescue: the child is off-book (no rep row), but the
                    # NODE's own propagated metrics ARE the pick's values.
                    mt = out["metrics"]
                    val = mt.get("value"); worst = mt.get("value_worst")
                    rob = mt.get("value_robust")
                gold = {
                    "san": best, "total": 0, "share": 0.0, "white_score": None,
                    "value": val, "value_worst": worst,
                    "value_robust": rob, "eval_score": ev,
                    "crush_potential": cr, "memo_cost": _c(rc_memo),
                    "crush_raw": None,   # unplayed engine move: no histogram edge
                    "key": (sign * val + cw * cr) if (val is not None and cr is not None)
                           else (sign * val if val is not None else None),
                    "in_book": (ch in sl["pos_set"]) if ch is not None else None,
                    "is_recommended": True, "augmented": True, "rank": None,
                }
            ordered = ([gold] if gold is not None else []) + [m for m in by_key if m is not gold]
            for rank, m in enumerate(ordered[:3], 1):
                m["rank"] = rank
            out["our_moves"] = ordered
        return out


# ── Frontend ────────────────────────────────────────────────────────────────

INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Repertoire Explorer</title>
<link rel="stylesheet"
      href="https://unpkg.com/@chrisoakman/chessboardjs@1.0.0/dist/chessboard-1.0.0.min.css">
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         margin: 1.5rem; max-width: 1150px; color: #222; }
  h1   { margin: 0 0 0.5rem; font-size: 1.4rem; }
  .meta { color: #666; font-size: 0.9rem; margin-bottom: 1rem; }
  .row { display: flex; gap: 2rem; align-items: flex-start; }
  #board { width: 460px; }
  .controls { margin-bottom: 0.8rem; display: flex; flex-wrap: wrap;
              gap: 0.4rem 1rem; align-items: center; }
  select, button { font-size: 0.95rem; padding: 0.3rem 0.5rem; }
  button { cursor: pointer; }
  .panel { flex: 1; min-width: 400px; }
  .info-box { background: #f4f6f8; border-left: 4px solid #2c5282;
              padding: 0.7rem 1rem; margin: 0.6rem 0; border-radius: 0 4px 4px 0; }
  .info-box.our { border-color: #2c8a47; background: #e8f5ec; }
  .info-box.opp { border-color: #d4a017; background: #fff8e6; }
  .info-box.warn { border-color: #c53030; background: #fed7d7; }
  table { border-collapse: collapse; width: 100%; margin: 0.4rem 0; }
  th, td { border-bottom: 1px solid #e0e0e0; padding: 0.35rem 0.55rem;
           text-align: left; font-size: 0.92rem; }
  th { background: #ebf2fa; }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
  td.move { font-family: "Cascadia Code", "Consolas", monospace; font-weight: 600; }
  tr.clickable { cursor: pointer; }
  tr.clickable:hover { background: #f0f7ff; }
  .pgn { font-family: "Cascadia Code", "Consolas", monospace; font-size: 0.9rem;
         background: #f8f8f8; padding: 0.5rem; border-radius: 4px;
         min-height: 1.5em; word-spacing: 0.3em; line-height: 1.6; }
  .small { color: #666; font-size: 0.85rem; }
  .metrics { display: grid; grid-template-columns: auto auto; gap: 0.1rem 1.2rem;
             margin-top: 0.4rem; font-size: 0.88rem; }
  .metrics .k { color: #555; }
  .metrics .v { font-variant-numeric: tabular-nums; font-weight: 600; text-align: right; }
  .badge { display: inline-block; padding: 0.05rem 0.45rem; border-radius: 10px;
           font-size: 0.78rem; background: #d7e9d9; color: #246b33; }
  .badge.off { background: #eee; color: #999; }
  .badge.rec { background: #c6e0c9; color: #1f5e2c; }
  .badge.aug { background: #e1d5f5; color: #5e35b1; }
  td.medal { text-align: center; font-size: 1.05rem; padding: 0.2rem 0.3rem; }
  tr.gold   td { background: #fcf2c0; }
  tr.silver td { background: #e8ebee; }
  tr.bronze td { background: #f2dfc8; }
  tr.gold:hover td, tr.silver:hover td, tr.bronze:hover td { filter: brightness(0.97); }
</style>
</head>
<body>

<h1>Repertoire Explorer</h1>
<p class="meta"><span class="small" id="source"></span></p>

<div class="controls">
  <label>Repertoire
    <select id="rep-select"></select>
  </label>
  <label>Time control
    <select id="event-select"></select>
  </label>
  <label>Elo band
    <select id="elo-select"></select>
  </label>
  <span class="small" id="slice-info"></span>
  <button id="reset-btn">Reset</button>
  <button id="back-btn">Take back</button>
  <button id="lichess-btn" title="Open the current line on the Lichess analysis board (new tab)">Lichess ↗</button>
  <label><input type="checkbox" id="autoplay" checked> Auto-play our move</label>
  <label><input type="checkbox" id="flip"> Flip</label>
</div>

<div class="row">
  <div id="board-wrap"><div id="board"></div></div>
  <div class="panel">
    <div id="status"></div>
    <h3 style="margin-bottom:0.3rem;">Line so far</h3>
    <div id="pgn" class="pgn">&nbsp;</div>
    <h3 style="margin-bottom:0.3rem;">Position info</h3>
    <div id="position-info" class="small"></div>
  </div>
</div>

<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
<script src="https://unpkg.com/@chrisoakman/chessboardjs@1.0.0/dist/chessboard-1.0.0.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chess.js/0.10.3/chess.min.js"></script>
<script>
const SOURCE = "__SOURCE__";
const CRUSH_W = __CRUSH_W__;   // Stage-3 crush selection weight (for the key column)
// "raw · n" cell: unshrunk win-event rate + histogram sample size for an edge.
const fmtN = n => n >= 1e6 ? (n/1e6).toFixed(1)+"M" : n >= 1e3 ? (n/1e3).toFixed(1)+"k" : String(n);
const crushRawCell = cr => (cr === null || cr === undefined)
  ? "&mdash;" : `${(cr.raw*100).toFixed(0)}% &middot; ${fmtN(cr.n)}`;
document.getElementById("source").textContent = SOURCE;

const METRIC_LABELS = {
  value: "value (white-score)",
  value_worst: "vs best defense (our book)",
  value_robust: "gate eval (objective best play)",
  crush_rate: "crush rate (local)",
  crush_potential: "crush potential (propagated)",
  memo_cost: "memorization cost (propagated)",
  cover_eff: "coverage efficiency (depth / lines)",
  decisiveness: "decisiveness (1 - draws)",
  opponent_error: "opponent error",
  forcingness: "forcingness (Simpson)",
  eval_score: "engine eval (white-score)",
};

const game = new Chess();
const moveStack = [];
let board;
let reps = [];                 // [{label, perspective}]
let perspByLabel = {};
let slices = [];
let currentLabel = null, currentPersp = null, currentEvent = null, currentElo = null;

function updateBoard() {
  board.position(game.fen());
  document.getElementById("pgn").innerHTML = renderPgn();
  fetchPosition();
}
function pgnText() {
  let out = [];
  for (let i = 0; i < moveStack.length; i++) {
    if (i % 2 === 0) out.push(`${(i / 2) + 1}.`);
    out.push(moveStack[i]);
  }
  return out.join(" ");
}
function renderPgn() {
  return moveStack.length === 0 ? "&nbsp;" : pgnText();
}
function openLichess() {
  // Analysis board seeded with the WHOLE line (not just the position), so you can
  // step back through the book moves and keep analyzing past where the book ends.
  const color = currentPersp === "black" ? "black" : "white";
  const url = moveStack.length === 0
    ? `https://lichess.org/analysis?color=${color}`
    : `https://lichess.org/analysis/pgn/${encodeURIComponent(pgnText())}?color=${color}`;
  window.open(url, "_blank", "noopener");
}
function tryMove(san) {
  const m = game.move(san, { sloppy: true });
  if (!m) return false;
  moveStack.push(m.san);
  updateBoard();
  return true;
}
function takeBack() { if (moveStack.length) { game.undo(); moveStack.pop(); updateBoard(); } }
function reset() { game.reset(); moveStack.length = 0; updateBoard(); }

async function fetchPosition() {
  if (!currentLabel || !currentEvent || currentElo === null) return;
  const epd = game.fen().split(" ").slice(0, 4).join(" ");
  const url = `/api/position?label=${encodeURIComponent(currentLabel)}&event=${encodeURIComponent(currentEvent)}` +
              `&elo_band=${currentElo}&epd=${encodeURIComponent(epd)}`;
  let data;
  try {
    const r = await fetch(url);
    if (!r.ok) { renderStatus(null, "Error: " + r.status); return; }
    data = await r.json();
  } catch (e) { renderStatus(null, "Network error: " + e); return; }
  renderStatus(data);
}

function renderStatus(data, errMsg) {
  const status = document.getElementById("status");
  const info = document.getElementById("position-info");
  if (errMsg) { status.innerHTML = `<div class="info-box warn">${errMsg}</div>`; info.innerHTML = ""; return; }
  if (!data) { status.innerHTML = ""; info.innerHTML = ""; return; }
  if (game.game_over()) {
    status.innerHTML = `<div class="info-box">Game over: ${gameResult()}</div>`;
    info.innerHTML = renderPositionInfo(data); return;
  }
  if (!data.in_repertoire && data.is_our_turn) {
    status.innerHTML = `<div class="info-box warn"><strong>Out of repertoire</strong>
      &mdash; no recommendation for this position in this slice. Take back or Reset.</div>`;
    info.innerHTML = renderPositionInfo(data); return;
  }
  status.innerHTML = data.is_our_turn ? renderOurTurn(data) : renderOpponentTurn(data);
  info.innerHTML = renderPositionInfo(data);

  document.querySelectorAll("tr.clickable").forEach(tr =>
    tr.addEventListener("click", () => tryMove(tr.dataset.san)));
  const playBtn = document.getElementById("play-our-btn");
  if (playBtn) playBtn.addEventListener("click", () => tryMove(playBtn.dataset.san));
  if (data.is_our_turn && data.our_best_move && document.getElementById("autoplay").checked) {
    setTimeout(() => tryMove(data.our_best_move), 250);
  }
}

function fmtMetric(k, v) {
  if (v === null || v === undefined) return "&mdash;";
  return Number(v).toFixed(3);
}
function renderOurTurn(data) {
  if (!data.our_moves || !data.our_moves.length) {
    if (!data.our_best_move) {
      return `<div class="info-box warn"><strong>No recommendation</strong> (position in book
        but no best_move — likely a cycle fallback).</div>`;
    }
    return `<div class="info-box our"><strong>${currentPersp} plays:
      <span class="move">${data.our_best_move}</span></strong> (no candidate data)
      <button id="play-our-btn" data-san="${data.our_best_move}" style="margin-top:0.5rem;">
        Play ${data.our_best_move}</button></div>`;
  }
  const medal = r => r === 1 ? "🥇" : r === 2 ? "🥈" : r === 3 ? "🥉" : "";
  const cls   = r => r === 1 ? "gold" : r === 2 ? "silver" : r === 3 ? "bronze" : "";
  const fx = v => (v === null || v === undefined) ? "&mdash;" : Number(v).toFixed(3);
  const rows = data.our_moves.map(m => {
    const raw = crushRawCell(m.crush_raw);
    let bk = m.in_book === true ? `<span class="badge">in book</span>`
           : m.in_book === false ? `<span class="badge off">off</span>` : "";
    const rec = m.is_recommended ? ` <span class="badge rec">rec</span>` : "";
    const eng = m.augmented ? ` <span class="badge aug">engine</span>` : "";
    return `<tr class="clickable ${cls(m.rank)}" data-san="${m.san}">
      <td class="medal">${medal(m.rank)}</td>
      <td class="move">${m.san}${rec}${eng}</td>
      <td class="num">${m.total.toLocaleString()}</td>
      <td class="num">${(m.share * 100).toFixed(1)}%</td>
      <td class="num">${fx(m.white_score)}</td>
      <td class="num">${fx(m.value)}</td>
      <td class="num">${fx(m.value_worst)}</td>
      <td class="num">${fx(m.value_robust)}</td>
      <td class="num">${fx(m.eval_score)}</td>
      <td class="num">${fx(m.crush_potential)}</td>
      <td class="num">${raw}</td>
      <td class="num">${fx(m.memo_cost)}</td>
      <td class="num">${fx(m.key)}</td>
      <td>${bk}</td></tr>`;
  }).join("");
  const playBtn = data.our_best_move
    ? `<button id="play-our-btn" data-san="${data.our_best_move}" style="margin-top:0.5rem;">
        Play ${data.our_best_move}</button>` : "";
  return `<div class="info-box our">
    <strong>${currentPersp} to move — all options (🥇 = repertoire pick, 🥈🥉 = runner-ups by key)</strong>
    <div class="small">click a row to play &middot; <b>emp score</b> = raw historical
      white-score of this move &middot; <b>rep value</b> = propagated expected white-score if
      you play this move and then follow the repertoire (opponent plays empirically; lower is
      better for Black) &middot; <b>vs def</b> = our-book value if the opponent defends best at
      every move (a true worst case, always &le; rep value in our favour) &middot; <b>gate</b> =
      the objective engine eval of the line assuming best play by BOTH sides (what the
      refutation gate uses; can exceed rep value when our book gambits) &middot; <b>eng eval</b>
      = engine white-score of the position the move reaches &middot; <b>crush</b> = propagated
      crush potential &middot; <b>memo</b> = propagated memorization cost of the subtree this
      move enters (lower = cheaper to maintain) &middot;
      <b>key</b> = selection score (sign&middot;rep value + ${CRUSH_W}&middot;crush; higher = preferred)
      &middot; <b>raw &middot; n</b> = UNSHRUNK win-event rate of this edge (winpos histogram) and its
      sample size — the crush column is this shrunk toward 0 by a 5,000-game prior
      (&asymp; raw&times;n/(n+5000)), so thin already-won lines show low crush despite raw &asymp; 100%</div>
    <table><thead><tr><th></th><th>move</th><th class="num">games</th><th class="num">share</th>
      <th class="num">emp score</th><th class="num">rep value</th><th class="num">vs def</th><th class="num">gate</th><th class="num">eng eval</th>
      <th class="num">crush</th><th class="num">raw &middot; n</th><th class="num">memo</th><th class="num">key</th><th></th></tr></thead>
      <tbody>${rows}</tbody></table>
    ${playBtn}
  </div>`;
}
function renderOpponentTurn(data) {
  if (!data.opp_top_moves.length) {
    return `<div class="info-box warn">No opponent moves recorded for this position.</div>`;
  }
  const rows = data.opp_top_moves.map(m => {
    let bk = "";
    if (m.in_book === true) bk = `<span class="badge">in book</span>`;
    else if (m.in_book === false) bk = `<span class="badge off">off</span>`;
    const fx = v => (v === null || v === undefined) ? "&mdash;" : v.toFixed(3);
    return `<tr class="clickable" data-san="${m.san}">
      <td class="move">${m.san}</td>
      <td class="num">${m.total.toLocaleString()}</td>
      <td class="num">${(m.share * 100).toFixed(1)}%</td>
      <td class="num">${m.white_score.toFixed(3)}</td>
      <td class="num">${fx(m.rep_value)}</td>
      <td class="num">${fx(m.eval_score)}</td>
      <td class="num">${fx(m.crush_potential)}</td>
      <td class="num">${crushRawCell(m.crush_raw)}</td>
      <td>${bk}</td></tr>`;
  }).join("");
  return `<div class="info-box opp">
    <strong>Opponent (${data.side_to_move}) — top empirical replies</strong>
    <div class="small">click a row to play it &middot; <b>emp score</b> = raw historical
      white-score from that position &middot; <b>rep value</b> = our repertoire's
      propagated expected white-score if we then follow the book &middot;
      <b>eng eval</b> = engine expected white-score of the position this reply reaches
      (lower = bigger White mistake) &middot; <b>crush</b> = propagated crush potential
      of punishing it &middot; <b>raw &middot; n</b> = unshrunk win-event rate of this reply's
      edge and its histogram sample size (crush = this shrunk by the 5,000-game prior)</div>
    <table><thead><tr><th>move</th><th class="num">games</th><th class="num">share</th>
      <th class="num">emp score</th><th class="num">rep value</th>
      <th class="num">eng eval</th><th class="num">crush</th><th class="num">raw &middot; n</th><th></th></tr></thead>
      <tbody>${rows}</tbody></table>
  </div>`;
}
function renderPositionInfo(data) {
  return `EPD: <code>${data.epd}</code><br>position_hash: <code>${data.position_hash}</code>
          &middot; ${data.side_to_move} to move`;
}
function gameResult() {
  if (game.in_checkmate()) return game.turn() === "w" ? "0-1" : "1-0";
  if (game.in_draw()) return "1/2-1/2";
  return "?";
}

async function loadSlices() {
  const r = await fetch("/api/slices");
  const d = await r.json();
  reps = d.reps;
  perspByLabel = {};
  reps.forEach(rp => perspByLabel[rp.label] = rp.perspective);
  slices = d.slices;
  const rSel = document.getElementById("rep-select");
  rSel.innerHTML = reps.map(rp => `<option value="${rp.label}">${rp.label}</option>`).join("");
  rSel.value = reps[0].label;
  const events = Array.from(new Set(slices.map(s => s.event)));
  const evSel = document.getElementById("event-select");
  evSel.innerHTML = events.map(e => `<option value="${e}">${e}</option>`).join("");
  evSel.value = events.includes("Blitz") ? "Blitz" : events[0];
  updateEloOptions();
}
function updateEloOptions() {
  const ev = document.getElementById("event-select").value;
  const elos = slices.filter(s => s.event === ev).map(s => s.elo_band).sort((a, b) => a - b);
  const eloSel = document.getElementById("elo-select");
  eloSel.innerHTML = elos.map(e => `<option value="${e}">${e}</option>`).join("");
  eloSel.value = elos.includes(1900) ? 1900 : elos[Math.floor(elos.length / 2)];
  applySlice();
}
async function applySlice() {
  currentLabel = document.getElementById("rep-select").value;
  currentPersp = perspByLabel[currentLabel];
  currentEvent = document.getElementById("event-select").value;
  currentElo = parseInt(document.getElementById("elo-select").value);
  board.orientation(currentPersp);
  document.getElementById("flip").checked = false;
  // slice game count
  try {
    const r = await fetch(`/api/slice_info?label=${encodeURIComponent(currentLabel)}` +
      `&event=${encodeURIComponent(currentEvent)}&elo_band=${currentElo}`);
    const d = await r.json();
    document.getElementById("slice-info").textContent =
      `(${d.n_games.toLocaleString()} games at start)`;
  } catch (e) {}
  reset();
}

document.addEventListener("DOMContentLoaded", async () => {
  board = Chessboard("board", {
    position: "start",
    pieceTheme: "https://cdn.jsdelivr.net/gh/oakmac/chessboardjs@master/website/img/chesspieces/wikipedia/{piece}.png",
    orientation: "white",
  });
  document.getElementById("reset-btn").addEventListener("click", reset);
  document.getElementById("back-btn").addEventListener("click", takeBack);
  document.getElementById("lichess-btn").addEventListener("click", openLichess);
  document.getElementById("rep-select").addEventListener("change", applySlice);
  document.getElementById("event-select").addEventListener("change", updateEloOptions);
  document.getElementById("elo-select").addEventListener("change", applySlice);
  document.getElementById("flip").addEventListener("change", e => {
    board.orientation(e.target.checked
      ? (currentPersp === "white" ? "black" : "white") : currentPersp);
  });
  await loadSlices();
});
</script>
</body>
</html>
"""


# ── HTTP server ────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        url = urlparse(self.path)
        path, params = url.path, parse_qs(url.query)
        if path in ("/", "/index.html"):
            html = (INDEX_HTML
                    .replace("__SOURCE__", self.server.source_label)
                    .replace("__CRUSH_W__", repr(self.server.explorer.crush_weight))
                    .encode("utf-8"))
            self._send(html, "text/html; charset=utf-8")
            return
        if path == "/api/slices":
            self._json(self.server.explorer.list_slices()); return
        if path == "/api/slice_info":
            lb = params.get("label", [None])[0]
            ev = params.get("event", [None])[0]
            eb = params.get("elo_band", [None])[0]
            if not (lb and ev and eb):
                self.send_error(400); return
            self._json(self.server.explorer.slice_info(lb, ev, int(eb))); return
        if path == "/api/position":
            lb = params.get("label", [None])[0]
            ev = params.get("event", [None])[0]
            eb = params.get("elo_band", [None])[0]
            epd = params.get("epd", [None])[0]
            if not (lb and ev and eb and epd):
                self.send_error(400, "missing param"); return
            data = self.server.explorer.get_position(lb, ev, int(eb), epd)
            if data is None:
                self.send_error(404, "slice/position not found"); return
            self._json(data); return
        self.send_error(404)

    def _send(self, body, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, data):
        self._send(json.dumps(data).encode("utf-8"), "application/json")

    def log_message(self, *a):  # silence per-request logs
        pass


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--white", default=None, help="White repertoire parquet (label 'White')")
    ap.add_argument("--white-e4", default=None, help="Forced-1.e4 White repertoire (label 'White 1.e4')")
    ap.add_argument("--white-d4", default=None, help="Forced-1.d4 White repertoire (label 'White 1.d4')")
    ap.add_argument("--white-nf3", default=None, help="Forced-1.Nf3 White repertoire (label 'White 1.Nf3')")
    ap.add_argument("--black", default=None, help="Black repertoire parquet (label 'Black')")
    ap.add_argument("--rep", action="append", default=[], metavar="LABEL=PERSP=PATH",
                    help="Generic labeled repertoire, repeatable. e.g. "
                         "--rep 'White 1.e4=white=E:/.../rep.parquet'")
    ap.add_argument("--stats",
                    default="E:/chess/position-stats/position_stats_pooled_ge1800_2019_2025_brc.parquet",
                    help="Position-stats parquet supplying opponent replies. Must contain "
                         "every loaded rep's (event, elo_band) slice (checked at startup). "
                         "Defaults to the canonical ge1800/2019-2025 pooled stats.")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--crush-weight", type=float, default=None,
                    help="Stage-3 crush selection weight, used only to reconstruct the "
                         "selection-key column / gold-silver-bronze ranking at our turn. "
                         "Default: read from the rep's .parquet.meta.json provenance sidecar "
                         "(written by build_sharp_reps.py); falls back to 25 if absent.")
    ap.add_argument("--crush-totals",
                    default="E:/chess/position-stats/crush_edge_totals_pooled_ge1800_2019_2025_brc.parquet",
                    help="Per-edge winpos totals parquet (parent_hash, move_san, n, "
                         "{white,black}_we[5]) for the raw-crush column. Built by collapsing "
                         "the winpos histogram (Pooled slice). Missing file = column off.")
    args = ap.parse_args()

    specs = []  # (label, perspective, path)
    if args.white_e4:
        specs.append(("White 1.e4", "white", args.white_e4))
    if args.white_d4:
        specs.append(("White 1.d4", "white", args.white_d4))
    if args.white_nf3:
        specs.append(("White 1.Nf3", "white", args.white_nf3))
    if args.white:
        specs.append(("White", "white", args.white))
    if args.black:
        specs.append(("Black", "black", args.black))
    for r in args.rep:
        label, persp, path = r.split("=", 2)
        specs.append((label, persp, path))
    if not specs:
        sys.exit("Pass at least one repertoire (--white-e4/--white-d4/--white/--black/--rep).")
    if not Path(args.stats).exists():
        sys.exit(f"Stats not found: {args.stats}")

    # Crush weight for the selection-key column: an explicit --crush-weight wins; else
    # recover it from the first rep's provenance sidecar (build_sharp_reps writes it),
    # else fall back to 25 (the canonical sharp build weight).
    crush_weight = args.crush_weight
    if crush_weight is None:
        crush_weight = _meta_crush_weight(specs[0][2])
        if crush_weight is None:
            crush_weight = 25.0
        else:
            print(f"Crush weight {crush_weight:g} (from {Path(specs[0][2]).name}.meta.json)",
                  flush=True)

    explorer = Explorer(specs, args.stats, crush_weight=crush_weight,
                        crush_totals=args.crush_totals)

    # Pre-warm each rep's per-slice view. The first access to a slice materialises
    # ~millions of rows into Python dicts (>10 s for the large pooled reps); doing it
    # lazily on the first browser request stalls the page on a blank screen. Warm it
    # up front (only a handful of (label, slice) pairs) so the UI is responsive
    # immediately. Cap the LRU cache to fit every warmed slice.
    warm_keys = [(lb, ev, eb) for lb in explorer.labels for (ev, eb) in explorer.slice_keys]
    explorer._cache_max = max(explorer._cache_max, len(warm_keys))
    print(f"Pre-warming {len(warm_keys)} slice view(s)…", flush=True)
    for lb, ev, eb in warm_keys:
        t0 = time.time()
        explorer._slice(lb, ev, eb)
        print(f"  warmed {lb} ({ev}, {eb}) in {time.time() - t0:.1f}s", flush=True)

    labels = [f"{lb}:{Path(p).name}" for (lb, _, p) in specs]
    labels.append(f"stats:{Path(args.stats).name}")

    server = HTTPServer((args.host, args.port), Handler)
    server.explorer = explorer
    server.source_label = "  +  ".join(labels)

    u = f"http://{args.host}:{args.port}"
    print(f"\n  Repertoire explorer ready at: {u}\n  Ctrl+C to stop.\n")
    if not args.no_browser:
        threading.Timer(0.5, lambda: webbrowser.open(u)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
