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
import webbrowser
from collections import OrderedDict
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import chess
import chess.polyglot
import polars as pl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


INT64_MAX = 2**63 - 1
INT64_RANGE = 2**64

# Per-move metric columns we surface if present in the repertoire parquet.
METRIC_COLS = ["value", "value_robust", "crush_rate", "decisiveness",
               "opponent_error", "forcingness", "eval_score"]


def zobrist_int64(board: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


# ── Data layer ────────────────────────────────────────────────────────────

class Explorer:
    def __init__(self, rep_specs, stats_path, cache_slices=6):
        # rep_specs: list of (label, perspective, path). label is the dropdown entry.
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
                + "\n  Pass the matching --stats — e.g. the pooled "
                  "position_stats_pooled_1900_2200_brc.parquet for pooled reps.")

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
            "opp_top_moves": [], "in_repertoire": ri is not None,
        }
        if ri is not None:
            rc = sl["rep_cols"]
            out["our_best_move"] = rc["best_move"][ri]
            out["metrics"] = {c: rc[c][ri] for c in METRIC_COLS if c in rc}

        if not is_our_turn and group:
            sc = sl["st_cols"]
            total_all = sum(sc["total"][i] for i in group) or 1
            for i in group[:12]:
                child = sc["child_hash"][i]
                out["opp_top_moves"].append({
                    "san": sc["move_san"][i],
                    "total": int(sc["total"][i]),
                    "share": sc["total"][i] / total_all,
                    "white_score": float(sc["white_score_avg"][i]),
                    "in_book": (child in sl["pos_set"]) if child is not None else None,
                })
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
document.getElementById("source").textContent = SOURCE;

const METRIC_LABELS = {
  value: "value (white-score)",
  value_robust: "value vs best defense",
  crush_rate: "crush rate (fast wins)",
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
function renderPgn() {
  if (moveStack.length === 0) return "&nbsp;";
  let out = [];
  for (let i = 0; i < moveStack.length; i++) {
    if (i % 2 === 0) out.push(`${(i / 2) + 1}.`);
    out.push(moveStack[i]);
  }
  return out.join(" ");
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
  if (!data.our_best_move) {
    return `<div class="info-box warn"><strong>No recommendation</strong> (position in book
      but no best_move — likely a cycle fallback).</div>`;
  }
  let rows = "";
  for (const k of Object.keys(METRIC_LABELS)) {
    if (k in data.metrics) {
      rows += `<div class="k">${METRIC_LABELS[k]}</div><div class="v">${fmtMetric(k, data.metrics[k])}</div>`;
    }
  }
  return `<div class="info-box our">
    <strong>${currentPersp} plays: <span class="move">${data.our_best_move}</span></strong>
    <div class="metrics">${rows}</div>
    <button id="play-our-btn" data-san="${data.our_best_move}" style="margin-top:0.5rem;">
      Play ${data.our_best_move}</button>
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
    return `<tr class="clickable" data-san="${m.san}">
      <td class="move">${m.san}</td>
      <td class="num">${m.total.toLocaleString()}</td>
      <td class="num">${(m.share * 100).toFixed(1)}%</td>
      <td class="num">${m.white_score.toFixed(3)}</td>
      <td>${bk}</td></tr>`;
  }).join("");
  return `<div class="info-box opp">
    <strong>Opponent (${data.side_to_move}) — top empirical replies</strong>
    <div class="small">click a row to play it</div>
    <table><thead><tr><th>move</th><th class="num">games</th><th class="num">share</th>
      <th class="num">white-score</th><th></th></tr></thead><tbody>${rows}</tbody></table>
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
            html = INDEX_HTML.replace("__SOURCE__", self.server.source_label).encode("utf-8")
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
                    default="E:/chess/position-stats/position_stats_pooled_1900_2200_brc.parquet",
                    help="Position-stats parquet supplying opponent replies. Must contain "
                         "every loaded rep's (event, elo_band) slice (checked at startup). "
                         "Defaults to the pooled stats that the canonical pooled reps use.")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-browser", action="store_true")
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

    explorer = Explorer(specs, args.stats)
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
