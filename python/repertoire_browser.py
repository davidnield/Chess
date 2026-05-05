"""
Interactive repertoire browser.

Loads a Stage 3 repertoire parquet plus its Stage 2 position-stats and serves
a small web app at http://127.0.0.1:8765/. The frontend is a chess board where:

  - At our turn (black, by default), the app shows our recommended move and
    its propagated value, with a "play" button.
  - At the opponent's turn (white), the app lists their top empirical replies
    with game counts and white-score, click-to-play.

Use this to walk down the repertoire tree manually and verify recommendations
look reasonable before kicking off a long pipeline run.

Usage:
    .venv/Scripts/python.exe python/repertoire_browser.py
    .venv/Scripts/python.exe python/repertoire_browser.py \\
        --repertoire D:/data/chess/repertoire/repertoire_2019_black_v2.parquet \\
        --stats      D:/data/chess/position-stats/position_stats_2019.parquet \\
        --perspective black
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import webbrowser
from collections import defaultdict
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


def zobrist_int64(board: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h


# ── Data layer ────────────────────────────────────────────────────────────

class RepertoireData:
    def __init__(self, repertoire_path: Path, stats_path: Path, perspective: str):
        self.perspective = perspective
        rep   = pl.read_parquet(repertoire_path)
        stats = pl.read_parquet(stats_path)

        self.slices: dict[tuple[str, int], dict] = {}
        slice_keys = (
            rep.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
        )

        start_hash = zobrist_int64(chess.Board())

        for sr in slice_keys.iter_rows(named=True):
            ev, eb = sr["event"], sr["elo_band"]

            rep_slice = rep.filter(
                (pl.col("event") == ev) & (pl.col("elo_band") == eb)
            )
            stats_slice = stats.filter(
                (pl.col("event") == ev) & (pl.col("elo_band") == eb)
            )

            rep_idx = {
                r["position_hash"]: r for r in rep_slice.iter_rows(named=True)
            }

            stats_idx: dict[int, list[dict]] = defaultdict(list)
            for r in stats_slice.iter_rows(named=True):
                stats_idx[r["parent_hash"]].append({
                    "san":         r["move_san"],
                    "total":       int(r["total"]),
                    "white_score": float(r["white_score_avg"]),
                })
            for k in stats_idx:
                stats_idx[k].sort(key=lambda m: -m["total"])

            n_games = sum(m["total"] for m in stats_idx.get(start_hash, []))

            self.slices[(ev, eb)] = {
                "rep":     rep_idx,
                "stats":   dict(stats_idx),
                "n_games": n_games,
            }

    def list_slices(self) -> list[dict]:
        return [
            {
                "event":    ev,
                "elo_band": eb,
                "n_games":  self.slices[(ev, eb)]["n_games"],
            }
            for (ev, eb) in sorted(self.slices.keys())
        ]

    def get_position(self, event: str, elo_band: int, epd: str) -> dict | None:
        sl = self.slices.get((event, elo_band))
        if sl is None:
            return None
        try:
            board = chess.Board(epd)
        except Exception:
            return None

        ph         = zobrist_int64(board)
        rep_row    = sl["rep"].get(ph)
        opp_moves  = sl["stats"].get(ph, [])
        is_our_turn = (
            (board.turn == chess.WHITE and self.perspective == "white")
            or (board.turn == chess.BLACK and self.perspective == "black")
        )

        out = {
            "epd":            epd,
            "position_hash":  ph,
            "side_to_move":   "white" if board.turn == chess.WHITE else "black",
            "is_our_turn":    is_our_turn,
            "our_best_move":  None,
            "our_value":      None,
            "our_forcingness": None,
            "opp_top_moves":  [],
            "in_repertoire":  rep_row is not None,
        }

        if rep_row is not None:
            out["our_best_move"] = rep_row.get("best_move")
            out["our_value"]     = rep_row.get("value")
            # forcingness column is present in newer outputs; older repertoires
            # may not have it.
            if "forcingness" in rep_row:
                out["our_forcingness"] = rep_row.get("forcingness")

        if not is_our_turn:
            top = opp_moves[:10]
            total_all = sum(m["total"] for m in opp_moves) or 1
            out["opp_top_moves"] = [
                {**m, "share": m["total"] / total_all}
                for m in top
            ]

        return out


# ── Frontend (single-file HTML) ────────────────────────────────────────────

INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Repertoire Browser</title>
<link rel="stylesheet"
      href="https://unpkg.com/@chrisoakman/chessboardjs@1.0.0/dist/chessboard-1.0.0.min.css">
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         margin: 1.5rem; max-width: 1100px; color: #222; }
  h1   { margin: 0 0 0.5rem; font-size: 1.4rem; }
  .meta { color: #666; font-size: 0.9rem; margin-bottom: 1rem; }
  .row { display: flex; gap: 2rem; align-items: flex-start; }
  #board { width: 460px; }
  .controls { margin-bottom: 0.8rem; }
  select, button { font-size: 0.95rem; padding: 0.3rem 0.5rem; }
  button { cursor: pointer; }
  .panel { flex: 1; min-width: 380px; }
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
  .badge { display: inline-block; padding: 0.1rem 0.5rem; border-radius: 10px;
           font-size: 0.8rem; background: #e0e7ef; color: #2c5282; margin-left: 0.4rem; }
</style>
</head>
<body>

<h1>Repertoire Browser</h1>
<p class="meta">
  Perspective: <strong id="perspective"></strong>
  &nbsp;&middot;&nbsp;
  <span class="small" id="source"></span>
</p>

<div class="controls">
  <label>Event:
    <select id="event-select"></select>
  </label>
  <label style="margin-left: 1rem;">Elo:
    <select id="elo-select"></select>
  </label>
  <span class="small" id="slice-info" style="margin-left: 1rem;"></span>
  <button id="reset-btn" style="margin-left: 1rem;">Reset</button>
  <button id="back-btn">Take back</button>
  <label style="margin-left: 1rem;">
    <input type="checkbox" id="autoplay" checked> Auto-play our move
  </label>
  <label style="margin-left: 0.5rem;">
    <input type="checkbox" id="flip"> Flip board
  </label>
</div>

<div class="row">
  <div id="board-wrap">
    <div id="board"></div>
  </div>

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
const PERSPECTIVE = "__PERSPECTIVE__";
const SOURCE = "__SOURCE__";
document.getElementById("perspective").textContent = PERSPECTIVE;
document.getElementById("source").textContent = SOURCE;

const game = new Chess();
const moveStack = [];   // SAN moves played so far

let board;
let slices = [];
let currentEvent = null;
let currentElo = null;

// ── Chessboard ────────────────────────────────────────────────────────────
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

function takeBack() {
  if (moveStack.length === 0) return;
  game.undo();
  moveStack.pop();
  updateBoard();
}

function reset() {
  game.reset();
  moveStack.length = 0;
  updateBoard();
}

// ── API ───────────────────────────────────────────────────────────────────
async function fetchPosition() {
  if (!currentEvent || currentElo === null) return;
  const epd = game.fen().split(" ").slice(0, 4).join(" ");
  const url = `/api/position?event=${encodeURIComponent(currentEvent)}` +
              `&elo_band=${currentElo}&epd=${encodeURIComponent(epd)}`;
  let data;
  try {
    const r = await fetch(url);
    if (!r.ok) {
      renderStatus(null, "Error: " + r.status);
      return;
    }
    data = await r.json();
  } catch (e) {
    renderStatus(null, "Network error: " + e);
    return;
  }
  renderStatus(data);
}

function renderStatus(data, errMsg) {
  const status = document.getElementById("status");
  const info = document.getElementById("position-info");
  if (errMsg) {
    status.innerHTML = `<div class="info-box warn">${errMsg}</div>`;
    info.innerHTML = "";
    return;
  }
  if (!data) {
    status.innerHTML = "";
    info.innerHTML = "";
    return;
  }
  if (game.game_over()) {
    status.innerHTML = `<div class="info-box">Game over: ${gameResult()}</div>`;
    info.innerHTML = renderPositionInfo(data);
    return;
  }
  if (!data.in_repertoire) {
    status.innerHTML = `<div class="info-box warn">
      <strong>Out of repertoire</strong>
      Position not in our recommended tree for this slice.
      <div class="small">Use Take back, or Reset to start over.</div>
    </div>`;
    info.innerHTML = renderPositionInfo(data);
    return;
  }
  let html = "";
  if (data.is_our_turn) {
    html = renderOurTurn(data);
  } else {
    html = renderOpponentTurn(data);
  }
  status.innerHTML = html;
  info.innerHTML = renderPositionInfo(data);

  // Wire up click handlers
  document.querySelectorAll("tr.clickable").forEach(tr => {
    tr.addEventListener("click", () => {
      const san = tr.dataset.san;
      tryMove(san);
    });
  });
  const playBtn = document.getElementById("play-our-btn");
  if (playBtn) playBtn.addEventListener("click", () => tryMove(playBtn.dataset.san));

  // Autoplay our recommended move
  if (data.is_our_turn && data.our_best_move &&
      document.getElementById("autoplay").checked) {
    setTimeout(() => tryMove(data.our_best_move), 250);
  }
}

function renderOurTurn(data) {
  if (!data.our_best_move) {
    return `<div class="info-box warn">
      <strong>No recommendation</strong>
      Position is in our repertoire but has no best_move set
      (likely a position in a cycle that fell into the fallback).
    </div>`;
  }
  const value = data.our_value !== null
    ? `value <strong>${data.our_value.toFixed(3)}</strong> (white-score)`
    : "";
  const forcing = data.our_forcingness !== null
    ? ` &middot; forcingness <strong>${data.our_forcingness.toFixed(3)}</strong>`
    : "";
  return `<div class="info-box our">
    <strong>${PERSPECTIVE}'s recommendation: <span class="move">${data.our_best_move}</span></strong>
    <div class="small">${value}${forcing}</div>
    <button id="play-our-btn" data-san="${data.our_best_move}"
            style="margin-top:0.4rem;">Play ${data.our_best_move}</button>
  </div>`;
}

function renderOpponentTurn(data) {
  if (!data.opp_top_moves.length) {
    return `<div class="info-box warn">No opponent moves recorded for this position.</div>`;
  }
  let rows = data.opp_top_moves.map(m => `
    <tr class="clickable" data-san="${m.san}">
      <td class="move">${m.san}</td>
      <td class="num">${m.total.toLocaleString()}</td>
      <td class="num">${(m.share * 100).toFixed(1)}%</td>
      <td class="num">${m.white_score.toFixed(3)}</td>
    </tr>
  `).join("");
  return `<div class="info-box opp">
    <strong>Opponent (${data.side_to_move}) — top empirical replies</strong>
    <div class="small">click a row to play that move</div>
    <table>
      <thead>
        <tr><th>move</th><th class="num">games</th><th class="num">share</th><th class="num">white-score</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function renderPositionInfo(data) {
  return `EPD: <code>${data.epd}</code><br>
          position_hash: <code>${data.position_hash}</code> &middot;
          ${data.side_to_move} to move
          ${data.in_repertoire ? '<span class="badge">in repertoire</span>' : ''}`;
}

function gameResult() {
  if (game.in_checkmate()) return game.turn() === "w" ? "0-1 (black wins)" : "1-0 (white wins)";
  if (game.in_stalemate()) return "1/2-1/2 (stalemate)";
  if (game.in_threefold_repetition()) return "1/2-1/2 (threefold)";
  if (game.insufficient_material()) return "1/2-1/2 (insufficient)";
  if (game.in_draw()) return "1/2-1/2 (draw)";
  return "?";
}

// ── Slice selection ──────────────────────────────────────────────────────
async function loadSlices() {
  const r = await fetch("/api/slices");
  slices = await r.json();
  const events = Array.from(new Set(slices.map(s => s.event)));
  const evSel = document.getElementById("event-select");
  evSel.innerHTML = events.map(e => `<option value="${e}">${e}</option>`).join("");
  evSel.value = events.includes("Blitz") ? "Blitz" : events[0];
  updateEloOptions();
}

function updateEloOptions() {
  const ev = document.getElementById("event-select").value;
  const elos = slices.filter(s => s.event === ev).map(s => s.elo_band).sort((a,b) => a-b);
  const eloSel = document.getElementById("elo-select");
  eloSel.innerHTML = elos.map(e => `<option value="${e}">${e}</option>`).join("");
  eloSel.value = elos.includes(1500) ? 1500 : elos[Math.floor(elos.length / 2)];
  applySlice();
}

function applySlice() {
  currentEvent = document.getElementById("event-select").value;
  currentElo   = parseInt(document.getElementById("elo-select").value);
  const sliceMeta = slices.find(s => s.event === currentEvent && s.elo_band === currentElo);
  document.getElementById("slice-info").textContent =
    sliceMeta ? `(${sliceMeta.n_games.toLocaleString()} games at start)` : "";
  reset();
}

// ── Init ─────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
  board = Chessboard("board", {
    position: "start",
    pieceTheme: "https://cdn.jsdelivr.net/gh/oakmac/chessboardjs@master/website/img/chesspieces/wikipedia/{piece}.png",
    orientation: PERSPECTIVE,
  });

  document.getElementById("reset-btn").addEventListener("click", reset);
  document.getElementById("back-btn").addEventListener("click", takeBack);
  document.getElementById("event-select").addEventListener("change", updateEloOptions);
  document.getElementById("elo-select").addEventListener("change", applySlice);
  document.getElementById("flip").addEventListener("change", e => {
    board.orientation(e.target.checked
      ? (PERSPECTIVE === "white" ? "black" : "white")
      : PERSPECTIVE);
  });

  await loadSlices();
});
</script>
</body>
</html>
"""


# ── HTTP server ────────────────────────────────────────────────────────────

class RepertoireHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        url = urlparse(self.path)
        path = url.path
        params = parse_qs(url.query)

        if path in ("/", "/index.html"):
            html = (
                INDEX_HTML
                .replace("__PERSPECTIVE__", self.server.repertoire.perspective)
                .replace("__SOURCE__", self.server.source_label)
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)
            return

        if path == "/api/slices":
            self._send_json(self.server.repertoire.list_slices())
            return

        if path == "/api/position":
            event    = params.get("event",    [None])[0]
            elo_band = params.get("elo_band", [None])[0]
            epd      = params.get("epd",      [None])[0]
            if not (event and elo_band and epd):
                self.send_error(400, "missing event/elo_band/epd")
                return
            try:
                eb = int(elo_band)
            except ValueError:
                self.send_error(400, "invalid elo_band")
                return
            data = self.server.repertoire.get_position(event, eb, epd)
            if data is None:
                self.send_error(404, "slice or position not found")
                return
            self._send_json(data)
            return

        self.send_error(404)

    def _send_json(self, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A002
        # Suppress per-request access logs; only errors will surface.
        pass


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--repertoire",
        default="D:/data/chess/repertoire/repertoire_2019_black_v2.parquet",
        help="Stage 3 repertoire parquet",
    )
    parser.add_argument(
        "--stats",
        default="D:/data/chess/position-stats/position_stats_2019.parquet",
        help="Stage 2 position-stats parquet",
    )
    parser.add_argument(
        "--perspective",
        choices=["white", "black"],
        default="black",
        help="Whose repertoire is loaded (controls UI: best-move display vs opponent table)",
    )
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--no-browser", action="store_true",
                        help="Don't auto-open the browser")
    args = parser.parse_args()

    rep_path   = Path(args.repertoire)
    stats_path = Path(args.stats)
    if not rep_path.exists():
        sys.exit(f"Repertoire not found: {rep_path}")
    if not stats_path.exists():
        sys.exit(f"Stats not found: {stats_path}")

    print(f"Loading repertoire: {rep_path}")
    print(f"Loading stats:      {stats_path}")
    print(f"Perspective:        {args.perspective}")
    rep = RepertoireData(rep_path, stats_path, args.perspective)
    print(f"Indexed {len(rep.slices)} (event, elo_band) slices.")

    server = HTTPServer((args.host, args.port), RepertoireHandler)
    server.repertoire = rep
    server.source_label = f"{rep_path.name}  +  {stats_path.name}"

    url = f"http://{args.host}:{args.port}"
    print(f"\n  Repertoire browser ready at: {url}")
    print( "  Press Ctrl+C to stop.\n")

    if not args.no_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
