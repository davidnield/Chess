"""Explorer section backend: the original repertoire_explorer.Explorer with a
memory-safe slice implementation, loaded lazily in a background thread.

Instead of copying get_position (150 lines of debugged edge cases — augmented
gold synthesis, child-metric lookups), we subclass Explorer and override ONLY
_slice(): the original materializes every column into Python dicts/lists
(~40 GB after pre-warm for the pooled reps); this version sorts the slice
frames once and serves the same interface through numpy-searchsorted views
(the technique _raw_crush already uses), keeping RSS at the polars frames
(~a few GB). get_position/list_slices/slice_info/_raw_crush are inherited
unchanged, so the JSON contract is identical by construction.

The frontend is likewise the original INDEX_HTML with its CDN asset URLs
rewritten to the locally vendored copies (offline-safe) and a tiny deep-link
shim appended (#line=1.e4 e5 ... replays a line on load).
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/
from repertoire_explorer import (  # noqa: E402
    INDEX_HTML, METRIC_COLS, Explorer, _meta_crush_weight)


# ── searchsorted-backed stand-ins for the original dict/list/set views ──────

class _SortedIndex:
    """hash -> row position in a hash-sorted frame (dict.get contract)."""
    def __init__(self, arr: np.ndarray):
        self.arr = arr

    def get(self, key, default=None):
        if key is None:
            return default
        i = int(np.searchsorted(self.arr, key))
        if i < len(self.arr) and int(self.arr[i]) == key:
            return i
        return default


class _SortedSet:
    def __init__(self, arr: np.ndarray):
        self.arr = arr

    def __contains__(self, key):
        i = int(np.searchsorted(self.arr, key))
        return i < len(self.arr) and int(self.arr[i]) == key


class _GroupLookup:
    """parent_hash -> list of row indices (contiguous: frame is sorted by
    (parent_hash, total desc), so ranges are already most-played-first)."""
    def __init__(self, arr: np.ndarray):
        self.arr = arr

    def get(self, key, default=None):
        lo = int(np.searchsorted(self.arr, key, "left"))
        hi = int(np.searchsorted(self.arr, key, "right"))
        return list(range(lo, hi)) if hi > lo else default


class _NoneCol:
    """Stand-in for an absent child_hash column."""
    def __getitem__(self, i):
        return None


class LazyExplorer(Explorer):
    """Explorer with lazy searchsorted slices instead of dict-expanded ones."""

    def _slice(self, label: str, ev: str, eb: int) -> dict | None:
        if label not in self.reps:
            return None
        key = (label, ev, eb)
        cached = self._cache.get(key)
        if cached is not None:
            self._cache.move_to_end(key)
            return cached

        rep = (self.reps[label]
               .filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
               .sort("position_hash"))
        if rep.height == self.reps[label].height:
            # Single-slice source (the pooled reps): free the unsorted original
            # instead of holding both copies. Future filters see the same rows.
            self.reps[label] = rep
        rep_ph = rep["position_hash"].to_numpy()
        rep_cols = {"best_move": rep["best_move"]}
        for c in METRIC_COLS:
            if c in rep.columns:
                rep_cols[c] = rep[c]

        # Stats slice shared across labels — cache it separately.
        st_key = (ev, eb)
        st_entry = getattr(self, "_st_cache", {}).get(st_key)
        if st_entry is None:
            st = (self.stats
                  .filter((pl.col("event") == ev) & (pl.col("elo_band") == eb))
                  # maintain_order: the original sorts group indices with a
                  # STABLE key sort, so tied totals keep file order — match it.
                  .sort(["parent_hash", "total"], descending=[False, True],
                        maintain_order=True))
            if st.height == self.stats.height:
                self.stats = st              # single-slice: drop unsorted copy
            st_parent = st["parent_hash"].to_numpy()
            st_cols = {"move_san": st["move_san"], "total": st["total"],
                       "white_score_avg": st["white_score_avg"],
                       "child_hash": (st["child_hash"] if "child_hash" in st.columns
                                      else _NoneCol())}
            groups = _GroupLookup(st_parent)
            n_games = sum(st_cols["total"][i]
                          for i in (groups.get(self.start_hash) or []))
            st_entry = {"groups": groups, "st_cols": st_cols, "n_games": n_games}
            if not hasattr(self, "_st_cache"):
                self._st_cache = {}
            self._st_cache[st_key] = st_entry

        entry = {"rep_index": _SortedIndex(rep_ph), "rep_cols": rep_cols,
                 "pos_set": _SortedSet(rep_ph), "groups": st_entry["groups"],
                 "st_cols": st_entry["st_cols"], "n_games": st_entry["n_games"]}
        self._cache[key] = entry
        if len(self._cache) > self._cache_max:
            self._cache.popitem(last=False)
        return entry


# ── vendored-asset rewrite of the original frontend ─────────────────────────

_ASSET_REWRITES = {
    "https://unpkg.com/@chrisoakman/chessboardjs@1.0.0/dist/chessboard-1.0.0.min.css":
        "/static/vendor/chessboard-1.0.0.min.css",
    "https://unpkg.com/@chrisoakman/chessboardjs@1.0.0/dist/chessboard-1.0.0.min.js":
        "/static/vendor/chessboard-1.0.0.min.js",
    "https://code.jquery.com/jquery-3.6.0.min.js":
        "/static/vendor/jquery-3.6.0.min.js",
    "https://cdnjs.cloudflare.com/ajax/libs/chess.js/0.10.3/chess.min.js":
        "/static/vendor/chess-0.10.3.min.js",
    "https://cdn.jsdelivr.net/gh/oakmac/chessboardjs@master/website/img/chesspieces/wikipedia/{piece}.png":
        "/static/vendor/img/chesspieces/wikipedia/{piece}.png",
}

_DEEPLINK_SHIM = """
<script>
window.addEventListener("load", () => {
  const m = location.hash.match(/line=([^&]+)/);
  if (!m) return;
  const sans = decodeURIComponent(m[1]).split(/\\s+/).filter(s => s && !/^\\d+\\.+$/.test(s));
  const play = () => {
    try { sans.forEach(s => tryMove(s)); } catch (e) { console.warn("deeplink:", e); }
  };
  setTimeout(play, 800);   // after initial slice load
});
</script>
</body>"""


def render_explorer_html(source_label: str, crush_weight: float) -> str:
    html = INDEX_HTML
    for old, new in _ASSET_REWRITES.items():
        html = html.replace(old, new)
    html = html.replace("__SOURCE__", source_label)
    html = html.replace("__CRUSH_W__", repr(crush_weight))
    return html.replace("</body>", _DEEPLINK_SHIM, 1)


# ── lazy loading service ────────────────────────────────────────────────────

class ExplorerService:
    """Owns the LazyExplorer and its background load. status: unloaded ->
    loading -> ready | error."""

    def __init__(self):
        self.status = "unloaded"
        self.error: str | None = None
        self.explorer: LazyExplorer | None = None
        self.source_label = ""
        self.crush_weight = 25.0
        self._lock = threading.Lock()

    def start_load(self, settings: dict) -> str:
        with self._lock:
            if self.status in ("loading", "ready"):
                return self.status
            self.status = "loading"
        threading.Thread(target=self._load, args=(settings,), daemon=True).start()
        return self.status

    def _load(self, settings: dict) -> None:
        try:
            specs = []
            for label, key in (("White", "white_rep"), ("Black", "black_rep")):
                p = settings.get(key)
                if p and Path(p).exists():
                    specs.append((label, label.lower(), p))
            if not specs:
                raise FileNotFoundError(
                    "no repertoire parquet found — check the White/Black repertoire "
                    "paths in Settings (these live on the pipeline machine's E: drive)")
            stats = settings.get("stats")
            if not stats or not Path(stats).exists():
                raise FileNotFoundError(
                    f"stats parquet not found ({stats}) — set it in Settings")
            cw = _meta_crush_weight(specs[0][2])
            self.crush_weight = 25.0 if cw is None else cw
            self.explorer = LazyExplorer(
                specs, stats, crush_weight=self.crush_weight,
                crush_totals=settings.get("crush_totals"))
            self.source_label = " + ".join(s[2].rsplit("/", 1)[-1] for s in specs)
            self.status = "ready"
        except SystemExit as e:          # Explorer.__init__ guards sys.exit
            self.error = str(e)
            self.status = "error"
        except Exception as e:
            self.error = f"{type(e).__name__}: {e}"
            self.status = "error"
