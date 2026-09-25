"""--epd-max-ply is an exact no-op at its default and fills only NULLs above it.

B1 lets the extract write parent_epd past EPD_MAX_PLY (16), so a month built at
--epd-max-ply 30 needs no EPD backfill. The flag threads through four places
(_walk_game, extract_file, _init_worker, _worker) because Windows spawn
re-imports the module in every worker, and a value set on the constant in the
parent would silently not arrive. Everything about that is quiet when wrong, so:

  (i)   at the default the output is frame-equal, on the full key, to d79a0c7's
        extract_file -- loaded from `git show` into a temp module, so the
        reference is the shipped code rather than a copy of it -- over 8,000
        real games (skipped without D:) plus synthetic ones;
  (ii)  at 30 only parent_epd changes, only where it was NULL, and every value
        it gains equals a direct python-chess replay's board.epd() of that
        position; the term partial is untouched;
  (iii) through the real CLI with a spawned worker, --epd-max-ply 30 reaches
        the worker (no NULL EPD in the partials), the params lock is written,
        a rerun with different settings is refused, and an identical rerun is
        accepted.

Run: .venv/Scripts/python.exe python/_test_epd_max_ply.py
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import chess
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
import build_pooled_stats as bps
from stage1_extract_positions import iter_san_moves
from zobrist import zobrist_int64

BASE_COMMIT = "d79a0c7"
REPO = Path(__file__).resolve().parent.parent
KEY = ["parent_hash", "move_san", "event", "elo_band"]
TERM_KEY = ["position_hash", "kind", "reason"]
EXCLUDED = frozenset({"Rules infraction", "Abandoned"})
# The explorer contract: what every banded partial was built with.
EXPLORER = dict(min_elo=0, max_ply=30, tiers=None, with_child_eval=False,
                exclude_bots=True, excluded_terminations=EXCLUDED)
REAL_GAMES = 8000
REAL_FILES = [
    Path("D:/data/chess/standard-chess-games-compressed/year=2024/month=6/event=Blitz/part-0.parquet"),
    Path("D:/data/chess/standard-chess-games-compressed/year=2013/month=1/event=Blitz/part-0.parquet"),
]

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> bool:
    _checks.append((bool(ok), label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    return bool(ok)


# ── synthetic games ───────────────────────────────────────────────────────────

RUY = ("e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6 O-O Be7 Re1 b5 Bb3 d6 c3 O-O h3 Nb8 d4 "
       "Nbd7 c4 c6 cxb5 axb5 Nc3 Bb7 Bg5 b4 Nb1 h6 Bh4 c5 dxe5 Nxe4 Bxe7 Qxe7 "
       "exd6 Qf6 Nbd2 Nxd6").split()
# Twelve plies of knight shuffling return to the start position, so this reaches
# RUY's ply-7 position at ply 19. It comes FIRST in the file: its key's first
# occurrence is past ply 16 (EPD NULL at the default) and RUY supplies the EPD
# later in the same chunk -- the drop_nulls().first() case. Both games sit in
# the 1800 band: elo_band is part of the key, so another band is another key.
SHUFFLE = ("Nf3 Nf6 Ng1 Ng8 " * 3).split() + RUY[:24]


def pgn(sans: list[str], result: str = "1-0") -> str:
    out = []
    for i, s in enumerate(sans):
        if i % 2 == 0:
            out.append(f"{i // 2 + 1}.")
        out.append(s)
    return " ".join(out + [result])


# (movetext, white_score, termination, mean_elo, white_title, black_title)
SYNTH = [
    (pgn(SHUFFLE), 1.0, "Normal", 1850, None, None),
    (pgn(RUY), 0.5, "Normal", 1850, None, None),
    (pgn(RUY[:19] + ["Qxz9"] + RUY[20:]), 0.0, "Time forfeit", 1500, None, None),
    ("1. e4 {best by test} e5 (1... c5 2. Nf3) 2. Nf3 $1 Nc6 3. Bb5!? a6?! 1-0",
     1.0, "Normal", 1650, None, None),
    ("1. e4 Nf6 2. e5 d5 3. exd6 cxd6 4. Nf3 g6 1-0", 1.0, "Normal", 1999, None, None),
    ("1. e4 d5 2. exd5 c6 3. dxc6 Nf6 4. cxb7 Nbd7 5. bxa8=Q Nb6 6. Qxb8 1-0",
     1.0, "Normal", 2600, None, None),
    ("1. e4 d5 2. exd5 c6 3. dxc6 Nf6 4. cxb7 Nbd7 5. bxa8=N Nb6 0-1",
     0.0, "Normal", 1200, None, None),
    (None, 0.5, "Normal", 1800, None, None),
    ("", 1.0, "Abandoned", 1800, None, None),
    ("1-0", 1.0, "Normal", 1400, None, None),
    (pgn(RUY[:10]), float("nan"), "Normal", 1800, None, None),
    (pgn(RUY[:6]), None, "Normal", 1800, None, None),
    (pgn(RUY[:6]), 1.0, "Normal", None, None, None),
    (pgn(RUY[:6]), 1.0, "Rules infraction", 1800, None, None),
    (pgn(RUY[:6]), 1.0, "Normal", 1800, "BOT", None),
    (pgn(RUY[:33]), 0.0, "Normal", 1000, None, "GM"),
]


def write_source(path: Path, games: list[tuple]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*games))
    pq.write_table(pa.table({
        "movetext": pa.array(cols[0], pa.string()),
        "white_score": pa.array(cols[1], pa.float64()),
        "termination": pa.array(cols[2], pa.string()),
        "move_count": pa.array([None] * len(games), pa.int16()),
        "mean_elo": pa.array(cols[3], pa.int16()),
        "white_title": pa.array(cols[4], pa.string()),
        "black_title": pa.array(cols[5], pa.string()),
        "white_elo": pa.array([None] * len(games), pa.int16()),
        "black_elo": pa.array([None] * len(games), pa.int16()),
    }), path)
    return path


# ── references ────────────────────────────────────────────────────────────────

def load_base(d: Path):
    """d79a0c7's build_pooled_stats as a separate module, or None without git.
    Its sibling imports (stage1_extract_positions, zobrist, ...) resolve to this
    checkout's, which B1 does not touch."""
    try:
        src = subprocess.run(
            ["git", "-C", str(REPO), "show", f"{BASE_COMMIT}:python/build_pooled_stats.py"],
            capture_output=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    d.mkdir(parents=True, exist_ok=True)
    f = d / "bps_base_d79a0c7.py"
    f.write_bytes(src)
    spec = importlib.util.spec_from_file_location("bps_base_d79a0c7", f)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def truth_epds(src: Path, limit: int | None) -> dict[int, str]:
    """hash -> board.epd() for every position a replay of the file reaches,
    walked exactly as far as the extract walks it."""
    out: dict[int, str] = {}
    t = pq.read_table(src, columns=["movetext"])
    mts = t.column(0).to_pylist()[:limit]
    for mt in mts:
        if not mt:
            continue
        board = chess.Board()
        for san in list(iter_san_moves(mt))[:30]:
            out[zobrist_int64(board)] = board.epd()
            try:
                board.push(board.parse_san(san))
            except (ValueError, AssertionError):
                break
    return out


def run(mod, src: Path, out: Path, tag: str, limit: int | None, **kw) -> tuple:
    ps, tm = out / f"{tag}.ps.parquet", out / f"{tag}.term.parquet"
    r = mod.extract_file(src, ps, None, limit_games=limit, term_out=tm,
                         event="Blitz", **EXPLORER, **kw)
    return pl.read_parquet(ps).sort(KEY), pl.read_parquet(tm).sort(TERM_KEY), r


def compare_sources(base, label: str, src: Path, limit: int | None, tmp: Path) -> None:
    print(f"\n{label}: {src.name}" + (f" ({limit:,} games)" if limit else ""))
    d16, t16, r16 = run(bps, src, tmp, f"{label}_new16", limit)
    if base is not None:
        b16, bt16, _ = run(base, src, tmp, f"{label}_base", limit)
        check(d16.equals(b16) and d16.height > 0,
              f"default output is frame-equal to {BASE_COMMIT}: ps "
              f"({d16.height:,} rows, all {len(d16.columns)} columns)")
        check(t16.equals(bt16), f"and term ({t16.height:,} rows)")
    d30, t30, r30 = run(bps, src, tmp, f"{label}_new30", limit, epd_max_ply=30)
    check(r16["games"] == r30["games"] and r16["kept"] == r30["kept"]
          and r16["failed"] == r30["failed"],
          f"same games, kept and failed ({r30['kept']:,}/{r30['games']:,}, "
          f"{r30['failed']} failed)")
    check(d30.drop("parent_epd").equals(d16.drop("parent_epd")),
          "at 30 every column but parent_epd is unchanged")
    check(t30.equals(t16), "and the term partial is unchanged")
    a, b = d16["parent_epd"], d30["parent_epd"]
    kept = a.is_not_null()
    check(bool((a.filter(kept) == b.filter(kept)).all()),
          f"every EPD present at 16 is identical at 30 ({int(kept.sum()):,} rows)")
    check(b.null_count() == 0,
          f"at 30 no row is NULL (was {a.null_count():,} of {a.len():,})")
    truth = truth_epds(src, limit)
    filled = d30.filter(d16["parent_epd"].is_null())
    wrong = [(h, e) for h, e in zip(filled["parent_hash"], filled["parent_epd"])
             if truth.get(h) != e]
    check(filled.height > 0 and not wrong,
          f"each of the {filled.height:,} filled EPDs equals a direct python-chess "
          f"replay's board.epd() ({len(wrong)} wrong)")


# ── the CLI, a spawned worker, and the lock ───────────────────────────────────

def cli(root: Path, pdir: Path, epd: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(Path(bps.__file__)), "--start-year", "2099",
         "--end-year", "2099", "--months", "1", "--phase", "extract",
         "--events", "Blitz", "--min-elo", "0", "--no-prune", "--max-ply", "30",
         "--no-fuse-winpos", "--no-child-eval", "--exclude-bots",
         "--exclude-terminations", "Rules infraction", "Abandoned",
         "--chunk-games", "250000", "--workers", "1", "--source", str(root),
         "--partial-dir", str(pdir), "--tag", "test_epd", "--epd-max-ply", str(epd)],
        capture_output=True, text=True, timeout=600)


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="test_epd_max_ply_"))
    try:
        base = load_base(tmp / "base")
        if base is None:
            print(f"  SKIP  {BASE_COMMIT} reference: `git show` unavailable")
        synth = write_source(tmp / "synth.parquet", SYNTH)
        compare_sources(base, "synthetic", synth, None, tmp)
        # The drop_nulls case, located rather than trusted to the sample.
        d16, _, _ = run(bps, synth, tmp, "loc16", None)
        b = chess.Board()
        for san in RUY[:6]:
            b.push(b.parse_san(san))
        row = d16.filter((pl.col("parent_hash") == zobrist_int64(b))
                         & (pl.col("move_san") == "Ba4") & (pl.col("elo_band") == 1800))
        check(row.height == 1 and row["ply"][0] == 19
              and row["parent_epd"][0] == b.epd(),
              "a key first seen at ply 19 keeps ply 19 and takes its EPD from a "
              "later ply-7 occurrence")

        real = next((f for f in REAL_FILES if f.exists()), None)
        if real is None:
            print(f"\n  SKIP  real games: none of {[str(f) for f in REAL_FILES]}")
        else:
            compare_sources(base, "real", real, REAL_GAMES, tmp)

        print("\nthe CLI: a spawned worker, and the params lock")
        root = tmp / "src"
        write_source(root / "year=2099" / "month=1" / "event=Blitz" / "part-0.parquet",
                     SYNTH)
        pdir = tmp / "partials"
        p30 = cli(root, pdir, 30)
        check(p30.returncode == 0, f"--epd-max-ply 30 extract runs (rc {p30.returncode})")
        parts = sorted(pdir.glob("*.ps.parquet"))
        got = pl.concat([pl.read_parquet(f) for f in parts]) if parts else None
        check(got is not None and got.height > 0
              and got["parent_epd"].null_count() == 0
              and int(got["ply"].max()) > bps.EPD_MAX_PLY,
              "the spawned worker received 30: rows past ply 16 carry an EPD")
        lock = pdir / bps.EXTRACT_PARAMS_FILE
        want = {"epd_max_ply": 30, "max_ply": 30, "chunk_games": 250000,
                "events": ["Blitz"], "min_elo": 0, "exclude_bots": True,
                "excluded_terminations": ["Abandoned", "Rules infraction"],
                "producer": "python"}
        check(lock.exists() and json.loads(lock.read_text()) == want,
              f"the lock records the run's settings ({lock.name})")
        p16 = cli(root, pdir, 16)
        check(p16.returncode != 0 and "epd_max_ply" in (p16.stdout + p16.stderr),
              "a rerun at --epd-max-ply 16 into that dir is refused, naming the field")
        again = cli(root, pdir, 30)
        check(again.returncode == 0 and "0 to process" in again.stdout,
              "an identical rerun is accepted and resumes (0 files to process)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
