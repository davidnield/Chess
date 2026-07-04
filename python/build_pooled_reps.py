"""
Build the canonical repertoire set: the POOLED (1900+2200 x Blitz/Rapid/Classical)
slice with the robustness-gate Stage 3, for three forced White first moves
(1.e4 / 1.d4 / 1.Nf3) and a Black repertoire.

This is the single runnable definition of the "current" repertoire. It replaces
the ad-hoc command lines that previously lived only in logs/pool_final2/*.log.
The legacy orchestrator run_2025_combined.py builds the OLD forcing-weight grid
(no crush / no gate / per-(event,elo) slices) and is NOT this pipeline.

Two crush modes (outputs coexist for comparison):
  --crush-mode relative-propagated (DEFAULT, canonical) discounted RELATIVE
                                   crush_potential propagated through the DAG —
                                   rewards steering into crushing positions at any
                                   depth. Tuned γ=0.9, weight=25 (the e4 line
                                   saturates at ~25): White → Danish proper + Ng5,
                                   Black → modern Scandinavian, both gate-sound.
                                   Suffix _crushprop.
  --crush-mode absolute            legacy crush@15 from the absolute histogram;
                                   per-edge bonus, not propagated. Suffix _crush15.

Pipeline (each step resumable via output skip-gate):

  position_stats_2024_2025.parquet + crush_hist[_rel]_2024_2025.parquet
     │  build_combined_slice.py   (pool the 6 sets into ONE synthetic slice)
     ▼
  position_stats_pooled_1900_2200_brc.parquet
  crush_hist[_rel]_pooled_1900_2200_brc.parquet
     │  stage3_backwards_induction.py  x4  (robustness gate, eval blend, chosen crush mode)
     ▼
  repertoire_pooled_{white_e4,white_d4,white_nf3,black}_{crush15|crushprop}.parquet

Prerequisites (built by earlier one-shot scripts, not re-run here):
  - position_stats_2024_2025.parquet   (merge_stats_2024_2025.py)
  - crush_hist_2024_2025.parquet       (build_crush_stats.py — absolute, for crush15)
  - crush_hist_rel_2024_2025.parquet   (build_crush_stats.py — relative, for crushprop)
  - lichess_eval_db.parquet            (build_lichess_eval_db.py)

Usage:
    .venv/Scripts/python.exe python/build_pooled_reps.py            # canonical (relative-propagated)
    .venv/Scripts/python.exe python/build_pooled_reps.py --crush-mode absolute   # legacy crush@15
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PROJECT   = Path(__file__).resolve().parent.parent
PY        = sys.executable  # the venv python that launched us
STATS_DIR = Path("E:/chess/position-stats")
REP_DIR   = Path("E:/chess/repertoire")
LOG_DIR   = PROJECT / "logs" / "pool_reps"

# ── Inputs / outputs ────────────────────────────────────────────────────────
POOLED_STATS     = STATS_DIR / "position_stats_pooled_1900_2200_brc.parquet"
POOLED_CRUSH     = STATS_DIR / "crush_hist_pooled_1900_2200_brc.parquet"       # absolute
POOLED_CRUSH_REL = STATS_DIR / "crush_hist_rel_pooled_1900_2200_brc.parquet"   # relative
EVAL_DB          = Path("E:/chess/lichess_eval_db.parquet")

# Output filename suffix per crush mode, so absolute and relative-propagated reps coexist.
MODE_SUFFIX = {"absolute": "crush15", "relative-propagated": "crushprop"}

# (tag, perspective-and-force flags).
REPS: list[tuple[str, list[str]]] = [
    ("white_e4",  ["--perspective", "white", "--force-root-move", "e4"]),
    ("white_d4",  ["--perspective", "white", "--force-root-move", "d4"]),
    ("white_nf3", ["--perspective", "white", "--force-root-move", "Nf3"]),
    ("black",     ["--perspective", "black"]),
]


def common_stage3(mode: str, weight: float, gamma: float, imm_window: int,
                  stats: Path, crush_db: Path, eval_db: Path) -> list[str]:
    """Stage 3 flags shared by all four reps for the chosen crush mode, parameterized by
    the resolved input paths. Tuned: crush_prior=20000 stops the small-sample spiral;
    robustness_floor=0.04 is the gate. Default crush_weight differs by mode (the two
    metrics live on different scales)."""
    flags = [
        "--input",            str(stats),
        "--eval-db",          str(eval_db),       "--eval-weight", "0.5",
        "--crush-weight",     str(weight),        "--crush-prior", "20000",
        "--robustness-floor", "0.04",
        "--crush-mode",       mode,
        "--crush-db",         str(crush_db),
    ]
    if mode == "relative-propagated":
        flags += ["--crush-gamma", str(gamma), "--crush-imm-window", str(imm_window)]
    else:
        flags += ["--crush-horizon", "15"]
    return flags


def run(name: str, cmd: list[str]) -> bool:
    """Run a child command, teeing a header/footer + exit status into a log file."""
    logfile = LOG_DIR / f"{name}.log"
    print(f"\n{'='*70}\n  {name}\n  log: {logfile}\n{'='*70}", flush=True)
    t0 = time.time()
    with open(logfile, "w", encoding="utf-8") as f:
        f.write(f"=== {name} @ {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        f.write("  " + " ".join(cmd) + "\n\n")
        f.flush()
        rc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT).returncode
        f.write(f"\n=== exit {rc}, {(time.time()-t0)/60:.1f} min ===\n")
    print(f"  {'OK' if rc == 0 else 'FAILED'}  ({(time.time()-t0)/60:.1f} min)", flush=True)
    return rc == 0


def rep_path(tag: str, mode: str) -> Path:
    return REP_DIR / f"repertoire_pooled_{tag}_{MODE_SUFFIX[mode]}.parquet"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true",
                    help="Rebuild reps even if their output already exists.")
    ap.add_argument("--crush-mode", choices=["absolute", "relative-propagated"],
                    default="relative-propagated",
                    help="relative-propagated (default, CANONICAL): discounted relative "
                         "crush_potential propagated through the DAG (suffix _crushprop); "
                         "tuned γ=0.9, weight=25 — sharpens both colours (Danish / modern "
                         "Scandinavian) while the gate keeps them sound. "
                         "absolute: legacy crush@15 reps (suffix _crush15). Both coexist.")
    ap.add_argument("--crush-weight", type=float, default=None,
                    help="Selection weight on crush. Default depends on mode (the two "
                         "metrics live on different scales): 10 for absolute, 25 for "
                         "relative-propagated (tuned — the e4 line saturates at ~25).")
    ap.add_argument("--crush-gamma", type=float, default=0.9,
                    help="relative-propagated only: per-move discount (tuned default 0.9).")
    ap.add_argument("--crush-imm-window", type=int, default=2,
                    help="relative-propagated only: immediate-window in full moves (default 2).")
    ap.add_argument("--input", default=None,
                    help="Pooled position-stats parquet. Default: the 1900+2200 pool this "
                         "script builds via build_combined_slice. Point at a pre-built pool "
                         "(e.g. build_pooled_stats.py's position_stats_pooled_ge1800_2019_2025_brc) "
                         "to skip Step 1.")
    ap.add_argument("--crush-db", default=None,
                    help="Crush histogram parquet matching --crush-mode. Default: the "
                         "mode-appropriate 1900+2200 pooled file.")
    ap.add_argument("--eval-db", default=None,
                    help="Stockfish eval DB parquet (default: lichess_eval_db.parquet).")
    args = ap.parse_args()
    mode = args.crush_mode
    weight = args.crush_weight if args.crush_weight is not None else (
        25.0 if mode == "relative-propagated" else 10.0)

    # Resolve inputs: explicit overrides, else the script's default 1900+2200 pool.
    stats = Path(args.input) if args.input else POOLED_STATS
    eval_db = Path(args.eval_db) if args.eval_db else EVAL_DB
    pooled_crush = Path(args.crush_db) if args.crush_db else (
        POOLED_CRUSH_REL if mode == "relative-propagated" else POOLED_CRUSH)
    override = args.input is not None or args.crush_db is not None

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REP_DIR.mkdir(parents=True, exist_ok=True)
    if not eval_db.exists():
        sys.exit(f"FATAL: missing eval DB {eval_db}")
    t_all = time.time()

    # ── Step 1: ensure the pooled slice exists. ──────────────────────────────
    print(f"\n{'#'*70}\n# Step 1: build pooled slice (mode={mode})\n{'#'*70}")
    if stats.exists() and pooled_crush.exists():
        print(f"  Pooled inputs already present — skipping build_combined_slice.")
    elif override:
        missing = stats if not stats.exists() else pooled_crush
        sys.exit(f"FATAL: --input/--crush-db not found: {missing}. Provide a pre-built pool "
                 "(build_pooled_stats.py --phase merge), or omit the overrides to build the "
                 "default 1900+2200 pool here.")
    else:
        # Default path: build the 1900+2200 pool via build_combined_slice — which needs the
        # per-(event,elo) 2024_2025 stats + the mode-matched crush histogram.
        crush_2425 = (STATS_DIR / ("crush_hist_rel_2024_2025.parquet"
                                   if mode == "relative-propagated"
                                   else "crush_hist_2024_2025.parquet"))
        for p in (STATS_DIR / "position_stats_2024_2025.parquet", crush_2425):
            if not p.exists():
                extra = (" (run build_crush_stats.py first)"
                         if p == crush_2425 and mode == "relative-propagated" else "")
                sys.exit(f"FATAL: missing prerequisite {p}{extra}")
        if not run("build_combined_slice",
                   [PY, str(PROJECT / "python/build_combined_slice.py")]):
            sys.exit("FATAL: build_combined_slice failed")
    for p in (stats, pooled_crush):
        if not p.exists():
            sys.exit(f"FATAL: pooled input not produced: {p}")

    # ── Step 2: the four Stage 3 reps. ───────────────────────────────────────
    print(f"\n{'#'*70}\n# Step 2: Stage 3 reps ({mode} crush + robustness gate)\n{'#'*70}")
    stage3_flags = common_stage3(mode, weight, args.crush_gamma, args.crush_imm_window,
                                 stats, pooled_crush, eval_db)
    failures = []
    for tag, extra in REPS:
        out = rep_path(tag, mode)
        if out.exists() and not args.force:
            print(f"  Skipping {tag} (exists: {out.name}). Use --force to rebuild.")
            continue
        cmd = ([PY, str(PROJECT / "python/stage3_backwards_induction.py"),
                "--output", str(out)] + stage3_flags + extra)
        if not run(f"stage3_{mode}_{tag}", cmd):
            failures.append(tag)

    # ── Summary: recommended first move + start value/crush per rep. ─────────
    print(f"\n{'#'*70}\n# Done in {(time.time()-t_all)/60:.1f} min\n{'#'*70}")
    try:
        import polars as pl
        import chess, chess.polyglot
        INT64_MAX, RANGE = 2**63 - 1, 2**64
        h = chess.polyglot.zobrist_hash(chess.Board())
        start = h - RANGE if h > INT64_MAX else h
        for tag, _ in REPS:
            out = rep_path(tag, mode)
            if not out.exists():
                print(f"  {tag:<10} (missing)"); continue
            cols = ["position_hash", "best_move", "value", "value_robust"]
            avail = pl.scan_parquet(out).collect_schema().names()
            if "crush_potential" in avail:
                cols.append("crush_potential")
            r = pl.read_parquet(out, columns=cols).filter(pl.col("position_hash") == start)
            if r.height:
                row = r.row(0, named=True)
                cp = f" crush_pot={row['crush_potential']:.4f}" if "crush_potential" in cols and row["crush_potential"] is not None else ""
                print(f"  {tag:<10} first={row['best_move'] or '-':<5} "
                      f"value={row['value']:.4f} robust={row['value_robust']:.4f}{cp}")
    except Exception as e:
        print(f"  (summary skipped: {e})")

    if failures:
        sys.exit(f"FAILED reps: {', '.join(failures)}")
    print(f"\nAll pooled {mode} reps present.")


if __name__ == "__main__":
    main()
