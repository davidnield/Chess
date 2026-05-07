#!/usr/bin/env bash
#
# Convergence analysis: compare repertoires across datasets of different
# sizes and eras.
#
#   Dataset 1: 2016 full year  (existing position-stats, ~118M games, 20 GB)
#   Dataset 2: January 2025    (~92M games, 48 GB)
#   Dataset 3: April 2025      (~92M games, 44 GB)
#
# Stage 3 weight grid (per perspective):
#   forcing_weight: 0.00, 0.30
#   eval_weight:    0.00, 0.50, 1.00
#   → 6 variants × 2 perspectives = 12 repertoire files per dataset
#
# Output naming: repertoire_{dataset}_{perspective}_f{FFF}_e{EEE}.parquet
#
# The Lichess eval DB (341M positions) is reused across all datasets.
#

set -uo pipefail

PROJECT=/c/Users/David/Documents/Chess
PY=$PROJECT/.venv/Scripts/python.exe
LOG_DIR=$PROJECT/logs/convergence
mkdir -p "$LOG_DIR"

EVAL_DB=E:/chess/lichess_eval_db.parquet
REP_DIR=E:/chess/repertoire

run_stage() {
    local name=$1
    local logfile=$LOG_DIR/$name.log
    shift
    {
        echo "=== $name started at $(date) ==="
        "$@"
        local rc=$?
        echo "=== $name finished at $(date) with exit $rc ==="
        return $rc
    } >> "$logfile" 2>&1
}

run_stage3_grid() {
    # Usage: run_stage3_grid <dataset_tag> <stats_parquet>
    local tag=$1
    local stats=$2
    for perspective in white black; do
        for fw in 0.00 0.30; do
            for ew in 0.00 0.50 1.00; do
                fsfx=$(printf "f%03.0f" "$(echo "$fw * 100" | bc)")
                esfx=$(printf "e%03.0f" "$(echo "$ew * 100" | bc)")
                vtag="${fsfx}_${esfx}"
                local outfile="$REP_DIR/repertoire_${tag}_${perspective}_${vtag}.parquet"
                if [[ -f "$outfile" ]]; then
                    echo "  Skipping ${tag}_${perspective}_${vtag} (exists)"
                    continue
                fi
                run_stage "stage3_${tag}_${perspective}_${vtag}" \
                    "$PY" "$PROJECT/python/stage3_backwards_induction.py" \
                        --input "$stats" \
                        --output "$outfile" \
                        --perspective "$perspective" \
                        --forcing-weight "$fw" \
                        --eval-db "$EVAL_DB" \
                        --eval-weight "$ew" || \
                    { echo "Stage 3 ${tag} ${perspective} ${vtag} failed" >&2; exit 1; }
                echo "  Done: ${tag}_${perspective}_${vtag}"
            done
        done
    done
}

# ══════════════════════════════════════════════════════════════════════════
# April 2025
# ══════════════════════════════════════════════════════════════════════════
APR25_MOVES=E:/chess/position-moves-2025-apr
APR25_STATS=E:/chess/position-stats/position_stats_2025_apr.parquet

echo "=== April 2025: Stage 1 starting at $(date) ==="

run_stage stage1_2025_apr \
    "$PY" "$PROJECT/python/stage1_run_all.py" \
        --output "$APR25_MOVES" \
        --year-range 2025 2025 \
        --months 4 \
        --max-ply 30 || { echo "Stage 1 (Apr 2025) failed" >&2; exit 1; }

echo "=== April 2025: Stage 2 starting at $(date) ==="

run_stage stage2_2025_apr \
    "$PY" "$PROJECT/python/stage2_aggregate.py" \
        --input "$APR25_MOVES" \
        --output "$APR25_STATS" || { echo "Stage 2 (Apr 2025) failed" >&2; exit 1; }

echo "=== April 2025: Stage 3 grid starting at $(date) ==="
run_stage3_grid "2025_apr" "$APR25_STATS"
echo "=== April 2025: complete at $(date) ==="

# ══════════════════════════════════════════════════════════════════════════
# January 2025
# ══════════════════════════════════════════════════════════════════════════
JAN25_MOVES=E:/chess/position-moves-2025-jan
JAN25_STATS=E:/chess/position-stats/position_stats_2025_jan.parquet

echo "=== January 2025: Stage 1 starting at $(date) ==="

run_stage stage1_2025_jan \
    "$PY" "$PROJECT/python/stage1_run_all.py" \
        --output "$JAN25_MOVES" \
        --year-range 2025 2025 \
        --months 1 \
        --max-ply 30 || { echo "Stage 1 (Jan 2025) failed" >&2; exit 1; }

echo "=== January 2025: Stage 2 starting at $(date) ==="

run_stage stage2_2025_jan \
    "$PY" "$PROJECT/python/stage2_aggregate.py" \
        --input "$JAN25_MOVES" \
        --output "$JAN25_STATS" || { echo "Stage 2 (Jan 2025) failed" >&2; exit 1; }

echo "=== January 2025: Stage 3 grid starting at $(date) ==="
run_stage3_grid "2025_jan" "$JAN25_STATS"
echo "=== January 2025: complete at $(date) ==="

# ══════════════════════════════════════════════════════════════════════════
# 2016 (Stage 3 only — stats already exist)
# ══════════════════════════════════════════════════════════════════════════
STATS_2016=E:/chess/position-stats/position_stats_2016.parquet

echo "=== 2016: Stage 3 grid starting at $(date) ==="
run_stage3_grid "2016" "$STATS_2016"
echo "=== 2016: complete at $(date) ==="

echo ""
echo "══════════════════════════════════════════════════════════════════════"
echo "  Convergence analysis pipeline complete at $(date)"
echo "  Repertoire files in $REP_DIR"
echo "══════════════════════════════════════════════════════════════════════"
