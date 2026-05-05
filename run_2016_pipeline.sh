#!/usr/bin/env bash
#
# Full pipeline (stages 1-3) for year=2016.
#
# Stage 3 runs a grid of forcing-weight × eval-weight variants for each
# perspective (white/black).  Output naming:
#   repertoire_2016_{perspective}_f{FFF}_e{EEE}.parquet
# where FFF = forcing_weight×100, EEE = eval_weight×100
# (e.g. f010_e030 → forcing=0.10, eval=0.30).
#
# Logs land under logs/2016_pipeline/.
#

set -uo pipefail

PROJECT=/c/Users/David/Documents/Chess
PY=$PROJECT/.venv/Scripts/python.exe
LOG_DIR=$PROJECT/logs/2016_pipeline
mkdir -p "$LOG_DIR"

POS_MOVES=E:/chess/position-moves-2016
POS_STATS=E:/chess/position-stats/position_stats_2016.parquet
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

# ── Stage 1 ────────────────────────────────────────────────────────────────
run_stage stage1 \
    "$PY" "$PROJECT/python/stage1_run_all.py" \
        --output "$POS_MOVES" \
        --year-range 2016 2016 \
        --max-ply 30 || { echo "Stage 1 failed" >&2; exit 1; }

# ── Stage 2 ────────────────────────────────────────────────────────────────
run_stage stage2 \
    "$PY" "$PROJECT/python/stage2_aggregate.py" \
        --input "$POS_MOVES" \
        --output "$POS_STATS" || { echo "Stage 2 failed" >&2; exit 1; }

# ── Eval DB ────────────────────────────────────────────────────────────────
# Lichess official evaluations dataset: 379M positions evaluated by Stockfish.
# Downloaded from HuggingFace, condensed to one eval per position (highest depth).
# Only needs to run once; reusable across all Stage 3 variants and years.
if [[ ! -f "$EVAL_DB" ]]; then
    run_stage eval_db \
        "$PY" "$PROJECT/python/build_lichess_eval_db.py" \
            --output "$EVAL_DB" \
            --cache-dir E:/chess/lichess-evals \
            --workers 8 || { echo "Eval DB failed" >&2; exit 1; }
else
    echo "Lichess eval DB already exists at $EVAL_DB — skipping build."
fi

# ── Stage 3: variants (white/black × forcing × eval weights) ──────────────
# Each entry: "forcing_weight:eval_weight"
for perspective in white black; do
    for fw in 0.00 0.10 0.20; do
        for ew in 0.00 0.30; do
            # Build suffix: f010_e030 for forcing=0.10, eval=0.30
            fsfx=$(printf "f%03.0f" "$(echo "$fw * 100" | bc)")
            esfx=$(printf "e%03.0f" "$(echo "$ew * 100" | bc)")
            tag="${fsfx}_${esfx}"
            run_stage "stage3_${perspective}_${tag}" \
                "$PY" "$PROJECT/python/stage3_backwards_induction.py" \
                    --input "$POS_STATS" \
                    --output "$REP_DIR/repertoire_2016_${perspective}_${tag}.parquet" \
                    --perspective "$perspective" \
                    --forcing-weight "$fw" \
                    --eval-db "$EVAL_DB" \
                    --eval-weight "$ew" || \
                { echo "Stage 3 ${perspective} ${tag} failed" >&2; exit 1; }
        done
    done
done

echo "=== 2016 pipeline complete at $(date) ==="
