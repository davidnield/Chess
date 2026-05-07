#!/usr/bin/env bash
#
# Run analyze_coverage.py on all 6 2016 forcing-weight variants and produce
# both per-variant text reports and a consolidated cross-variant markdown
# summary.
#

set -uo pipefail

PROJECT=/c/Users/David/Documents/Chess
PY=$PROJECT/.venv/Scripts/python.exe
REP_DIR=E:/chess/repertoire
STATS=E:/chess/position-stats/position_stats_2016.parquet
LOG_DIR=$PROJECT/logs/2016_pipeline
mkdir -p "$LOG_DIR"

YEAR=2016

for perspective in white black; do
    for wsfx in w000 w010 w020; do
        rep="$REP_DIR/repertoire_${YEAR}_${perspective}_${wsfx}.parquet"
        out="$LOG_DIR/coverage_${perspective}_${wsfx}.txt"
        echo "=== ${perspective} ${wsfx} ==="
        if [[ ! -f "$rep" ]]; then
            echo "  SKIP: $rep does not exist" | tee "$out"
            continue
        fi
        "$PY" "$PROJECT/python/analyze_coverage.py" \
            --repertoire "$rep" \
            --stats "$STATS" \
            --perspective "$perspective" \
            --output "$out"
    done
done

echo "=== All 6 variants done. Reports in $LOG_DIR/coverage_*.txt ==="
