#!/usr/bin/env bash
# Runs Stage 1 -> Stage 2 -> Stage 3 for year 2019, capping each Stage 1
# partition at 200,000 games. All output goes to dedicated 2019-only paths so
# the existing 2013 single-partition data is untouched.

set -u

LOGDIR="/c/Users/David/Documents/Chess/logs/2019_pipeline"
mkdir -p "$LOGDIR"

PYTHON="/c/Users/David/Documents/Chess/.venv/Scripts/python.exe"
MAIN_DIR="/c/Users/David/Documents/Chess"
WORKTREE_DIR="/c/Users/David/Documents/Chess/.claude/worktrees/sleepy-curie-602c99"

S1_OUT="D:/data/chess/position-moves-2019"
S2_OUT="D:/data/chess/position-stats/position_stats_2019.parquet"
S3_OUT="D:/data/chess/repertoire/repertoire_2019.parquet"

# Stage 1
echo "=== Stage 1 started at $(date) ===" | tee "$LOGDIR/stage1.log"
cd "$MAIN_DIR"
"$PYTHON" python/stage1_run_all.py \
    --year-range 2019 2019 \
    --output "$S1_OUT" \
    --limit-games 200000 \
    --overwrite >> "$LOGDIR/stage1.log" 2>&1
S1_EXIT=$?
echo "=== Stage 1 finished at $(date) with exit $S1_EXIT ===" | tee -a "$LOGDIR/stage1.log"

if [ $S1_EXIT -ne 0 ]; then
    echo "Stage 1 failed; aborting." | tee "$LOGDIR/done.txt"
    exit $S1_EXIT
fi

# Stage 2
echo "=== Stage 2 started at $(date) ===" | tee "$LOGDIR/stage2.log"
"$PYTHON" python/stage2_aggregate.py \
    --input "$S1_OUT" \
    --output "$S2_OUT" \
    --min-games 10 >> "$LOGDIR/stage2.log" 2>&1
S2_EXIT=$?
echo "=== Stage 2 finished at $(date) with exit $S2_EXIT ===" | tee -a "$LOGDIR/stage2.log"

if [ $S2_EXIT -ne 0 ]; then
    echo "Stage 2 failed; aborting." | tee "$LOGDIR/done.txt"
    exit $S2_EXIT
fi

# Stage 3 (lives in worktree)
echo "=== Stage 3 started at $(date) ===" | tee "$LOGDIR/stage3.log"
cd "$WORKTREE_DIR"
"$PYTHON" python/stage3_backwards_induction.py \
    --input "$S2_OUT" \
    --output "$S3_OUT" \
    --perspective white >> "$LOGDIR/stage3.log" 2>&1
S3_EXIT=$?
echo "=== Stage 3 finished at $(date) with exit $S3_EXIT ===" | tee -a "$LOGDIR/stage3.log"

# Also run a black perspective for comparison
S3B_OUT="D:/data/chess/repertoire/repertoire_2019_black.parquet"
echo "=== Stage 3 (black) started at $(date) ===" | tee "$LOGDIR/stage3_black.log"
"$PYTHON" python/stage3_backwards_induction.py \
    --input "$S2_OUT" \
    --output "$S3B_OUT" \
    --perspective black >> "$LOGDIR/stage3_black.log" 2>&1
S3B_EXIT=$?
echo "=== Stage 3 (black) finished at $(date) with exit $S3B_EXIT ===" | tee -a "$LOGDIR/stage3_black.log"

echo "Pipeline complete at $(date)" | tee "$LOGDIR/done.txt"
echo "Stage 1 exit: $S1_EXIT" | tee -a "$LOGDIR/done.txt"
echo "Stage 2 exit: $S2_EXIT" | tee -a "$LOGDIR/done.txt"
echo "Stage 3 exit (white): $S3_EXIT" | tee -a "$LOGDIR/done.txt"
echo "Stage 3 exit (black): $S3B_EXIT" | tee -a "$LOGDIR/done.txt"
