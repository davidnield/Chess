"""Data-dir resolution and DB-backed settings.

The data dir is the unit of portability: it holds trainer.db and pack/.
Resolution order: explicit --data CLI arg > TRAINER_DATA env var >
<repo root>/trainer_data (repo root = two levels above this file).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Every known setting with its default. Values are stored as JSON strings in
# the settings table; unknown keys are rejected by put_settings so typos in
# the UI can't silently create dead settings.
DEFAULTS: dict[str, object] = {
    # Lichess import
    "lichess_username": "",
    "lichess_token": "",            # optional; raises API throttle
    "import_since": "",             # ISO date (YYYY-MM-DD); empty = never imported
    # Scheduling
    "priority_mode": "balanced",    # frequency | value | balanced | custom
    "reach_exp_a": 1.0,
    "memo_exp_b": 1.0,
    "new_per_day": 10,
    "desired_retention": 0.9,
    "review_once_per_day": True,
    "extend_past_cap": True,
    # Explorer source parquets (canonical pipeline outputs; may be absent on
    # a satellite PC — the Explorer tab degrades gracefully, see plan).
    "white_rep": "E:/chess/repertoire/repertoire_pooled_white_sharp.parquet",
    "black_rep": "E:/chess/repertoire/repertoire_pooled_black_sharp.parquet",
    # Must match the pool the reps above were built on (2013-2025 --no-prune).
    # No 2013-2025 crush_edge_totals has been built, so the raw-crush column is
    # off by design rather than showing another pool's rates.
    "stats": "E:/chess/position-stats/position_stats_pooled_ge1800_2013_2025_brc.parquet",
    "crush_totals": "E:/chess/position-stats/crush_edge_totals_pooled_ge1800_2013_2025_brc.parquet",
}

PRIORITY_PRESETS = {
    "frequency": (1.0, 0.0),
    "value": (0.3, 1.0),
    "balanced": (1.0, 1.0),
}


def resolve_data_dir(cli_arg: str | None) -> Path:
    if cli_arg:
        d = Path(cli_arg)
    elif os.environ.get("TRAINER_DATA"):
        d = Path(os.environ["TRAINER_DATA"])
    else:
        d = REPO_ROOT / "trainer_data"
    d.mkdir(parents=True, exist_ok=True)
    (d / "pack").mkdir(exist_ok=True)
    return d


def get_settings(con) -> dict[str, object]:
    """All settings: DB rows overlaid on DEFAULTS."""
    out = dict(DEFAULTS)
    for k, v in con.execute("SELECT key, value FROM settings"):
        if k in out:
            try:
                out[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                pass  # keep default on a corrupt row
    return out


def put_settings(con, updates: dict[str, object]) -> dict[str, object]:
    unknown = set(updates) - set(DEFAULTS)
    if unknown:
        raise KeyError(f"unknown settings: {sorted(unknown)}")
    # A named preset overrides the exponents unless custom exponents are also
    # being set in the same request (then mode flips to custom).
    mode = updates.get("priority_mode")
    if mode in PRIORITY_PRESETS and not ({"reach_exp_a", "memo_exp_b"} & set(updates)):
        a, b = PRIORITY_PRESETS[mode]
        updates = {**updates, "reach_exp_a": a, "memo_exp_b": b}
    with con:
        con.executemany(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [(k, json.dumps(v)) for k, v in updates.items()],
        )
    return get_settings(con)
