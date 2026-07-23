"""Shared position-hash helper. Every pipeline stage keys positions by this
exact hash (parquet position_hash columns, dict/join keys, DAG nodes) — a
second, drifting definition would silently break joins across stages."""
from __future__ import annotations

import chess
import chess.polyglot

INT64_MAX = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(board: chess.Board) -> int:
    """Polyglot Zobrist hash cast to signed int64 (parquet/polars compatible)."""
    h = chess.polyglot.zobrist_hash(board)
    return h - INT64_RANGE if h > INT64_MAX else h
