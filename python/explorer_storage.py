"""Logical views of merged explorer move slices. DuckDB can prune unused expressions."""
from __future__ import annotations

from glob import iglob
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

RANKS = tuple(f"rank{i}" for i in range(8, 0, -1))
LOGICAL = ("parent_hash", "move_san", "event", "elo_band", "parent_epd", "child_hash",
           "ply", "white_wins", "draws", "black_wins", "total", "white_score_avg",
           "parent_hash_hi", "child_hash_hi")
DEPS = {"parent_epd": (*RANKS, "epd_state"), "child_hash": ("parent_hash", "move_xor_lo"),
        "child_hash_hi": ("parent_hash_hi", "move_xor_hi")}


def _compact(schema):
    if "move_xor_lo" not in schema.names:
        return False
    required = {c for n in LOGICAL for c in DEPS.get(n, (n,))}
    if required != set(schema.names):
        raise ValueError("incomplete or unrecognized compact move schema")
    return True


def relation(files, *, filename=False):
    """Read one generation. Legacy move files, monthly inputs and terminals pass through."""
    files = list(files)
    if not files:
        raise ValueError("no explorer input files")
    paths = [Path(f).as_posix() for f in files]
    sample = next(iglob(paths[0])) if "*" in paths[0] else paths[0]
    literals = ", ".join("'" + p.replace("'", "''") + "'" for p in paths)
    extra = ", filename=true" if filename else ""
    raw = f"read_parquet([{literals}], hive_partitioning=false{extra})"
    if not _compact(pq.read_schema(sample)):
        return raw
    expr = {"parent_epd": "concat(" + ", '/', ".join(RANKS) + ", ' ', epd_state)",
            "child_hash": "xor(parent_hash, move_xor_lo)",
            "child_hash_hi": "xor(parent_hash_hi, move_xor_hi)"}
    columns = [f"{expr.get(k, k)} AS {k}" for k in LOGICAL]
    if filename:
        columns.append("filename")
    return f"(SELECT {', '.join(columns)} FROM {raw})"


def read_columns(file, columns):
    """A projected PyArrow read for consumers doing independent checks."""
    pf = pq.ParquetFile(file)
    if not _compact(pf.schema_arrow):
        return pf.read(columns=columns)
    raw = pf.read(columns=list(dict.fromkeys(c for name in columns for c in DEPS.get(name, (name,)))))
    out = {}
    for name in columns:
        if name == "parent_epd":
            board = pc.binary_join_element_wise(*(raw[c] for c in RANKS), "/")
            out[name] = pc.binary_join_element_wise(board, raw["epd_state"], " ")
        elif name in DEPS:
            out[name] = pc.bit_wise_xor(*(raw[c] for c in DEPS[name]))
        else:
            out[name] = raw[name]
    return pa.table(out)
