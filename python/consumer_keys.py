"""Full-key I/O for the budget builder and referee. IDs live for one run only."""
import hashlib
import json
from pathlib import Path

import chess
import numpy as np
import polars as pl

from eval_lookup128 import WideEvalLookup
from zobrist128 import zobrist128

PAIR = pl.Struct({"lo": pl.Int64, "hi": pl.Int64})


def is_wide(path, column):
    schema = pl.read_parquet_schema(path)
    return schema[column] == PAIR or column + "_hi" in schema


class ConsumerKeys:
    def __init__(self, lookup, *, expected_sha=None):
        from book_builder_rs import RsPositionIds
        self.native = RsPositionIds()
        manifest = Path(lookup) / "lookup.json"
        self.sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
        if expected_sha is not None and self.sha != expected_sha:
            raise ValueError("full-key eval lookup differs from the pool recipe")
        self.evals = WideEvalLookup(lookup)

    @classmethod
    def for_pool(cls, path):
        if not is_wide(path, "parent_hash"):
            return None
        meta = json.loads(Path(str(path) + ".meta.json").read_text(encoding="utf8"))
        if meta.get("key_bits") != 128:
            raise ValueError("wide pool lacks a 128-bit recipe")
        return cls(meta["eval_lookup128"], expected_sha=meta["eval_lookup_sha256"])

    def board(self, board):
        return self.native.encode_one(*zobrist128(board))

    def child(self, epd, san):
        board = chess.Board(epd)
        try:
            board.push_san(san)
        except ValueError:
            return None
        return self.board(board)

    def halves(self, ids):
        return self.native.decode(np.ascontiguousarray(ids, np.int64))

    def scores(self, ids):
        return self.evals.lookup(*self.halves(ids))[0]

    def external(self, series):
        """Reconstruct a nullable struct; never decode a missing ID as position 0."""
        valid = series.is_not_null()
        lo = np.zeros(len(series), np.int64)
        hi = np.zeros(len(series), np.int64)
        mask = valid.to_numpy()
        lo[mask], hi[mask] = self.halves(series.drop_nulls().to_numpy())
        return (pl.DataFrame({"lo": lo, "hi": hi, "valid": valid})
                .select(pl.when("valid").then(pl.struct("lo", "hi")).alias(series.name)).to_series())

    def encode(self, frame, *columns):
        """Convert full external keys at one boundary, including nullable children."""
        for name in columns:
            companion = name + "_hi"
            if frame.schema[name] == PAIR:
                series = frame[name]
            elif frame.schema[name] == pl.Int64 and frame.schema.get(companion) == pl.Int64:
                if not frame[name].is_null().equals(frame[companion].is_null()):
                    raise ValueError(f"incomplete Zobrist halves in {name}")
                series = frame.select(pl.when(pl.col(name).is_not_null()).then(
                    pl.struct(lo=pl.col(name), hi=pl.col(companion))).alias(name)).to_series()
                frame = frame.drop(companion)
            else:
                raise ValueError(f"{name} requires both signed Zobrist halves")
            valid = series.is_not_null()
            pair = series.drop_nulls()
            lo, hi = (pair.struct.field(k) for k in ("lo", "hi"))
            if lo.null_count() or hi.null_count():
                raise ValueError(f"incomplete Zobrist halves in {name}")
            ids = self.native.encode(np.ascontiguousarray(lo, np.int64), np.ascontiguousarray(hi, np.int64))
            if pair.len() == series.len():
                encoded = pl.Series(name, ids)
            else:
                values = np.zeros(len(series), np.int64)
                values[valid.to_numpy()] = ids
                encoded = pl.Series(name, values).set(~valid, None)
            frame = frame.with_columns(encoded)
        return frame

    def restore(self, frame, *columns):
        return frame.with_columns(self.external(frame[c]) for c in columns)

    def records(self, rows, columns):
        if not rows:
            return rows
        df = pl.DataFrame(rows, infer_schema_length=None)
        return self.restore(df, *columns).to_dicts()

    def register(self, con, name, ids):
        series = pl.Series("h", ids, dtype=pl.Int64)
        con.register(name, self.external(series).to_frame().to_arrow())

    def book_rows(self, path, columns, *, predicate=None, subset=None):
        """Legacy source and comparison books can be identified by their stored board."""
        scan = pl.scan_parquet(path)
        if predicate is not None:
            scan = scan.filter(predicate)
        schema = scan.collect_schema()
        name = "position_hash"
        if subset is not None:
            query = self.external(pl.Series(name, subset, dtype=pl.Int64))
            if schema[name] != PAIR:
                query = query.struct.field("lo")
            scan = scan.filter(pl.col(name).is_in(query.implode()))
        if schema[name] == PAIR or name + "_hi" in schema:
            names = [*columns, *([name + "_hi"] if name + "_hi" in schema else [])]
            frame = self.encode(scan.select(names).collect(), name)
        else:
            if "position_epd" not in schema:
                raise ValueError(f"{path} has neither full position keys nor boards for upgrading them")
            frame = scan.select(list(dict.fromkeys([*columns, "position_epd"]))).collect()
            # Bound the temporary Python and Rust copies of the EPD strings on large source books.
            ids = np.empty(frame.height, dtype=np.int64)
            offset = 0
            for part in frame.iter_slices(65536):
                ids[offset:offset+len(part)] = self.native.encode_epds(
                    part[name].to_numpy(), part["position_epd"].to_list())
                offset += len(part)
            frame = frame.with_columns(pl.Series(name, ids)).select(columns)
        if subset is not None and schema[name] != PAIR:
            frame = frame.filter(pl.col(name).is_in(pl.Series(subset, dtype=pl.Int64).implode()))
        return frame
