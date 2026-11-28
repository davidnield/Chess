"""Build and look up a published eval-db-v2 by both signed Zobrist halves."""
from __future__ import annotations

from explorer_storage import relation
import argparse
from pathlib import Path

import chess
import duckdb
import numpy as np
import pyarrow.parquet as pq

from eval_arrays import MISSING, dir_fingerprint
from eval_arrays_build import book_index
from month_archive import bind, read_json, root_lock, sha, write_json
from verify_book import _lit
from zobrist128 import zobrist128

SCHEMA = "eval-lookup-128-v1"
KEY = np.dtype([("lo", "<i8"), ("hi", "<i8")])
ROW = np.dtype(KEY.descr + [("cp", "<i2"), ("source", "u1")])
OVERLAY_SCHEMA = "eval-lookup-128-score-overlay-v1"
OVERLAY_ROW = np.dtype(KEY.descr + [("cp", "<f8"), ("source", "u1")])
POLICY = "cloud-nnue-chessdb-score-334-v1"
CLOUD, FISHNET, TERMINAL = 1, 2, 3


SPARSE_STEP = 4096


def sparse_index(arr, step=SPARSE_STEP):
    """Every ``step``-th low half of a (lo, hi)-sorted bucket, contiguous: ~6,000 int64 per bucket."""
    return np.array(arr["lo"][::step], dtype=np.int64, copy=True)   # a copy: never a view that pins the mmap


def left_index(arr, sparse, q, step=SPARSE_STEP):
    """``np.searchsorted(arr[["lo", "hi"]], q)`` (side left) without touching the whole bucket.

    The direct call is slow on these arrays: numpy walks or copies the memory-mapped two-field view
    each time (77% of a pilot build's collection time, 27 Nov 2026). The sparse index narrows each
    query to one block of ``step`` rows; equal low halves (rare) are resolved on the high half. The
    result equals searchsorted's for any (lo, hi)-sorted array (``_test_eval_lookup128``)."""
    n = len(arr)
    out = np.empty(len(q), dtype=np.int64)
    if n == 0:
        out[:] = 0
        return out
    lo_col, hi_col = arr["lo"], arr["hi"]
    qlo, qhi = q["lo"], q["hi"]
    k = np.searchsorted(sparse, qlo, side="left")       # sparse[k-1] < qlo <= sparse[k]
    for j in range(len(q)):
        kj = int(k[j])
        start = 0 if kj == 0 else (kj - 1) * step + 1
        end = min(kj * step + 1, n)
        i = start + int(np.searchsorted(lo_col[start:end], qlo[j], side="left"))
        lo_j, hi_j = qlo[j], qhi[j]
        while i < n and lo_col[i] == lo_j and hi_col[i] < hi_j:
            i += 1
        out[j] = i
    return out


def keys(lo, hi):
    lo, hi = np.broadcast_arrays(np.asarray(lo, dtype=np.int64), np.asarray(hi, dtype=np.int64))
    out = np.empty(lo.shape, dtype=KEY)
    out["lo"], out["hi"] = lo, hi
    return out


class WideEvalLookup:
    def __init__(self, root, *, allow_partial=False, verify_hashes=False):
        self.root = Path(root)
        manifest = self.root / "lookup.json"
        if allow_partial and not manifest.exists():
            manifest = self.root / "pilot.json"
        meta = read_json(manifest)
        self.base = None
        self.row = ROW
        self.verify_hashes = verify_hashes
        if meta["schema"] == OVERLAY_SCHEMA:
            if meta.get("policy") != POLICY or (not meta.get("complete") and not allow_partial):
                raise ValueError("unpublished or unknown score-policy overlay")
            if meta.get("complete") and set(meta["buckets"]) != {f"bkt{b:03d}.npy" for b in range(512)}:
                raise ValueError("incomplete overlay bucket set")
            base_root = Path(meta["base"]["root"])
            if base_root.resolve() == self.root.resolve():
                raise ValueError("self-referential evaluation overlay")
            if sha(base_root / "lookup.json") != meta["base"]["sha256"]:
                raise ValueError("bound fallback generation changed")
            if read_json(base_root / "lookup.json")["schema"] != SCHEMA:
                raise ValueError("overlay must bind the pre-change lookup, not another overlay")
            self.base = WideEvalLookup(base_root, verify_hashes=True)
            self.row = OVERLAY_ROW
            self.verify_hashes = True
        elif meta["schema"] != SCHEMA:
            raise ValueError(f"unexpected lookup schema in {self.root}")
        self.files = meta["buckets"]
        self.mapped = {}
        self.sparse = {}

    def _verify(self, names):
        if not names:
            return
        from sweep.verified_files import verify_many
        try:
            verify_many([(self.root / n, self.files[n]["sha256"]) for n in names])
        except ValueError as exc:
            bad = Path(str(exc).split(": ")[0]).name      # verify_many names the file first
            raise ValueError(f"lookup content differs from receipt: {bad}") from exc

    def lookup(self, lo, hi):
        """Return White-POV cp and source. Missing pairs return (MISSING, 0)."""
        queries = keys(lo, hi)
        q = queries.reshape(-1)
        if self.base is None:
            cp = np.full(len(q), MISSING, dtype=np.int16)
            sources = np.zeros(len(q), dtype=np.uint8)
        else:
            cp, sources = self.base.lookup(q["lo"], q["hi"])
            cp = cp.astype(np.float64)
        buckets = q["lo"] % 512
        if self.verify_hashes:
            # Hash the arrays this lookup opens together, once per process (sweep.verified_files).
            self._verify([f"bkt{b:03d}.npy" for b in np.unique(buckets)
                          if f"bkt{b:03d}.npy" in self.files and f"bkt{b:03d}.npy" not in self.mapped])
        for b in np.unique(buckets):
            name = f"bkt{b:03d}.npy"
            if name not in self.files:
                continue
            if name not in self.mapped:
                if self.verify_hashes:
                    self._verify([name])
                arr = np.load(self.root / name, mmap_mode="r", allow_pickle=False)
                if arr.dtype != self.row or len(arr) != self.files[name]["rows"]:
                    raise ValueError(f"unexpected array layout in {name}")
                self.mapped[name] = arr
            arr = self.mapped[name]
            selected = np.flatnonzero(buckets == b)
            both = arr[["lo", "hi"]]
            if name not in self.sparse:
                self.sparse[name] = sparse_index(arr)
            index = left_index(arr, self.sparse[name], q[selected])
            valid = index < len(both)
            index, selected = index[valid], selected[valid]
            equal = both[index] == q[selected]
            index, selected = index[equal], selected[equal]
            if self.base is not None:
                values, provenance = arr["cp"][index], arr["source"][index]
                if (not np.isfinite(values).all() or (abs(values) > 2000).any()
                        or not np.isin(provenance, [6, 7]).all()
                        or (sources[selected] == TERMINAL).any()):
                    raise ValueError("invalid overlay value/provenance or terminal override")
            cp[selected], sources[selected] = arr["cp"][index], arr["source"][index]
        return cp.reshape(queries.shape), sources.reshape(queries.shape)


def gather_mates(con, book, work):
    files = []
    for bucket, inputs in book_index(book).items():
        out = work / f"bkt{bucket:03d}.parquet"
        marker = out.with_suffix(".json")
        if marker.is_file():
            if read_json(marker)["sha256"] != sha(out):
                raise ValueError(f"mate checkpoint differs at {out}")
        else:
            tmp = out.with_suffix(".tmp")
            path = tmp.as_posix().replace("'", "''")
            con.execute(f"""CREATE OR REPLACE TEMP TABLE hits AS
                SELECT parent_epd,move_san,child_hash AS lo,child_hash_hi AS hi,ply
                FROM {relation(inputs)}
                WHERE ends_with(move_san, '#')""")
            for epd, san, lo, hi, ply in con.execute(
                    "SELECT * FROM hits USING SAMPLE reservoir(8 ROWS) REPEATABLE (20261115)").fetchall():
                board = chess.Board(epd)
                board.push_san(san)
                if (not board.is_checkmate() or zobrist128(board) != (lo, hi)
                        or (board.turn == chess.WHITE) != (ply % 2 == 0)):
                    raise ValueError(f"checkmate replay mismatch at {epd} {san}")
            con.execute(f"""COPY (SELECT lo,hi,min(ply % 2) AS pmin,max(ply % 2) AS pmax
                FROM hits GROUP BY lo,hi) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
            tmp.replace(out)
            write_json(marker, {"sha256": sha(out)})
        files.append(out)
    if not files:
        raise ValueError(f"{book} contains no book buckets")
    con.execute(f"""CREATE TEMP TABLE mates AS SELECT lo,hi,lo & 511 AS bucket,
                     min(pmin) AS pmin,max(pmax) AS pmax
                     FROM read_parquet({_lit(files)}) GROUP BY lo,hi""")
    bad = con.execute("SELECT lo,hi FROM mates WHERE pmin<>pmax LIMIT 1").fetchone()
    if bad:
        raise ValueError(f"inconsistent mate parity at full key {bad}")


def make_bucket(con, source_file, bucket, terminal):
    table = pq.ParquetFile(source_file).read(columns=["position_hash", "position_hash_hi", "eval_cp", "source",
                                                       "hash_ambiguous"])
    if any(c.null_count for c in table.columns) or table["hash_ambiguous"].to_numpy().any():
        raise ValueError(f"{source_file}: missing fields or ambiguous full keys")
    lo, hi = table["position_hash"].to_numpy(), table["position_hash_hi"].to_numpy()
    if (lo % 512 != bucket).any():
        raise ValueError(f"misplaced source row in {source_file}")
    score = table["eval_cp"].to_numpy()
    if ((score < -2000) | (score > 2000)).any():
        raise ValueError(f"invalid score in {source_file}")
    import pyarrow.compute as pc
    cloud = pc.equal(table["source"], "cloud").to_numpy()
    fishnet = pc.equal(table["source"], "fishnet").to_numpy()
    if not (cloud | fishnet).all():
        raise ValueError(f"unknown score source in {source_file}")
    vals = np.empty(len(lo), dtype=ROW)
    vals["lo"], vals["hi"], vals["cp"], vals["source"] = lo, hi, score, np.where(cloud, CLOUD, FISHNET)
    order = np.lexsort((hi, lo))
    vals = vals[order]
    both = vals[["lo", "hi"]]
    if (both[1:] == both[:-1]).any():
        raise ValueError(f"duplicate full position key in {source_file}")
    if not terminal:
        return vals
    m = con.execute("SELECT lo,hi,pmin FROM mates WHERE bucket=? ORDER BY lo,hi", [bucket]).fetchnumpy()
    q = keys(m["lo"], m["hi"])
    pos = np.searchsorted(both, q)
    matched = pos < len(vals)
    matched[matched] = both[pos[matched]] == q[matched]
    cp = np.where(m["pmin"] == 1, 2000, -2000)
    different = np.flatnonzero(matched)
    different = different[vals["cp"][pos[different]] != cp[different]]
    if len(different):
        epds = pq.ParquetFile(source_file).read(columns=["epd"])["epd"]
        for i in different:
            board = chess.Board(epds[int(order[pos[i]])].as_py())
            if not board.is_checkmate() or (board.turn == chess.WHITE) != (cp[i] < 0) or zobrist128(board) != (q["lo"][i],q["hi"][i]):
                raise ValueError(f"unconfirmed terminal override at {q[i]}")
    vals["cp"][pos[matched]], vals["source"][pos[matched]] = cp[matched], TERMINAL
    if matched.all():
        return vals
    add = np.empty(np.count_nonzero(~matched), dtype=ROW)
    add["lo"],add["hi"],add["cp"],add["source"] = q["lo"][~matched],q["hi"][~matched],cp[~matched],TERMINAL
    vals = np.concatenate([vals,add])
    vals.sort(order=["lo","hi"])
    return vals


def build(db, book, out, terminal=True):
    db, book, out = (Path(p).resolve() for p in (db, book, out))
    if any(out == p or p in out.parents or out in p.parents for p in (book, db)):
        raise ValueError("use a separate lookup generation directory")
    meta = read_json(db / "_build.meta.json")
    if meta["schema"] != "eval-db-v2" or not (db / "_DONE").is_file() or not (book / "_BOOK.DONE").is_file():
        raise ValueError("lookup requires a completed wide eval DB and explorer")
    book_sha = sha(book / "_book.meta.json")
    if book_sha != meta["inputs"]["book_meta_sha256"]:
        raise ValueError("eval DB references another explorer")
    identity = {"schema": SCHEMA, "source": dir_fingerprint(db), "book_meta_sha256": book_sha,
                "terminal":terminal, "producer_sha256":sha(Path(__file__))}
    with root_lock(out):
        settings = out / "_settings.json"
        if not settings.exists() and any(f.name != "_launcher.lock" for f in out.iterdir()):
            raise ValueError(f"unbound lookup output directory {out}")
        bind(settings, identity)
        rows = pq.ParquetFile(db / "_manifest.parquet").read(columns=["bucket","rows","sha256"]).to_pylist()
        buckets = sorted(int(r["bucket"]) for r in rows)
        if not buckets or len(buckets) != len(set(buckets)) or not all(0 <= b < 512 for b in buckets):
            raise ValueError("invalid eval bucket manifest")
        records = {}
        with duckdb.connect(config={"threads":2,"memory_limit":"4GB"}) as con:
            if terminal:
                work = out / "mate-shards"
                work.mkdir(exist_ok=True)
                gather_mates(con, book, work)
                # A partial eval pilot exports only its declared bucket set.
            for r in rows:
                b = int(r["bucket"])
                source = db / f"bkt{b:03d}.parquet"
                if sha(source) != r["sha256"] or pq.ParquetFile(source).metadata.num_rows != r["rows"]:
                    raise ValueError(f"eval DB file differs from manifest: {source}")
                dst = out / f"bkt{b:03d}.npy"
                checkpoint = out / "_done" / f"{b:03d}.json"
                if checkpoint.exists():
                    rec = read_json(checkpoint)
                    if rec["sha256"] != sha(dst):
                        raise ValueError(f"lookup checkpoint differs at {dst}")
                else:
                    values = make_bucket(con, source, b, terminal)
                    tmp = dst.with_suffix(".tmp")
                    with tmp.open("wb") as f:
                        np.save(f, values, allow_pickle=False)
                    tmp.replace(dst)
                    rec = {"rows": len(values), "sha256": sha(dst)}
                    write_json(checkpoint, rec)
                records[dst.name] = rec
        write_json(out / "lookup.json", {**identity, "buckets":records})
        return sum(r["rows"] for r in records.values())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("eval-db","book","out"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--no-terminal", action="store_true")
    args = parser.parse_args()
    print(build(args.eval_db,args.book,args.out,terminal=not args.no_terminal),"full-key scores published")
