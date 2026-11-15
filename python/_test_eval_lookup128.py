"""Regression for full-key lookup, source attribution, checkmate override and restart."""
import tempfile
import unittest
from pathlib import Path

import chess
import pyarrow as pa
import pyarrow.parquet as pq

from eval_arrays import MISSING
from eval_lookup128 import CLOUD, FISHNET, TERMINAL, WideEvalLookup, build
from month_archive import sha, write_json
from zobrist128 import zobrist128


class WideLookupTest(unittest.TestCase):
    def test_same_low_half(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            book, db, out = root / "book", root / "db", root / "lookup"
            db.mkdir()
            book.mkdir()
            (db / "_DONE").touch()
            (book / "_BOOK.DONE").touch()
            write_json(book / "_book.meta.json", {"schema": "book-v3"})
            write_json(db / "_build.meta.json", {"schema":"eval-db-v2","inputs":{
                "book_meta_sha256":sha(book / "_book.meta.json")}})
            board = chess.Board()
            for san in "f3 e5 g4".split():
                board.push_san(san)
            parent_epd = board.epd()
            board.push_san("Qh4#")
            lo,hi = zobrist128(board)
            b = lo % 512
            target = book / "ps/event=Blitz/elo_band=1800" / f"bkt{b:03d}.parquet"
            target.parent.mkdir(parents=True)
            pq.write_table(pa.table({"parent_epd":[parent_epd],"move_san":["Qh4#"],"ply":[4],
                                     "child_hash":[lo],"child_hash_hi":[hi]}),target)
            source = db / f"bkt{b:03d}.parquet"
            # The first pair is deliberately a low-half twin of the checkmated board.
            pq.write_table(pa.table({"position_hash":[lo,lo],"position_hash_hi":[hi-1,hi],
                                     "source":["cloud","fishnet"],"eval_cp":[0,84],
                                     "hash_ambiguous":[False,False],"epd":[chess.Board().epd(),board.epd()]}),source)
            pq.write_table(pa.table({"bucket":[b],"rows":[2],"sha256":[sha(source)]}),db / "_manifest.parquet")
            self.assertEqual(build(db, book, out),2)
            self.assertEqual(build(db, book, out),2)
            cp,src = WideEvalLookup(out).lookup([lo,lo,lo,lo+512,lo+512], [hi-1,hi,hi+1,hi+1,hi])
            self.assertEqual(cp.tolist(),[0,-2000,MISSING,MISSING,MISSING])
            self.assertEqual(src.tolist(),[CLOUD,TERMINAL,0,0,0])
            without = root / "raw"
            self.assertEqual(build(db,book,without,terminal=False),2)
            cp,src = WideEvalLookup(without).lookup([lo,lo], [hi,hi+1])
            self.assertEqual(cp.tolist(),[84,MISSING])
            self.assertEqual(src.tolist(),[FISHNET,0])
            with self.assertRaisesRegex(ValueError,"changed"):
                build(db,book,out,terminal=False)
            with (out / f"bkt{b:03d}.npy").open("ab") as f:
                f.write(b"x")
            with self.assertRaisesRegex(ValueError,"checkpoint differs"):
                build(db,book,out)
            # Only the twin has a DB score, so add the mating pair from the explorer.
            t = pq.ParquetFile(source).read().slice(0, 1)
            pq.write_table(t, source)
            pq.write_table(pa.table({"bucket":[b],"rows":[1],"sha256":[sha(source)]}),db / "_manifest.parquet")
            added = root / "added"
            self.assertEqual(build(db,book,added),2)
            cp,src = WideEvalLookup(added).lookup([lo,lo,lo],[hi-1,hi,hi+1])
            self.assertEqual(cp.tolist(),[0,-2000,MISSING])
            self.assertEqual(src.tolist(),[CLOUD,TERMINAL,0])


if __name__ == "__main__":
    unittest.main()
