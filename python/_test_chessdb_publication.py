import json
import os
import tempfile
import unittest
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from month_archive import sha, write_json
from chessdb_integrity import fingerprint
from chessdb_publication import MARKER, publish

class Publication(unittest.TestCase):
    def make(self, root):
        for d in ("positions", "moves", "_done"):
            (root/d).mkdir()
        p=root/"positions/bkt000.parquet"
        m=root/"moves/bkt000.parquet"
        pq.write_table(pa.table(dict(lo=[0],hi=[1],found=[True],best_score=[20],white_to_move=[True])),p)
        pq.write_table(pa.table(dict(lo=[0],hi=[1],uci=["e2e4"])),m)
        write_json(root/"_params.json",{"book_meta_sha256":"book"})
        write_json(root/"_done/bkt000.json",dict(bucket=0,positions=1,moves=1,found=1,positions_sha256=sha(p),moves_sha256=sha(m)))
    def test_verify_and_wrong_bucket_set(self):
        with tempfile.TemporaryDirectory() as td:
            r=Path(td); self.make(r)
            x=publish(r,1,[0]); self.assertEqual(x["totals"]["positions"],1)
            self.assertFalse((r/MARKER).exists())
            write_json(r/"_done/bkt999.json",{})
            with self.assertRaisesRegex(ValueError,"wrong bucket set"): publish(r,1,[0])
    def test_directory_marker_never_accepted(self):
        self.assertNotEqual(MARKER.lower(),"_done")
        with tempfile.TemporaryDirectory() as td:
            r=Path(td); self.make(r)
            with self.assertRaisesRegex(ValueError,"wrong bucket set"): publish(r,1)
    def test_hidden_byte_change_detected(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/"data"; p.write_bytes(b"abc")
            s=p.stat(); first=fingerprint(p)
            p.write_bytes(b"xyz"); os.utime(p,ns=(s.st_atime_ns,s.st_mtime_ns))
            with self.assertRaisesRegex(ValueError,"content differs"): fingerprint(p,first["sha256"])
    def test_invalid_original_checksum(self):
        with tempfile.TemporaryDirectory() as td:
            r=Path(td); self.make(r)
            p=r/"_done/bkt000.json"; d=json.loads(p.read_text()); d["moves_sha256"]="0"; write_json(p,d)
            with self.assertRaisesRegex(ValueError,"content differs"): publish(r,1,[0])

if __name__=="__main__": unittest.main()
