"""Run existing independent pool acceptance gates before a shared export is receipted."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--work",type=Path,required=True)
    p.add_argument("--out-dir",type=Path,required=True)
    p.add_argument("--tmp-dir",type=Path,required=True)
    a=p.parse_args()
    code=Path(__file__).resolve().parents[1]
    params=json.loads((a.work/"_params.json").read_text(encoding="utf8"))
    if params["min_games"]!=1 or params.get("key_bits")!=128:
        raise ValueError("pool is not the uncut full-key recipe")
    for gate in ("term-vs-book","reimpl","conservation","evals"):
        subprocess.run([sys.executable,"-X","utf8",str(code/"pool_from_book_gates.py"),gate,
                        "--work",str(a.work),"--out-dir",str(a.out_dir),"--tmp-dir",str(a.tmp_dir),
                        "--threads","2","--workers","1","--random-bucket"],
                       cwd=code,check=True)
    # Gate details stay in _gates.json. Do not treat sampled source cross-checks
    # as a census; the bucket extraction itself checks mass identities throughout.
    print("Pool acceptance gates passed")

if __name__=="__main__": main()
