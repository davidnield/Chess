"""Time the native eval pipeline on a real explorer bucket using identical eval source excerpts."""
import argparse
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from time import perf_counter
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq

from month_archive import read_json, sha, write_json


def measure(exe, plain, compact, cloud, fishnet, report):
    bucket = 7
    if report.is_file():
        old = read_json(report)
        if old["identity"] == identity(exe, plain, compact, bucket, cloud, fishnet):
            return
        raise ValueError("native eval comparison belongs to different inputs")
    with TemporaryDirectory(prefix="native-eval-ab-", dir=report.parent) as temp:
        root = Path(temp)
        inputs = {"cloud":cloud, "fishnet":fishnet}
        for label, path in inputs.items():
            short = root/label/path.name
            short.parent.mkdir()
            batch = next(pq.ParquetFile(path).iter_batches(batch_size=262144))
            pq.write_table(pa.Table.from_batches([batch]),short,compression="zstd")
        for label, src in (("plain",plain),("compact",compact)):
            book = root/label
            for file in [src/"_merge_params.json", *sorted((src/"ps").glob(f"event=*/elo_band=*/bkt{bucket:03}.parquet"))]:
                dest = book/file.relative_to(src)
                dest.parent.mkdir(parents=True,exist_ok=True)
                copyfile(file,dest)
        results = []
        reference = None
        for i,label in enumerate(("plain","compact","compact","plain")):
            work, out = root/f"work{i}", root/f"out{i}"
            start = perf_counter()
            subprocess.run([str(exe),"evals","--test-inputs","--book",str(root/label),"--cloud",str(root/"cloud"),
                            "--fishnet",str(root/"fishnet"),"--child-sources",str(bucket),"--buckets",str(bucket),
                            "--work",str(work),"--out",str(out),"--threads","4","--mem-gb","8"],check=True)
            seconds = perf_counter()-start
            c = read_json(work/"c/_done/g000.json")
            j = read_json(out/f"_bucket_done/bkt{bucket:03}.json")
            db = pq.ParquetFile(out/f"bkt{bucket:03}.parquet").read()
            if not len(db) or not c["rows_in"] or not c["rows"]:
                raise ValueError("empty real-bucket calibration")
            if reference is None:
                reference = db
            elif not reference.equals(db):
                raise ValueError("native eval output differs")
            results.append({"layout":label,"wall_seconds":seconds,"phase_c_seconds":c["secs"],
                            "phase_j_book_seconds":j["secs"]["book"],"source_move_rows":c["rows_in"],
                            "output_eval_rows":len(db)})
        write_json(report, {"identity":identity(exe, plain, compact, bucket, cloud, fishnet),
                            "source_rows_per_dataset":{k: min(262144,pq.ParquetFile(v).metadata.num_rows) for k,v in inputs.items()},
                            "bucket":bucket,"trials":results})
        print(results,flush=True)


def identity(exe, plain, compact, b, cloud, fishnet):
    done = f"_done/bkt{b:03}.DONE"
    return {"exe":sha(exe),"plain":sha(plain/done),"compact":sha(compact/done),
            "sources":[[str(p),p.stat().st_size,p.stat().st_mtime_ns] for p in (cloud,fishnet)]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("exe","plain","compact","cloud","fishnet","report"):
        parser.add_argument(key,type=Path)
    args = parser.parse_args()
    measure(**vars(args))
