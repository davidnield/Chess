"""Verify and close the five completed cutoff controls. New builds use run_uncut_sweep."""
import json
from pathlib import Path

import polars as pl

import run_practical_rebuild as r
import screen_repertoires as screen


def main():
    screen.ROOT = r.OUT / "cp-screen-030-060-100"
    with screen.workspace():
        caps = {"black": [30, 60, 100], "white": [30, 60]}
        receipts = {}
        for colour, gates in caps.items():
            for cap in gates:
                name = f"{colour}-cp{cap}"
                receipt = screen.ROOT / "stages" / (name + ".json")
                book = screen.ROOT / colour / f"cp{cap}" / "book_b2000.parquet"
                outputs = json.loads(receipt.read_text())["outputs"]
                for path in (book, Path(str(book) + ".meta.json")):
                    if r.digest(path) != outputs[str(path)]:
                        raise ValueError(f"Cutoff control changed: {path}")
                moves = (pl.scan_parquet(book)
                         .filter((pl.col("side_to_move") == colour) & pl.col("best_move").is_not_null())
                         .select("position_hash").collect())
                if moves.height != 2000 or moves["position_hash"].n_unique() != 2000:
                    raise ValueError(f"Invalid cutoff control footprint: {book}")
                receipts[name] = r.digest(receipt)
        r.status(state="closed", stage="cutoff-construction", completed_caps=caps,
                 cancelled=["white-cp100"], receipts=receipts,
                 reason="White 100 cp will be built only in the corrected uncut sweep.")


if __name__ == "__main__":
    main()
