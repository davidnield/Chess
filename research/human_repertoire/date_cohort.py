"""Bounded date-spread frames; footer discovery never reads move/outcome columns.

Run --discover to freeze frame metadata, then --extract to replay selected games.
All writes are under _artifacts/m3; canonical Parquet inputs remain read-only.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import heapq
import json
from pathlib import Path
import time

import pyarrow.parquet as pq

from .cohort import COLUMNS, rank_id, replay, select_reason
from .io import ROOT, fingerprint, output_path, sha256, write_json

CONFIG = ROOT / "docs/human_repertoire/milestone3_config.json"


def choose_frames(groups, count, width):
    """Spread across row groups, or across rows when fewer groups exist.

    Large strata use prefixes of separated row groups to avoid decoding skipped
    megabytes. Small strata use disjoint centered windows in their row stream.
    This is a convenience frame: footer dates verify spread, not random coverage.
    """
    if not groups or count <= 0 or width <= 0:
        raise ValueError("invalid frame request")
    if len(groups) >= count:
        return [{**groups[min(len(groups)-1, int((i+.5)*len(groups)/count))],
                 "start": 0, "rows": width} for i in range(count)]
    total = sum(g["row_group_rows"] for g in groups)
    frames = []
    for i in range(count):
        target = int((i+.5)*total/count)
        offset = 0
        for group in groups:
            n = group["row_group_rows"]
            if target < offset+n:
                start = max(0, min(n-width, target-offset-width//2))
                frames.append({**group, "start": start, "rows": width})
                break
            offset += n
    return frames


def validate_frames(frames, c):
    grouped = defaultdict(list)
    for f in frames:
        if (f["rows"] <= 0 or f["start"] < 0 or
                f["start"]+f["rows"] > f["row_group_rows"] or
                f["row_group_rows"] > c["max_row_group_rows"] or
                f["projected_uncompressed_bytes"] > c["max_projected_row_group_bytes"]):
            raise ValueError("frame exceeds declared bounds")
        grouped[(f["path"], f["row_group"])].append(f)
    for same in grouped.values():
        intervals = sorted((f["start"], f["start"]+f["rows"]) for f in same)
        if any(a[1] > b[0] for a, b in zip(intervals, intervals[1:])):
            raise ValueError("overlapping frames")
    decoded = sum(max(f["start"]+f["rows"] for f in same) for same in grouped.values())
    if decoded > c["max_decoded_rows"]:
        raise ValueError("decoded-row cap exceeded")
    return decoded


def discover(c):
    path = output_path("m3/frames.json")
    if path.exists():
        saved = json.loads(path.read_text())
        if saved["config"] != c:
            raise ValueError("frozen frame config drift")
        print("Reusing frozen frame discovery", flush=True)
        return
    all_frames, inventory = [], []
    for split, month in c["months"].items():
        for event in c["events"]:
            directory = Path(c["source_root"]) / f"year={c['year']}" / f"month={month}" / f"event={event}"
            paths = sorted(directory.glob("part-*.parquet"), key=lambda p: int(p.stem.split("-")[-1]))
            if not paths or len(paths) > c["max_files_per_stratum"]:
                raise ValueError("missing or excessive file inventory")
            groups = []
            for source in paths:
                pf = pq.ParquetFile(source)
                names = pf.schema_arrow.names
                inventory.append(fingerprint(source))
                for rg in range(pf.num_row_groups):
                    md = pf.metadata.row_group(rg)
                    dates = md.column(names.index("utc_date")).statistics
                    groups.append({"path": str(source), "row_group": rg,
                        "row_group_rows": md.num_rows,
                        "projected_uncompressed_bytes": sum(md.column(names.index(k)).total_uncompressed_size for k in COLUMNS),
                        "date_min": str(dates.min) if dates and dates.has_min_max else None,
                        "date_max": str(dates.max) if dates and dates.has_min_max else None})
            frames = choose_frames(groups, c["frames_per_stratum"], c["frame_rows"])
            for i, frame in enumerate(frames):
                frame.update(split=split, event=event, frame_id=f"{split}_{event}_{i}")
            all_frames.extend(frames)
            print(split, event, [(Path(f["path"]).name, f["row_group"], f["start"], f["date_min"], f["date_max"]) for f in frames], flush=True)
    decoded = validate_frames(all_frames, c)
    write_json("m3/frames.json", {"config": c, "created_utc": datetime.now(timezone.utc).isoformat(),
        "frames": all_frames, "inventory": inventory, "max_decoded_rows_used": decoded,
        "discovery_code_sha256": sha256(__file__)})


def extract(c):
    started = time.monotonic()
    frozen_path = output_path("m3/frames.json")
    frozen = json.loads(frozen_path.read_text())
    if frozen["config"] != c:
        raise ValueError("frame config drift")
    frames = frozen["frames"]
    validate_frames(frames, c)
    sources = sorted({f["path"] for f in frames})
    identity = {"config": c, "frames_sha256": sha256(frozen_path),
        "sources": [fingerprint(p) for p in sources], "extractor_sha256": sha256(__file__),
        "replay_sha256": sha256(ROOT / "research/human_repertoire/cohort.py"),
        "hash_adapter_sha256": sha256(ROOT / "python/zobrist.py")}
    inv = {p["path"]: p for p in frozen["inventory"]}
    if any(p != inv[p["path"]] for p in identity["sources"]):
        raise ValueError("source drift since footer discovery")
    manifest_path = output_path("m3/cohort_manifest.json")
    if manifest_path.exists():
        old = json.loads(manifest_path.read_text())
        if old["identity"] != identity or any(sha256(output_path(p)) != h for p, h in old["snapshots"].items()):
            raise ValueError("frozen cohort identity drift")
        print("Verified frozen milestone-3 cohorts", flush=True)
        return
    groups = defaultdict(list)
    for f in frames:
        groups[(f["path"], f["row_group"])].append(f)
    cohorts = {split: [] for split in c["months"]}
    audits, global_ids = {}, set()
    for (path, rg), windows in groups.items():
        if time.monotonic()-started > c["extract_runtime_seconds"]:
            raise TimeoutError("extraction runtime guard")
        counts = {f["frame_id"]: Counter() for f in windows}
        heaps = {f["frame_id"]: [] for f in windows}
        seen = set()
        offset = 0
        end = max(f["start"]+f["rows"] for f in windows)
        pf = pq.ParquetFile(path)
        for batch in pf.iter_batches(batch_size=c["batch_rows"], row_groups=[rg], columns=COLUMNS, use_threads=False):
            for f in windows:
                lo = max(offset, f["start"])
                hi = min(offset+len(batch), f["start"]+f["rows"])
                if lo >= hi:
                    continue
                counter, heap = counts[f["frame_id"]], heaps[f["frame_id"]]
                for row in batch.slice(lo-offset, hi-lo).to_pylist():
                    counter["scanned"] += 1
                    reason = select_reason(row, c)
                    if reason:
                        counter[reason] += 1
                        continue
                    if row["game_id"] in seen:
                        raise ValueError("duplicate eligible game in frame group")
                    seen.add(row["game_id"])
                    counter["eligible"] += 1
                    item = (-rank_id(row["game_id"], c["rank_seed"]), row["game_id"], row)
                    if len(heap) < c["games_per_frame"]:
                        heapq.heappush(heap, item)
                    elif item > heap[0]:
                        heapq.heapreplace(heap, item)
            offset += len(batch)
            if offset >= end:
                break
        for f in windows:
            counter, heap = counts[f["frame_id"]], heaps[f["frame_id"]]
            if counter["scanned"] != f["rows"]:
                raise ValueError("incomplete frame")
            games = []
            for _, game_id, row in sorted(heap, reverse=True):
                if game_id in global_ids:
                    raise ValueError("duplicate selected game across frames/splits")
                global_ids.add(game_id)
                try:
                    game = replay(row, c)
                except (ValueError, AssertionError, IndexError):
                    counter["invalid_opening"] += 1
                    continue
                game.update(event=f["event"], frame_id=f["frame_id"])
                games.append(game)
            counter["selected"] = len(heap)
            counter["accepted"] = len(games)
            counter["rank_cap_excluded"] = counter["eligible"]-len(heap)
            audits[f["frame_id"]] = {"counts": dict(counter),
                "dates": dict(Counter(g["utc_date"] for g in games)),
                "time_controls": dict(Counter(g["time_control"] for g in games)),
                "observed_ends": sum(g["observed_end"] for g in games)}
            cohorts[f["split"]].extend(games)
            print(f["frame_id"], dict(counter), audits[f["frame_id"]]["dates"], flush=True)
    if [fingerprint(p) for p in sources] != identity["sources"]:
        raise ValueError("source changed during extraction")
    snapshots = {f"m3/{split}_games.json": sha256(write_json(f"m3/{split}_games.json", games)) for split, games in cohorts.items()}
    players = {split: {g[color].casefold() for g in games for color in ("white", "black")} for split, games in cohorts.items()}
    write_json("m3/cohort_manifest.json", {"identity": identity, "snapshots": snapshots,
        "created_utc": datetime.now(timezone.utc).isoformat(), "audits": audits,
        "elapsed_seconds": time.monotonic()-started,
        "cohort_counts": {split: dict(Counter(g["event"] for g in games)) for split, games in cohorts.items()},
        "unique_players": {k: len(v) for k, v in players.items()},
        "shared_players": {f"{a}/{b}": len(players[a]&players[b]) for i, a in enumerate(players) for b in list(players)[i+1:]}})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--discover", action="store_true")
    action.add_argument("--extract", action="store_true")
    args = parser.parse_args()
    c = json.loads(args.config.read_text())
    (discover if args.discover else extract)(c)


if __name__ == "__main__":
    main()
