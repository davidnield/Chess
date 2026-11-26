"""Read-only publication review; producer performed the full payload SHA pass.

This independently checks manifest/config/receipt identities, coverage, sizes and
Parquet footers. It deliberately does not repeat the 625 GB payload hash pass.
"""
from pathlib import Path
import datetime as dt
import hashlib
import json
import pyarrow.parquet as pq


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def digest(value):
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(data.encode()).hexdigest()


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for data in iter(lambda: stream.read(8 << 20), b""):
            h.update(data)
    return h.hexdigest()


config_path = Path("D:/chess/full-policy-pools-20261125/config-v2.json")
config_sha = "5962ddd95e67e47e1e2d4ade355225a62b87155e6cd80cb361cf91236aa52b7e"
assert sha(config_path) == config_sha
cfg = read(config_path)
recipe = next(r for r in cfg["pools"] if r["id"] == "weak1000-1799-p30")
root = Path(recipe["output"])
manifest_path = root / "pool.json"
manifest_sha = sha(manifest_path)
manifest = read(manifest_path)
assert manifest_sha == "f08b894687646fd89d93891538fa880f9d3e573bd94bf475a347dc2227758d1f"
assert manifest["schema"] == "complete-policy-counts-v1"
assert manifest["complete"] is True
assert (manifest["key_bits"], manifest["min_games"], manifest["max_ply"]) == (128, 1, 30)
assert (manifest["buckets"], manifest["parts"]) == (512, 8)
assert manifest["recipe"] == recipe
assert manifest["population"] == {"events": ["Blitz", "Rapid", "Classical"],
                                  "elo_bands": [1000, 1200, 1400, 1600]}
assert recipe["rating_semantics"] == "game-mean"
assert recipe["training_months"] == ["2013-01", "2026-07"]
assert manifest["source_identity"] == digest(cfg["source"])
identity = digest(dict(recipe=recipe, source=cfg["source"], producer=cfg["producer"]))
assert manifest["identity"] == identity
assert read(root / "pool.json.meta.json") == {k: v for k, v in manifest.items() if k != "files"}
for name, expected in cfg["producer"].items():
    assert sha(name) == expected, name

expected_names = {f"b{b:03d}-r{p:02d}.parquet" for b in range(512) for p in range(8)}
assert len(manifest["files"]) == 4096
assert {r["file"] for r in manifest["files"]} == expected_names
assert {p.name for p in root.glob("b???-r??.parquet")} == expected_names
assert {p.name.removesuffix(".receipt.json") for p in root.glob("b???-r??.parquet.receipt.json")} == expected_names
totals = dict(rows=0, bytes=0, white_wins=0, draws=0, black_wins=0, total=0)
for record in manifest["files"]:
    b, p = record["bucket"], record["part"]
    assert record["file"] == f"b{b:03d}-r{p:02d}.parquet"
    assert record["identity"] == digest(dict(identity=identity, bucket=b, part=p, parts=8))
    low = -(1 << 63) + p * (1 << 61)
    assert (record["low"], record["high"]) == (low, low + (1 << 61) - 1)
    assert read(root / (record["file"] + ".receipt.json")) == record
    path = root / record["file"]
    assert path.stat().st_size == record["bytes"], str(path)
    footer = pq.read_metadata(path)
    assert footer.num_rows == record["rows"], str(path)
    schema = footer.schema.to_arrow_schema()
    assert schema.field("parent_hash").type.num_fields == 2
    assert schema.field("parent_hash").type.field("lo").type.bit_width == 64
    assert schema.field("parent_hash").type.field("hi").type.bit_width == 64
    assert schema.names == ["parent_hash", "move_san", "move_xor_lo", "move_xor_hi", "ply",
                            "white_wins", "draws", "black_wins", "total"]
    mass = record["mass"]
    assert mass["total"] == sum(mass[k] for k in ("white_wins", "draws", "black_wins"))
    assert mass["total"] >= record["rows"]
    for key in ("rows", "bytes"):
        totals[key] += record[key]
    for key in mass:
        totals[key] += mass[key]
assert sha(manifest_path) == manifest_sha
result = dict(
    verified_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
    manifest=str(manifest_path), manifest_sha256=manifest_sha, config_sha256=config_sha,
    producer_files_checked=len(cfg["producer"]), complete_buckets=512, complete_ranges=4096,
    manifest_receipt_identity_size_and_footer_checks="passed", totals=totals,
    independent_payload_rehash=False,
    payload_hash_evidence="Frozen producer check_receipt SHA-256 verifies every payload before publishing pool.json; this review checks its receipts and full inventory, not a second payload hash pass.",
    limitations="Does not repeat source aggregation or a full semantic row census. Prior regression and real-range parity evidence remain separate. Pool completion does not accept a model or repertoire.",
)
Path(__file__).with_name("verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps(result, indent=2))
