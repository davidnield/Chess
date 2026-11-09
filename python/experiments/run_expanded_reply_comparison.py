"""Checkpointed Black book comparison with all observed replies on a common local inventory."""
import argparse
from contextlib import ExitStack
import errno
import json
import subprocess
import sys
import time
from pathlib import Path

from run_opponent_prior_challenger import copy_once, digest, lock, pin


def read(path):
    return json.loads(path.read_text(encoding="utf8"))


def run(checkout, broad, out, code):
    import nomination_trial as nt
    import practical_scenarios as scenarios
    import polars as pl
    import run_practical_rebuild as r
    from expand_opponent_replies import recover

    r.ROOT = checkout
    r.STATUS_FILE = out/"status.json"
    r.SELECTION = r.DEPTH = r.CODE = code

    def use_workspace(path):
        r.OUT = path
        for folder in ("logs","stages","duckdb-temp"):
            (path/folder).mkdir(parents=True,exist_ok=True)

    use_workspace(out)
    data = out/"data"
    data.mkdir(exist_ok=True)
    prior = data/"replies.parquet"
    meta = Path(str(prior)+".json")
    if not meta.exists():
        r.status(state="recovering-archived-replies",buckets=data/"buckets")
        # An extraction already in progress holds the same lock. Resume any
        # incomplete buckets ourselves if that process exits or after a reboot.
        while not meta.exists():
            with ExitStack() as stack:
                try:
                    stack.enter_context(lock(data/"extract.lock"))
                except OSError as exc:
                    if exc.errno != errno.EACCES:
                        raise
                    time.sleep(30)
                    continue
                if not meta.exists():
                    recover(broad,data)
    prior_meta = read(meta)
    if (prior_meta["schema"] != 2 or digest(prior) != prior_meta["sha256"] or
            digest(Path(str(prior)+".aux.parquet")) != prior_meta["extra_aux_sha256"]):
        raise ValueError("Expanded reply data changed")

    previous = broad/"opponent-prior-challenger"
    r.status(state="waiting-for-original-comparison",dependency=previous/"status.json",
             expanded_positions=prior_meta["positions"],all_subfloor=prior_meta["all_subfloor_parents"])
    while True:
        prior_status = read(previous/"status.json") if (previous/"status.json").exists() else {}
        if prior_status.get("state") == "complete":
            break
        if prior_status.get("state") == "failed":
            raise RuntimeError(f"Original Black comparison failed: {prior_status.get('error')}")
        time.sleep(30)
    reference = read(broad/"black/depth-policy/selected.json")
    props = Path(reference["verified_proposals"])
    baseline = Path(reference["book"])
    if digest(props) != reference["verified_proposals_sha256"] or digest(baseline) != reference["book_sha256"]:
        raise ValueError("Baseline selection changed")
    design = {"baseline":reference, "reply_inputs":prior_meta, "pseudogames":[0,30], "budget":2000,
              "horizon":30, "seeds":r.SEEDS, "scenarios":list(scenarios.SCENARIOS),
              "eligibility":"Same frozen inventory in both builds. Parents without a historical reply are excluded.",
              "nonmove":"Hold empirical termination and horizon probabilities fixed. Smooth only conditional reply frequencies.",
              "feedback":"Both final books must pass verification against the same corrected proposal file."}
    if (out/"design.json").exists() and read(out/"design.json") != design:
        raise ValueError("The paired design changed")
    r.publish(out/"design.json",design)
    workspaces = {k:out/f"k{k}" for k in (0,30)}
    for root in (out,*workspaces.values()):
        for suffix in ("",".meta.json"):
            source = previous/("training-sf.parquet"+suffix)
            copy_once(source,root/source.name)
    common = out/"black/depth-policy"
    for cache in ("verification-cache","transposition-cache-v2"):
        for file in (previous/"black/depth-policy"/cache).glob("*.json"):
            copy_once(file,common/cache/file.name)

    for epoch in range(100):
        built = {}
        for k,arm in workspaces.items():
            use_workspace(arm)
            label = f"expanded-k{k}-{epoch:02d}"
            work = arm/"black/depth-policy"/f"round-{epoch:02d}"
            work.mkdir(parents=True,exist_ok=True)
            args = [*r.recipe("black"),"--opponent-prior",str(prior),"--opponent-pseudogames",str(k)]
            book = nt.exact_provisional("black",work,label,args,props)
            use_workspace(out)
            corrected = work/"checked-proposals.parquet"
            receipt = Path(str(corrected)+".meta.json")
            r.stage(label+"-verify",r.cmd(code/"cloud_feedback.py","--book",book,"--colour","black",
                    "--proposals",props,"--cache",common/"verification-cache","--out",corrected),[corrected,receipt])
            if read(receipt)["searched_positions"]:
                # Restart both arms when either one adds evidence. The other
                # arm's earlier selection is no longer a matched comparison.
                props = r.resolve_proposals("black",label+"-feedback",corrected)
                break
            if digest(props) != digest(corrected):
                raise ValueError("Verification changed proposals without recording feedback")
            built[k] = book
        if built.keys() == workspaces.keys():
            break
    else:
        raise RuntimeError("Common verification did not converge")

    books = {"CANON":r.incumbent("black"), "BASE":baseline}
    audits = {label:read(broad/"black"/f"audit-{name}.json") for label,name in (("CANON","CANON"),("BASE","NEW"))}
    for k,book in built.items():
        use_workspace(workspaces[k])
        label = f"EXP{k}"
        audits[label] = r.audit("black",{"NEW":book})["NEW"]
        books[label] = book
    if any(digest(book) != audits[label]["book_sha256"] for label,book in books.items()):
        raise ValueError("Audit and book fingerprints differ")
    use_workspace(out)
    r.publish(out/"selected.json",{"epoch":epoch,"books":books,"common_proposals":props,
                                  "common_proposals_sha256":digest(props)})
    decisions = {label:scenarios.decisions(book,"black") for label,book in books.items()}
    for name,scenario in scenarios.SCENARIOS.items():
        r.compare("black",books,audits,name,scenario,private_tables=previous/"cohorts")
        dest = out/"black/comparison"/name
        scores = pl.read_parquet(dest/"scores.parquet")
        for ref in ("BASE","EXP0"):
            r.publish(dest/f"paired-vs-{ref}.json",scenarios.paired_summary(scores,ref,decisions,audits))
    r.status(state="complete",comparison=out/"comparison.csv",selected=out/"selected.json")


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--checkout",type=Path,default=Path(__file__).resolve().parents[2])
    cli.add_argument("--pinned",action="store_true")
    args = cli.parse_args()
    checkout = args.checkout.resolve()
    broad = checkout/"outputs/rebuild-2026-11-06"
    out = broad/"expanded-opponent-comparison"
    out.mkdir(exist_ok=True)
    if not args.pinned:
        with lock(out/"pin.lock"):
            script = Path(__file__).resolve()
            code = pin(checkout,out,(script,script.with_name("expand_opponent_replies.py")))
        raise SystemExit(subprocess.call([sys.executable,"-X","utf8","-u",str(code/script.name),
                                          "--checkout",str(checkout),"--pinned"],cwd=checkout))
    with lock(out/"run.lock"):
        try:
            run(checkout,broad,out,Path(__file__).resolve().parent)
        except Exception as exc:
            import run_practical_rebuild as r
            r.publish(out/"status.json",{"state":"failed","error":str(exc),"updated_utc":r.now()})
            raise
