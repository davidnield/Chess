"""Full-key ladder, stability and paired-evaluation contracts."""
from __future__ import annotations
import math
from collections import defaultdict
from .spec import digest, evaluation_horizon

def key(value):
    if isinstance(value,dict): return (value["lo"],value["hi"])
    if isinstance(value,(tuple,list)) and len(value)==2: return tuple(value)
    raise ValueError("sweep reports require full position keys")

def prescriptions(rows):
    result={}
    for row in rows:
        k=key(row["position_hash"]); move=row.get("move_uci") or row.get("best_move")
        if not move: continue
        if k in result and result[k]!=move: raise ValueError("conflicting prescriptions at one board")
        result[k]=move
    return result

def check_ladder(small,medium,master):
    for child,parent in ((small,medium),(medium,master)):
        if any(parent.get(k)!=v for k,v in child.items()):
            raise ValueError("ladder changes or drops an inherited prescription")
    return {"sizes":[len(small),len(medium),len(master)],"conflicts":0}

def inherit_extensions(base, smaller_extended, extended_master):
    result=dict(base)
    for k,m in smaller_extended.items():
        if extended_master.get(k)!=m or (k in result and result[k]!=m):
            raise ValueError("extension conflict requires a new master version")
        result[k]=m
    if any(extended_master.get(k)!=m for k,m in result.items()):
        raise ValueError("ladder leaves its extended master")
    return result, {"inherited_additional":len(result)-len(base)}

def stability(books):
    """Each book supplies audited eligibility/reach records, not just its selected moves.
    Reduced copies and sensitivity runs are excluded from headline independent votes.
    Named-opening aggregation can use the same rows with an externally versioned ECO map.
    """
    chosen=[b for b in books if b["family"]=="primary" and b["mode"]=="independent"]
    seen=set(); totals=defaultdict(int)
    for b in chosen: totals[b["colour"]]+=1
    rows=defaultdict(lambda:dict(gate_tested=0,eligible=0,reachable=0,selected=0,reach_sum=0.))
    for b in chosen:
        if b["id"] in seen: raise ValueError("duplicate book vote")
        seen.add(b["id"])
        colour=b["colour"]
        choices_seen=set()
        for r in b["choices"]:
            k=(colour,key(r["position_hash"]),r["move"])
            if k in choices_seen: raise ValueError("duplicate choice vote inside one book")
            choices_seen.add(k)
            if not math.isfinite(r["reach"]) or r["reach"]<0: raise ValueError("invalid reach")
            if r["selected"] and (not r["eligible"] or not r["reachable"]):
                raise ValueError("selection without eligibility/reach")
            if r["reachable"] and not r["eligible"]:
                # Eligibility of a move and reachability of its parent differ.
                pass
            a=rows[k]; a["gate_tested"]+=r["eligible"] is not None
            a["eligible"]+=bool(r["eligible"])
            a["reachable"]+=bool(r["eligible"] and r["reachable"])
            a["selected"]+=bool(r["selected"])
            a["reach_sum"]+=float(r["reach"]) if r["selected"] else 0.
    return [dict(colour=c,position_hash=dict(lo=k[0],hi=k[1]),move=m,**v,
                 unknown_gate_count=totals[c]-v["gate_tested"],
                 selection_rate=v["selected"]/v["reachable"] if v["reachable"] else None)
            for (c,k,m),v in sorted(rows.items())]

def evaluation_contract(books, *, model, policy, seeds, panel_receipt, phase="development"):
    if not model.get("revision") or not model.get("backend"):
        raise ValueError("evaluator model/backend still unresolved")
    if not seeds or len(seeds)!=len(set(seeds)): raise ValueError("invalid paired seeds")
    horizon=evaluation_horizon(max(b["deepest_ply"] for b in books))
    value=dict(book_receipts=sorted(b["receipt_sha256"] for b in books),model=model,
               policy=policy,seeds=list(seeds),panel_receipt=panel_receipt,horizon=horizon,
               phase=phase)
    return dict(value,id=digest(value))

def pilot_acceptance(report):
    required=("full_keys","candidate_menu_complete","mass_conserved","gate_audit",
              "lookahead_audit","quality_closed","evidence_monotone","clean_rebuild_parity")
    missing=[k for k in required if report.get(k) is not True]
    if report.get("distinct_prescriptions")!=2000: missing.append("distinct_prescriptions")
    if not report.get("receipt_sha256"): missing.append("receipt_sha256")
    # New production acceptance cannot be granted by an old closure report or by
    # descriptive booleans for the November 25 model-contract requirements.
    from pathlib import Path
    import json
    from .stages import file_sha
    if report.get("schema")!="pilot-acceptance-v2":missing.append("model_acceptance_schema")
    contract=report.get("model_contract_sha256")
    if not isinstance(contract,str) or len(contract)!=64:missing.append("model_contract_sha256")
    for name in ("policy_continuity","own_calibration","uncertainty_accounting","horizon_semantics",
                 "evaluation_binding","native_referee","effective_objective","input_cost"):
        proof=(report.get("model_evidence") or {}).get(name)
        try:
            if not proof or not contract or file_sha(proof["path"])!=proof["sha256"]:
                raise ValueError("unbound evidence")
            record=json.loads(Path(proof["path"]).read_text(encoding="utf8"))
            if (record.get("schema")!="model-check-v1" or record.get("check")!=name
                or record.get("passed") is not True or record.get("model_contract_sha256")!=contract):
                raise ValueError("wrong model evidence")
        except (OSError,ValueError,KeyError,TypeError):missing.append(name)
    return {"ready_for_matrix":not missing,"missing":missing}
