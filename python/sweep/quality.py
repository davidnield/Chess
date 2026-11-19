"""Count-independent evidence decisions and shared, monotone verification requests."""
from __future__ import annotations
from dataclasses import dataclass
import math

def score(cp):
    return 1/(1+math.exp(-.00368208*cp))

@dataclass(frozen=True)
class Evidence:
    cp: float | None
    nodes: int = 0
    independent: bool = False
    compatible: bool = True
    source: str = "stored"

def review(best_cp, child, *, absolute_floor, relative_limit, band=.01,
           selected=True, tactical=False, disputed=False):
    """Own-side cp. Frequency intentionally is not an input."""
    reasons=[]
    missing=child.cp is None or not math.isfinite(child.cp) or not child.compatible
    if missing: reasons.append("missing-or-incompatible")
    if selected and (not child.independent or child.nodes<5_000_000):
        reasons.append("independent-final-prescription")
    boundary=False
    if not missing:
        relative=(best_cp is not None and abs(score(best_cp)-score(child.cp)-relative_limit)<=band+1e-12)
        boundary=relative or abs(score(child.cp)-absolute_floor)<=band+1e-12
    deep=boundary or tactical or disputed
    if boundary: reasons.append("score-boundary")
    if tactical: reasons.append("tactical-gaps")
    if disputed: reasons.append("inconsistent-evidence")
    required=20_000_000 if deep else 5_000_000
    if not missing and child.independent and child.nodes>=required:
        # An unresolved disagreement is not certified by node count alone.
        reasons=["inconsistent-evidence"] if disputed else []
    return {"required_nodes":max(required,child.nodes), "reasons":reasons}

def coalesce(requests):
    """Union complete comparison menus, across arms, per full board/engine contract."""
    groups={}
    for r in requests:
        if len(r["key"])!=2: raise ValueError("request needs both hash halves")
        identity=(tuple(r["key"]),r["epd"],r["engine_contract"])
        item=groups.setdefault(identity,dict(key=list(r["key"]),epd=r["epd"],
                    engine_contract=r["engine_contract"],nodes=0,moves=set(),arms=set(),reasons=set()))
        item["nodes"]=max(item["nodes"],r["nodes"])
        for k in ("moves","arms","reasons"): item[k].update(r[k])
    return [dict(r,**{k:sorted(r[k]) for k in ("moves","arms","reasons")})
            for _,r in sorted(groups.items())]

def prefer(existing, incoming):
    """Compatible independent evidence is monotone; never replace a 20M verdict by 5M."""
    if existing is None: return incoming
    if existing["engine_contract"]!=incoming["engine_contract"]:
        raise ValueError("evidence from another engine/search contract must stay separate")
    if existing["key"]!=incoming["key"] or existing["move"]!=incoming["move"]:
        raise ValueError("evidence identities differ")
    return incoming if incoming["nodes"]>existing["nodes"] else existing
