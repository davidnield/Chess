"""--max-eval-loss (budget_core.eval_loss_keep / gate_candidates), 2026-10-22.

  A  keeps candidates within CP of the best evaluated candidate, drops the rest
  B  the position's own eval raises the baseline; if nothing is close enough the best
     evaluated candidate stays (a node never loses its choice set)
  C  Black: cp is taken from our side's view
  D  the source book's move is filtered too (the Stage-3 gates always admit it)
  E  off (None) is an exact no-op on gate_candidates
  F  candidates without an eval go once any candidate has one; none evaluated = unchanged
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
from stage3_backwards_induction import cp_to_expected_score as ws  # noqa: E402

fails = 0


def check(name, ok):
    global fails
    print(("PASS " if ok else "FAIL ") + name)
    fails += 0 if ok else 1


def edge(san, child):
    return {"move_san": san, "child_hash": child, "total": 1000, "white_score_avg": 0.5}


def sans(pairs):
    return sorted(e["move_san"] for e, _ in pairs)


# White to move at node 1; children 10 (+50), 11 (+30), 12 (-40); evals white-view.
ES = [edge("a", 10), edge("b", 11), edge("c", 12)]
EV = {10: ws(50), 11: ws(30), 12: ws(-40)}
pairs = [(e, EV[e["child_hash"]]) for e in ES]

check("A: within 30 cp of +50 keeps a, b", sans(bc.eval_loss_keep(pairs, None, True, 30)) == ["a", "b"])
check("A: within 100 cp keeps all", sans(bc.eval_loss_keep(pairs, None, True, 100)) == ["a", "b", "c"])
check("B: own eval +100 drops all but the best", sans(bc.eval_loss_keep(pairs, ws(100), True, 30)) == ["a"])
check("B: own eval +60 with 25 cp keeps a only",
      sans(bc.eval_loss_keep(pairs, ws(60), True, 25)) == ["a"])
check("C: Black's view: -40 white-view is the best for Black",
      sans(bc.eval_loss_keep(pairs, None, False, 30)) == ["c"])

# D/E through gate_candidates with the gates effectively open (rel floor 1.0)
kw = dict(robustness_floor=1.0, gate_rel_floor=1.0)
base = bc.gate_candidates(1, ES, "c", EV, True, 0.5, **kw)
check("E: None is an exact no-op",
      base == bc.gate_candidates(1, ES, "c", EV, True, 0.5, max_eval_loss=None, **kw))
check("D: source move c is admitted with the gate off", "c" in sans(base))
check("D: ...and filtered with it on",
      sans(bc.gate_candidates(1, ES, "c", EV, True, 0.5, max_eval_loss=30, **kw)) == ["a", "b"])

ES2 = ES + [edge("d", 13)]
EV2 = dict(EV)
pairs2 = [(e, EV2.get(e["child_hash"])) for e in ES2]
check("F: an unevaluated candidate goes once others are evaluated",
      sans(bc.eval_loss_keep(pairs2, None, True, 1000)) == ["a", "b", "c"])
none_ev = [(e, None) for e in ES]
check("F: nothing evaluated -> unchanged", bc.eval_loss_keep(none_ev, None, True, 30) == none_ev)
check("ws_to_cp inverts cp_to_expected_score",
      abs(bc.ws_to_cp(ws(123), True) - 123) < 1e-6 and abs(bc.ws_to_cp(ws(123), False) + 123) < 1e-6)

sys.exit(1 if fails else 0)
