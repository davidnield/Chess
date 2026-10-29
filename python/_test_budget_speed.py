"""The compiled price engine against the dict engine it replaced, bit for bit (2026-10-29).

budget_compiled.py runs price_book, policy_reach, policy_value and polish_book's region
evaluator as array sweeps and index walks; topo_up runs its Tarjan on integer ids; and
price_search's default engine='auto' compiles every graph of PRICE_COMPILED_MIN_NODES
nodes or more. None of it may change a result. The dict
implementations stay in budget_core as the reference (compiled=None, engine='legacy',
_topo_up_dict), and this test holds the two to tolerance 0: every float compared by its
bits, every dict by its items IN ORDER (insertion order decides polish's tie-breaks and
the row order of the written books).

  T  topo_up == _topo_up_dict (order and cut) on every graph below, and on random graphs
     full of repetition cycles.
  K  price_book with compiled= equals the dict walk on every key, over a lambda grid,
     with the optimistic and a source-seeded reach, with a fixed policy, and with an
     incumbent (stickiness bonus); policy_reach and policy_value(detail=True) on random
     books, including moves that are not candidates and positions outside the graph.
  S  price_search(engine='compiled') == price_search(engine='legacy'): the result dict and
     info (sweep, bound, nearest sizes, polish stats bar their seconds) for EVERY graph and
     budget of _test_budget_dag.py (the 300-DAG report, checks E and N/P) and EVERY graph
     of _test_budget_price.py under the production polish build (--price-seed source
     --price-reprice max --price-rounds 1 --price-select best --price-dag-mode polish);
     and under every flag set those tests run (defaults; rounds 3 with reprice max; the
     production DAG flags with --price-rounds 4 and dag_mode None / fixpoint / polish /
     fixpoint+polish) on every graph of _test_budget_price.py, checks E and N/P, the hand
     cases and the report's first 60 DAGs. --full runs every flag set on every graph
     (about five minutes; the compiled engine's fixed numpy cost per solve is ~0.4 ms,
     which on these 20-node graphs is 30x a dict solve).
  P  polish_book(compiled=...) == polish_book() from the price book and from nothing,
     with and without a fixed policy. Throughout S and P, budget_compiled.CHECK_ADVANCE
     re-evaluates every incrementally rebuilt polish region in full and fails on any
     difference (dicts, their order, parents lists, floats).
  F  a graph the arrays cannot hold (an in-subgraph child that is not a node) compiles
     with ok=False and every call falls back to the dict code.
  G  price_search leaves the garbage collector as it found it, also when it raises.

Usage:  _test_budget_speed.py [--full]        the checks (exit 1 on any difference)
        _test_budget_speed.py --scale 10000,100000,1000000 400,2000 [--rounds 1]
                wall-clock, result fingerprint and peak memory of both engines with the
                production polish flags on _test_budget_dag.big_dag graphs, each
                (engine, size) in its own process so peaks do not mix
"""
from __future__ import annotations

import gc
import hashlib
import json
import math
import random
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import budget_compiled  # noqa: E402
import budget_core as bc  # noqa: E402
from budget_compiled import compile_graph  # noqa: E402
from budget_core import Graph, OppNode, OurNode  # noqa: E402
import _test_budget_dag as tdag  # noqa: E402
import _test_budget_price as tprice  # noqa: E402

FAIL = 0
QUIET = lambda *_: None  # noqa: E731


def check(name, ok, detail=""):
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


def same(a, b) -> bool:
    """Exact equality: floats by their bits (0.0 != -0.0), dicts by items in order."""
    if isinstance(a, float) and isinstance(b, float):
        return (a == b and math.copysign(1.0, a) == math.copysign(1.0, b)) or (a != a and b != b)
    if isinstance(a, dict) and isinstance(b, dict):
        return list(a) == list(b) and all(same(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return type(a) is type(b) and len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and a == b if not isinstance(a, (int, float)) else a == b


def strip_seconds(info: dict) -> dict:
    out = {k: v for k, v in info.items() if k != "timing"}
    if out.get("polish"):
        out["polish"] = [{k: v for k, v in p.items() if k != "seconds"} for p in out["polish"]]
    return out


def diff_keys(a: dict, b: dict) -> list[str]:
    return [k for k in set(a) | set(b) if k not in a or k not in b or not same(a[k], b[k])]


# ── graphs ──────────────────────────────────────────────────────────────────

def cyclic_graph(rng: random.Random, n: int) -> Graph:
    """Arbitrary edges among n nodes: repetition cycles everywhere, duplicate edges,
    our-to-our and opponent-to-opponent edges, reaches that tie."""
    g = Graph(root=0, our={}, opp={}, epd={}, ply={}, reach={})
    for h in range(n):
        if h % 2 == 0:
            g.our[h] = OurNode(l_node=rng.random())
        else:
            g.opp[h] = OppNode(const_base=rng.random() * 0.2)
    for h in range(n):
        for k in range(rng.randint(0, 3)):
            ch = rng.randrange(n)
            if h % 2 == 0:
                g.our[h].cands.append((f"m{k}", ch, True, rng.random()) if rng.random() < 0.8
                                      else (f"m{k}", None, False, rng.random()))
            else:
                g.opp[h].kids.append((rng.random() / 3, ch, True, rng.random())
                                     if rng.random() < 0.8 else (0.1, None, False, rng.random()))
    g.reach = {h: rng.choice([1.0, 0.5, rng.random()]) for h in range(n)}
    return g


def dag_graphs() -> list[tuple[str, Graph, list[int]]]:
    """Every graph (and its budgets) _test_budget_dag.py searches."""
    out = [(f"report{t}", g, b) for t, (g, b) in enumerate(tdag.report_instances())]
    out += [(f"E{t}", g, [2, 5]) for t, (g, _books) in enumerate(tdag.e_instances())]
    out += [(f"NP{t}", g, [2, 5]) for t, g in enumerate(tdag.np_graphs())]
    out += [("D", tdag.case_d(), [1, 2, 3]), ("F50", tdag.case_f(50), [1, 2, 3]),
            ("F5", tdag.case_f(5), [1, 2, 3]), ("T8", tdag.case_t8(), [1, 2])]
    return out


def price_graphs() -> list[tuple[str, Graph, list[int]]]:
    """Every graph (and its targets) _test_budget_price.py searches."""
    out = [(f"AB{t}", g, tprice.ab_targets(g)) for t, g in enumerate(tprice.ab_trees())]
    out += [(f"H{t}", g, [1, max(1, len(g.our) // 2)]) for t, g in enumerate(tprice.h_trees())]
    out += [(f"I{t}", g, tprice.i_targets(g)) for t, g in enumerate(tprice.i_trees())]
    out += [("C", tprice.graph_c(), [3, 6]), ("D", tprice.graph_d(), [3]),
            ("F50", tprice.graph_f(50), [3]), ("F5", tprice.graph_f(5), [3]),
            ("E", tprice.graph_e(), [1])]
    return out


def flag_sets(g: Graph, order: list[int], every: bool) -> list[tuple[str, dict]]:
    """The production polish build; with every=True also each flag set the two test files
    run."""
    seed = tdag.source_seed(g, order)
    prod = dict(rounds=4, select="best", reprice="max", seed_reach=seed)
    out = [("prod1+polish", dict(prod, rounds=1, dag_mode="polish"))]
    if every:
        out += [("defaults", {}), ("rounds3-max", dict(rounds=3, reprice="max")),
                ("prod", prod)]
        out += [(f"prod+{m}", dict(prod, dag_mode=m)) for m in tdag.DAG_MODES]
    return out


# ── checks ──────────────────────────────────────────────────────────────────

def check_topo(graphs) -> None:
    rng = random.Random(3)
    cyc = [cyclic_graph(rng, rng.randint(5, 200)) for _ in range(300)]
    bad = cuts = 0
    for g in [g for _n, g, _b in graphs] + cyc:
        a, b = bc.topo_up(g), bc._topo_up_dict(g)
        bad += a[0] != b[0] or a[1] != b[1]
        cuts += bool(a[1])
    check("T: topo_up's order and cut set are _topo_up_dict's", bad == 0,
          f"{len(graphs) + len(cyc)} graphs ({cuts} with cycles cut), {bad} differ")


def check_kernels(graphs) -> None:
    rng = random.Random(29)
    bad_pb = bad_pr = bad_pv = n_pb = n_pv = 0
    for _name, g, _b in graphs:
        order, cut = bc.topo_up(g)
        cg = compile_graph(g, order, cut)
        if not cg.ok:
            bad_pb += 1
            continue
        seed = tdag.source_seed(g, order)
        fixed = {h: rng.choice(n.cands)[0] for h, n in g.our.items()
                 if n.cands and rng.random() < 0.7}
        stick = ({h: rng.choice(n.cands)[0].rstrip("+#") for h, n in g.our.items()
                  if n.cands and rng.random() < 0.5}, 0.02)
        for meta in (None, stick):
            if meta is not None:
                g.meta["stick"] = meta
            for lam in (1e-10, 1e-5, 1e-4, 1e-3, 0.01, 0.045, 0.3):
                for reach in (None, seed):
                    for fp in (None, fixed):
                        a = bc.price_book(g, lam, order, reach, fp)
                        b = bc.price_book(g, lam, order, reach, fp, compiled=cg)
                        n_pb += 1
                        bad_pb += bool(diff_keys(a, b))
            g.meta.pop("stick", None)
        for _ in range(4):
            moves = {h: rng.choice(n.cands)[0] for h, n in g.our.items()
                     if n.cands and rng.random() < 0.6}
            if g.our and rng.random() < 0.5:
                moves[rng.choice(list(g.our))] = "not-a-candidate"
            moves[10 ** 12 + rng.randrange(100)] = "outside"
            n_pv += 1
            bad_pr += not same(bc.policy_reach(g, moves, order),
                               bc.policy_reach(g, moves, order, compiled=cg))
            bad_pv += bool(diff_keys(bc.policy_value(g, moves, order, detail=True),
                                     bc.policy_value(g, moves, order, detail=True,
                                                     compiled=cg)))
    check("K: price_book (compiled) is the dict walk, every key, bit for bit", bad_pb == 0,
          f"{n_pb} solves, {bad_pb} differ")
    check("K: policy_reach and policy_value(detail) are the dict walks'",
          bad_pr == 0 and bad_pv == 0, f"{n_pv} books, {bad_pr} / {bad_pv} differ")


def check_search(label: str, graphs, every) -> None:
    """every(name) -> whether that graph runs every flag set"""
    bad, n, t0 = [], 0, time.time()
    for name, g, budgets in graphs:
        order, _cut = bc.topo_up(g)
        for fname, kw in flag_sets(g, order, every(name)):
            for B in budgets:
                a, ia = bc.price_search(g, B, log=QUIET, engine="legacy", **kw)
                b, ib = bc.price_search(g, B, log=QUIET, engine="compiled", **kw)
                n += 1
                d = diff_keys(a, b) + [f"info.{k}" for k in
                                       diff_keys(strip_seconds(ia), strip_seconds(ib))]
                if d:
                    bad.append(f"{name} B={B} {fname}: {sorted(d)}")
    check(f"S: price_search compiled == legacy on {label}", not bad,
          f"{n} searches in {time.time() - t0:.0f}s, {len(bad)} differ"
          + (f"; first: {bad[0]}" if bad else ""))


def check_polish(graphs) -> None:
    rng = random.Random(31)
    bad = n = 0
    for _name, g, budgets in graphs:
        order, cut = bc.topo_up(g)
        cg = compile_graph(g, order, cut)
        fixed = {h: rng.choice(n.cands)[0] for h, n in g.our.items() if n.cands}
        for B in budgets:
            start = bc.price_search(g, B, log=QUIET)[0]["booked"]
            for st, fp in ((start, None), ({}, None), ({}, fixed)):
                ma, sa = bc.polish_book(g, st, B, order, fixed_policy=fp)
                mb, sb = bc.polish_book(g, st, B, order, fixed_policy=fp, compiled=cg)
                n += 1
                sa.pop("seconds")
                sb.pop("seconds")
                bad += not (same(ma, mb) and same(sa, sb))
    check("P: polish_book (compiled) is the dict polish: book, value, every stat", bad == 0,
          f"{n} polishes, {bad} differ")


def check_sticky(graphs) -> None:
    """The polish-start merge: under a stick margin polish_book runs the region evaluator
    whatever the engine (the compiled path carries no churn term), so price_search with
    polish, polish_from and a stick must be the same on both engines; a margin of 0 is
    the unsticky search on the compiled path too."""
    rng = random.Random(41)
    bad, n = [], 0
    kw = dict(rounds=1, select="best", reprice="max", dag_mode="polish")
    for name, g, budgets in graphs:
        inc = {h: rng.choice(nd.cands)[0].rstrip("+#") for h, nd in g.our.items()
               if nd.cands and rng.random() < 0.7}
        for B in budgets:
            plain, ip = bc.price_search(g, B, log=QUIET, engine="compiled", **kw)
            g.meta["stick"] = (inc, 0.0)
            zero, iz = bc.price_search(g, B, log=QUIET, engine="compiled", **kw)
            d = diff_keys(plain, zero) + diff_keys(strip_seconds(ip), strip_seconds(iz))
            g.meta["stick"] = (inc, 0.01)
            frm = [plain["booked"]]
            a, ia = bc.price_search(g, B, log=QUIET, engine="legacy", polish_from=frm, **kw)
            b, ib = bc.price_search(g, B, log=QUIET, engine="compiled", polish_from=frm, **kw)
            g.meta.pop("stick")
            n += 1
            d += diff_keys(a, b) + [f"info.{k}" for k in
                                    diff_keys(strip_seconds(ia), strip_seconds(ib))]
            if d:
                bad.append(f"{name} B={B}: {sorted(d)}")
    check("St: sticky price_search + polish (+ polish_from) compiled == legacy; margin 0 "
          "== no stick", not bad, f"{n} instances, {len(bad)} differ"
          + (f"; first: {bad[0]}" if bad else ""))


def check_fallback() -> None:
    g = tprice.graph_d()
    g.our[2].cands.append(("ghost", 999, True, 0.0))      # in subgraph, but no node 999
    order, cut = bc.topo_up(g)
    cg = compile_graph(g, order, cut)
    errs = []
    for kw in ({}, {"compiled": cg}):
        try:
            bc.price_book(g, 0.01, order, **kw)
            errs.append(None)
        except KeyError as e:
            errs.append(repr(e))
    moves = {2: "ghost", 3: "b", 6: "x"}
    check("F: a dangling child compiles ok=False; the calls are the dict code's (same "
          "result, same KeyError)", not cg.ok and errs[0] is not None and errs[0] == errs[1]
          and same(bc.policy_reach(g, moves, order), bc.policy_reach(g, moves, order,
                                                                     compiled=cg)),
          f"ok={cg.ok}, errors {errs}")


def check_gc() -> None:
    g = tprice.graph_d()
    was = gc.isenabled()
    gc.enable()
    bc.price_search(g, 3, log=QUIET)
    on_after = gc.isenabled()
    try:
        bc.price_search(g, 3, log=QUIET, dag_mode="nonsense")
    except ValueError:
        pass
    on_after_raise = gc.isenabled()
    gc.disable()
    bc.price_search(g, 3, log=QUIET)
    off_after = not gc.isenabled()
    if was:
        gc.enable()
    check("G: price_search restores the garbage collector's state (also after raising)",
          on_after and on_after_raise and off_after)


# ── scaling (wall-clock and peak memory) ────────────────────────────────────

def peak_gb() -> float:
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6   # KiB on Linux
    except ImportError:                                                    # Windows
        import psutil
        return psutil.Process().memory_info().peak_wset / 1e9


def bench(engine: str, n: int, targets: list[int], rounds: int) -> None:
    """One engine on one big_dag graph, every target: prints a JSON line per target."""
    g = tdag.big_dag(random.Random(n), n)
    order, _cut = bc._topo_up_dict(g)
    seed = tdag.source_seed(g, order)
    del order
    base = peak_gb()
    for B in targets:
        t0 = time.time()
        res, info = bc.price_search(g, B, log=QUIET, rounds=rounds, select="best",
                                    reprice="max", seed_reach=seed, dag_mode="polish",
                                    engine=engine)
        dt = time.time() - t0
        fp = hashlib.sha1(json.dumps([
            list(res["booked"].items()), res["root_value_realized"].hex(),
            [(s["lam"].hex(), s["booked"], s["value"].hex(), s["round"]) for s in info["sweep"]],
            res["visited"], [v.hex() for v in res["realized"].values()],
            strip_seconds(info).get("polish")]).encode()).hexdigest()[:12]
        print(json.dumps({"engine": engine, "n": len(g.our) + len(g.opp), "B": B, "s": dt,
                          "timing": info["timing"], "solves": len(info["sweep"]),
                          "size": res["spent_distinct"], "value": res["root_value_realized"],
                          "fp": fp, "base_gb": base, "peak_gb": peak_gb()}), flush=True)


def scale(sizes: list[int], targets: list[int], rounds: int) -> None:
    print(f"  production polish flags (rounds {rounds}, select best, reprice max, source seed)")
    print(f"  {'nodes':>9}{'target':>7}{'legacy s':>10}{'compiled s':>11}{'x':>6}"
          f"{'solves':>7}  same  {'peak GB legacy / compiled (graph alone)':>40}")
    for n in sizes:
        rows = {}
        for eng in ("legacy", "compiled"):
            out = subprocess.run([sys.executable, __file__, "--bench", eng, str(n),
                                  ",".join(map(str, targets)), str(rounds)],
                                 capture_output=True, text=True, check=True).stdout
            for line in out.splitlines():
                r = json.loads(line)
                rows.setdefault(r["B"], {})[eng] = r
        for B in targets:
            a, b = rows[B]["legacy"], rows[B]["compiled"]
            print(f"  {a['n']:>9,}{B:>7,}{a['s']:>10.1f}{b['s']:>11.1f}{a['s'] / b['s']:>6.1f}"
                  f"{a['solves']:>7}  {'yes' if a['fp'] == b['fp'] else 'NO':<4}  "
                  f"{a['peak_gb']:>12.2f} / {b['peak_gb']:.2f} ({a['base_gb']:.2f})", flush=True)
            print(f"  {'':>16}compiled split: order+compile {b['timing']['order_s']:.1f}s, "
                  f"solves {b['timing']['solves_s']:.1f}s, polish {b['timing']['dag_s']:.1f}s;"
                  f" legacy: order {a['timing']['order_s']:.1f}s, solves "
                  f"{a['timing']['solves_s']:.1f}s, polish {a['timing']['dag_s']:.1f}s")


def main() -> int:
    if "--bench" in sys.argv:
        i = sys.argv.index("--bench")
        bench(sys.argv[i + 1], int(sys.argv[i + 2]),
              [int(x) for x in sys.argv[i + 3].split(",")], int(sys.argv[i + 4]))
        return 0
    if "--scale" in sys.argv:
        i = sys.argv.index("--scale")
        rounds = int(sys.argv[sys.argv.index("--rounds") + 1]) if "--rounds" in sys.argv else 1
        targets = ([int(x) for x in sys.argv[i + 2].split(",")]
                   if len(sys.argv) > i + 2 and not sys.argv[i + 2].startswith("--")
                   else [400, 2000])
        scale([int(x) for x in sys.argv[i + 1].split(",")], targets, rounds)
        return 0
    full = "--full" in sys.argv
    budget_compiled.CHECK_ADVANCE = True
    dags, prices = dag_graphs(), price_graphs()
    check_topo(dags + prices)
    check_kernels(dags + prices)
    check_search("_test_budget_dag.py's graphs", dags,
                 lambda name: full or not name.startswith("report")
                 or int(name[len("report"):]) < 60)
    check_search("_test_budget_price.py's graphs", prices, lambda name: True)
    check_polish([x for x in dags if x[0].startswith("NP")] + prices)
    check_sticky([x for x in dags if not x[0].startswith("report")
                  or int(x[0][len("report"):]) < 60] + prices)
    check_fallback()
    check_gc()
    print("\nPASS" if FAIL == 0 else "\nFAIL")
    return FAIL


if __name__ == "__main__":
    sys.exit(main())
