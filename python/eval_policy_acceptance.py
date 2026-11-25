"""Selection-only White review through native scoring, serialization and lookup.

Uses a small, explicitly non-publishable slice of the frozen source panel. This
does not replace the real-bucket materialization or paired Black book pilot.
"""
from pathlib import Path
import argparse
import json
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import book_builder_rs as native
import chessdb_policy_validation as ref
import eval_white_audit as audit
import eval_policy_overlay as policy
from eval_lookup128 import ROW, SCHEMA, OVERLAY_SCHEMA, POLICY, WideEvalLookup
from month_archive import read_json, sha, write_json


def review(panel, cases, references, out):
    panel, cases, references, out = map(Path, (panel, cases, references, out))
    m = audit.checked(references)
    if m['sources'] != {str(cases.resolve()): sha(cases)}:
        raise ValueError('reference run covers a different review')
    results = ref.load_results(references, m)
    d = pd.read_parquet(panel)
    c = pd.read_csv(cases, dtype={'id': str})
    q = c[['id']].merge(d, on='id', validate='one_to_one')
    if len(q) != len(c) or not q.fishnet_tier.eq('nnue').all():
        raise ValueError('review/panel coverage or eligibility mismatch')
    prep = read_json(cases.with_suffix('.json'))
    if sha(panel) != prep['input_sha256'] or sha(cases) != prep['output_sha256']:
        raise ValueError('frozen review input changed')
    if out.exists():
        raise ValueError('use a new diagnostic output directory')
    out.mkdir(parents=True)
    base, overlay, inputs = (out/n for n in ('base', 'overlay', 'inputs'))
    for p in (base, overlay, inputs): p.mkdir()
    bm, om = {}, {}
    for b, group in q.groupby('bucket'):
        name = f'bkt{int(b):03d}.npy'
        g = group.sort_values(['lo', 'hi'])
        rows = np.empty(len(g), dtype=ROW)
        rows['lo'], rows['hi'] = g.lo.to_numpy(np.int64), g.hi.to_numpy(np.int64)
        if not np.equal(g.current, np.rint(g.current)).all():
            raise ValueError('unexpected fractional pre-change source')
        rows['cp'] = g.current
        rows['source'] = np.where(g.source.eq('cloud'), 1, 2)
        np.save(base/name, rows)
        bm[name] = dict(rows=len(rows), sha256=sha(base/name))
        db = inputs/f'db{b}.parquet'
        menu = inputs/f'menu{b}.parquet'
        frame = g[['lo', 'hi', 'cloud_cp', 'cloud_mate', 'fishnet_cp', 'fishnet_mate', 'fishnet_tier']]
        frame = frame.rename(columns={'lo': 'position_hash', 'hi': 'position_hash_hi'})
        frame = frame.assign(hash_ambiguous=False)
        frame.to_parquet(db, index=False)
        # Older panel records retain normalized White ChessDB CP rather than
        # raw best_score. Reconstruct its equivalent STM CP, without changing it.
        best = g.chessdb.to_numpy(float)*np.where(g.white_to_move, 1, -1)
        pq.write_table(pa.table(dict(lo=rows['lo'], hi=rows['hi'],
            best_score=best, white_to_move=g.white_to_move.to_numpy(bool), found=[True]*len(g))), menu)
        counts = policy.make_bucket(dict(base=base/name, db=db, menus=menu), overlay/name, int(b))
        if counts['ensemble'] != len(g):
            raise ValueError('review case did not use the native three-source path')
        om[name] = dict(rows=counts['rows'], sha256=sha(overlay/name))
    write_json(base/'lookup.json', dict(schema=SCHEMA, buckets=bm, scope='selection-slice-only'))
    write_json(overlay/'pilot.json', dict(schema=OVERLAY_SCHEMA, policy=POLICY, complete=False,
        base=dict(root=str(base.resolve()), sha256=sha(base/'lookup.json')), buckets=om,
        scope='selection-slice-only', producer=policy.producer()))
    lookup = WideEvalLookup(overlay, allow_partial=True)
    cp, src = lookup.lookup(q.lo.to_numpy(np.int64), q.hi.to_numpy(np.int64))
    scores = native.cp_to_expected_score(np.ascontiguousarray(cp))
    native_by_id = dict(zip(q.id, scores))
    c['native_score'] = c.id.map(native_by_id)
    c['native_parity_error'] = abs(c.native_score-c.candidate_score)
    if c.native_parity_error.max() > 1e-12 or not (src == 6).all():
        raise ValueError('serialized native recipe differs from frozen policy')
    c['reference_upgraded'] = c.id.map(lambda i: bool(results.get(i, {}).get('qualified')))
    c['review_cp'] = c.apply(lambda r: results[r.id]['cp_white'] if r.reference_upgraded else r.reference_cp, axis=1)
    c['review_depth'] = c.apply(lambda r: results[r.id]['depth'] if r.reference_upgraded else r.reference_actual_depth, axis=1)
    c['review_score'] = ref.score(c.review_cp)
    c['review_admits'] = c.review_score.ge(c.white_floor)
    c['native_admits'] = c.native_score.ge(c.white_floor)
    c['native_false_admission'] = c.native_admits & ~c.review_admits
    c['control_false_admission'] = c.control_admits & ~c.review_admits
    c['reference_admission_changed'] = c.review_admits != c.reference_admits
    c['near_review_floor'] = abs(c.review_score-c.white_floor) <= .002
    c['rounded_score'] = c.id.map(dict(zip(q.id, ref.score(np.rint(cp)))))
    c['integer_rounding_changes_gate'] = c.rounded_score.ge(c.white_floor) != c.native_admits
    c.to_csv(out/'case-results.csv', index=False)
    counts = []
    for gate, g in c.groupby('gate'):
        counts.append(dict(gate=gate, reviewed=len(g), reference_admission_changes=int(g.reference_admission_changed.sum()),
            candidate_false_admissions=int(g.native_false_admission.sum()),
            candidate_clear_false_admissions=int((g.native_false_admission & ~g.near_review_floor).sum()),
            control_false_admissions=int(g.control_false_admission.sum()),
            candidate_only_false_admissions=int((g.native_false_admission & ~g.control_admits).sum())))
    report = dict(schema='eval-white-production-path-review-v1', selection_only=True,
        inputs=dict(panel=dict(path=str(panel.resolve()), sha256=sha(panel)), cases=dict(path=str(cases.resolve()),sha256=sha(cases)),
                    references=dict(path=str(references.resolve()), identity=ref.digest(m))),
        producer=policy.producer(), review_script_sha256=sha(Path(__file__)), reviewed=len(c),
        stronger_requested=len(m['tasks']), stronger_qualified=sum(r['qualified'] for r in results.values()),
        all_review_references_depth_at_least_28=bool(c.review_depth.ge(28).all()),
        max_native_parity_error=float(c.native_parity_error.max()),
        max_integer_rounding_score_error=float(abs(c.native_score-c.rounded_score).max()),
        integer_rounding_gate_changes=int(c.integer_rounding_changes_gate.sum()), counts=counts,
        results_sha256=sha(out/'case-results.csv'), production_binding_changed=False,
        interpretation='Targeted admission errors remain diagnostic: neither random nor reach-weighted. Control is the frozen research proposal, not the step-1 Black pilot control. Relative gates and actual book effects require the paired pilot.')
    write_json(out/'report.json', report)
    return report


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description=__doc__)
    for n in ('panel', 'cases', 'references', 'out'): ap.add_argument('--'+n, type=Path, required=True)
    a = ap.parse_args()
    print(json.dumps(review(a.panel, a.cases, a.references, a.out), indent=2))
