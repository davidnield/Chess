"""Publish the verified bounded builder's compact report and inspectable PGNs.

python -B -m research.human_repertoire.builder_report [--config path]
Writes tracked deliverables only below docs/human_repertoire/builder_books/ and
builder_report.md. Numerical snapshots remain in the isolated artifact folder.
"""
import argparse
from collections import Counter
import json
import shutil
import chess.pgn
from .context_recall import solve
from .io import ROOT,output_path,sha256
from .repertoire_builder import Builder
from .nested_pgn import publish as publish_nested


def cutoff_masses(graph,book,recall,horizon,reasons):
    """Probability ending at each explicit frontier category, including horizon."""
    states={graph.root:1.}; cuts=Counter()
    for ply in range(horizon+1):
        following=Counter()
        for k,mass in states.items():
            node=graph.nodes[k]
            if node.side=='frontier': cuts[reasons.get(k,'unspecified')]+=mass; continue
            if node.side not in ('us','opp') or ply==horizon: continue
            r=recall.get(k,0) if node.side=='us' else 0
            for edge in node.edges:
                following[edge.child]+=mass*(1-node.end_probability)*((1-r)*edge.probability+r*(book.get(k)==edge.move))
        states=following
    return dict(cuts)


def line_san(moves):
    game=chess.pgn.Game(); node=game; board=game.board()
    for san in moves:
        move=board.parse_san(san); node=node.add_variation(move); board.push(move)
    return game.accept(chess.pgn.StringExporter(headers=False,variations=False,comments=False)).removesuffix(' *')


def report(b):
    manifest=b.verified(); result=b.read('results'); verification=b.read('verification')
    assert verification['status']=='passed'
    assert verification['results_sha256']==sha256(output_path(b.prefix+'results.json'))
    pool=b.read('candidates'); paths=b.read('paths'); graphs=b.graphs(); engine=b.read('engine'); reasons=b.read('reasons')
    target=ROOT/'docs/human_repertoire/builder_books'; target.mkdir(exist_ok=True)
    for name,digest in verification['pgn_sha256'].items():
        source=output_path(b.prefix+name); assert sha256(source)==digest
        shutil.copyfile(source,target/name)
    best_available={}
    for budget,methods in result['results'].items():
        method,row=max(methods.items(),key=lambda pair:(round(pair[1]['mean_human_score'],12),-pair[1]['cost']))
        name=f'best_available_{budget}.pgn'
        shutil.copyfile(target/f'{method}_{budget}.pgn',target/name)
        best_available[budget]={'method':method,'cost':row['cost'],'pgn':name,'sha256':sha256(target/name)}
    publish_nested(target)
    def mean(row,metric):
        return sum(c['conditional_metrics'][metric] for c in row['contexts'].values())/len(row['contexts'])
    empty={'contexts':{e:solve(g,{},b.cfg,b.c['entry_probability'],anchors=result['anchors_used'],root_ply=b.root_ply) for e,g in graphs.items()}}
    root_line=line_san(b.c['root_moves']); end_ply=b.root_ply+b.cfg.horizon
    end_side='Black' if end_ply%2==0 else 'White'; end_move=(end_ply+1)//2
    budget_text=', '.join(str(v) for v in sorted(b.c['budgets']))
    number_books=sum(len(v) for v in result['results'].values())
    cross_winners=[(budget,row['method']) for budget,row in best_available.items() if row['method']!='human']
    search_note=('A useful search limitation appeared: '+', '.join(f'at budget {budget}, {method} found the highest-scoring book under the common human evaluator' for budget,method in cross_winners)+
        '. The best-available exports use those books. Different search paths can reach different local solutions; these comparisons do not isolate recall assumptions as the sole cause of score differences.' if cross_winners else
        'Human selection provided the best-available books in this run. Different search paths can still reach different local solutions; these comparisons do not isolate recall assumptions as the sole cause of score differences.')
    lines=['# Bounded multi-branch repertoire builder', '',
        'The reusable selection loop is working: it generates sound candidate moves, searches connected preparation packages under a position budget, recomputes encounter frequency and recall, compares all books with the same evaluator, and exports annotated legal PGNs.', '',
        f'This run produces {number_books} provisional White books after **{root_line}**, across budgets **{budget_text}**. They are research outputs, not a validated replacement for the existing repertoire. The lookahead ends after move {end_move} by {end_side}; unexpanded positions remain explicitly unresolved.', '',
        'To inspect the highest-scoring book found at each budget under the common human evaluator: '+', '.join(f'[budget {budget}: {row["cost"]} cards](builder_books/{row["pgn"]})' for budget,row in sorted(best_available.items(),key=lambda x:int(x[0])))+'. These copies preserve the originating method in their PGN headers; no search results were changed after comparison.', '',
        '**Nested PGNs for study import:** '+', '.join(f'[{row["cost"]} positions](builder_books/nested/{row["pgn"]})' for budget,row in sorted(best_available.items(),key=lambda x:int(x[0])))+'. Each file contains one game with nested variations and preserved STUDY comments. Original per-card exports remain available above.', '',
        '## Books and common evaluation', '',
        f'Each percentage is conditional on reaching the starting position after {root_line}. The score is discounted probability of reaching +300 cp before -300 cp within the horizon, on paths with enough known evaluation. The interval adds unresolved first-hit mass to that lower bound; it is a conservative model-coverage bound, **not a statistical confidence interval or win rate**. Scores average Blitz and Rapid equally.', '',
        '| Budget | Selection method / PGN | Actual positions | Known early advantage | Coverage bound | Observed first disadvantage |',
        '|---:|---|---:|---:|---:|---:|']
    summaries={}
    for budget,methods in sorted(result['results'].items(),key=lambda x:int(x[0])):
        summaries[budget]={}
        for method in ['reference','perfect_selection','human']:
            row=methods[method]; lo=mean(row,'certain_early_advantage'); hi=min(1.,lo+mean(row,'unresolved_first_hit_mass'))
            label={'reference':'Existing-reference projection','perfect_selection':'Perfect-recall selection','human':'Human-recall selection'}[method]
            lines.append(f'| {budget} | [{label}](builder_books/{method}_{budget}.pgn) | {row["cost"]} | {lo:.3%} | {lo:.2%}–{hi:.2%} | {mean(row,"disadvantage"):.3%} |')
            summaries[budget][method]={'cost':row['cost'],'lower':lo,'upper':hi,'disadvantage':mean(row,'disadvantage'),
                'cutoff_mass':mean(row,'frontier_before_horizon_mass'),'unknown_eval_mass':mean(row,'unknown_eval_mass'),
                'branches_by_representative_route':dict(Counter(paths[k][0] for k in row['book'])),
                'book':row['book'],'contexts':row['contexts'],
                'cutoffs':{e:cutoff_masses(g,row['book'],row['contexts'][e]['recall'],b.cfg.horizon,reasons) for e,g in graphs.items()}}
            for event,cuts in summaries[budget][method]['cutoffs'].items():
                assert abs(sum(cuts.values())-row['contexts'][event]['conditional_metrics']['frontier_mass'])<1e-9
    emptylo=mean(empty,'certain_early_advantage'); emptyhi=min(1.,emptylo+mean(empty,'unresolved_first_hit_mass'))
    first=str(min(b.c['budgets'])); last=str(max(b.c['budgets']))
    small=summaries[first]['human']; large=summaries[last]['human']
    lines+=['',f'With no studied cards, the same model gives {emptylo:.3%} known early advantage and a {emptylo:.2%}–{emptyhi:.2%} coverage bound. Adding cards is optional; an allowed budget is a ceiling, not a target to fill. Observed disadvantage also has missing-path uncertainty and is not a certified risk bound.', '',
        f'The human-selected score moves from {small["lower"]:.3%} with {small["cost"]} cards to {large["lower"]:.3%} with {large["cost"]} cards. Compare this small covered-mass difference with the much wider coverage bounds before interpreting the additional study cost. The run demonstrates automatic allocation across branches; it does not resolve a whole-book ranking.', '',
        search_note, '',
        'The reference retains canonical move choices, ordered by baseline fallback encounter frequency to fit each budget. This adaptation is not a fresh run of the original unrestricted optimiser. Its existing learnability settings are preserved in the source metadata; it is not described as a perfect-recall method. Both searches can start from this reference seed. The perfect-recall arm changes **selection** only: every exported book is finally scored using the same human recall model.', '',
        'The optimiser maximises the known early-advantage lower bound, breaking ties by fewer positions. It can substitute moves, add connected follow-ups and remove/replace prescriptions. Search stops at the declared time/evaluation cap or a local plateau; none of these results proves a global optimum. Wider coverage could change the rankings.', '',
        '## Branch allocation and selected moves', '',
        'Counts below use one representative legal route per card. A transposed position still costs one card and can be recognised through other routes.', '',
        '| Budget | Human-book positions by Black first reply | Changes from reference | Human vs perfect-selection prescriptions differing |', '|---:|---|---|---:|']
    for budget,methods in sorted(result['results'].items(),key=lambda x:int(x[0])):
        human=methods['human']['book']; ref=methods['reference']['book']
        branches=summaries[budget]['human']['branches_by_representative_route']
        changed=sum(k in ref and ref[k]!=v for k,v in human.items()); added=len(human.keys()-ref.keys()); removed=len(ref.keys()-human.keys())
        perfect=methods['perfect_selection']['book']; disagreements=sum(human.get(k)!=perfect.get(k) for k in human.keys()|perfect.keys())
        lines.append(f'| {budget} | '+', '.join(f'1...{k}: {v}' for k,v in sorted(branches.items()))+f' | {changed} substituted, {added} added, {removed} omitted | {disagreements} |')
    budget=str(max(b.c['budgets'])); row=result['results'][budget]['human']
    lines+=['',f'The human-selected book with budget {budget} contains the following prescriptions. Full routes are shown to make side and move number unambiguous. Recall percentages are model assumptions applied to candidate-dependent exposure, not measured memory.', '',
        '| Full route through prescribed White move | Predicted recall, Blitz / Rapid |', '|---|---|']
    for k,m in sorted(row['book'].items(),key=lambda kv:(len(paths[kv[0]]),paths[kv[0]],kv[1])):
        recall=' / '.join(f'{100*row["contexts"][e]["recall"][k]:.1f}%' for e in b.c['events'])
        lines.append(f'| {line_san(b.c["root_moves"]+paths[k]+[m])} | {recall} |')
    lines+=['', 'PGNs contain one game per distinct study card. **STUDY** comments mark prescribed moves; unmarked White moves provide a route to the card and are not additional prescriptions. Alternative move orders can therefore appear without adding to the budget.', '',
        '## Scope, evidence and limitations', '',
        f'- Candidate pool: {result["candidate_positions"]} positions / {result["candidate_moves"]} moves after gates, with {result["reference_positions"]} eligible canonical prescriptions. Exclusions: {dict(Counter(x["reason"] for x in pool["excluded"]))}.',
        f'- Graph: {len(next(iter(graphs.values())).nodes):,} positions per time-control policy; {sum(len(n.edges) for n in next(iter(graphs.values())).nodes.values()):,} edges. Preparation used {manifest["new_maia_calls"]} new Maia inferences in its completing session ({manifest["seconds"]:.1f} seconds).',
        f'- Engine gates: {len(engine["records"])} positions audited at {b.c["engine_nodes"]:,} nodes / up to {b.c["engine_position_seconds"]} seconds each. Every candidate requires both parent and child in this audit, child ≥−100 cp, and loss ≤150 cp. This is bounded engine evidence, not a deep soundness certificate.',
        f'- Independent verification passed for {verification["scenario_books"]} time-control/book cases and {verification["legal_edges"]:,} legal graph edges, including empirical mass accounting, path-sum scores, exposure, budgets, candidate gates and PGN reconstruction.',
        '- Opponents use the authoritative pooled mean-Elo≥1800 BRC position counts. This pool is not a both-players≥1800/rating-gap≤200 filtered sample. Blitz and Rapid labels distinguish White fallback models, not separate opponent datasets; Classical uses the declared Rapid transfer and is not an independent third validation arm.',
        f'- White fallback is Maia-2 at 1400, conditioned on a 2000-rated opponent. Per-all-games entry probability {b.c["entry_probability"]:.0%} is an explicit provisional scenario. The familiar e4 e5 Nf3 Nc6 tabiya has strength .99; no universal familiarity boost was assigned to the initial board.',
        '- Recall combines per-game frequency, distance from a declared familiar tabiya and absolute ply. It is recomputed for every proposed book. These settings need personal calibration; the shallow horizon barely exercises the deeper-memory decay.',
        '- Known earlier evaluations are overlaid from the canonical arrays; candidate parents/children use this run\'s fresh engine audit. Mixed engine budgets and incomplete graph expansion limit outcome interpretation. Computational cutoffs are not evidence that empirical opponent data is sparse.',
        '- Horizon W/D/L uses the existing neutral placeholder. These outputs do not establish acceptable full-game performance or causal improvement in playing strength.', '',
        '## Search effort', '', '| Method | Budget | New book evaluations | Seconds | Stop |', '|---|---:|---:|---:|---|']
    for method,rows in result['search'].items():
        for budget,row in sorted(rows.items(),key=lambda x:int(x[0])):
            lines.append(f'| {method} | {budget} | {row["evaluations"]} | {row["seconds"]:.1f} | {row["stopped"]} |')
    largest=str(max(b.c['budgets'])); cutoff_rows=summaries[largest]['human']['cutoffs']
    cutoff_mean={k:sum(r.get(k,0) for r in cutoff_rows.values())/len(cutoff_rows) for k in set().union(*(r.keys() for r in cutoff_rows.values()))}
    lines+=['',f'For the budget-{largest} human book, mean route mass by frontier reason: '+', '.join(f'{k}: {v:.3%}' for k,v in sorted(cutoff_mean.items()))+'. These are route-exit masses, not additional uncertainty to add to the bounds above.']
    lines+=['', '## Reproduction and decisions', '',
        'Configuration and prospective limits: [builder_config.json](builder_config.json), [builder_plan.md](builder_plan.md). Run instructions: [research README](../../research/human_repertoire/README.md). Tracked [results and recall details](builder_books/summary.json) and [source manifest](builder_books/source_manifest.json) preserve the delivered experiment. Larger input snapshots, engine audit and checkpoints live under `research/human_repertoire/_artifacts/builder/'+b.c['run_name']+'/`.', '',
        'Decisions made for this delivery: use a bounded e4 region across multiple replies; use 10/25/50 position ceilings; compare against a canonical projection and a distinct perfect-selection ablation; optimise the common covered-mass objective; retain provisional books at resource caps; and report unresolved coverage instead of opening another single-move investigation. The delivered capability now answers which cards the current model selects across branches, rather than only whether one extra card helps.', '',
        'The main next development constraint is coverage-aware continuation allocation for competing books, especially beyond the fourth move. That work can now operate through this builder and report whole-book tradeoffs; it need not restart another chain of isolated Be3 experiments.', '']
    (ROOT/'docs/human_repertoire/builder_report.md').write_text('\n'.join(lines),encoding='utf-8')
    summary={'config':b.c,'manifest_sha256':sha256(output_path(b.prefix+'manifest.json')),'verification':verification,'best_available':best_available,
        'books':summaries,'empty_lower':emptylo,'empty_upper':emptyhi,'search':result['search']}
    (target/'summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    (target/'source_manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print(json.dumps({'books':{budget:{method:{k:row[k] for k in ['cost','lower','upper','branches_by_representative_route']}
          for method,row in methods.items()} for budget,methods in summaries.items()},'empty':[emptylo,emptyhi]},indent=2))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('--config'); args=parser.parse_args()
    report(Builder(args.config) if args.config else Builder())
