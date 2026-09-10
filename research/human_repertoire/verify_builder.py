"""Independent path and legal-PGN checks for the bounded builder's final books."""
from dataclasses import replace
import io
import chess
import chess.pgn
from .budget_search import DagScorer
from .exposure_recall import fixed_recall_exposure
from .hashed_board import HashedBoard
from .io import output_path,sha256
from .oracle import path_evaluate
from python.zobrist import zobrist_int64


def verify_pgn(text, book):
    stream=io.StringIO(text); cards={}; marked={}
    while (game:=chess.pgn.read_game(stream)) is not None:
        assert not game.errors,game.errors
        board=HashedBoard(); last=None
        for node in game.mainline():
            key=str(zobrist_int64(board)); san=board.san(node.move)
            if 'STUDY:' in node.comment:
                assert book.get(key)==san,(key,san,'unexpected prescription')
                marked[key]=san
            last=(key,san); board.push(node.move)
        assert last is not None and game.headers['CardPosition']==last[0]
        assert last[0] not in cards,'duplicate card'
        cards[last[0]]=last[1]
    assert cards==book,('card export mismatch',cards,book)
    assert marked==book,('study marks mismatch',marked,book)
    return len(cards)


def verify(builder):
    p=builder.verified(); graphs=builder.graphs(); results=builder.read('results')
    assert results['identity']=={'manifest':sha256(output_path(builder.prefix+'manifest.json')),
        'engine':sha256(output_path(builder.prefix+'engine.json'))}
    pool=builder.read('candidates'); data=builder.read('data'); fens=builder.read('fens')
    policies=builder.read('policies'); aliases=builder.read('aliases'); records=builder.read('engine')['records']
    edges_checked=0; cases=0; pgn_hashes={}
    for event,g in graphs.items():
        g.validate()
        for key,n in g.nodes.items():
            if n.side not in ('us','opp'): continue
            board=HashedBoard(fens[key]); assert str(zobrist_int64(board))==key
            rows=data['stats'][key]; aux=data['aux'][key]
            continuation=sum(r['total'] for r in rows)+aux['other_total']+aux['horizon_total']
            ends=sum(aux[k+'_total'] for k in ['term_normal','term_flag','term_other'])
            assert abs(n.end_probability-ends/(continuation+ends))<1e-12
            probs={e.move:e.probability for e in n.edges}
            if n.side=='us':
                ps=policies[event][key]
                assert set(ps)=={m.uci() for m in board.legal_moves}
                assert probs=={board.san(chess.Move.from_uci(m)):v for m,v in ps.items()}
            else:
                assert set(probs)=={r['move_san'] for r in rows}|({'__unresolved__'} if aux['other_total']+aux['horizon_total'] else set())
                for r in rows: assert abs(probs[r['move_san']]-r['total']/continuation)<1e-12
                assert abs(probs.get('__unresolved__',0)-(aux['other_total']+aux['horizon_total'])/continuation)<1e-12
            for edge in n.edges:
                if edge.move=='__unresolved__': continue
                child=board.copy(); child.push_san(edge.move)
                assert str(zobrist_int64(child))==aliases.get(edge.child,edge.child)
                edges_checked+=1
        scorer=DagScorer(g,builder.cfg,builder.c['entry_probability'],results['anchors_used'],builder.root_ply)
        for budget,methods in results['results'].items():
            for method,row in methods.items():
                book=row['book']; context=row['contexts'][event]
                assert len(book)==row['cost']<=int(budget)
                if method=='reference':
                    assert book=={k:pool['reference'][k] for k in pool['reference_order'][:int(budget)]}
                for key,move in book.items():
                    assert move in pool['candidates'][key]
                    board=HashedBoard(fens[key]); board.push_san(move); child=str(zobrist_int64(board))
                    assert key in records and child in records
                    assert records[child]['cp']>=builder.cfg.soundness_floor_cp
                    assert records[key]['cp']-records[child]['cp']<=builder.cfg.max_engine_loss_cp
                exposure=fixed_recall_exposure(g,book,builder.cfg,builder.c['entry_probability'],context['recall'])
                for key in book: assert abs(exposure[key]-context['frequencies'][key])<1e-11
                metrics=path_evaluate(g,book,replace(builder.cfg,recall_by_position=context['recall'])).report()
                for name,value in metrics.items():
                    if value is not None: assert abs(value-context['conditional_metrics'][name])<1e-9,(event,budget,method,name)
                assert abs(metrics['advantage']+metrics['disadvantage']+metrics['no_hit']-1)<1e-9
                assert abs(metrics['win']+metrics['draw']+metrics['loss']-1)<1e-9
                score=scorer.score(book)
                assert abs(score['score']-metrics['certain_early_advantage'])<1e-10
                assert abs(score['unresolved']-metrics['unresolved_first_hit_mass'])<1e-10
                if method=='perfect_selection':
                    perfect=path_evaluate(g,book,replace(builder.cfg,recall=1,recall_by_position={})).report()
                    assert abs(scorer.score(book,True)['score']-perfect['certain_early_advantage'])<1e-10
                name=f'{method}_{budget}.pgn'; path=output_path(builder.prefix+name)
                verify_pgn(path.read_text(encoding='utf-8'),book); pgn_hashes[name]=sha256(path)
                cases+=1
                print('verified',event,budget,method,flush=True)
    out={'status':'passed','scenario_books':cases,'legal_edges':edges_checked,'pgn_sha256':pgn_hashes,
         'results_sha256':sha256(output_path(builder.prefix+'results.json')),'verifier_sha256':sha256(__file__)}
    builder.save('verification',out); print(out,flush=True)

