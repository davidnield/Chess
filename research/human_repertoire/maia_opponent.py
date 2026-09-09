"""Opponent-policy sensitivity without changing the Maia-1400 fallback."""
import argparse
import json
import chess
from python.zobrist import zobrist_int64
from . import maia_stratified as baseline, paired_panel as panel
from .board_benchmark import isolated
from .io import ROOT, output_path, sha256
from .maia_policy import MaiaFallbackKernels, model_type, validate_probabilities

PREFIX='maia_opponent/'


class MaiaOpponentKernels:
    name='maia_opponent'

    def __init__(self,policy,event,our_color,opponent_rating,player_rating,cap=6000):
        model_type(event)
        if any(type(r) is not int or not 1000<=r<=3000 for r in (opponent_rating,player_rating)):
            raise ValueError('explicit opponent/player scenario ratings required')
        self.policy,self.event,self.our_color=policy,event,our_color
        self.opponent_rating,self.player_rating=opponent_rating,player_rating
        self.cap,self.cache=cap,{}

    def get(self,key,fen):
        b=chess.Board(fen)
        if b.turn==self.our_color: raise ValueError('opponent kernel called on our turn')
        if str(zobrist_int64(b))!=key: raise ValueError('opponent kernel hash mismatch')
        if b.outcome(claim_draw=False) is not None: raise ValueError('terminal has no opponent kernel')
        if key not in self.cache:
            if len(self.cache)>=self.cap: raise ValueError('opponent kernel cap')
            ps=self.policy.probabilities(b,self.event,self.player_rating,self_rating=self.opponent_rating)
            validate_probabilities(b,ps)
            moves={b.san(chess.Move.from_uci(u)):u for u in ps}
            self.cache[key]=(moves,{self.name:{s:ps[u] for s,u in moves.items()}},fen.split()[:4])
        moves,ps,core=self.cache[key]
        if core!=fen.split()[:4]: raise ValueError('opponent FEN drift')
        return moves,ps


def config():
    return {**baseline.config(),'opponent_policy':'maia2','player_rating_conditioning':2000}


def identity(c):
    return {'config':c,'code_sha256':sha256(__file__),'baseline_identity':baseline.identity(baseline.config()),
        'plan_sha256':sha256(ROOT/'docs/human_repertoire/maia_opponent_plan.md'),
        'baseline_manifest_sha256':sha256(output_path(baseline.PREFIX+'case_manifest.json')),
        'baseline_engine_sha256':sha256(output_path(baseline.PREFIX+'engine_audit.json'))}


def source(c):
    p=baseline.verified(baseline.config()); _,c6,base,records=baseline.source(c); audit=baseline.read('engine_audit')
    if not audit['complete'] or audit['identity']!={'case_manifest_sha256':sha256(output_path(baseline.PREFIX+'case_manifest.json')),'engine':p['engine_identity']}:
        raise ValueError('baseline engine drift')
    for phase,nodes,seconds in [('base',c['engine_nodes'],c['engine_time_seconds']),('deeper',c['deeper_nodes'],c['deeper_time_seconds'])]:
        records[phase].update(panel.checked_records(audit,phase,nodes,seconds))
    return p,c6,base,records


def runner():
    # Reuse the frozen runner's evaluator, sampler and checks in an isolated
    # module namespace. No mutation of the baseline module or source files.
    module=isolated('maia_stratified.py','maia_stratified/',PREFIX)
    c=config()
    def kernels(policy,event,our_color,opponent_rating,unused_kernel,unused_model,cap):
        opponent=MaiaOpponentKernels(policy,event,our_color,opponent_rating,c['player_rating_conditioning'],cap)
        return MaiaFallbackKernels(policy,event,our_color,opponent_rating,opponent,opponent.name,cap)
    module.MaiaFallbackKernels=kernels
    module.identity=identity; module.source=source; module.config=config
    def engine(c):
        child=isolated('calibration_experiment.py','m7/',PREFIX)
        child.verified_cases=lambda unused:module.verified(c)
        child.engine(c)
    module.engine=engine
    return module


def verify():
    module=isolated('verify_maia_stratified.py','maia_stratified/',PREFIX)
    module.run=runner()
    module.main()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('phase',choices=('prepare','engine','compare','verify'))
    args=p.parse_args()
    if args.phase=='verify': verify()
    else: getattr(runner(),args.phase)(config())
