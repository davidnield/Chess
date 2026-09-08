"""Bounded real-checkpoint Maia-2 integration verification and diagnostic export."""
from collections import Counter
import importlib.metadata
import json
import math
from pathlib import Path
import time

import chess

from .io import ROOT, output_path, sha256, write_json
from .maia_policy import CONFIG, MaiaPolicy


def main():
    import torch
    from maia2 import inference, utils
    torch.set_num_interop_threads(1)
    config = json.loads(CONFIG.read_text())
    if torch.__version__ != config['torch_version']:
        raise ValueError('unexpected PyTorch build')
    start = time.monotonic(); policy = MaiaPolicy(config)
    for event in ('Blitz', 'Rapid'):
        policy.load(event)
    # Full probabilities should agree with the upstream four-decimal display,
    # and mirroring the board must preserve the side-to-move interpretation.
    fixtures = [chess.Board(), chess.Board('r3k2r/1P6/8/3pP3/8/8/8/R3K2R w KQkq d6 0 1')]
    checks = 0; max_round_difference = 0
    for event in ('Blitz', 'Rapid'):
        for board in fixtures:
            original = board.fen()
            ps = policy.probabilities(board, event, 2100)
            mirrored = policy.probabilities(board.mirror(), event, 2100)
            display, _ = inference.inference_each(policy.load(event), policy.prepared, board.fen(), 1400, 2100)
            for move, probability in ps.items():
                delta = abs(probability-display[move]); max_round_difference = max(max_round_difference, delta)
                assert delta <= .000051
                assert abs(probability-mirrored[utils.mirror_move(move)]) < 1e-12
                checks += 1
            assert board.fen() == original
    assert utils.map_to_category(1400, policy.prepared[1]) == policy.prepared[1]['1400-1499']
    for fen in ('7k/5Q2/6K1/8/8/8/8/8 b - - 0 1', '7k/6Q1/6K1/8/8/8/8/8 b - - 0 1', '7k/8/8/8/8/8/8/K7 w - - 0 1'):
        assert policy.probabilities(chess.Board(fen), 'Rapid', 2100) == {}
    cases_path = output_path('m8/calibration/cases.json')
    manifest = json.loads(output_path('m8/calibration/case_manifest.json').read_text())
    assert sha256(cases_path) == manifest['snapshots']['m8/calibration/cases.json']
    cases = json.loads(cases_path.read_text())['candidate']
    assert len(cases) == 96
    rows = []; per_event = {}; started_inference = time.monotonic()
    for case in cases:
        if time.monotonic()-start > 180:
            raise TimeoutError('bounded Maia smoke runtime exceeded')
        board = chess.Board(case['fen']); opponent = case['black_elo'] if board.turn else case['white_elo']
        ps = policy.probabilities(board, case['event'], opponent)
        human = board.parse_san(case['human_san']).uci()
        entropy = -math.fsum(p*math.log(p) for p in ps.values())
        row = {'case_id': case['case_id'], 'event': case['event'], 'support': case['support'],
            'fen': case['fen'], 'self_rating': 1400, 'opponent_rating': opponent,
            'opponent_bucket': utils.map_to_category(opponent, policy.prepared[1]), 'human_uci': human,
            'human_move_probability': ps[human], 'entropy_nats': entropy, 'probabilities_uci': ps}
        rows.append(row)
    for event in ('Blitz', 'Rapid', 'Classical'):
        subset = [r for r in rows if r['event'] == event]
        per_event[event] = {'cases': len(subset), 'mean_log_loss_against_actual_1800plus_moves': math.fsum(-math.log(r['human_move_probability']) for r in subset)/len(subset),
            'mean_entropy_nats': math.fsum(r['entropy_nats'] for r in subset)/len(subset)}
    package = Path(inference.__file__).parent
    installed = {d.metadata['Name']: d.version for d in importlib.metadata.distributions()}
    result = {'status': 'passed', 'config': config, 'config_sha256': sha256(CONFIG),
        'adapter_sha256': sha256(ROOT/'research/human_repertoire/maia_policy.py'), 'smoke_sha256': sha256(__file__),
        'packages': installed, 'upstream_source_sha256': {n: sha256(package/n) for n in ('model.py', 'inference.py', 'main.py', 'utils.py', 'train.py')},
        'checkpoints': {kind: {'sha256': sha256(output_path('maia2/models/'+kind+'_model.pt')),
            'bytes': output_path('maia2/models/'+kind+'_model.pt').stat().st_size,
            'parameters': sum(p.numel() for p in policy.models[kind].parameters())} for kind in ('blitz', 'rapid')},
        'mirror_and_upstream_probability_checks': checks, 'max_upstream_round_difference': max_round_difference,
        'terminal_fixtures': 3, 'diagnostic_cases': len(rows), 'diagnostic_seconds': time.monotonic()-started_inference,
        'total_seconds': time.monotonic()-start, 'diagnostic_source_sha256': sha256(cases_path),
        'diagnostic_rows_sha256': sha256(write_json('maia2/diagnostic_probabilities.json', rows)),
        'by_event': per_event,
        'interpretation': 'Retrospective integration diagnostic. Maia self rating 1400 is a user-chosen fallback assumption, not a fit to >=1800 human behavior or measured intuitiveness.'}
    write_json('maia2/integration_verification.json', result)
    print(json.dumps({k: result[k] for k in ('status', 'mirror_and_upstream_probability_checks', 'diagnostic_cases', 'diagnostic_seconds', 'total_seconds', 'by_event')}, indent=2))


if __name__ == '__main__':
    main()
