# Opponent-policy sensitivity after Maia integration

Freeze before generating new paths. The current shared-position opponent prior
has known sparse-position weaknesses, so absolute early-advantage predictions
may be inflated. Test a pretrained Maia-2 opponent as a sensitivity scenario,
not as a newly validated model of the user's actual opposition.

Keep the user's fallback at 1400, conditioned on opponent rating 2000. On
opponent turns only, use Maia self rating 2000 conditioned on player rating
2000. The latter is an explicit scenario, not an inferred personal rating:
the opponent's view of player strength must not inherit the deliberately low
1400 fallback preference. Expose both inputs separately in the adapter.
Use Blitz/Rapid checkpoints as before; Classical remains Rapid transfer.

Reuse the exact three two-card books, 85% recall, horizon 12, +/-300 cp first-hit
objective, .99 discount, ending/repetition semantics, gates and stratified
sampling machinery. The change is opponent move probabilities only. Preserve
the baseline seed and common uniforms for a matched scenario comparison, but
recompute exact prefix weights under the new opponent. Do not reuse the old
weights or describe the samples as fresh held-out human evidence.

Use 32 pairs in each of four strata, three books and two time-control models:
768 paths. First four indices per stratum form the deeper subset. Retain
caps of 6,000 base/1,000 deeper positions, 2,000 prefixes, 360 sampling seconds,
900 engine seconds, one thread, Hash 32 and no Syzygy. Engine limits remain
100k nodes/1 second and 400k/2 seconds. Reuse compatible fingerprinted records.
No outcome-dependent expansion, discarded cases or candidate replacement.

Report the four within-scenario paired book contrasts with the existing
Bonferroni-weighted Hoeffding bounds. Compare old/new absolute utility and
book effects descriptively; their branch weights differ, so an ordinary
equal-weight paired SE is not valid for cross-scenario differences. Do not
claim statistically resolved model differences from that descriptive table.
Check unchanged White probabilities on common positions, correct rating/color
conditioning, full legal mass, exact prefix weights, replay and engine coverage.

Interpretation: stability supports the robustness of the bounded result;
large changes identify opponent-model dependence. Neither outcome substitutes
for held-out opponent validation, measured personal recall or a broad repertoire
optimisation. No canonical artifacts or original-checkout processes are changed.
