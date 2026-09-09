# Prospective branch-stratified Maia comparison

Keep the exact three packages, 85% recall, Maia-1400/2000 conditioning,
shared-position opponent policy, full twelve-ply utility and soundness gates
of the equal-budget comparison. Change only sampling machinery. Candidate
prescriptions remain fixed, including the permissive but suboptimal-looking
Nc3 after ...Nf6. Do not replace a candidate after inspecting new outcomes.

Enumerate the complete distribution of the first two plies, integrating root
recall and any ending hazards. Partition prefixes into d4/exd4, d4/Nf6,
d4/d6 and all remaining prefixes (including early endings). Sum their exact
model probabilities; independently sample 32 continuations per stratum and
package, using a new seed and shared uniforms across packages. Total: 768
paths (two policies, four strata, three packages, 32 replicates).

Continuation preserves the full prefix history, original horizon, first-hit
times, repetition and book re-entry. Weight stratum means by their probabilities.
Do not force all play through d4 or drop other moves/transpositions. Stratum
weights must sum to one. The root card is common to all packages, so prefix
distributions must be identical across packages within each fallback policy.

Estimate paired effects with weighted stratum means and SE from independent
stratum variances. For outcomes in [-1,1], Hoeffding margin is
sqrt(2 log(2/alpha) sum_s(w_s^2/n_s)). Use alpha .0125 for the same four primary
contrasts, giving family-wise 95% coverage conditional on model correctness.
Conditional stratum means are diagnostic; a rare branch is not weighted as
common. No claim that changing the sampler validates the opponent model.

First four replicates in each stratum form the matched deeper subset. Bounds:
6,000 base positions, 1,000 deeper, 2,000 enumerated prefixes per policy,
360 sampling seconds, 900 one-thread engine seconds, Hash 32, no Syzygy.
Base and deeper limits remain 100k nodes/1 second and 400k/2 seconds. No
adaptive expansion or path deletion. Reuse compatible fingerprinted engine
records. Verify independent tiny-book oracle parity, mass conservation and
cross-prefix repetition before sampling; replay all real paths and independently
reconstruct weighted outcomes, gates and complete coverage afterward.
