# WARDEN PERMISSION FILE (v1, 2026-10-29)

**What this is:** Warden's autonomy contract — what it can do alone, what needs the lead, what needs the operator. Earned by track record, demoted on failure, reviewed per cycle.

**Source:** EdgeDelta Guardrails five-rung pattern (production monitoring agent, per-domain levels), AWS auto-scaling throttled remediation, k8s liveness-probe backoff, Argo Rollouts analysis-gate/abort. Adapted for a git/CSV estate with one human.

---

## Tiers

| Tier | What Warden does | Earned by | Demoted by |
|---|---|---|---|
| **T0 Observe** (current) | Detect dead sources, prepare evidence packets, build the dual-universe queue, file alarms. Writes only to own state store + the queue artifact. | Starting tier | Any wrong-fix escalation |
| **T1 Recommend** | Opens evidence-backed ticket per dead source with proposed fix + confidence + blast radius. Human applies. | 30 clean detections across all families, zero wrong-fix escalations, over >= 2 weeks | 1 wrong-fix escalation |
| **T2 Propose** | Drafts the fix as a reviewable artifact (config PR, re-enable patch, fetcher uncap PR). Nothing applied. | 50 T1 recommendations with >= 90% lead-acceptance rate | 3 rejected proposals |
| **T3 Act with approval** | Executes known-safe fixes from an approval queue (one-click approve/deny). Pre-conditions -> action -> post-verify -> rollback baked into each runbook. | 30 T2 proposals with 100% post-verify pass | 1 post-verify failure |
| **T4 Autonomous** | Fully auto-fixes a narrow allowlisted class per domain (restart hung fetcher, refresh expired token, uncapp a rate-limited board). | 50 T3 auto-approved fixes with 100% post-verify pass | 1 failed post-verify |

---

## Rules

1. **Per-family levels, not global.** After 30 clean detections in a family -> promote that family's ticket quality. New family starts at T0 regardless of the rest.
2. **Auto-fix only the well-understood, reversible class.** Restart a hung fetcher, refresh an expired token, uncapp a rate-limited board. Config rebuilds, scope changes, and anything touching shared state stay at <= T2 forever.
3. **Blast-radius throttle.** Max 1 auto-fix per source per day; max 10% of a family's sources per 24h; if >30% of a family flaps simultaneously, stop fixing (suspect systemic cause) and escalate.
4. **Post-verify + auto-rollback.** Every T3/T4 fix must confirm liveness restored within 2x normal cadence; failure -> rollback, file T0 incident, demote domain one tier with cooldown.
5. **One-click demotion + audit.** The lead can drop any domain (or everything) one tier instantly; every fix, approval, skip, and level change is logged in the kit's run log.
6. **The verdict monopoly holds for T3+.** Kits recommend and propose; the lead issues verdicts on anything irreversible or cross-domain. T4 auto-fixes are pre-authorized by this file, not by kit judgment.

---

## Current level per family

| Family | Tier | Basis |
|---|---|---|
| All 22 families | T0 Observe | Starting tier. First Warden v2 run was 2026-10-28. No track record yet. |

---

## Escalation contract

Warden escalates to the lead when:
- A source's fix class is unknown or novel (not in the runbook set)
- The fix would touch pipeline code, config, or shared state
- The detection is ambiguous (could be dead, could be moved, could be tarpitted)
- The silent-state file is corrupt or missing (the loud-loss guard fired)

Warden does NOT escalate to the lead when:
- A known fix class fires and the runbook is clear (T3+ auto-apply)
- A source flaps once (the throttle absorbs it; alarm on the pattern, not the event)
- The artifact is stale but within the 48h gate (the freshness leg handles it)
