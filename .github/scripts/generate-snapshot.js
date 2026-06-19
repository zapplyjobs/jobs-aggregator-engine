#!/usr/bin/env node

/**
 * ZJP Snapshot Generator — Component 1 of ZJP-INTEL
 *
 * Runs after main aggregation in fetch-jobs.yml.
 * Reads local data files + GitHub API → writes ZJP_SNAPSHOT.json + ZJP_CONTEXT.md
 * Both files are committed to jobs-data-2026 alongside all_jobs.json.
 *
 * Data sources (all local at run time):
 *   - .github/data/all_jobs.json         (JSONL — pipeline output)
 *   - .github/data/jobs-metadata.json    (aggregator stats)
 *   - .github/data/dedupe-store.json     (TTL expiry data)
 *   - /tmp/jobs-data-2026/.github/data/enrichment-stats.json  (enrichment rates)
 *   - .github/scripts/shared/ZJP_OPEN_DECISIONS.md            (human-authored decisions — private submodule)
 *   - .github/data/ZJP_SNAPSHOT.json                          (previous snapshot for deltas — aggregator private)
 *   - GitHub API: stars per consumer repo
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

const DATA_DIR = path.join(process.cwd(), '.github', 'data');
const JOBS_DATA_DIR = '/tmp/jobs-data-2026/.github/data';
const RUN_ID = process.env.GITHUB_RUN_ID || 'local';
const STALE_HOURS = 2;

const CONSUMER_REPOS = [
  { owner: 'zapplyjobs', repo: 'New-Grad-Jobs-2027',                             name: 'New-Grad'      },
  { owner: 'zapplyjobs', repo: 'Internships-2027',                               name: 'Internships'   },
  { owner: 'zapplyjobs', repo: 'New-Grad-Software-Engineering-Jobs-2027',        name: 'Software'      },
  { owner: 'zapplyjobs', repo: 'New-Grad-Data-Science-Jobs-2027',                name: 'Data-Science'  },
  { owner: 'zapplyjobs', repo: 'New-Grad-Hardware-Engineering-Jobs-2027',        name: 'Hardware'      },
  { owner: 'zapplyjobs', repo: 'New-Grad-Healthcare-Jobs-2027',                  name: 'Healthcare'    },
  { owner: 'zapplyjobs', repo: 'jobs-aggregator-private',                        name: 'Aggregator'    },
];

// ─── GitHub API helpers ────────────────────────────────────────────────────

function ghRequest(url) {
  return new Promise((resolve, reject) => {
    https.get(url, {
      headers: {
        'User-Agent': 'ZJP-Snapshot-Bot',
        'Authorization': `Bearer ${process.env.GH_PAT || process.env.GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json'
      }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, body: null }); }
      });
    }).on('error', reject);
  });
}

async function getStars(owner, repo) {
  try {
    const res = await ghRequest(`https://api.github.com/repos/${owner}/${repo}`);
    return res.status === 200 ? (res.body?.stargazers_count ?? null) : null;
  } catch { return null; }
}

async function getSubmoduleHash(owner, repo) {
  try {
    // Get the tree for .github/scripts/shared in the repo's default branch
    const res = await ghRequest(`https://api.github.com/repos/${owner}/${repo}/contents/.github/scripts/shared`);
    if (res.status === 200 && res.body?.sha) return res.body.sha.slice(0, 7);
    return null;
  } catch { return null; }
}

async function getLastWorkflowStatus(owner, repo) {
  try {
    const res = await ghRequest(
      `https://api.github.com/repos/${owner}/${repo}/actions/workflows/update-jobs.yml/runs?per_page=1`
    );
    if (res.status !== 200 || !res.body?.workflow_runs?.length) return null;
    return res.body.workflow_runs[0].conclusion || null;
  } catch { return null; }
}

// ─── Local data readers ────────────────────────────────────────────────────

function readMetadata() {
  const p = path.join(DATA_DIR, 'jobs-metadata.json');
  if (!fs.existsSync(p)) return null;
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch { return null; }
}

function readDedupeStore() {
  const p = path.join(DATA_DIR, 'dedupe-store.json');
  if (!fs.existsSync(p)) return null;
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch { return null; }
}

function readEnrichmentStats() {
  const p = path.join(JOBS_DATA_DIR, 'enrichment-stats.json');
  if (!fs.existsSync(p)) return null;
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch { return null; }
}

function readPreviousSnapshot() {
  // Read from jobs-data clone — snapshot is persisted there between runs.
  const p = path.join(JOBS_DATA_DIR, 'ZJP_SNAPSHOT.json');
  if (!fs.existsSync(p)) return null;
  try { return JSON.parse(fs.readFileSync(p, 'utf8')); }
  catch { return null; }
}

function readOpenDecisions() {
  // File lives in private submodule (job-board-scripts), not jobs-data-2026
  const p = path.join(process.cwd(), '.github', 'scripts', 'shared', 'ZJP_OPEN_DECISIONS.md');
  if (!fs.existsSync(p)) return [];
  try {
    return fs.readFileSync(p, 'utf8')
      .split('\n')
      .map(l => l.trim())
      .filter(l => l && !l.startsWith('#'));
  } catch { return []; }
}

function readDescriptionSidecarLines(source) {
  const p = path.join(JOBS_DATA_DIR, `descriptions-${source}.jsonl`);
  if (!fs.existsSync(p)) return 0;
  try {
    return fs.readFileSync(p, 'utf8').split('\n').filter(l => l.trim()).length;
  } catch { return 0; }
}

function readSupplementalMetadata(source) {
  const p = path.join(DATA_DIR, `supplemental-${source}-metadata.json`);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, 'utf8'));
  } catch {
    return null;
  }
}


// ─── Computation helpers ───────────────────────────────────────────────────

const TECH_DOMAINS = new Set(['software', 'data_science', 'hardware', 'ai']);

/**
 * Compute G1 metric: general-tag rate among US jobs from all_jobs.json JSONL.
 * Returns both all-US and tech-US rates.
 * - all-US: generals / all US jobs (stable, tracks pool composition)
 * - tech-US: generals / (tech-US + general-US) (user-connected, measures tech noise)
 */
function computeG1Metric() {
  const p = path.join(JOBS_DATA_DIR, 'all_jobs.json');
  if (!fs.existsSync(p)) return null;
  try {
    const lines = fs.readFileSync(p, 'utf8').split('\n').filter(l => l.trim());
    let usTotal = 0, usGeneral = 0, usTech = 0;
    for (const line of lines) {
      try {
        const job = JSON.parse(line);
        const locations = job.tags?.locations || [];
        if (!locations.includes('us')) continue;
        if (job.tags?.employment === 'senior') continue; // G1 measures entry-level audience only
        const domains = job.tags?.domains || [];
        const isGeneral = domains.length === 1 && domains[0] === 'general';
        const hasTech = domains.some(d => TECH_DOMAINS.has(d));
        usTotal++;
        if (isGeneral) usGeneral++;
        if (hasTech) usTech++;
      } catch { /* skip malformed */ }
    }
    if (usTotal === 0) return null;
    const techPool = usTech + usGeneral; // tech-eligible denominator
    return {
      us_total: usTotal,
      us_general: usGeneral,
      us_general_rate: Math.round((usGeneral / usTotal) * 1000) / 10,
      tech_us_total: usTech,
      tech_us_general_rate: techPool > 0 ? Math.round((usGeneral / techPool) * 1000) / 10 : null,
    };
  } catch { return null; }
}

function computeTtlExpiring7d(dedupeStore) {
  if (!dedupeStore?.ids) return null;
  const now = Date.now();
  const in7d = now + 7 * 24 * 60 * 60 * 1000;
  return Object.values(dedupeStore.ids)
    .filter(v => typeof v === 'number' && v > now && v <= in7d)
    .length;
}

function computeDeltas(currentPool, previousSnapshot) {
  if (!previousSnapshot?.pool) return null;
  const prev = previousSnapshot.pool;
  const totalDelta = currentPool.total !== null && prev.total !== null
    ? currentPool.total - prev.total : null;
  const prevTs = previousSnapshot.meta?.generated_at || null;
  return { compared_to: prevTs, total_delta: totalDelta };
}

// ─── Snapshot builder ──────────────────────────────────────────────────────

async function buildSnapshot(meta, repoStats) {
  const metadata = readMetadata();
  const dedupeStore = readDedupeStore();
  const enrichStats = readEnrichmentStats();
  const previousSnapshot = readPreviousSnapshot();
  const openDecisions = readOpenDecisions();

  // Pool counts
  const tagStats = metadata?.tag_stats || {};
  const g1 = computeG1Metric();
  const pool = {
    total: metadata?.total_jobs ?? null,
    by_source: metadata?.by_source
      ? Object.fromEntries(Object.entries(metadata.by_source).map(([k, v]) => [k, v.total ?? v]))
      : null,
    by_domain: tagStats.domains || null,
    us_entry_level: tagStats.locations?.us ?? null,
    us_interns: tagStats.employment?.internship ?? null,
    g1_metric: g1,
    ats_stats: metadata?.ats_stats || null,
  };

  // Enrichment / supplemental lane visibility
  const jsearchSidecarLines = readDescriptionSidecarLines('jsearch');
  const oracleSupplemental = readSupplementalMetadata('oracle');
  const customSupplemental = readSupplementalMetadata('custom');
  const enrichment = enrichStats ? {
    total_enriched: enrichStats.total_enriched ?? null,
    total_has_description: enrichStats.total_has_description ?? null,
    workday_waiting_for_desc: enrichStats.workday_waiting_for_desc ?? null,
    jsearch_sidecar_lines: jsearchSidecarLines,
  } : null;

  // Pipeline
  const submoduleHead = metadata ? null : null; // fetched per-repo below
  const ttlExpiring7d = computeTtlExpiring7d(dedupeStore);

  // TAG-AUDIT-6: Tag history — append entry from this run, keep last 30 days.
  const tagHistoryEntry = g1 ? {
    date: meta.generatedAt,
    g1_rate: g1.us_general_rate,
    us_general: g1.us_general,
    us_total: g1.us_total,
    senior: tagStats.employment?.senior ?? null,
    mid_level: tagStats.employment?.mid_level ?? null,
    entry_level: tagStats.employment?.entry_level ?? null,
    internship: tagStats.employment?.internship ?? null,
  } : null;
  const prevTagHistory = previousSnapshot?.tag_history || [];
  const thirtyDaysAgo = Date.now() - 30 * 24 * 60 * 60 * 1000;
  const tagHistory = [
    ...prevTagHistory.filter(e => new Date(e.date).getTime() > thirtyDaysAgo),
    ...(tagHistoryEntry ? [tagHistoryEntry] : []),
  ];

  const snapshot = {
    meta: {
      generated_at: meta.generatedAt,
      generated_by: 'zjp-snapshot-script v1',
      aggregator_run_id: RUN_ID,
      stale_if_older_than_hours: STALE_HOURS,
    },
    pool,
    tag_history: tagHistory,
    enrichment,
    pipeline: {
      submodule_head: repoStats.aggregatorSubmodule,
      last_run_status: 'success',   // we only run on success
      last_run_at: meta.generatedAt,
      last_run_id: RUN_ID,
      ttl_expiring_7d: ttlExpiring7d,
      supplemental_lanes: {
        oracle: oracleSupplemental ? {
          generated_at: oracleSupplemental.generated_at ?? null,
          jobs_fetched: oracleSupplemental.jobs_fetched ?? null,
          duration_ms: oracleSupplemental.duration_ms ?? null,
        } : null,
        custom: customSupplemental ? {
          generated_at: customSupplemental.generated_at ?? null,
          jobs_fetched: customSupplemental.jobs_fetched ?? null,
          duration_ms: customSupplemental.duration_ms ?? null,
          sources: customSupplemental.sources ?? null,
        } : null,
      },
    },
    repos: repoStats.repos,
    open_decisions: openDecisions,
    deltas: computeDeltas(pool, previousSnapshot),
  };

  return snapshot;
}

// ─── Context file generator ────────────────────────────────────────────────

function formatNumber(n) {
  if (n === null || n === undefined) return 'n/a';
  return n.toLocaleString('en-US');
}

function staleness(generatedAt) {
  const ageMs = Date.now() - new Date(generatedAt).getTime();
  const ageMin = Math.round(ageMs / 60000);
  const ageHrs = ageMs / 3600000;
  if (ageHrs > STALE_HOURS) return `⚠️ STALE (>${STALE_HOURS}h old — ${Math.round(ageHrs)}h) — verify before relying on counts`;
  if (ageMin < 2) return `✅ Fresh (just generated)`;
  return `✅ Fresh (generated ${ageMin} min ago)`;
}

function generateContextMd(snapshot) {
  const s = snapshot;
  const generatedAt = s.meta.generated_at;
  const displayDate = generatedAt.replace('T', ' ').replace('.000Z', ' UTC').replace(/\.\d+Z/, ' UTC');

  const pool = s.pool || {};
  const byDomain = pool.by_domain || {};
  const bySource = pool.by_source || {};
  const enrichment = s.enrichment || {};
  const pipeline = s.pipeline || {};
  const repos = s.repos || {};
  const decisions = s.open_decisions || [];

  // Domain row formatting
  const domainOrder = ['software', 'healthcare', 'hardware', 'data_science', 'ai', 'sales',
    'marketing', 'operations', 'finance', 'legal', 'hr', 'product', 'general'];
  const domainRow = domainOrder
    .filter(d => byDomain[d] !== undefined)
    .map(d => `${d}: ${formatNumber(byDomain[d])}`)
    .join(' · ');

  // Source row
  const sourceOrder = ['workday', 'greenhouse', 'smartrecruiters', 'ashby', 'lever', 'amazon', 'jsearch', 'eightfold'];
  const sourceRow = sourceOrder
    .filter(src => bySource[src] !== undefined)
    .map(src => `${src}: ${formatNumber(bySource[src])}`)
    .join(' · ');

  // Consumer repo table
  const consumerNames = ['New-Grad', 'Internships', 'Software', 'Data-Science', 'Hardware', 'Healthcare'];
  const repoRows = consumerNames.map(name => {
    const r = repos[name] || {};
    const status = r.workflowStatus === 'success' ? '✅' : r.workflowStatus === 'failure' ? '❌' : '⚠️';
    const submod = r.submodule || pipeline.submodule_head || '?';
    const stars = r.stars !== null && r.stars !== undefined ? `⭐${r.stars}` : '';
    return `| ${name} | ${status} ${r.workflowStatus || '?'} | ${submod} | ${stars} |`;
  }).join('\n');

  // Delta line
  let deltaLine = '';
  if (s.deltas?.total_delta !== null && s.deltas?.total_delta !== undefined) {
    const sign = s.deltas.total_delta >= 0 ? '+' : '';
    const comparedTo = s.deltas.compared_to ? ` vs ${s.deltas.compared_to.slice(0, 10)}` : '';
    deltaLine = `\n**Pool delta vs prior run:** ${sign}${formatNumber(s.deltas.total_delta)} jobs${comparedTo}`;
  }

  // TTL line
  const ttlLine = pipeline.ttl_expiring_7d !== null && pipeline.ttl_expiring_7d !== undefined
    ? `\n**Jobs expiring next 7 days:** ${formatNumber(pipeline.ttl_expiring_7d)}`
    : '';

  // Open decisions
  const decisionsSection = decisions.length > 0
    ? decisions.map(d => `- ${d}`).join('\n')
    : '_No open decisions_';

  // Enrichment line
  const enrichLine = enrichment.total_enriched !== null && enrichment.total_enriched !== undefined
    ? `**Enriched:** ${formatNumber(enrichment.total_enriched)} records` +
      (enrichment.jsearch_sidecar_lines !== undefined ? ` · JSearch sidecar: ${enrichment.jsearch_sidecar_lines} lines` : '')
    : '';

  return `# ZJP Context — ${displayDate}
> Generated by zjp-snapshot-script v1. Aggregator run #${RUN_ID}.
> ${staleness(generatedAt)}

---

## SYSTEM STATUS (all roles read this)

| | |
|-|-|
| Pipeline | ✅ LIVE — last run ${displayDate} |
| Pool total | ${formatNumber(pool.total)} jobs |
| US entry-level | ${formatNumber(pool.us_entry_level)} |
| Interns (US) | ${formatNumber(pool.us_interns)} |
| G1 general rate | ${pool.g1_metric ? `${pool.g1_metric.us_general_rate}% all-US (${formatNumber(pool.g1_metric.us_general)}/${formatNumber(pool.g1_metric.us_total)}) / ${pool.g1_metric.tech_us_general_rate ?? '?'}% tech-US (${formatNumber(pool.g1_metric.us_general)}/${formatNumber(pool.g1_metric.tech_us_total + pool.g1_metric.us_general)})` : 'n/a'} |
| Submodule | \`${pipeline.submodule_head || '?'}\` |
${deltaLine}
---

## STRATEGIC (Strategist reads this)

**By domain:**
${domainRow}

**By source:**
${sourceRow}

**ATS coverage:** ${pool.ats_stats ? `GH=${pool.ats_stats.greenhouse_companies} · Lever=${pool.ats_stats.lever_companies} · Ashby=${pool.ats_stats.ashby_companies} · WD=${pool.ats_stats.workday_tenants} · SR=${pool.ats_stats.smartrecruiters_companies} · Eightfold=${pool.ats_stats.eightfold_tenants}` : 'n/a'}

**Consumer repos:**
| Repo | Workflow | Submodule | Stars |
|------|----------|-----------|-------|
${repoRows}

**Open decisions needing input:**
${decisionsSection}

---

## OPERATIONAL (Auditor reads this)

**Last run:** ${displayDate} · status: success · run #${RUN_ID}
${deltaLine}${ttlLine}

${enrichLine}

**Submodule consistency:** All 7 repos should be at \`${pipeline.submodule_head || '?'}\` — verify with \`git submodule status\`.

---

## IMPLEMENTATION (Coder reads this)

**Submodule HEAD:** \`${pipeline.submodule_head || '?'}\`
**Repos to bump after submodule change:** jobs-aggregator-private · New-Grad-Jobs-2027 · Internships-2027 · Software · DS · HW · Healthcare (7 total)
**Next unblocked task:** See \`projects/zjp/TODO.md\` — not auto-generated.

---

*Full machine-readable data: ZJP_SNAPSHOT.json (co-located)*
`;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log('[generate-snapshot] Starting ZJP snapshot generation...');

  const generatedAt = new Date().toISOString();

  // Fetch repo stats in parallel (stars + submodule hash + workflow status)
  console.log('[generate-snapshot] Fetching repo data from GitHub API...');
  const repoData = await Promise.all(CONSUMER_REPOS.map(async (r) => {
    const [stars, submodule, workflowStatus] = await Promise.all([
      getStars(r.owner, r.repo),
      r.name !== 'Aggregator' ? getSubmoduleHash(r.owner, r.repo) : Promise.resolve(null),
      r.name !== 'Aggregator' ? getLastWorkflowStatus(r.owner, r.repo) : Promise.resolve(null),
    ]);
    return { name: r.name, stars, submodule, workflowStatus };
  }));

  const repos = {};
  let aggregatorSubmodule = null;
  for (const r of repoData) {
    if (r.name === 'Aggregator') {
      // Aggregator submodule hash = the submodule itself
      aggregatorSubmodule = r.submodule;
    } else {
      repos[r.name] = { submodule: r.submodule, stars: r.stars, workflowStatus: r.workflowStatus };
    }
  }

  // Use the first consumer repo's submodule hash as the canonical HEAD (they should all match)
  const canonicalSubmodule = aggregatorSubmodule
    || Object.values(repos).find(r => r.submodule)?.submodule
    || null;
  if (canonicalSubmodule) {
    for (const r of Object.values(repos)) {
      if (!r.submodule) r.submodule = canonicalSubmodule;
    }
  }

  const snapshot = await buildSnapshot(
    { generatedAt },
    { repos, aggregatorSubmodule: canonicalSubmodule }
  );

  // Write ZJP_SNAPSHOT.json (full — private, stays in aggregator repo)
  const snapshotPath = path.join(JOBS_DATA_DIR, 'ZJP_SNAPSHOT.json');
  fs.writeFileSync(snapshotPath, JSON.stringify(snapshot, null, 2) + '\n');
  console.log(`[generate-snapshot] ZJP_SNAPSHOT.json written`);

  // Write public snapshot (stripped — committed to jobs-data-2026 for dashboard)
  const publicSnapshot = {
    meta: {
      generated_at: snapshot.meta.generated_at,
      generated_by: snapshot.meta.generated_by,
      stale_if_older_than_hours: snapshot.meta.stale_if_older_than_hours,
    },
    pool: {
      total: snapshot.pool.total,
      by_source: snapshot.pool.by_source,
      by_domain: snapshot.pool.by_domain,
      us_entry_level: snapshot.pool.us_entry_level,
      us_interns: snapshot.pool.us_interns,
      g1_metric: snapshot.pool.g1_metric,
      ats_stats: snapshot.pool.ats_stats,
    },
    pipeline: {
      submodule_head: snapshot.pipeline.submodule_head,
      last_run_status: snapshot.pipeline.last_run_status,
      last_run_at: snapshot.pipeline.last_run_at,
      supplemental_lanes: snapshot.pipeline.supplemental_lanes,
    },
    repos: snapshot.repos,
    tag_history: snapshot.tag_history,
    deltas: snapshot.deltas,
  };
  const publicSnapshotPath = path.join(JOBS_DATA_DIR, 'zjp-public-snapshot.json');
  fs.writeFileSync(publicSnapshotPath, JSON.stringify(publicSnapshot, null, 2) + '\n');
  console.log(`[generate-snapshot] zjp-public-snapshot.json written`);

  // Write ZJP_CONTEXT.md
  const contextPath = path.join(JOBS_DATA_DIR, 'ZJP_CONTEXT.md');
  fs.writeFileSync(contextPath, generateContextMd(snapshot));
  console.log(`[generate-snapshot] ZJP_CONTEXT.md written`);

  // Summary
  const pool = snapshot.pool;
  console.log(`[generate-snapshot] Pool: ${pool.total} total · ${pool.us_entry_level} US entry-level · ${pool.us_interns} interns · tag_history: ${snapshot.tag_history.length} entries`);
  if (snapshot.deltas?.total_delta !== null && snapshot.deltas?.total_delta !== undefined) {
    const sign = snapshot.deltas.total_delta >= 0 ? '+' : '';
    console.log(`[generate-snapshot] Delta vs prior: ${sign}${snapshot.deltas.total_delta} jobs`);
  }
  console.log(`[generate-snapshot] ✅ Done`);
}

main().catch(err => {
  console.error('[generate-snapshot] ❌ Fatal:', err.message);
  // Non-fatal — don't fail the whole workflow if snapshot fails
  process.exit(0);
});
