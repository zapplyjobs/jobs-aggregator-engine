#!/usr/bin/env node

/**
 * Main Orchestrator - Jobs Data Fetcher
 *
 * Coordinates all fetchers, normalizes jobs, deduplicates,
 * and writes the shared output file.
 *
 * Usage:
 *   node index.js                    # Normal run
 *   node index.js --dry-run          # Dry run (no git commit)
 *   node index.js --verbose          # Verbose logging
 */

const fs = require('fs');
const path = require('path');
const assert = require('assert');

// Import from aggregator submodule (job-board-aggregator/lib/)
const SHARED = path.join(__dirname, 'aggregator', 'lib');

// Import fetchers
const { fetchFromAllATS, getUsageStats: getATSUsageStats } = require(`${SHARED}/fetchers/ats-fetcher`);
const { fetchAllAmazonJobs } = require(`${SHARED}/fetchers/amazon`);
const { fetchAllNetflixJobs } = require(`${SHARED}/fetchers/netflix`);
// AGG-HOTPATH-1: WD descriptions are no longer fetched in the hot publish path.
const { fetchAllAppleJobs } = require(`${SHARED}/fetchers/apple`);
const { fetchAllTwoSigmaJobs } = require(`${SHARED}/fetchers/twosigma`);
const { fetchAllUberJobs } = require(`${SHARED}/fetchers/uber`);
const { fetchAllGoogleJobs } = require(`${SHARED}/fetchers/google`);
const { fetchAllSimplifyJobs } = require(`${SHARED}/fetchers/simplify`);
const { fetchAllMicrosoftJobs } = require(`${SHARED}/fetchers/microsoft`);
const { fetchAllOracleJobs } = require(`${SHARED}/fetchers/oracle`);
const { fetchAllAmdJobs } = require(`${SHARED}/fetchers/amd`);
const { fetchAllTiktokJobs } = require(`${SHARED}/fetchers/tiktok`);
const { applyFamilyCache, buildFamilyCache } = require(`${SHARED}/fetchers/workday`);

// Import processors
const { validateAndNormalizeJobs, printValidationSummary, normalizeJob } = require(`${SHARED}/processors/validator`);
const { filterSeniorJobs, printSeniorFilterSummary, isSeniorJob, buildCompanyOverrideMap } = require(`${SHARED}/processors/senior-filter`);
const { deduplicateJobs, DEDUPE_TTL_MS, DEDUPE_TTL_DAYS, INTERNSHIP_TTL_MS } = require(`${SHARED}/processors/deduplicator`);
const { tagJobs, generateTagStats, tagEmployment, tagDomains, tagLocations, setCompanyOverrideMap, TAG_ENGINE_VERSION, getKeywordMap } = require(`${SHARED}/processors/tag-engine`);
const { printTagDistribution, checkTagDrift, printDriftReport, checkDomainPrecision, printPrecisionReport, checkKeywordHealth, checkKeywordOverlap } = require(`${SHARED}/processors/tag-monitor`);

const SKIP_WD_FAMILY_CACHE_BUILD = process.env.SKIP_WD_FAMILY_CACHE_BUILD === '1';
// Import utils
const { writeJobsJSONL, writeMetadata } = require(`${SHARED}/utils/file-writer`);
const { EMPLOYMENT_NORMALIZE_MAP } = require(`${SHARED}/utils/helpers`);
const { writeSidecars } = require(`${SHARED}/utils/sidecar-writer`);
const { runTagMonitoring } = require(`${SHARED}/utils/monitoring`);

// AGG-36: Company override map (populated by loadCompanyOverrides)
const COMPANY_LIST_PATH = path.join(SHARED, 'fetchers', 'company-list.json');
let companyOverrideMap = new Map();

function loadCompanyOverrides() {
  try {
    const companyList = JSON.parse(fs.readFileSync(COMPANY_LIST_PATH, 'utf8'));
    companyOverrideMap = buildCompanyOverrideMap(companyList);
    if (companyOverrideMap.size > 0) {
      console.log(`📋 AGG-36: Loaded ${companyOverrideMap.size} company title overrides`);
      setCompanyOverrideMap(companyOverrideMap);
    }
  } catch (e) {
    console.warn(`⚠️ AGG-36: Could not load company overrides: ${e.message}`);
  }
}
loadCompanyOverrides();

// Paths
const DATA_DIR = path.join(process.cwd(), '.github', 'data');
const JOBS_OUTPUT_FILE = path.join(DATA_DIR, 'all_jobs.json');
const US_JOBS_OUTPUT_FILE = path.join(DATA_DIR, 'us_jobs.json');
const METADATA_OUTPUT_FILE = path.join(DATA_DIR, 'jobs-metadata.json');

// Command line args
const args = process.argv.slice(2);
const isDryRun = args.includes('--dry-run');
const isVerbose = args.includes('--verbose');

// --- Extracted invariants (AGG-PIPE-6) ---
// Each function wraps a pipeline invariant that was previously inline in main().
// Named functions make accidental removal structurally harder.

function appendAll(target, items) {
  if (!Array.isArray(items) || items.length === 0) return;
  for (const item of items) target.push(item);
}

function isUsSnapshotJob(job) {
  return job?.tags?.locations?.includes('us') === true;
}

function buildUsSnapshotJobs(jobs) {
  return jobs.filter(isUsSnapshotJob);
}

/**
 * AGG-32: Filter stale jobs by posted_at TTL.
 * AGG-6 date overwrite disabled A86 — jobs keep their source-reported posted_at.
 */
function resolvePostedAt(publicJobs, prevLines) {
  // AGG-32: filter stale — single pass
  // SUP-TTL-1: Internships get wider TTL window (120d vs 14d)
  let staleRemoved = 0;
  const filtered = publicJobs.filter(job => {
    if (!job.posted_at) { staleRemoved++; return false; }
    const jobTtlMs = job.tags?.employment === 'internship' ? INTERNSHIP_TTL_MS : DEDUPE_TTL_MS;
    if (new Date(job.posted_at).getTime() < Date.now() - jobTtlMs) { staleRemoved++; return false; }
    return true;
  });

  if (staleRemoved > 0) {
    console.log(`🧹 AGG-32: Removed ${staleRemoved} stale jobs (posted_at >${DEDUPE_TTL_DAYS}d regular / 120d internship) from post-merge pool`);
  }

  publicJobs.length = 0;
  appendAll(publicJobs, filtered);
}

// AGG-PIPE-4: Sources excluded from closed-job detection.
// Workday: per-tenant failures inside an otherwise-successful aggregate fetch
// would falsely close hundreds of jobs (33K+ WD jobs, ~138 tenants).
// SmartRecruiters: owned by enrichment workflow (DESC-MIGRATE-1), not main pipeline.
const PIPE4_EXCLUDED_SOURCES = new Set(['workday', 'smartrecruiters']);

// Sources that are no longer fetched by any active lane must not survive through
// the internship 120-day rolling window. If a retired source is absent from the
// current run, carry-forward would otherwise preserve stale skeleton listings.
const RETIRED_CARRY_FORWARD_SOURCES = new Set(['jsearch']);

// AGG-PIPE-4: Map Phase B fetcher display names to job source field values.
const FETCHER_NAME_TO_SOURCE = {
  'Amazon': 'amazon',
  'Netflix': 'eightfold',
  'Apple': 'apple',
  'Two Sigma': 'twosigma',
  'Uber': 'uber',
  'Google': 'google',
  'SimplifyJobs': 'simplify',
  'Microsoft': 'microsoft',
  'Oracle': 'oracle',
  'AMD': 'amd',
  'TikTok': 'tiktok',
};

// AGG-HOTPATH-1: fetchers explicitly removed from the fast publish path.
// They may return via separate workflowing or slower lanes later, but they must not
// consume Tier A runtime budget inside the main 15-minute-capped workflow.
const HOTPATH_DEMOTED_FETCHERS = new Set(['Apple', 'Google', 'Microsoft', 'Oracle', 'TikTok']);
const HOTPATH_DEMOTED_SOURCES = new Set(
  [...HOTPATH_DEMOTED_FETCHERS].map(name => FETCHER_NAME_TO_SOURCE[name] || name.toLowerCase())
);

const SUPPLEMENTAL_LANE_FILES = [
  {
    lane: 'oracle',
    jobsFile: path.join(DATA_DIR, 'supplemental-oracle-jobs.json'),
    metaFile: path.join(DATA_DIR, 'supplemental-oracle-metadata.json'),
  },
  {
    lane: 'custom',
    jobsFile: path.join(DATA_DIR, 'supplemental-custom-jobs.json'),
    metaFile: path.join(DATA_DIR, 'supplemental-custom-metadata.json'),
  },
];

function normalizeSupplementalJobForMerge(job, laneMeta) {
  if (!job || typeof job !== 'object') return null;
  const source = typeof job.source === 'string' ? job.source.toLowerCase() : null;
  if (!source) return null;

  const normalized = { ...job, source };
  if (!normalized.posted_at && laneMeta?.generated_at) {
    const generatedTs = new Date(laneMeta.generated_at).getTime();
    if (!Number.isNaN(generatedTs)) {
      normalized.posted_at = laneMeta.generated_at;
      normalized.posted_at_basis = 'supplemental_generated_at';
    }
  }
  return normalized;
}

function summarizeSupplementalLaneForMerge(laneName, laneJobs, laneMeta, nowMs = Date.now()) {
  const info = {
    status: 'missing',
    skip_reason: null,
    generated_at: laneMeta?.generated_at || null,
    age_minutes: null,
    jobs_loaded: Array.isArray(laneJobs) ? laneJobs.length : 0,
    by_source: {},
    publish_contract: laneMeta?.publish_contract || null,
  };
  if (!Array.isArray(laneJobs)) {
    info.status = 'skipped_invalid';
    info.skip_reason = 'jobs_not_array';
    return { info, jobs: [], sourcesUsed: new Set() };
  }
  if (!laneMeta || typeof laneMeta !== 'object') {
    info.status = 'skipped_invalid';
    info.skip_reason = 'metadata_not_object';
    return { info, jobs: [], sourcesUsed: new Set() };
  }
  if (laneMeta.lane_name && laneMeta.lane_name !== laneName) {
    info.status = 'skipped_invalid';
    info.skip_reason = 'lane_name_mismatch';
    return { info, jobs: [], sourcesUsed: new Set() };
  }

  const normalizedJobs = [];
  const sourcesUsed = new Set();
  for (const job of laneJobs) {
    const normalized = normalizeSupplementalJobForMerge(job, laneMeta);
    if (!normalized) continue;
    const source = normalized.source;
    sourcesUsed.add(source);
    info.by_source[source] = (info.by_source[source] || 0) + 1;
    normalizedJobs.push(normalized);
  }

  const generatedTs = info.generated_at ? new Date(info.generated_at).getTime() : Number.NaN;
  if (info.generated_at) {
    if (Number.isNaN(generatedTs)) {
      info.status = 'skipped_invalid';
      info.skip_reason = 'invalid_generated_at';
      return { info, jobs: [], sourcesUsed: new Set() };
    }
    info.age_minutes = Math.max(0, Math.floor((nowMs - generatedTs) / 60000));
  }

  const maxStalenessMinutes = Number(info.publish_contract?.max_staleness_minutes);
  if (Number.isFinite(maxStalenessMinutes) && maxStalenessMinutes > 0 && Number.isFinite(generatedTs)) {
    if (nowMs - generatedTs > maxStalenessMinutes * 60 * 1000) {
      info.status = 'skipped_stale';
      info.skip_reason = 'stale_artifact';
      return { info, jobs: [], sourcesUsed: new Set() };
    }
  }

  if (Number.isFinite(laneMeta.jobs_fetched) && laneMeta.jobs_fetched !== laneJobs.length) {
    info.status = 'skipped_invalid';
    info.skip_reason = 'jobs_fetched_mismatch';
    return { info, jobs: [], sourcesUsed: new Set() };
  }

  if (laneName === 'custom') {
    const declaredSources = laneMeta.sources && typeof laneMeta.sources === 'object'
      ? Object.keys(laneMeta.sources).sort()
      : [];
    const actualSources = Object.keys(info.by_source).sort();
    if (!declaredSources.length || declaredSources.join(',') !== actualSources.join(',')) {
      info.status = 'skipped_invalid';
      info.skip_reason = 'source_set_mismatch';
      return { info, jobs: [], sourcesUsed: new Set() };
    }
    for (const source of declaredSources) {
      if (laneMeta.sources[source] !== info.by_source[source]) {
        info.status = 'skipped_invalid';
        info.skip_reason = 'source_count_mismatch';
        return { info, jobs: [], sourcesUsed: new Set() };
      }
    }
  } else if (laneName === 'oracle') {
    const actualSources = Object.keys(info.by_source);
    if (actualSources.some(source => source !== 'oracle')) {
      info.status = 'skipped_invalid';
      info.skip_reason = 'unexpected_source';
      return { info, jobs: [], sourcesUsed: new Set() };
    }
  }

  info.status = 'merged';
  return { info, jobs: normalizedJobs, sourcesUsed };
}


function loadSupplementalInputs(nowMs = Date.now()) {
  const jobs = [];
  const inputs = {};
  const sourcesUsed = new Set();
  for (const lane of SUPPLEMENTAL_LANE_FILES) {
    if (!fs.existsSync(lane.jobsFile) || !fs.existsSync(lane.metaFile)) {
      inputs[lane.lane] = {
        status: 'missing',
        skip_reason: 'artifact_missing',
        generated_at: null,
        age_minutes: null,
        jobs_loaded: 0,
        by_source: {},
        publish_contract: null,
      };
      continue;
    }
    try {
      const laneJobs = JSON.parse(fs.readFileSync(lane.jobsFile, 'utf8'));
      const laneMeta = JSON.parse(fs.readFileSync(lane.metaFile, 'utf8'));
      const summary = summarizeSupplementalLaneForMerge(lane.lane, laneJobs, laneMeta, nowMs);
      inputs[lane.lane] = summary.info;
      if (summary.info.status !== 'merged') {
        console.warn(`⚠️ Supplemental lane ${lane.lane}: skipped (${summary.info.skip_reason})`);
        continue;
      }
      for (const source of summary.sourcesUsed) sourcesUsed.add(source);
      appendAll(jobs, summary.jobs);
    } catch (e) {
      inputs[lane.lane] = {
        status: 'skipped_invalid',
        skip_reason: 'load_error',
        generated_at: null,
        age_minutes: null,
        jobs_loaded: 0,
        by_source: {},
        publish_contract: null,
      };
      console.warn(`⚠️ Supplemental lane ${lane.lane}: could not load artifact (${e.message})`);
    }
  }
  return { jobs, inputs, sourcesUsed };
}




/**
 * Carry-forward merge: re-adds prior-run jobs not in current run (rolling window).
 * Re-tags employment, re-tags domains on version change, refreshes empty locations.
 *
 * AGG-PIPE-4: Jobs from sources that had a successful fetch this run are NOT carried
 * forward — if the source fetched successfully and the job isn't in the results,
 * the job is genuinely closed. Excludes WD and SR (multi-tenant false-positive risk).
 */
function shouldTreatCompanyScopedSourceJobClosed(job, fetcherHealth) {
  const source = (job.source || '').toLowerCase();
  if (source !== 'workday' && source !== 'smartrecruiters') return false;
  if (!fetcherHealth || typeof fetcherHealth !== 'object') return false;
  const companyKey = job.company_name;
  if (!companyKey) return false;
  const health = fetcherHealth[companyKey];
  return health?.source === source && health?.status === 'alive';
}

function mergeCarryForward(publicJobs, prevLines, currentIds, currentFingerprints, stripFields, successfulSources, companyFetchHealth) {

  let mergedCount = 0;
  let nullDateCount = 0;
  let fpSkipCount = 0;
  let closedDetected = 0;
  let retiredSourceDropped = 0;
  for (const line of prevLines) {
    try {
      const job = JSON.parse(line);
      if (currentIds.has(job.id)) continue;
      if (job.fingerprint && currentFingerprints.has(job.fingerprint)) { fpSkipCount++; continue; }
      if (!job.posted_at) { nullDateCount++; continue; }
      const postedTs = new Date(job.posted_at).getTime();
      const jobTtlMs = job.tags?.employment === 'internship' ? INTERNSHIP_TTL_MS : DEDUPE_TTL_MS;
      if (postedTs < Date.now() - jobTtlMs) continue;
      if (isSeniorJob(job)) continue;
      if (RETIRED_CARRY_FORWARD_SOURCES.has((job.source || '').toLowerCase())) {
        retiredSourceDropped++;
        continue;
      }
      // AGG-PIPE-4: Skip carry-forward for successfully-fetched sources (job is closed)
      if (successfulSources.has(job.source) && !PIPE4_EXCLUDED_SOURCES.has(job.source)) {
        closedDetected++;
        continue;
      }
      // A141: For Workday / SmartRecruiters, source-level success is too coarse because one tenant can fail
      // or be skipped unchanged while others succeed. But if the specific company fetched successfully and
      // returned >0 jobs this run, an absent prior job from that same company/source is safe to treat as closed.
      if (shouldTreatCompanyScopedSourceJobClosed(job, companyFetchHealth)) {
        closedDetected++;
        continue;
      }
      const strippedJob = { ...job };
      for (const field of stripFields) delete strippedJob[field];
      // AGG-DATA-13: normalize employment types on carry-forward jobs (AGG-PIPE-13: shared constant)
      if (Array.isArray(strippedJob.employment_types)) {
        strippedJob.employment_types = strippedJob.employment_types.map(t => EMPLOYMENT_NORMALIZE_MAP[t] || t);
      }
      // A86: Restore posted_at from first_published for carry-forward jobs.
      // Pre-A86 runs used FRESHNESS-2 which inflated posted_at to Date.now() when source date >7d.
      // This one-time correction restores the real source date where first_published is available.
      if (strippedJob.first_published && strippedJob.posted_at) {
        const fpTs = new Date(strippedJob.first_published).getTime();
        const paTs = new Date(strippedJob.posted_at).getTime();
        if (!isNaN(fpTs) && !isNaN(paTs) && paTs > fpTs) {
          strippedJob.posted_at = strippedJob.first_published;
        }
      }
      publicJobs.push(strippedJob);
      mergedCount++;
    } catch { /* skip malformed lines */ }
  }
  if (nullDateCount > 0) {
    console.log(`⚠️ Rolling window: dropped ${nullDateCount} prior-run jobs with null posted_at (cannot verify TTL)`);
  }
  if (fpSkipCount > 0) {
    console.log(`🔄 Rolling window: skipped ${fpSkipCount} prior-run jobs with matching fingerprint (ID changed, current version wins)`);
  }
  if (closedDetected > 0) {
    console.log(`🔍 AGG-PIPE-4: ${closedDetected} carry-forward jobs detected as closed (source fetched successfully, job absent)`);
  }
  if (retiredSourceDropped > 0) {
    console.log(`🧹 Retired sources: dropped ${retiredSourceDropped} prior-run jobs from disabled sources (${[...RETIRED_CARRY_FORWARD_SOURCES].join(', ')})`);
  }
  if (mergedCount > 0) {
    publicJobs.sort((a, b) => new Date(b.posted_at || 0) - new Date(a.posted_at || 0));
    let empRetagged = 0;
    let domainRetagged = 0;
    let locRefreshed = 0;
    let locRetagged = 0;
    for (const job of publicJobs) {
      if (currentIds.has(job.id)) continue;
      if (!job.tags) job.tags = {};
      const newEmp = tagEmployment(job);
      if (job.tags.employment !== newEmp) {
        job.tags.employment = newEmp;
        empRetagged++;
      }
      // AGG-PIPE-12: Always re-tag domains (not just on version change).
      // Between version bumps, keyword/guard changes silently miss carry-forward jobs.
      const freshDomains = tagDomains(job);
      const oldDomains = (job.tags.domains || []).slice().sort().join(',');
      const newDomains = (freshDomains || []).slice().sort().join(',');
      if (oldDomains !== newDomains) {
        job.tags.domains = freshDomains;
        domainRetagged++;
      }
      if ((!job.job_state || job.job_state === '') || (!job.job_city || job.job_city === '')) {
        const hadState = job.job_state && job.job_state !== '';
        const hadCity = job.job_city && job.job_city !== '';
        normalizeJob(job);
        if ((!hadState && job.job_state) || (!hadCity && job.job_city)) locRefreshed++;
      }
      const freshLocations = tagLocations(job);
      const oldLocations = (job.tags.locations || []).slice().sort().join(',');
      const newLocations = (freshLocations || []).slice().sort().join(',');
      if (oldLocations !== newLocations) {
        job.tags.locations = freshLocations;
        locRetagged++;
      }
      job.tags.tag_engine_version = TAG_ENGINE_VERSION;
    }
    console.log(`🔄 Merged ${mergedCount} prior-run jobs into rolling window (total: ${publicJobs.length}${closedDetected > 0 ? `, ${closedDetected} closed detected` : ''}${empRetagged > 0 ? `, ${empRetagged} employment re-tagged` : ''}${domainRetagged > 0 ? `, ${domainRetagged} domains re-tagged (version ${TAG_ENGINE_VERSION})` : ''}${locRefreshed > 0 ? `, ${locRefreshed} location fields refreshed` : ''}${locRetagged > 0 ? `, ${locRetagged} locations re-tagged` : ''})`);
  } else {
    console.log(`🔄 No prior-run jobs to merge${closedDetected > 0 ? ` (${closedDetected} closed detected)` : ''}`);
  }
}

/**
 * Generate tag stats from full pool (post-merge + post-AGG-32).
 * Must run after both carry-forward merge and AGG-32 filter for accurate counts.
 */
function computeFullPoolTagStats(publicJobs) {
  const stats = generateTagStats(publicJobs);
  console.log(`📊 Tag stats: ${stats.total} jobs (full pool)`);
  return stats;
}

/**
 * Main execution function
 */
async function main() {
  const startTime = Date.now();
  const stageTimings = {};
  const pipelineTimestamps = { started_at: new Date(startTime).toISOString() };

  console.log('🚀 Jobs Data Fetcher - Starting...');
  console.log('═'.repeat(60));
  console.log(`Mode: ${isDryRun ? 'DRY RUN (no commits)' : 'NORMAL'}`);
  console.log('');

  // AGG-PIPE-7: Startup assertions — fail fast when critical files are missing.
  // Origin: d06bb81 deleted tag-engine.js, pipeline ran 25+ failing runs over 7h.
  try {
    // (a) Submodule directory exists and isn't empty
    const sharedFiles = fs.readdirSync(SHARED);
    assert(sharedFiles.length > 0, `Submodule directory ${SHARED} is empty`);

    // (b) tag-engine.js exports expected functions
    assert(typeof tagJobs === 'function', 'tag-engine.js does not export tagJobs');
    assert(typeof tagEmployment === 'function', 'tag-engine.js does not export tagEmployment');
    assert(typeof tagDomains === 'function', 'tag-engine.js does not export tagDomains');
    assert(typeof generateTagStats === 'function', 'tag-engine.js does not export generateTagStats');

    // (c) Senior filter exports expected functions
    assert(typeof filterSeniorJobs === 'function', 'senior-filter.js does not export filterSeniorJobs');
    assert(typeof isSeniorJob === 'function', 'senior-filter.js does not export isSeniorJob');
    assert(typeof buildCompanyOverrideMap === 'function', 'senior-filter.js does not export buildCompanyOverrideMap');

    // (d) Deduplicator exports TTL constants
    assert(typeof DEDUPE_TTL_MS === 'number' && DEDUPE_TTL_MS > 0, 'deduplicator.js does not export valid DEDUPE_TTL_MS');
    assert(typeof DEDUPE_TTL_DAYS === 'number' && DEDUPE_TTL_DAYS > 0, 'deduplicator.js does not export valid DEDUPE_TTL_DAYS');

    // (e) company-list.json is parseable with expected platforms
    const companyList = JSON.parse(fs.readFileSync(COMPANY_LIST_PATH, 'utf8'));
    const platforms = Object.keys(companyList);
    const requiredPlatforms = ['greenhouse', 'lever', 'ashby', 'workday', 'smartrecruiters'];
    for (const p of requiredPlatforms) {
      assert(platforms.includes(p), `company-list.json missing required platform: ${p}`);
    }

    console.log('✅ Startup assertions passed (AGG-PIPE-7)');
  } catch (assertionErr) {
    console.error(`❌ STARTUP ASSERTION FAILED: ${assertionErr.message}`);
    console.error('Pipeline cannot proceed — critical file/module missing or corrupt.');
    process.exit(1);
  }

  try {
    // Ensure data directory exists
    if (!fs.existsSync(DATA_DIR)) {
      fs.mkdirSync(DATA_DIR, { recursive: true });
    }

    // Step 1: Fetch from all sources
    console.log('📡 Step 1: Fetching jobs from all sources...');
    let _stepStart = Date.now();
    console.log('━'.repeat(60));

    // AGG-5 (S229): Overall timeout per fetcher. Prevents one hung API from blocking
    // the entire pipeline (Amazon API hung for 23 min on 2026-03-26T00:15 run).
    // Rolling window merge preserves prior-run jobs — skipping a source is safe.
    function withTimeout(promise, ms, label) {
      return Promise.race([
        promise,
        new Promise((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms/1000}s`)), ms))
      ]).catch(err => {
        console.error(`⚠️ ${label}: ${err.message} — continuing with 0 jobs`);
        return label.includes('ATS') ? { jobs: [], stats: { by_source: {}, by_company: {} } } : [];
      });
    }

    let allJobs = [];

    // Read previous counts (needed for initial-population detection) before fetch
    let prevAppleCount = 0, prevGoogleCount = 0, prevMicrosoftCount = 0;
    let prevAppleIds = new Set();
    let wdPreviousTotals = null;
    try {
      if (fs.existsSync(JOBS_OUTPUT_FILE)) {
        const content = fs.readFileSync(JOBS_OUTPUT_FILE, 'utf8');
        prevAppleCount = (content.match(/"source":"apple"/g) || []).length;
        prevGoogleCount = (content.match(/"source":"google"/g) || []).length;
        prevMicrosoftCount = (content.match(/"source":"microsoft"/g) || []).length;
        if (prevAppleCount > 0) {
          prevAppleIds = new Set((content.match(/"id":"apple-[^"]+"/g) || []).map(m => m.slice(6, -1)));
          console.log(`  Previous Apple count: ${prevAppleCount} (${prevAppleIds.size} IDs)`);
        }
        if (prevGoogleCount > 0) console.log(`  Previous Google count: ${prevGoogleCount}`);
        if (prevMicrosoftCount > 0) console.log(`  Previous Microsoft count: ${prevMicrosoftCount}`);
      }
    } catch (e) { /* first run */ }

    // AGG-SPEED-4: Load Microsoft description cache from seeded sidecar
    // IDs are extracted from descriptions-microsoft*.jsonl files in DATA_DIR
    let microsoftCachedIds = new Set();
    try {
      const msSidecarFiles = fs.readdirSync(DATA_DIR)
        .filter(f => f.startsWith('descriptions-microsoft') && f.endsWith('.jsonl'));
      for (const fname of msSidecarFiles) {
        const lines = fs.readFileSync(path.join(DATA_DIR, fname), 'utf8').trim().split('\n').filter(Boolean);
        for (const line of lines) {
          try { const { id } = JSON.parse(line); if (id) microsoftCachedIds.add(id); } catch {}
        }
      }
      if (microsoftCachedIds.size > 0) console.log(`  Microsoft description cache: ${microsoftCachedIds.size} IDs`);
    } catch (e) { /* no cache yet */ }

    // AGG-FETCH-10: Load description caches from sidecar files.
    // Same pattern as Microsoft — avoids re-fetching detail pages every run.
    // Oracle only treats rich sidecars as cached; short listing snippets must
    // stay fetchable so responsibilities/qualifications can progressively land.
    function loadDescriptionCacheState(prefix, { minChars = 1, marker = null, trackShort = false } = {}) {
      const cachedIds = new Set();
      const priorityIds = new Set();
      try {
        const sidecarFiles = fs.readdirSync(DATA_DIR)
          .filter(f => f.startsWith(prefix) && f.endsWith('.jsonl'));
        for (const fname of sidecarFiles) {
          const lines = fs.readFileSync(path.join(DATA_DIR, fname), 'utf8').trim().split('\n').filter(Boolean);
          for (const line of lines) {
            try {
              const { id, description_text } = JSON.parse(line);
              const text = description_text || '';
              const rich = text.length >= minChars || (marker && marker.test(text));
              if (id && rich) cachedIds.add(id);
              else if (id && trackShort && text) priorityIds.add(id);
            } catch {}
          }
        }
        if (cachedIds.size > 0 || priorityIds.size > 0) {
          const source = prefix.replace('descriptions-', '');
          const cacheLabel = minChars > 1 ? `rich IDs (>=${minChars} chars or marker)` : 'IDs';
          const priorityLabel = priorityIds.size > 0 ? `, ${priorityIds.size} short sidecar IDs prioritized` : '';
          console.log(`  ${source} description cache: ${cachedIds.size} ${cacheLabel}${priorityLabel}`);
        }
      } catch (e) { /* no cache yet */ }
      return { cachedIds, priorityIds };
    }

    const googleCachedIds = loadDescriptionCacheState('descriptions-google').cachedIds;
    const appleCachedIds = loadDescriptionCacheState('descriptions-apple').cachedIds;
    const oracleCache = loadDescriptionCacheState('descriptions-oracle', { minChars: 2000, marker: /^(Responsibilities|Qualifications):/m, trackShort: true });


    // AGG-SPEED-2: Load WD totals cache from prior run
    const WD_TOTALS_CACHE = path.join(DATA_DIR, 'wd-totals-cache.json');
    try {
      if (fs.existsSync(WD_TOTALS_CACHE)) {
        wdPreviousTotals = JSON.parse(fs.readFileSync(WD_TOTALS_CACHE, 'utf8'));
        const cachedCount = Object.keys(wdPreviousTotals).length;
        if (cachedCount > 0) console.log(`  WD incremental cache: ${cachedCount} tenants`);
      }
    } catch (e) { /* first run or corrupt cache */ }

    // Phase A+B: Run ATS and custom fetchers in parallel (AGG-SPEED-5)
    // ~5.5 min savings: max(PhaseA, PhaseB) instead of PhaseA + PhaseB
    console.log('  Phase A+B: ATS + custom fetchers (parallel)...');
    const [phaseAResult, ...phaseBSettled] = await Promise.allSettled([
      withTimeout(fetchFromAllATS({ wdPreviousTotals }), 720_000, 'ATS'),
      withTimeout(fetchAllAmazonJobs(), 120_000, 'Amazon'),
      withTimeout(fetchAllNetflixJobs(), 60_000, 'Netflix'),
      HOTPATH_DEMOTED_FETCHERS.has('Apple')
        ? Promise.resolve([])
        : withTimeout(fetchAllAppleJobs({ previousJobCount: prevAppleCount, previousJobIds: prevAppleIds, cachedDescriptionIds: appleCachedIds, dataDir: DATA_DIR }), 1200_000, 'Apple'),
      withTimeout(fetchAllTwoSigmaJobs(), 30_000, 'Two Sigma'),
      withTimeout(fetchAllUberJobs(), 60_000, 'Uber'),
      HOTPATH_DEMOTED_FETCHERS.has('Google')
        ? Promise.resolve([])
        : withTimeout(fetchAllGoogleJobs({ previousJobCount: prevGoogleCount, cachedDescriptionIds: googleCachedIds, dataDir: DATA_DIR }), 600_000, 'Google'),
      withTimeout(fetchAllSimplifyJobs(), 30_000, 'SimplifyJobs'),
      HOTPATH_DEMOTED_FETCHERS.has('Microsoft')
        ? Promise.resolve([])
        : withTimeout(fetchAllMicrosoftJobs({ previousJobCount: prevMicrosoftCount, cachedDescriptionIds: microsoftCachedIds }), 600_000, 'Microsoft'),
      HOTPATH_DEMOTED_FETCHERS.has('Oracle')
        ? Promise.resolve([])
        : withTimeout(fetchAllOracleJobs(JSON.parse(fs.readFileSync(COMPANY_LIST_PATH, 'utf8')).oracle || undefined, { cachedDescriptionIds: oracleCache.cachedIds, priorityDescriptionIds: oracleCache.priorityIds }), 600_000, 'Oracle'),
      withTimeout(fetchAllAmdJobs(), 120_000, 'AMD'),
      HOTPATH_DEMOTED_FETCHERS.has('TikTok')
        ? Promise.resolve([])
        : withTimeout(fetchAllTiktokJobs(), 120_000, 'TikTok'),
    ]);

    if (HOTPATH_DEMOTED_FETCHERS.size > 0) {
      console.log(`  AGG-HOTPATH-1: demoted from hot path -> ${[...HOTPATH_DEMOTED_FETCHERS].join(', ')}`);
    }

    // Collect ATS results
    const atsResult = phaseAResult.status === 'fulfilled' ? phaseAResult.value : { jobs: [] };
    appendAll(allJobs, atsResult.jobs);
    console.log(`  ATS: ${atsResult.jobs.length} jobs`);

    // AGG-SPEED-2: Save WD totals cache for next run
    if (atsResult.wdCurrentTotals && Object.keys(atsResult.wdCurrentTotals).length > 0) {
      fs.writeFileSync(WD_TOTALS_CACHE, JSON.stringify(atsResult.wdCurrentTotals, null, 2));
      console.log(`  WD totals cache saved: ${Object.keys(atsResult.wdCurrentTotals).length} tenants`);
    }

    // Collect custom fetcher results
    const fetcherNames = ['Amazon', 'Netflix', 'Apple', 'Two Sigma', 'Uber', 'Google', 'SimplifyJobs', 'Microsoft', 'Oracle', 'AMD', 'TikTok'];
    const fetcherResults = {};
    phaseBSettled.forEach((result, i) => {
      const name = fetcherNames[i];
      const jobs = result.status === 'fulfilled' ? result.value : [];
      fetcherResults[name] = Array.isArray(jobs) ? jobs : [];
      appendAll(allJobs, fetcherResults[name]);
    });

    const supplementalInputs = loadSupplementalInputs();
    if (supplementalInputs.jobs.length > 0) {
      appendAll(allJobs, supplementalInputs.jobs);
      console.log(`  Supplemental lanes merged: ${supplementalInputs.jobs.length} jobs`);
      for (const [lane, info] of Object.entries(supplementalInputs.inputs)) {
        console.log(`   - ${lane}: ${info.jobs_loaded} jobs from supplemental artifact`);
      }
    }

    // Step 1a-post: apply cached Workday family/department mapping before tagging.
    const familyCacheReport = await applyFamilyCache(allJobs, JSON.parse(fs.readFileSync(COMPANY_LIST_PATH, 'utf8')).workday || [], DATA_DIR);
    stageTimings.step1a_family_cache_ms = familyCacheReport.durationMs;

    console.log('');
    console.log(`📊 Step 1 complete: ${allJobs.length} jobs fetched`);
    stageTimings.step1_fetch_ms = Date.now() - _stepStart;
    pipelineTimestamps.fetch_completed_at = new Date().toISOString();
    console.log(`   - ATS: ${atsResult.jobs.length} jobs`);
    for (const name of fetcherNames) {
      console.log(`   - ${name}: ${fetcherResults[name].length} jobs`);
    }

    // GAP-6: Compute zero-yield companies from raw fetch results (pre-filter).
    // Compares companies configured in company-list.json against companies that
    // produced jobs this run. A company returning 0 raw jobs may have a broken
    // slug, API change, or auth issue (LLNL incident class).
    const zeroYieldCompanies = computeZeroYield(atsResult, fetcherResults, COMPANY_LIST_PATH, atsResult.wdCurrentTotals);
    if (zeroYieldCompanies.length > 0) {
      console.log(`   ⚠️  GAP-6: ${zeroYieldCompanies.length} companies returned 0 raw jobs`);
    }

    // AGG-PIPE-4: Build set of sources that fetched successfully this run.
    // Used by mergeCarryForward to detect closed jobs (source fetched OK, job absent = closed).
    const successfulSources = new Set();
    for (const [source, count] of Object.entries((atsResult.stats || {}).by_source || {})) {
      if (count > 0) successfulSources.add(source);
    }
    for (const [fetcherName, jobs] of Object.entries(fetcherResults)) {
      const sourceKey = FETCHER_NAME_TO_SOURCE[fetcherName] || fetcherName.toLowerCase();
      if (Array.isArray(jobs) && jobs.length > 0) successfulSources.add(sourceKey);
    }
    for (const source of supplementalInputs.sourcesUsed) {
      successfulSources.add(source);
    }
    if (successfulSources.size > 0) {
      console.log(`   🔍 AGG-PIPE-4: ${successfulSources.size} sources fetched successfully: ${[...successfulSources].join(', ')}`);
    }

    console.log('');

    // AGG-HOTPATH-1: Workday description fetch is removed from the hot publish path.
    // Enrichment/sidecar lanes own this slower detail work; the fast publish path must
    // preserve Tier A freshness under the 15-minute hard cap.
    const wdJobs = allJobs.filter(j => j.source === 'workday');
    if (wdJobs.length > 0) {
      console.log(`📄 Step 1b: Skipping WD descriptions in hot path (${wdJobs.length} WD jobs)`);
    } else {
      console.log('📄 Step 1b: No WD jobs this run — skipping description fetch');
    }
    console.log('');

    // Step 2: Enhance jobs (add fingerprints, employment_types arrays, etc.)
    console.log('🔄 Step 2: Enhancing jobs with required fields...');
    _stepStart = Date.now();
    console.log('━'.repeat(60));

    // Add missing fields (fingerprints, normalize employment_types to arrays)
    const helpers = require(`${SHARED}/utils/helpers`);
    const enhancedJobs = allJobs.map(job => {
      // Add fingerprint if missing
      if (!job.fingerprint) {
        job.fingerprint = helpers.generateFingerprint(job);
      }

      // AGG-DATA-13: Normalize employment_type/employment_types to canonical array (AGG-PIPE-13: shared constant)
      if (!job.employment_types) {
        const types = job.employment_type || [];
        if (Array.isArray(types)) {
          job.employment_types = types.map(t => EMPLOYMENT_NORMALIZE_MAP[String(t).toUpperCase()] || String(t).toUpperCase());
        } else if (typeof types === 'string') {
          job.employment_types = types.split(',').map(t => EMPLOYMENT_NORMALIZE_MAP[t.trim().toUpperCase()] || t.trim().toUpperCase());
        } else {
          job.employment_types = [];
        }
      } else {
        // Carry-forward: re-normalize existing array
        job.employment_types = job.employment_types.map(t => EMPLOYMENT_NORMALIZE_MAP[t] || t);
      }

      return job;
    });

    console.log('');
    console.log(`✅ Step 2 complete: ${enhancedJobs.length} jobs enhanced`);
    stageTimings.step2_enhance_ms = Date.now() - _stepStart;
    console.log('');

    // Step 3: Validate and fix malformed fields
    console.log('📝 Step 3: Validating and fixing malformed fields...');
    _stepStart = Date.now();
    console.log('━'.repeat(60));

    const { validJobs, invalidJobs, metrics: validationMetrics } = validateAndNormalizeJobs(enhancedJobs);

    console.log('');
    printValidationSummary(validationMetrics);
    console.log('');
    console.log(`✅ Step 3 complete: ${validJobs.length} valid jobs (${invalidJobs.length} filtered)`);
    stageTimings.step3_validate_ms = Date.now() - _stepStart;
    console.log('');

    // Step 4: Filter senior jobs
    console.log('🎓 Step 4: Filtering senior-level jobs...');
    _stepStart = Date.now();
    console.log('━'.repeat(60));

    const { entryLevelJobs, seniorJobs, metrics: seniorFilterMetrics } = filterSeniorJobs(validJobs, companyOverrideMap);

    console.log('');
    printSeniorFilterSummary(seniorFilterMetrics);
    console.log('');
    const overrideCount = seniorFilterMetrics.override_applied || 0;
    console.log(`✅ Step 4 complete: ${entryLevelJobs.length} entry-level jobs (${seniorJobs.length} senior filtered${overrideCount > 0 ? `, ${overrideCount} overrides applied` : ''})`);
    stageTimings.step4_filter_ms = Date.now() - _stepStart;
    console.log('');

    // Step 4b: Write senior-filter analytics summary (PIPELINE-1)
    // Summary counts only — full job objects are ~180 MB and exceed GitHub's 100 MB limit.
    // Tags not yet available (Step 5), so breakdown is by source only.
    const FILTERED_OUTPUT_FILE = path.join(DATA_DIR, 'filtered_jobs.json');
    const seniorBySource = {};
    for (const job of seniorJobs) {
      seniorBySource[job.source || 'unknown'] = (seniorBySource[job.source || 'unknown'] || 0) + 1;
    }

    // AGG-SELF-4 Check C: FP rate tracking for trend alerting
    let fpStats = { sample_size: 0, potential_fp_count: 0, fp_rate_pct: '0.0' };
    // AGG-DATA-8 / AGG-PIPE-11: Sample 500 filtered jobs for false-positive measurement.
    // 500 jobs gives ±4.3pp CI (vs ±14pp with 50). File rotated weekly (7-day TTL).
    {
      const SAMPLE_SIZE = 500;
      const SAMPLES_FILE = path.join(DATA_DIR, 'filtered-samples.jsonl');
      const now = new Date();

      // Rotate: remove entries older than 7 days
      let existingLines = [];
      if (fs.existsSync(SAMPLES_FILE)) {
        const cutoff = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000).toISOString();
        existingLines = fs.readFileSync(SAMPLES_FILE, 'utf8').trim().split('\n')
          .filter(line => { try { return JSON.parse(line).sampled_at >= cutoff; } catch { return false; } });
      }

      // Random sample without bias (Fisher-Yates partial shuffle)
      const sampleIndices = [];
      if (seniorJobs.length <= SAMPLE_SIZE) {
        sampleIndices.push(...Array.from({ length: seniorJobs.length }, (_, i) => i));
      } else {
        const pool = Array.from({ length: seniorJobs.length }, (_, i) => i);
        for (let i = 0; i < SAMPLE_SIZE; i++) {
          const j = i + Math.floor(Math.random() * (pool.length - i));
          [pool[i], pool[j]] = [pool[j], pool[i]];
          sampleIndices.push(pool[i]);
        }
      }

      // AGG-PIPE-11: Flag potential FPs — jobs where title has no senior keyword.
      // Not definitive (could be filtered by experience in description), but surfaces
      // likely FPs for review. A high potential_fp rate warrants investigation.
      const SENIOR_TITLE_RE = /\b(senior|sr\.?|lead|principal|staff|director|vp|vice president|head of|chief|manager|mgr\.?)\b/i;

      const newSamples = sampleIndices.map(idx => {
        const job = seniorJobs[idx];
        const hasSeniorKeyword = SENIOR_TITLE_RE.test(job.title || '');
        return {
          sampled_at: now.toISOString(),
          id: job.id,
          title: job.title,
          company_name: job.company_name,
          source: job.source,
          location: job.location || null,
          filter_reason: job._filter_reason || 'unknown',
          potential_fp: !hasSeniorKeyword,
        };
      });

      const fpCount = newSamples.filter(s => s.potential_fp).length;
      const fpRate = newSamples.length > 0 ? (fpCount / newSamples.length * 100).toFixed(1) : '0.0';
      console.log(`📋 FP estimate: ${fpCount}/${newSamples.length} (${fpRate}%) potential false positives in sample`);
      fpStats = { sample_size: newSamples.length, potential_fp_count: fpCount, fp_rate_pct: fpRate };

      const allLines = [...existingLines, ...newSamples.map(s => JSON.stringify(s))];
      fs.writeFileSync(SAMPLES_FILE, allLines.join('\n') + '\n', 'utf8');
      console.log(`📋 Step 4b-2: Filtered samples → filtered-samples.jsonl (${newSamples.length} sampled, ${allLines.length} total)`);
    }

    // Write summary AFTER fpStats is computed
    const filteredSummary = {
      generated: new Date().toISOString(),
      total_senior_filtered: seniorJobs.length,
      by_source: seniorBySource,
      ...fpStats,
    };
    fs.writeFileSync(FILTERED_OUTPUT_FILE, JSON.stringify(filteredSummary, null, 2), 'utf8');
    console.log(`📋 Step 4b: Senior-filter summary → filtered_jobs.json (${seniorJobs.length} total)`);

    console.log('');

    // Step 4c: Inject descriptions from ALL sidecar files for tag engine's description fallback.
    // Carried-forward jobs lose their inline descriptions in Step 9 (stripped from all_jobs.json).
    // This re-injects them from per-source sidecar files so the description-fallback layer can classify.
    // Guard: !job.description prevents double-injection for freshly-fetched jobs with inline descriptions.
    // TAG-9 S237: expanded from enriched-only to ALL sidecars — 1,317 additional jobs gain descriptions.
    const descSidecarFiles = fs.readdirSync(DATA_DIR)
      .filter(f => f.startsWith('descriptions-') && f.endsWith('.jsonl'));
    if (descSidecarFiles.length > 0) {
      const descMap = new Map();
      for (const fname of descSidecarFiles) {
        const fpath = path.join(DATA_DIR, fname);
        const descLines = fs.readFileSync(fpath, 'utf8').trim().split('\n').filter(Boolean);
        for (const line of descLines) {
          try {
            const { id, description_text } = JSON.parse(line);
            if (id && description_text) descMap.set(id, description_text);
          } catch { /* skip malformed */ }
        }
      }
      let injected = 0;
      for (const job of entryLevelJobs) {
        if (!job.description && descMap.has(job.id)) {
          job.description = descMap.get(job.id);
          injected++;
        }
      }
      console.log(`📄 Step 4c: Injected ${injected} descriptions from ${descSidecarFiles.length} sidecar files (${descMap.size} available)`);
    } else {
      console.log('📄 Step 4c: No description sidecar files found — description fallback inactive');
    }
    console.log('');

    // Step 5: Apply tags
    console.log('🏷️  Step 5: Applying tags...');
    _stepStart = Date.now();
    console.log('━'.repeat(60));

    const taggedJobs = tagJobs(entryLevelJobs);

    console.log(`✅ Step 5 complete: ${taggedJobs.length} jobs tagged`);
    stageTimings.step5_tag_ms = Date.now() - _stepStart;
    console.log('');

    // Step 6: Deduplicate
    console.log('🔍 Step 6: Deduplicating jobs...');
    _stepStart = Date.now();
    console.log('━'.repeat(60));

    const { unique: dedupedJobs, duplicates, stats: dedupeStats } = deduplicateJobs(taggedJobs);

    console.log('');
    console.log(`✅ Step 6 complete: ${dedupedJobs.length} unique jobs (${duplicates} duplicates removed)`);
    stageTimings.step6_dedup_ms = Date.now() - _stepStart;
    console.log('');

    // Step 7: Tag statistics deferred to post-merge (after Step 9).
    // Previously computed from dedupedJobs (current-run only), missing ~4K carry-forward jobs.
    // Now computed after carry-forward merge + AGG-32 stale filter for full-pool accuracy.
    console.log('📊 Step 7: Tag statistics deferred to post-merge');

    let tagStats = null;
    console.log('');

    // Step 8: Sort by date (newest first)
    console.log('📊 Step 8: Sorting jobs by date...');
    _stepStart = Date.now();
    console.log('━'.repeat(60));

    const sortedJobs = dedupedJobs.sort((a, b) => {
      const dateA = new Date(a.posted_at || 0);
      const dateB = new Date(b.posted_at || 0);
      return dateB - dateA; // Newest first
    });

    console.log(`✅ Step 8 complete: Jobs sorted`);
    stageTimings.step8_sort_ms = Date.now() - _stepStart;
    console.log('');

    // Step 8b: Write per-source description sidecars (AGG-PIPE-13: extracted to sidecar-writer.js)
    console.log('📄 Step 8b: Writing description sidecars...');
    _stepStart = Date.now();
    const { writtenFiles: sidecarFiles, stats: sidecarStats } = writeSidecars(sortedJobs, DATA_DIR);
    stageTimings.step8b_sidecars_ms = Date.now() - _stepStart;
    pipelineTimestamps.sidecars_written_at = new Date().toISOString();
    console.log('');

    // Step 9: Write output files
    console.log('💾 Step 9: Writing output files...');
    _stepStart = Date.now();
    console.log('━'.repeat(60));

    // Strip pipeline internals before writing public output file
    // (source_url, source_id, _raw are internal — not needed downstream)
    // Note: 'source' is kept for downstream observability (which ATS produced each job)
    const STRIP_FIELDS = ['source_url', '_raw', 'description', 'enriched', 'enriched_at', 'is_internship', 'is_new_grad', 'is_us_only', 'remote'];
    let publicJobs = sortedJobs.map(job => {
      const stripped = { ...job };
      for (const field of STRIP_FIELDS) {
        delete stripped[field];
      }
      return stripped;
    });

    // Merge previous all_jobs.json into current run (rolling window — TTL from deduplicator)
    // Jobs from prior runs that weren't re-fetched this run are preserved until their TTL expires.
    if (fs.existsSync(JOBS_OUTPUT_FILE)) {
      const currentIds = new Set(publicJobs.map(j => j.id));
      // Fingerprint guard: prevents re-injection of jobs that changed ID (e.g. WD-ID-BUG fix)
      const currentFingerprints = new Set(publicJobs.map(j => j.fingerprint).filter(Boolean));
      const prevLines = fs.readFileSync(JOBS_OUTPUT_FILE, 'utf8').trim().split('\n').filter(Boolean);

      // AGG-32: Filter stale jobs by posted_at TTL
      resolvePostedAt(publicJobs, prevLines);

      mergeCarryForward(publicJobs, prevLines, currentIds, currentFingerprints, STRIP_FIELDS, successfulSources, atsResult.health || {});
    }

    // Generate tag stats from full pool (post-merge + post-AGG-32 filter).
    tagStats = computeFullPoolTagStats(publicJobs);

    // Tag monitoring diagnostics (AGG-PIPE-13: extracted to monitoring.js)
    const monitoringReports = runTagMonitoring(publicJobs, tagStats, {
      dataDir: DATA_DIR,
      checkTagDrift, printDriftReport,
      tagDomainsFn: tagDomains,
      checkDomainPrecision, printPrecisionReport,
      checkKeywordHealth, checkKeywordOverlap,
      getKeywordMap, tagEngineVersion: TAG_ENGINE_VERSION,
      seniorJobs, companyOverrideMap,
    });
    ({ tagDriftReport, tagPrecisionReport, keywordHealthReport, keywordOverlapReport } = monitoringReports);

    // Archive expiring jobs BEFORE overwriting all_jobs.json
    const { getExpiringJobs, appendToWeeklyArchive, appendToDailyArchive } = require(`${SHARED}/utils/archiver`);
    const ARCHIVE_DIR = path.join(DATA_DIR, 'archive');
    const expiringJobs = getExpiringJobs(JOBS_OUTPUT_FILE, publicJobs);
    if (expiringJobs.length > 0) {
      const weeklyFile = appendToWeeklyArchive(expiringJobs, ARCHIVE_DIR);
      const dailyFile = appendToDailyArchive(expiringJobs, ARCHIVE_DIR);
      console.log(`📦 Archived ${expiringJobs.length} expiring jobs → weekly: ${path.basename(weeklyFile)}, daily: ${path.basename(dailyFile)}`);
    } else {
      console.log('📦 No expiring jobs this run');
    }

    // AGG-PIPE-10: Filter senior-tagged jobs before writing output.
    // Step 4 (senior-filter) runs before Step 5 (tag-engine), so tag-engine independently
    // tags seniors that the filter missed. This write-step filter catches them at the output boundary.
    const preFilterCount = publicJobs.length;
    publicJobs = publicJobs.filter(job => job.tags?.employment !== 'senior');
    const seniorLeaked = preFilterCount - publicJobs.length;
    if (seniorLeaked > 0) {
      console.log(`🛡️  AGG-PIPE-10: Filtered ${seniorLeaked} senior-tagged jobs from output (passed senior-filter but tagged senior by tag-engine)`);
    }

    // Write jobs (JSONL format)
    await writeJobsJSONL(publicJobs, JOBS_OUTPUT_FILE);

    const usJobs = buildUsSnapshotJobs(publicJobs);
    await writeJobsJSONL(usJobs, US_JOBS_OUTPUT_FILE);
    console.log(`🇺🇸 AGG-US-SNAPSHOT-1: Wrote ${usJobs.length} US-tagged jobs to us_jobs.json`);

    // Write metadata
    // Use publicJobs (full 7-day rolling window) for pool-level stats (by_source, top_companies, freshness).
    // sortedJobs is current-run only — stats must use publicJobs (full 7-day window).
    const duration = Date.now() - startTime;
    stageTimings.step9_write_ms = Date.now() - _stepStart;
    pipelineTimestamps.output_ready_at = new Date().toISOString();
    // Build fetch_results: per-source counts from current fetch attempts (before carry-forward).
    // Demoted hot-path sources are intentionally absent: they were not attempted in this
    // workflow, so reporting 0 would create a false "source fetch failure" alert.
    const fetchResults = {};
    for (const [source, count] of Object.entries((atsResult.stats || {}).by_source || {})) {
      if (count > 0) fetchResults[source] = (fetchResults[source] || 0) + count;
    }
    for (const [fetcherName, jobs] of Object.entries(fetcherResults)) {
      if (HOTPATH_DEMOTED_FETCHERS.has(fetcherName)) continue;
      const sourceKey = FETCHER_NAME_TO_SOURCE[fetcherName] || fetcherName.toLowerCase();
      fetchResults[sourceKey] = Array.isArray(jobs) ? jobs.length : 0;
    }

    // AGG-FETCH-14: Build fetcher_health from ATS + custom fetcher results.
    // Enables check-19 to classify zero-yield companies without HTTP probing.
    const fetcherHealth = {};
    const healthNow = new Date().toISOString();
    // ATS health (from ats-fetcher.js)
    if (atsResult.health) {
      Object.assign(fetcherHealth, atsResult.health);
    }
    // Custom fetcher health. Demoted sources are marked as skipped, not zero:
    // "zero" means attempted successfully and returned no jobs.
    for (let i = 0; i < fetcherNames.length; i++) {
      const name = fetcherNames[i];
      const source = FETCHER_NAME_TO_SOURCE[name] || name.toLowerCase();
      if (HOTPATH_DEMOTED_FETCHERS.has(name)) {
        fetcherHealth[name] = {
          status: 'skipped',
          source,
          jobs: null,
          reason: 'hot_path_demoted',
          timestamp: healthNow,
        };
        continue;
      }
      const result = phaseBSettled[i];
      if (!result) continue;
      const jobs = result.status === 'fulfilled' ? result.value : [];
      const count = Array.isArray(jobs) ? jobs.length : 0;
      fetcherHealth[name] = {
        status: result.status === 'rejected' ? 'error' : count > 0 ? 'alive' : 'zero',
        source,
        jobs: count,
        timestamp: healthNow,
        ...(result.status === 'rejected' ? { detail: result.reason?.message } : {}),
      };
    }

    const metadata = generateMetadata({
      startTime,
      jobs: publicJobs,
      uniqueCount: dedupedJobs.length,
      duplicateCount: duplicates,
      duration,
      tagStats,
      validationMetrics,
      seniorFilterMetrics,
      seniorJobs,
      zeroYieldCompanies,
      stageTimings,
      pipelineTimestamps,
      tagDriftReport,
      tagPrecisionReport,
      keywordHealthReport,
      keywordOverlapReport,
      fpStats,
      fetchResults,
      fetcherHealth,
      supplementalInputs: supplementalInputs.inputs,
    });
    await writeMetadata(metadata, METADATA_OUTPUT_FILE);

    // Step 9c: build / refresh Workday family cache for future runs. Output is already written, so
    // cache refresh cannot block user-visible publish correctness.
    if (!isDryRun && !SKIP_WD_FAMILY_CACHE_BUILD) {
      const familyCacheBuildReport = await buildFamilyCache(JSON.parse(fs.readFileSync(COMPANY_LIST_PATH, 'utf8')).workday || [], DATA_DIR);
      stageTimings.step9c_family_cache_build_ms = familyCacheBuildReport.durationMs;
    } else {
      stageTimings.step9c_family_cache_build_ms = 0;
      const reason = isDryRun ? 'dry run' : 'SKIP_WD_FAMILY_CACHE_BUILD=1';
      console.log(`⏭️  Step 9c: Skipping Workday family cache build (${reason})`);
    }

    console.log('');
    console.log(`✅ Step 9 complete: Output files written`);
    console.log('');

    // Step 10: Print summary
    printSummary(sortedJobs, dedupedJobs.length, duplicates, duration);

    // Step 11: Print tag distribution
    printTagDistribution(sortedJobs);

    // Step 12: Git commit (unless dry run)
    _stepStart = Date.now();
    if (!isDryRun) {
      console.log('📝 Step 12: Committing to git...');
      console.log('━'.repeat(60));

      await gitCommit(sortedJobs.length);

      console.log('');
      console.log(`✅ Step 12 complete: Changes committed`);
      stageTimings.step12_commit_ms = Date.now() - _stepStart;
    } else {
      console.log('⏭️  Step 12: Skipping git commit (dry run)');
      stageTimings.step12_commit_ms = 0;
    }

    console.log('');
    console.log('═'.repeat(60));
    console.log('🎉 Jobs Data Fetcher - Complete!');
    console.log('═'.repeat(60));

    process.exit(0);

  } catch (error) {
    console.error('');
    console.error('❌ Fatal error:');
    console.error(error.message);
    console.error('');
    console.error('Stack trace:');
    console.error(error.stack);
    process.exit(1);
  }
}

/**
 * GAP-6: Compute companies configured in company-list.json that returned 0 raw jobs.
 * Uses company_name from ATS stats (not slug) to match against configured company names.
 * Excludes custom-fetcher companies (Apple, Amazon, etc.) — only ATS companies are checked.
 * @param {Object} atsResult - ATS fetch result with stats.by_company
 * @param {Object} fetcherResults - Custom fetcher results by name
 * @param {string} companyListPath - Path to company-list.json
 * @returns {Array<string>} Company names that returned 0 raw jobs
 */
function computeZeroYield(atsResult, fetcherResults, companyListPath, wdCache) {
  try {
    const companyList = JSON.parse(fs.readFileSync(companyListPath, 'utf8'));

    // Build set of company names that produced jobs this run (ATS only)
    const companiesWithJobs = new Set(Object.keys((atsResult.stats || {}).by_company || {}));

    // AGG-ZEROYIELD-1: Include WD tenants from incremental cache.
    // When the cache skips a tenant, it doesn't appear in atsResult.by_company,
    // but it still has jobs (verified by wd-totals-cache.json).
    if (wdCache && typeof wdCache === 'object') {
      for (const [name, count] of Object.entries(wdCache)) {
        if (count > 0) companiesWithJobs.add(name);
      }
    }

    // AGG-SR-NAME-1: Build slug set for SR name mismatch.
    // SR API returns legal names (e.g. "RE/SPEC Inc.") that differ from config names (e.g. "RESPEC").
    // Matching by slug resolves 6 false positives per run.
    const slugsWithJobs = new Set();
    if (Array.isArray(atsResult.jobs)) {
      for (const job of atsResult.jobs) {
        if (job.company_slug) slugsWithJobs.add(job.company_slug);
      }
    }

    // Build set of configured company names per ATS source
    const zeroYield = [];
    const sources = [
      { key: 'greenhouse', entries: companyList.greenhouse },
      { key: 'lever', entries: companyList.lever },
      { key: 'ashby', entries: companyList.ashby },
      { key: 'workday', entries: companyList.workday },
      { key: 'smartrecruiters', entries: companyList.smartrecruiters },
    ];

    for (const { key, entries } of sources) {
      if (!Array.isArray(entries)) continue;
      for (const entry of entries) {
        const name = entry.name;
        const slug = entry.slug;
        const hasName = name && companiesWithJobs.has(name);
        const hasSlug = slug && slugsWithJobs.has(slug);
        if (!hasName && !hasSlug) {
          zeroYield.push(`${name} (${key})`);
        }
      }
    }

    return zeroYield;
  } catch (e) {
    console.warn(`⚠️ GAP-6: Could not compute zero-yield companies: ${e.message}`);
    return [];
  }
}

/**
 * Generate metadata object
 * @param {Array} jobs - All jobs
 * @param {number} uniqueCount - Unique job count
 * @param {number} duplicateCount - Duplicate count
 * @param {number} duration - Duration in ms
 * @param {Object} tagStats - Tag statistics from tag engine
 * @param {Object} validationMetrics - Validation metrics
 * @param {Object} seniorFilterMetrics - Senior filter metrics
 * @returns {Object} - Metadata object
 */


const SOURCE_TIER_POLICY = {
  workday: 'tier_a_core',
  greenhouse: 'tier_a_core',
  ashby: 'tier_a_core',
  lever: 'tier_a_optional',
  smartrecruiters: 'tier_b_async_material',
  oracle: 'tier_b_c_scrutiny',
  apple: 'tier_b_c_scrutiny',
  google: 'tier_b_c_scrutiny',
  microsoft: 'tier_b_c_scrutiny',
  amd: 'tier_b_c_scrutiny',
  amazon: 'tier_b_c_scrutiny',
  tiktok: 'tier_b_c_scrutiny',
  icims: 'tier_b_c_scrutiny',
  simplify: 'tier_c_fallback',
  jsearch: 'tier_c_fallback',
  eightfold: 'tier_c_fallback',
  uber: 'tier_c_fallback',
  twosigma: 'tier_c_fallback',
};

const TECH_US_DOMAINS = new Set(['software', 'data_science', 'hardware', 'ai', 'finance']);

function buildLatencyMarkers({ startTime, duration, stageTimings, pipelineTimestamps }) {
  return {
    pipeline_started_at: new Date(startTime).toISOString(),
    fetch_completed_at: pipelineTimestamps.fetch_completed_at || null,
    sidecars_written_at: pipelineTimestamps.sidecars_written_at || null,
    output_ready_at: pipelineTimestamps.output_ready_at || null,
    total_runtime_ms: duration,
    step_timings_ms: {
      step1_fetch_ms: stageTimings.step1_fetch_ms || 0,
      step2_enhance_ms: stageTimings.step2_enhance_ms || 0,
      step3_validate_ms: stageTimings.step3_validate_ms || 0,
      step4_filter_ms: stageTimings.step4_filter_ms || 0,
      step5_tag_ms: stageTimings.step5_tag_ms || 0,
      step6_dedup_ms: stageTimings.step6_dedup_ms || 0,
      step8_sort_ms: stageTimings.step8_sort_ms || 0,
      step8b_sidecars_ms: stageTimings.step8b_sidecars_ms || 0,
      step9_write_ms: stageTimings.step9_write_ms || 0,
      step12_commit_ms: stageTimings.step12_commit_ms || 0,
    },
  };
}

function buildSourceVisibilitySummary(jobs) {
  const now = Date.now();
  const sources = {};

  for (const job of jobs || []) {
    const source = job.source || 'unknown';
    const row = sources[source] || (sources[source] = {
      tier: SOURCE_TIER_POLICY[source] || 'unclassified',
      total_jobs: 0,
      tech_us_jobs: 0,
      freshness: {
        last_24h: 0,
        last_72h: 0,
      },
      newest_posted_at: null,
      oldest_posted_at: null,
    });

    row.total_jobs++;

    const domains = job.tags?.domains || [];
    const locations = job.tags?.locations || [];
    if (locations.includes('us') && domains.some(domain => TECH_US_DOMAINS.has(domain))) {
      row.tech_us_jobs++;
    }

    if (!job.posted_at) continue;
    const postedMs = new Date(job.posted_at).getTime();
    if (!Number.isFinite(postedMs)) continue;

    const ageMs = now - postedMs;
    if (ageMs <= 24 * 60 * 60 * 1000) row.freshness.last_24h++;
    if (ageMs <= 72 * 60 * 60 * 1000) row.freshness.last_72h++;

    if (!row.newest_posted_at || postedMs > new Date(row.newest_posted_at).getTime()) {
      row.newest_posted_at = new Date(postedMs).toISOString();
    }
    if (!row.oldest_posted_at || postedMs < new Date(row.oldest_posted_at).getTime()) {
      row.oldest_posted_at = new Date(postedMs).toISOString();
    }
  }

  return {
    generated_at: new Date().toISOString(),
    policy_version: 'source-tier-a141',
    metric_basis: 'final_public_pool',
    tech_us_definition: ['software', 'data_science', 'hardware', 'ai', 'finance'].join('+') + ' with us location tag',
    tiers: {
      tier_a_core: ['workday', 'greenhouse', 'ashby'],
      tier_a_optional: ['lever'],
      tier_b_async_material: ['smartrecruiters'],
      tier_b_c_scrutiny: ['oracle', 'apple', 'google', 'microsoft', 'amd', 'amazon', 'tiktok', 'icims'],
      tier_c_fallback: ['simplify', 'jsearch', 'eightfold', 'uber', 'twosigma'],
    },
    sources,
  };
}

function buildDescriptionDeliverySummary(jobs, dataDir) {
  const excludedSources = new Set(['enriched', 'workday', 'smartrecruiters']);
  const sidecarRows = {};
  const sidecarNonempty = {};
  const sidecarIds = new Map();

  try {
    const files = fs.readdirSync(dataDir)
      .filter(f => /^descriptions-.+\.jsonl$/.test(f))
      .filter(f => !f.startsWith('descriptions-enriched'))
      .filter(f => !f.startsWith('descriptions-workday'))
      .filter(f => !f.startsWith('descriptions-smartrecruiters'));
    for (const fname of files) {
      const source = fname.replace(/^descriptions-/, '').replace(/-\d+\.jsonl$/, '').replace(/\.jsonl$/, '').toLowerCase();
      sidecarRows[source] = sidecarRows[source] || 0;
      sidecarNonempty[source] = sidecarNonempty[source] || 0;
      if (!sidecarIds.has(source)) sidecarIds.set(source, new Set());
      const lines = fs.readFileSync(path.join(dataDir, fname), 'utf8').trim().split('\n').filter(Boolean);
      for (const line of lines) {
        sidecarRows[source]++;
        try {
          const row = JSON.parse(line);
          if (typeof row.description_text === 'string' && row.description_text.trim()) sidecarNonempty[source]++;
          if (row.id) sidecarIds.get(source).add(row.id);
        } catch {
          // malformed rows still count toward file size but not toward matches/nonempty text
        }
      }
    }
  } catch {
    // Missing sidecar files are reflected as zero coverage below.
  }

  const finalRows = {};
  const finalInlineDescriptions = {};
  const finalRowsWithSidecarMatch = {};
  for (const job of jobs || []) {
    const source = (job.source || 'unknown').toLowerCase();
    if (excludedSources.has(source)) continue;
    finalRows[source] = (finalRows[source] || 0) + 1;
    if (typeof job.description === 'string' && job.description.trim()) {
      finalInlineDescriptions[source] = (finalInlineDescriptions[source] || 0) + 1;
    }
    if (sidecarIds.get(source)?.has(job.id)) {
      finalRowsWithSidecarMatch[source] = (finalRowsWithSidecarMatch[source] || 0) + 1;
    }
  }

  const summary = {};
  const sources = new Set([
    ...Object.keys(sidecarRows),
    ...Object.keys(finalRows),
  ]);
  for (const source of [...sources].sort()) {
    const finalPool = finalRows[source] || 0;
    const matched = finalRowsWithSidecarMatch[source] || 0;
    const sidecars = sidecarRows[source] || 0;
    let mode = 'none_visible';
    if (finalPool === 0) mode = 'no_final_rows';
    else if (matched === finalPool && finalPool > 0) mode = 'sidecar_only';
    else if (matched > 0) mode = 'partial_sidecar_coverage';
    else if ((finalInlineDescriptions[source] || 0) > 0) mode = 'inline_only';
    summary[source] = {
      final_rows: finalPool,
      final_inline_description_rows: finalInlineDescriptions[source] || 0,
      sidecar_rows: sidecars,
      sidecar_nonempty_description_rows: sidecarNonempty[source] || 0,
      final_rows_with_sidecar_match: matched,
      coverage_pct: finalPool > 0 ? Math.round((matched / finalPool) * 1000) / 10 : null,
      mode,
    };
  }

  return {
    generated_at: new Date().toISOString(),
    basis: 'final_public_pool_vs_description_sidecars',
    sources: summary,
  };
}


function buildSeniorRolloutProjection(seniorJobs, seniorBySource) {
  const projection = {
    generated_at: new Date().toISOString(),
    sample_basis: {
      type: 'current_run_filtered_senior_jobs',
      total_senior_filtered: Array.isArray(seniorJobs) ? seniorJobs.length : 0,
    },
    by_source: seniorBySource || {},
    by_domain: {},
    by_surface: {
      ngj_main: 0,
      software: 0,
      data_science: 0,
      hardware: 0,
      healthcare: 0,
    },
    quality: {
      surface_projection_exact: false,
      surface_projection_method: 'directional_current_run_domain_location_mapping',
      notes: 'Directional projection for current-run senior-filtered jobs. Source counts are exact; domain and surface counts use tag-engine projection on filtered jobs and should guide rollout decisions, not exact publish counts.',
    },
  };

  if (!Array.isArray(seniorJobs) || seniorJobs.length === 0) return projection;

  const mainSurfaceDomains = new Set(['software', 'data_science', 'hardware', 'ai', 'finance']);
  for (const job of seniorJobs) {
    const domains = tagDomains(job);
    const normalizedDomains = domains.length > 0 ? domains : ['general'];
    for (const domain of normalizedDomains) {
      projection.by_domain[domain] = (projection.by_domain[domain] || 0) + 1;
    }

    const locations = tagLocations(job);
    const isUs = locations.includes('us');
    if (!isUs) continue;

    if (normalizedDomains.some(domain => mainSurfaceDomains.has(domain))) projection.by_surface.ngj_main++;
    if (normalizedDomains.includes('software')) projection.by_surface.software++;
    if (normalizedDomains.includes('data_science') || normalizedDomains.includes('ai')) projection.by_surface.data_science++;
    if (normalizedDomains.includes('hardware')) projection.by_surface.hardware++;
    if (normalizedDomains.includes('healthcare')) projection.by_surface.healthcare++;
  }

  return projection;
}

/**
 * TAG-DIM-1: Build G1 by-domain breakdown for DASH visibility.
 * Categorizes US general jobs by source, company, fix category, engine version, employment type.
 * @param {Array} jobs - Full public pool (post-merge + post-AGG-32)
 * @returns {Object} g1_breakdown object for metadata
 */
function buildG1Breakdown(jobs) {
  const usJobs = jobs.filter(j => j.tags && j.tags.locations && j.tags.locations.includes('us'));
  const g1Jobs = usJobs.filter(j => j.tags.domains && j.tags.domains.includes('general'));

  if (g1Jobs.length === 0) return null;

  // By source
  const bySource = {};
  const sourceTotals = {};
  for (const j of usJobs) {
    sourceTotals[j.source] = (sourceTotals[j.source] || 0) + 1;
  }
  for (const j of g1Jobs) {
    bySource[j.source] = (bySource[j.source] || 0) + 1;
  }
  const bySourceFormatted = {};
  for (const [src, g1] of Object.entries(bySource)) {
    const total = sourceTotals[src] || 0;
    bySourceFormatted[src] = { g1, total, rate: total > 0 ? Math.round(g1 / total * 1000) / 10 : 0 };
  }

  // Top 20 companies
  const companyCounts = {};
  const companySource = {};
  const companyDepts = {};
  for (const j of g1Jobs) {
    const c = j.company_name || 'unknown';
    companyCounts[c] = (companyCounts[c] || 0) + 1;
    companySource[c] = j.source;
    companyDepts[c] = companyDepts[c] || j.departments?.length > 0;
  }
  const top20 = Object.entries(companyCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([company, g1]) => ({
      company,
      g1,
      source: companySource[company],
      has_departments: !!companyDepts[company],
    }));

  // Fix categories (heuristic based on source + departments)
  let needsFamilyMapping = 0; // WD jobs with no departments
  let familyMapGap = 0;       // WD jobs with departments but still G1
  let noDescription = 0;      // Simplify T0 with no desc
  let genuinelyAmbiguous = 0;  // Non-WD, non-Simplify, no domain signal
  let other = 0;
  for (const j of g1Jobs) {
    if (j.source === 'workday') {
      if (j.departments && j.departments.length > 0) {
        familyMapGap++;
      } else {
        needsFamilyMapping++;
      }
    } else if (j.source === 'simplify') {
      noDescription++;
    } else {
      // Check for domain keywords in title as signal
      const title = (j.title || '').toLowerCase();
      const hasSignal = /\b(engineer|developer|scientist|analyst|designer|manager|accountant|nurse|pharmacist|attorney|sales|marketing|operations|manufacturing|logistics|hardware|software|data|ai|product|finance|hr|legal)\b/i.test(title);
      if (hasSignal) {
        other++;
      } else {
        genuinelyAmbiguous++;
      }
    }
  }

  // Engine version distribution
  const byVersion = {};
  for (const j of g1Jobs) {
    const v = j.tags.tag_engine_version || 'none';
    byVersion['v' + v] = (byVersion['v' + v] || 0) + 1;
  }

  // Employment type distribution
  const byEmployment = {};
  for (const j of g1Jobs) {
    const e = j.tags.employment || 'unknown';
    byEmployment[e] = (byEmployment[e] || 0) + 1;
  }

  return {
    updated: new Date().toISOString(),
    us_total: usJobs.length,
    us_g1: g1Jobs.length,
    us_g1_rate: Math.round(g1Jobs.length / usJobs.length * 1000) / 10,
    by_source: bySourceFormatted,
    by_company_top20: top20,
    by_fix_category: {
      needs_family_mapping: { count: needsFamilyMapping, description: 'WD jobs with no departments — blocked on AGG-PIPE-16' },
      family_map_gap: { count: familyMapGap, description: 'WD jobs with departments but still G1' },
      genuinely_ambiguous: { count: genuinelyAmbiguous, description: 'Non-WD/Simplify jobs with no domain signal in title' },
      no_description: { count: noDescription, description: 'Simplify T0 jobs — no description for L4 fallback' },
      other: { count: other, description: 'Mixed ATS — some keyword gaps, some structural' },
    },
    by_engine_version: byVersion,
    by_employment: byEmployment,
  };
}

function generateMetadata({ startTime, jobs, uniqueCount, duplicateCount, duration, tagStats, validationMetrics, seniorFilterMetrics, seniorJobs, zeroYieldCompanies, stageTimings, pipelineTimestamps, tagDriftReport, tagPrecisionReport, keywordHealthReport, keywordOverlapReport, fpStats, fetchResults, fetcherHealth, supplementalInputs }) {
  const bySource = {};
  const byEmploymentType = {};
  const byInternship = { internship: 0, 'new-grad': 0, mid_level: 0 };
  const byRemote = { remote: 0, onsite: 0 };
  const companyCounts = {};
  const companyDomains = {};  // DASH-4b: track domain distribution per company

  const now = Date.now();
  const freshness = { last_1h: 0, last_6h: 0, last_24h: 0, last_48h: 0 };

  for (const job of jobs) {
    // Count by source
    bySource[job.source] = (bySource[job.source] || 0) + 1;

    // Count by employment type (AGG-DATA-13: normalize to canonical forms)
    // AGG-PIPE-13: shared EMPLOYMENT_NORMALIZE_MAP (includes compound types)
    const types = (job.employment_types || []).map(t => EMPLOYMENT_NORMALIZE_MAP[t] || t);
    if (Array.isArray(types)) {
      for (const type of types) {
        byEmploymentType[type] = (byEmploymentType[type] || 0) + 1;
      }
    }

    // Count by job type (use tags.employment — is_internship/is_new_grad stripped by STRIP_FIELDS)
    if (job.tags?.employment === 'internship') {
      byInternship.internship++;
    } else if (job.tags?.employment === 'entry_level') {
      byInternship['new-grad']++;
    } else {
      byInternship.mid_level++;
    }

    // Count by remote (use tags.locations — is_remote stripped by STRIP_FIELDS)
    if (job.tags?.locations?.includes('remote')) {
      byRemote.remote++;
    } else {
      byRemote.onsite++;
    }

    // Freshness buckets
    if (job.posted_at) {
      const ageMs = now - new Date(job.posted_at).getTime();
      if (ageMs <= 1 * 60 * 60 * 1000)  freshness.last_1h++;
      if (ageMs <= 6 * 60 * 60 * 1000)  freshness.last_6h++;
      if (ageMs <= 24 * 60 * 60 * 1000) freshness.last_24h++;
      if (ageMs <= 48 * 60 * 60 * 1000) freshness.last_48h++;
    }

    // Company counts + domain tracking (for top-N with domain)
    const co = job.company_name;
    if (co) {
      companyCounts[co] = (companyCounts[co] || 0) + 1;
      // DASH-4b: track primary domain per company
      const domains = (job.tags && job.tags.domains) || [];
      if (!companyDomains[co]) companyDomains[co] = {};
      for (const d of domains) {
        companyDomains[co][d] = (companyDomains[co][d] || 0) + 1;
      }
    }
  }

  // Top 20 companies by job count
  // DASH-4b: includes primary domain (most common domain tag for that company)
  const top_companies = Object.entries(companyCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([company, count]) => {
      const domains = companyDomains[company] || {};
      const primaryDomain = Object.entries(domains).sort((a, b) => b[1] - a[1])[0];
      return { company, count, domain: primaryDomain ? primaryDomain[0] : 'general' };
    });

  // Senior-filtered breakdown by source
  const seniorBySource = {};
  if (Array.isArray(seniorJobs)) {
    for (const job of seniorJobs) {
      const src = job.source || 'unknown';
      seniorBySource[src] = (seniorBySource[src] || 0) + 1;
    }
  }

  return {
    version: '1.0',
    generated: new Date().toISOString(),
    duration_ms: duration,

    total_jobs: jobs.length,
    unique_jobs: uniqueCount,
    duplicates_removed: duplicateCount,

    by_source: bySource,
    by_employment_type: byEmploymentType,
    by_job_type: byInternship,
    by_location: byRemote,

    ats_stats: getATSUsageStats(),

    // Validation statistics
    validation_stats: validationMetrics,

    // Senior filter statistics
    senior_filter_stats: {
      ...seniorFilterMetrics,
      by_source: seniorBySource,
      ...(fpStats || {}),
    },

    // AGG-MEASURE-1: Directional rollout projection for filtered senior jobs.
    senior_rollout_projection: buildSeniorRolloutProjection(seniorJobs, seniorBySource),

    // AGG-SOURCE-1: Source tier/value/freshness visibility for operator decisions.
    source_visibility: buildSourceVisibilitySummary(jobs),

    // AGG-SIDECAR-HEALTH-1: Producer-owned truth for sources whose downstream descriptions
    // depend on sidecars because all_jobs strips description text.
    description_delivery: buildDescriptionDeliverySummary(jobs, DATA_DIR),

    // AGG latency markers: producer-owned timing anchors for downstream latency measurement.
    latency_markers: buildLatencyMarkers({ startTime, duration, stageTimings, pipelineTimestamps }),

    // Tag statistics (Phase 1)
    tag_stats: tagStats,

    // TAG-SELF-2: Tag monitoring snapshots for metrics pipeline.
    tag_drift: tagDriftReport ? {
      drift_rate: tagDriftReport.drift_rate,
      sample_size: tagDriftReport.sample_size,
      drifted: tagDriftReport.drifted,
      warnings: tagDriftReport.warnings,
    } : null,
    tag_precision: tagPrecisionReport ? {
      domains: Object.fromEntries(
        Object.entries(tagPrecisionReport.domains).map(([d, r]) => [d, { total: r.total, fps: r.fps, fp_rate: r.fp_rate }])
      ),
      warnings: tagPrecisionReport.warnings,
    } : null,
    keyword_health: keywordHealthReport ? Object.fromEntries(
      Object.entries(keywordHealthReport.domains).map(([d, r]) => [d, {
        total_jobs: r.total_jobs,
        keyword_count: r.keyword_count,
        keywords_with_matches: r.keywords_with_matches,
        top_5: r.top_contributors.slice(0, 5).map(tc => ({ keyword: tc.keyword, matches: tc.matches, rate_pct: tc.rate_pct })),
        high_volume: r.high_volume,
      }])
    ) : null,
    keyword_overlap: keywordOverlapReport ? Object.fromEntries(
      Object.entries(keywordOverlapReport.domains).filter(([, r]) => r.foreign_keyword_overlaps > 0).map(([d, r]) => [d, {
        total_jobs: r.total_jobs,
        overlap_count: r.foreign_keyword_overlaps,
        top_overlaps: r.top_overlaps.slice(0, 3),
      }])
    ) : null,

    // TAG-DIM-1: G1 by-domain breakdown for DASH visibility.
    // Categorizes G1 jobs by source, company, fix category, engine version, and employment type.
    g1_breakdown: buildG1Breakdown(jobs),

    // Freshness — jobs posted within last N hours (entry-level pool)
    freshness,

    // Top 20 companies by job count (entry-level pool)
    top_companies,

    // GAP-6: Companies that returned 0 raw jobs this run (pre-filter).
    // Used by pipeline-alert.js for consecutive-failure detection.
    zero_yield_companies: zeroYieldCompanies || [],

    stage_timings: stageTimings || {},

    // AGG-HOTPATH-1: Sources intentionally excluded from the fast publish workflow.
    // Alerting must distinguish "not attempted here" from "attempted and fetched 0".
    hot_path_demoted_sources: [...HOTPATH_DEMOTED_SOURCES],

    supplemental_inputs: supplementalInputs || {},

    // TAG-DIM-1: Tag engine version deployed in this run.
    // Enables zjp-metrics to surface engine_version (was null because this field was missing).
    tag_engine_version: TAG_ENGINE_VERSION,

    // A91: Per-source fetch counts from current run (before carry-forward merge).
    // Enables alert checks to detect source fetch failures masked by carry-forward.
    fetch_results: fetchResults || {},

    // AGG-FETCH-14: Per-company fetcher health from current run.
    // status: 'alive' (has jobs), 'zero' (fetched ok but 0 jobs), 'error' (fetch failed).
    // Enables check-19 to classify without HTTP probing.
    fetcher_health: fetcherHealth || {},
  };
}

/**
 * Print execution summary
 * @param {Array} jobs - Final job array
 * @param {number} uniqueCount - Unique job count
 * @param {number} duplicateCount - Duplicate count
 * @param {number} duration - Duration in ms
 */
function printSummary(jobs, uniqueCount, duplicateCount, duration) {
  console.log('📊 Execution Summary:');
  console.log('━'.repeat(60));

  // Count by job type (use tag fields — is_internship/is_new_grad/is_remote are never set)
  const internships = jobs.filter(j => j.tags?.employment === 'internship').length;
  const newGrad = jobs.filter(j => j.tags?.employment === 'entry_level').length;
  const remote = jobs.filter(j => j.tags?.locations?.includes('remote')).length;

  console.log(`Total jobs in output: ${jobs.length}`);
  console.log(`  - Internships: ${internships}`);
  console.log(`  - New Grad: ${newGrad}`);
  console.log(`  - Remote: ${remote}`);
  console.log('');
  console.log(`Duplicates removed: ${duplicateCount}`);
  console.log(`Duration: ${(duration / 1000).toFixed(1)}s`);
}

/**
 * Commit changes to git
 * @param {number} jobCount - Number of jobs for commit message
 */
async function gitCommit(jobCount) {
  const { execSync } = require('child_process');

  try {
    // Configure git
    execSync('git config user.email "bot@zapplyjobs.com"');
    execSync('git config user.name "Data Bot"');

    // Add output files
    execSync('git add .github/data/all_jobs.json');
    execSync('git add .github/data/us_jobs.json');
    execSync('git add .github/data/jobs-metadata.json');
    execSync('git add .github/data/dedupe-store.json');
    execSync('git add .github/data/wd-totals-cache.json 2>/dev/null || true'); // AGG-SPEED-2: WD incremental fetch cache
    execSync('git add .github/data/filtered_jobs.json 2>/dev/null || true'); // senior-filter summary for analytics (PIPELINE-1)
    execSync('git add .github/data/filtered-samples.jsonl 2>/dev/null || true'); // AGG-DATA-8: sampled filtered jobs for FP spot-check
    // archive/ is NOT staged here — pushed separately to jobs-archive-private repo via workflow
    execSync('git add .github/data/descriptions-*.jsonl 2>/dev/null || true'); // per-source description sidecars (published)
    execSync('git add .github/data/tag-history.jsonl 2>/dev/null || true'); // TAG-SELF-2: tag drift/precision trend data
    // descriptions.jsonl is Workday fetch cache — NOT staged (local state only, managed by Step 1b)

    // Check if there are changes
    const status = execSync('git status --porcelain', { encoding: 'utf8' });

    if (!status.trim()) {
      console.log('ℹ️ No changes to commit');
      return;
    }

    // Create commit message
    const now = new Date();
    const dateStr = now.toISOString().split('T')[0];
    const timeStr = now.toTimeString().split(' ')[0].substring(0, 5);

    const commitMessage = `Update jobs - ${dateStr} ${timeStr}\n\n${jobCount} jobs in shared database`;

    // Commit
    execSync(`git commit -m "${commitMessage}"`);

    console.log(`✅ Committed: ${jobCount} jobs`);

  } catch (error) {
    console.error('⚠️ Git commit failed:', error.message);
    throw error;
  }
}

// Run main function
if (require.main === module) {
  main();
}

module.exports = { main, mergeCarryForward, RETIRED_CARRY_FORWARD_SOURCES, normalizeSupplementalJobForMerge, summarizeSupplementalLaneForMerge, buildDescriptionDeliverySummary, buildUsSnapshotJobs };
