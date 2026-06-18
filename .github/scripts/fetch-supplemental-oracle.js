#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
let SHARED = path.join(__dirname, 'aggregator', 'lib');
if (!fs.existsSync(SHARED)) {
  const siblingFallback = path.join(path.dirname(process.cwd()), 'job-board-aggregator', 'lib');
  if (fs.existsSync(siblingFallback)) SHARED = siblingFallback;
}
const { fetchAllOracleJobs } = require(`${SHARED}/fetchers/oracle`);

const DATA_DIR = path.join(process.cwd(), '.github', 'data');
const COMPANY_LIST_PATH = path.join(SHARED, 'fetchers', 'company-list.json');
const JOBS_FILE = path.join(DATA_DIR, 'supplemental-oracle-jobs.json');
const META_FILE = path.join(DATA_DIR, 'supplemental-oracle-metadata.json');
const PREV_US_JOBS_FILE = path.join(DATA_DIR, 'us_jobs.json');
const PREV_ALL_JOBS_FILE = path.join(DATA_DIR, 'all_jobs.json');
const TECH_DOMAINS = new Set(['software', 'data_science', 'hardware', 'ai']);

function hasR2Env() {
  return Boolean(process.env.R2_ACCESS_KEY_ID && process.env.R2_SECRET_ACCESS_KEY && process.env.R2_ENDPOINT && process.env.R2_BUCKET_NAME);
}

function isGitHubActions() {
  return process.env.GITHUB_ACTIONS === 'true';
}

function looksLikeRichOracleDescription(text) {
  if (!text || typeof text !== 'string') return false;
  return /^(Responsibilities|Qualifications):/m.test(text) || text.length >= 2000;
}

function parseJsonOrNdjson(text) {
  const trimmed = String(text || '').trim();
  if (!trimmed) return [];
  if (trimmed[0] === '[') {
    try {
      const parsed = JSON.parse(trimmed);
      return Array.isArray(parsed) ? parsed : [];
    } catch {}
  }
  return trimmed.split('\n').filter(Boolean).map(line => {
    try { return JSON.parse(line); } catch { return null; }
  }).filter(Boolean);
}

function isCurrentUsTechOracle(job) {
  if (!job || job.source !== 'oracle') return false;
  const tags = job.tags || {};
  const locations = tags.locations || [];
  const domains = tags.domains || [];
  return locations.includes('us') && domains.some(domain => TECH_DOMAINS.has(domain));
}

function loadOracleDetailCache() {
  const cachedIds = new Set();
  const priorityIds = new Set();
  try {
    const files = fs.readdirSync(DATA_DIR)
      .filter(f => f.startsWith('descriptions-oracle') && f.endsWith('.jsonl'));
    for (const file of files) {
      const lines = fs.readFileSync(path.join(DATA_DIR, file), 'utf8').trim().split('\n').filter(Boolean);
      for (const line of lines) {
        try {
          const { id, description_text } = JSON.parse(line);
          if (!id || !description_text) continue;
          if (looksLikeRichOracleDescription(description_text)) cachedIds.add(id);
          else priorityIds.add(id);
        } catch {}
      }
    }
  } catch {}
  if (cachedIds.size > 0 || priorityIds.size > 0) {
    console.log(`  Oracle detail cache: ${cachedIds.size} rich IDs, ${priorityIds.size} short sidecar IDs prioritized`);
  }
  return { cachedIds, priorityIds };
}

function resolveBoardJobsPath() {
  if (fs.existsSync(PREV_US_JOBS_FILE)) return PREV_US_JOBS_FILE;
  if (fs.existsSync(PREV_ALL_JOBS_FILE)) return PREV_ALL_JOBS_FILE;
  return null;
}

function loadCurrentBoardPriorityIds(cachedIds, existingPriorityIds, boardJobsPath = resolveBoardJobsPath()) {
  const priorityIds = new Set(existingPriorityIds);
  if (!boardJobsPath || !fs.existsSync(boardJobsPath)) return priorityIds;
  try {
    const rows = parseJsonOrNdjson(fs.readFileSync(boardJobsPath, 'utf8'));
    for (const job of rows) {
      if (!isCurrentUsTechOracle(job)) continue;
      if (cachedIds.has(job.id)) continue;
      priorityIds.add(job.id);
    }
  } catch {}
  if (priorityIds.size > existingPriorityIds.size) {
    const sourceLabel = path.basename(boardJobsPath);
    console.log(`  Oracle board priority IDs: +${priorityIds.size - existingPriorityIds.size} current US tech rows from ${sourceLabel}`);
  }
  return priorityIds;
}

async function uploadRequired(r2, name, file, contentType) {
  const uploaded = await r2.uploadRaw(name, fs.readFileSync(file, 'utf8'), contentType);
  if (!uploaded) {
    throw new Error(`R2 upload failed for ${name}`);
  }
}

async function main() {
  const start = Date.now();
  fs.mkdirSync(DATA_DIR, { recursive: true });

  const companyList = JSON.parse(fs.readFileSync(COMPANY_LIST_PATH, 'utf8'));
  const oracleCompanies = companyList.oracle || [];

  const oracleCache = loadOracleDetailCache();
  const priorityDescriptionIds = loadCurrentBoardPriorityIds(oracleCache.cachedIds, oracleCache.priorityIds);
  console.log(`🏛️ Supplemental Oracle lane: ${oracleCompanies.length} companies configured`);
  const jobs = await fetchAllOracleJobs(oracleCompanies, {
    cachedDescriptionIds: oracleCache.cachedIds,
    priorityDescriptionIds,
  });
  const durationMs = Date.now() - start;

  const payload = jobs.map(job => ({
    id: job.id,
    title: job.title,
    company_name: job.company_name,
    source: job.source,
    location: job.location,
    url: job.url,
    posted_at: job.posted_at,
    description: job.description || null,
  }));

  const metadata = {
    schema: 'supplemental-lane-v1',
    generated_at: new Date().toISOString(),
    lane_name: 'oracle',
    publish_contract: {
      blocks_fast_publish: false,
      included_in_main_all_jobs: false,
      merge_mode: 'separate_artifact',
      visibility: 'snapshot_only',
      expected_cadence_minutes: 15,
      max_staleness_minutes: 90,
    },
    source: 'oracle',
    companies_configured: oracleCompanies.length,
    jobs_fetched: payload.length,
    duration_ms: durationMs,
    current_board_priority_ids: priorityDescriptionIds.size,
  };

  fs.writeFileSync(JOBS_FILE, JSON.stringify(payload, null, 2), 'utf8');
  fs.writeFileSync(META_FILE, JSON.stringify(metadata, null, 2), 'utf8');

  console.log(`✅ Oracle supplemental lane wrote ${payload.length} jobs in ${Math.round(durationMs/1000)}s`);

  if (hasR2Env()) {
    try {
      const { createR2Client } = require(`${SHARED}/storage/r2-client`);
      const r2 = createR2Client({ prefix: 'data/' });
      await uploadRequired(r2, 'supplemental-oracle-jobs.json', JOBS_FILE, 'application/json');
      await uploadRequired(r2, 'supplemental-oracle-metadata.json', META_FILE, 'application/json');
      console.log('☁️ Uploaded Oracle supplemental artifacts to R2');
    } catch (err) {
      if (isGitHubActions()) {
        throw new Error(`Oracle supplemental R2 publish failed: ${err.message}`);
      }
      console.log(`⚠️ R2 upload unavailable locally — skipped (${err.message})`);
    }
  } else if (isGitHubActions()) {
    throw new Error('R2 env missing in GitHub Actions; refusing to mark Oracle supplemental publish successful');
  } else {
    console.log('⚠️ R2 env missing — skipped R2 upload (local mode)');
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error('❌ Supplemental Oracle lane failed:', err.message);
    process.exit(1);
  });
}

module.exports = {
  looksLikeRichOracleDescription,
  parseJsonOrNdjson,
  isCurrentUsTechOracle,
  loadCurrentBoardPriorityIds,
  resolveBoardJobsPath,
};
