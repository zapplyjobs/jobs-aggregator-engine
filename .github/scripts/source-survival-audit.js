#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

const TECH_DOMAINS = new Set(['software', 'data_science', 'hardware', 'ai']);
const STRUCTURAL_NO_DESCRIPTION = new Set(['simplify', 'eightfold', 'jsearch']);
const INLINE_DESCRIPTION_SOURCES = new Set(['greenhouse', 'lever', 'ashby', 'amazon', 'netflix', 'microsoft', 'oracle']);

function parseArgs(argv) {
  const args = { dataDir: path.join(process.cwd(), '.github', 'data'), json: false };
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--json') args.json = true;
    else if (arg === '--data-dir') args.dataDir = argv[++i];
    else if (arg.startsWith('--data-dir=')) args.dataDir = arg.slice('--data-dir='.length);
    else throw new Error(`Unknown argument: ${arg}`);
  }
  return args;
}

function readTextIfExists(file) {
  return fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : null;
}

function readJsonOrJsonl(file, fallback = null) {
  const text = readTextIfExists(file);
  if (text === null) return fallback;
  const trimmed = text.trim();
  if (trimmed[0] === '[' || trimmed[0] === '{') {
    try { return JSON.parse(trimmed); } catch { /* fall through to JSONL */ }
  }
  return trimmed.split('\n').filter(Boolean).map(line => JSON.parse(line));
}

function jobId(job) {
  return job.id || job.job_id;
}

function sourceOf(job) {
  return String(job.source || job.fetcher_type || 'unknown').toLowerCase();
}

function hasDescription(job) {
  return String(job.description || job.description_text || job.extraction_text || '').trim().length > 0;
}

function isTechUS(job) {
  const tags = job.tags || {};
  const domains = tags.domains || job.domain_tags || [];
  const locations = tags.locations || job.location_tags || [];
  return domains.some(domain => TECH_DOMAINS.has(domain)) && locations.includes('us');
}

function isInternship(job) {
  const employment = String(job.tags?.employment || job.employment_type || job.employment || '').toLowerCase();
  return employment === 'internship' || /\b(intern|internship|co[- ]?op|coop)\b/i.test(job.title || '');
}

function addSource(map, source) {
  const key = String(source || 'unknown').toLowerCase();
  if (!map.has(key)) {
    map.set(key, {
      source: key,
      fetched_raw: null,
      supplemental_input: 0,
      final_rows: 0,
      tech_us_rows: 0,
      internship_rows: 0,
      tech_us_internship_rows: 0,
      inline_description_rows: 0,
      posted_at_missing: 0,
      posted_at_basis: {},
      sidecar_rows: 0,
      enriched_rows: 0,
      enriched_with_skills: 0,
      sample_missing_posted_at: [],
      sample_no_description: [],
      flags: [],
    });
  }
  return map.get(key);
}

function incrementBasis(row, basis) {
  const key = basis || 'source_or_none';
  row.posted_at_basis[key] = (row.posted_at_basis[key] || 0) + 1;
}

function loadSidecarCoverage(dataDir, allJobsById) {
  const coverageBySource = new Map();
  if (!fs.existsSync(dataDir)) return coverageBySource;

  for (const file of fs.readdirSync(dataDir)) {
    if (!/^descriptions-.+\.jsonl$/.test(file)) continue;
    const fullPath = path.join(dataDir, file);
    const rows = readJsonOrJsonl(fullPath, []);
    const sourceFromFile = file
      .replace(/^descriptions-/, '')
      .replace(/\.jsonl$/, '')
      .replace(/-\d+$/, '');

    for (const row of rows) {
      const id = jobId(row);
      if (!id || !allJobsById.has(id)) continue;
      const source = sourceFromFile.startsWith('enriched')
        ? sourceOf(allJobsById.get(id) || {})
        : sourceFromFile;
      if (!source || source === 'unknown') continue;
      if (!coverageBySource.has(source)) coverageBySource.set(source, new Set());
      coverageBySource.get(source).add(id);
    }
  }
  return coverageBySource;
}

function ratio(numerator, denominator) {
  if (!denominator) return null;
  return numerator / denominator;
}

function pct(value) {
  return value === null ? null : Number((value * 100).toFixed(1));
}

function buildAudit(dataDir) {
  const metadata = readJsonOrJsonl(path.join(dataDir, 'jobs-metadata.json'));
  const allJobs = readJsonOrJsonl(path.join(dataDir, 'all_jobs.json'), []);
  const enrichedJobs = readJsonOrJsonl(path.join(dataDir, 'enriched_jobs.json'), []);

  const allJobsById = new Map(allJobs.map(job => [jobId(job), job]).filter(([id]) => id));
  const sources = new Map();

  for (const [source, count] of Object.entries(metadata.fetch_results || {})) {
    addSource(sources, source).fetched_raw = count;
  }

  for (const input of Object.values(metadata.supplemental_inputs || {})) {
    for (const [source, count] of Object.entries(input.by_source || {})) {
      addSource(sources, source).supplemental_input += count;
    }
  }

  for (const job of allJobs) {
    const row = addSource(sources, sourceOf(job));
    row.final_rows++;
    if (isTechUS(job)) row.tech_us_rows++;
    if (isInternship(job)) row.internship_rows++;
    if (isTechUS(job) && isInternship(job)) row.tech_us_internship_rows++;
    if (hasDescription(job)) row.inline_description_rows++;
    if (!job.posted_at) {
      row.posted_at_missing++;
      if (row.sample_missing_posted_at.length < 3) row.sample_missing_posted_at.push({ id: jobId(job), title: job.title });
    } else {
      incrementBasis(row, job.posted_at_basis);
    }
    if (!hasDescription(job) && row.sample_no_description.length < 3) {
      row.sample_no_description.push({ id: jobId(job), title: job.title });
    }
  }

  const sidecarCoverage = loadSidecarCoverage(dataDir, allJobsById);
  for (const [source, ids] of sidecarCoverage.entries()) {
    addSource(sources, source).sidecar_rows = ids.size;
  }

  for (const job of enrichedJobs) {
    const row = addSource(sources, sourceOf(job));
    row.enriched_rows++;
    const skills = job.required_skills || job.skills || [];
    if (Array.isArray(skills) && skills.length > 0) row.enriched_with_skills++;
  }

  const rows = [...sources.values()].map(row => {
    const upstream = row.supplemental_input || row.fetched_raw || null;
    const finalSurvival = ratio(row.final_rows, upstream);
    const descriptionCoverage = ratio(row.inline_description_rows + row.sidecar_rows, row.final_rows);
    const enrichedCoverage = ratio(row.enriched_rows, row.tech_us_rows || row.final_rows);
    const skillCoverage = ratio(row.enriched_with_skills, row.enriched_rows);

    if (row.supplemental_input >= 100 && row.final_rows < row.supplemental_input * 0.5) {
      row.flags.push({ severity: 'high', code: 'low_supplemental_survival', detail: `${row.final_rows}/${row.supplemental_input} supplemental rows survived final output` });
    }
    if (row.final_rows >= 50 && row.posted_at_missing > 0) {
      row.flags.push({ severity: 'high', code: 'missing_posted_at_final_rows', detail: `${row.posted_at_missing}/${row.final_rows} final rows lack posted_at` });
    }
    if (row.final_rows >= 100 && !STRUCTURAL_NO_DESCRIPTION.has(row.source) && !INLINE_DESCRIPTION_SOURCES.has(row.source) && (descriptionCoverage === null || descriptionCoverage < 0.2)) {
      row.flags.push({ severity: 'high', code: 'low_description_input', detail: `${row.inline_description_rows + row.sidecar_rows}/${row.final_rows} rows have inline or sidecar text` });
    }
    if (row.tech_us_rows >= 50 && (enrichedCoverage === null || enrichedCoverage < 0.5)) {
      row.flags.push({ severity: 'medium', code: 'low_enrichment_survival', detail: `${row.enriched_rows}/${row.tech_us_rows} tech-US rows have enriched output` });
    }
    if (row.enriched_rows >= 50 && (skillCoverage === null || skillCoverage < 0.5)) {
      row.flags.push({ severity: 'medium', code: 'low_skill_fill', detail: `${row.enriched_with_skills}/${row.enriched_rows} enriched rows have skills` });
    }

    return {
      source: row.source,
      fetched_raw: row.fetched_raw,
      supplemental_input: row.supplemental_input,
      final_rows: row.final_rows,
      final_survival_pct: pct(finalSurvival),
      tech_us_rows: row.tech_us_rows,
      internship_rows: row.internship_rows,
      tech_us_internship_rows: row.tech_us_internship_rows,
      inline_description_rows: row.inline_description_rows,
      sidecar_rows: row.sidecar_rows,
      description_coverage_pct: pct(descriptionCoverage),
      posted_at_missing: row.posted_at_missing,
      posted_at_basis: row.posted_at_basis,
      enriched_rows: row.enriched_rows,
      enriched_coverage_pct: pct(enrichedCoverage),
      enriched_with_skills: row.enriched_with_skills,
      skill_coverage_pct: pct(skillCoverage),
      flags: row.flags,
      samples: {
        missing_posted_at: row.sample_missing_posted_at,
        no_description: row.sample_no_description,
      },
    };
  }).sort((a, b) => {
    const severity = flag => flag.severity === 'high' ? 2 : flag.severity === 'medium' ? 1 : 0;
    const aScore = a.flags.reduce((sum, flag) => sum + severity(flag), 0);
    const bScore = b.flags.reduce((sum, flag) => sum + severity(flag), 0);
    return bScore - aScore || b.tech_us_rows - a.tech_us_rows || a.source.localeCompare(b.source);
  });

  return {
    generated_at: new Date().toISOString(),
    data_dir: dataDir,
    total_sources: rows.length,
    flagged_sources: rows.filter(row => row.flags.length > 0).length,
    summary: {
      final_rows: allJobs.length,
      enriched_rows: enrichedJobs.length,
      high_flags: rows.flatMap(row => row.flags.filter(flag => flag.severity === 'high').map(flag => ({ source: row.source, code: flag.code, detail: flag.detail }))),
    },
    sources: rows,
  };
}

function printText(audit) {
  console.log(`Source survival audit (${audit.generated_at})`);
  console.log(`sources=${audit.total_sources} flagged=${audit.flagged_sources} final_rows=${audit.summary.final_rows} enriched_rows=${audit.summary.enriched_rows}`);
  for (const row of audit.sources.filter(source => source.flags.length > 0)) {
    console.log(`\n${row.source}: final=${row.final_rows} tech_us=${row.tech_us_rows} enriched=${row.enriched_rows} desc=${row.description_coverage_pct ?? 'n/a'}%`);
    for (const flag of row.flags) console.log(`  ${flag.severity.toUpperCase()} ${flag.code}: ${flag.detail}`);
  }
}

if (require.main === module) {
  try {
    const args = parseArgs(process.argv);
    const audit = buildAudit(args.dataDir);
    if (args.json) console.log(JSON.stringify(audit, null, 2));
    else printText(audit);
  } catch (err) {
    console.error(`[source-survival-audit] ${err.message}`);
    process.exit(1);
  }
}

module.exports = { buildAudit, parseArgs };
