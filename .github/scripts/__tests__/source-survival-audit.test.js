#!/usr/bin/env node
'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { buildAudit } = require('../source-survival-audit');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'source-survival-audit-'));

fs.writeFileSync(path.join(dir, 'jobs-metadata.json'), JSON.stringify({
  fetch_results: { direct: 10 },
  supplemental_inputs: {
    custom: { by_source: { direct: 3, weak: 200 } },
  },
}));

fs.writeFileSync(path.join(dir, 'all_jobs.json'), JSON.stringify([
  {
    id: 'direct-1',
    source: 'direct',
    title: 'Software Engineer Intern',
    posted_at: '2026-06-17T00:00:00.000Z',
    description: '',
    tags: { domains: ['software'], locations: ['us'], employment: 'internship' },
  },
  {
    id: 'direct-2',
    source: 'direct',
    title: 'Data Intern',
    posted_at: '2026-06-17T00:00:00.000Z',
    description: '',
    tags: { domains: ['data_science'], locations: ['us'], employment: 'internship' },
  },
  ...Array.from({ length: 80 }, (_, i) => ({
    id: `weak-${i}`,
    source: 'weak',
    title: `Weak Job ${i}`,
    posted_at: '2026-06-17T00:00:00.000Z',
    description: '',
    tags: { domains: ['software'], locations: ['us'], employment: 'entry_level' },
  })),
]));

fs.writeFileSync(path.join(dir, 'enriched_jobs.json'), JSON.stringify([
  { id: 'direct-1', source: 'direct', required_skills: ['python'] },
  { id: 'direct-2', source: 'direct', required_skills: [] },
]));

fs.writeFileSync(path.join(dir, 'descriptions-direct.jsonl'), [
  JSON.stringify({ id: 'direct-1', description_text: 'Python required' }),
  JSON.stringify({ id: 'direct-2', description_text: 'SQL required' }),
].join('\n') + '\n');

const audit = buildAudit(dir);
const direct = audit.sources.find(row => row.source === 'direct');
assert.strictEqual(direct.sidecar_rows, 2);
assert.strictEqual(direct.description_coverage_pct, 100);
assert.strictEqual(direct.tech_us_internship_rows, 2);
assert(!direct.flags.some(flag => flag.code === 'low_description_input'));

const weak = audit.sources.find(row => row.source === 'weak');
assert.strictEqual(weak.final_rows, 80);
assert(weak.flags.some(flag => flag.code === 'low_supplemental_survival'));
assert(weak.flags.some(flag => flag.code === 'low_enrichment_survival'));

fs.rmSync(dir, { recursive: true, force: true });
console.log('PASS source survival audit');
