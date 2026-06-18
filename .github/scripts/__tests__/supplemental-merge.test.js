#!/usr/bin/env node
'use strict';

const assert = require('assert');
const { normalizeSupplementalJobForMerge, summarizeSupplementalLaneForMerge } = require('../index');

const generatedAt = '2026-06-17T06:49:17.331Z';

{
  const row = normalizeSupplementalJobForMerge(
    { id: 'tiktok-1', source: 'TikTok', title: 'Software Engineer Intern', description: 'Rich text' },
    { generated_at: generatedAt }
  );
  assert.strictEqual(row.source, 'tiktok');
  assert.strictEqual(row.posted_at, generatedAt);
  assert.strictEqual(row.posted_at_basis, 'supplemental_generated_at');
}

{
  const row = normalizeSupplementalJobForMerge(
    { id: 'oracle-1', source: 'oracle', posted_at: '2026-01-01T00:00:00.000Z' },
    { generated_at: generatedAt }
  );
  assert.strictEqual(row.posted_at, '2026-01-01T00:00:00.000Z');
  assert.strictEqual(row.posted_at_basis, undefined);
}

assert.strictEqual(normalizeSupplementalJobForMerge(null, { generated_at: generatedAt }), null);
assert.strictEqual(normalizeSupplementalJobForMerge({ id: 'missing-source' }, { generated_at: generatedAt }), null);

{
  const summary = summarizeSupplementalLaneForMerge(
    'custom',
    [
      { id: 'google-1', source: 'Google', title: 'Software Engineer' },
      { id: 'microsoft-1', source: 'Microsoft', title: 'Software Engineer' },
    ],
    {
      generated_at: '2026-06-17T06:49:17.331Z',
      lane_name: 'custom',
      jobs_fetched: 2,
      publish_contract: { max_staleness_minutes: 90 },
      sources: { google: 1, microsoft: 1 },
    },
    new Date('2026-06-17T07:30:00.000Z').getTime()
  );
  assert.strictEqual(summary.info.status, 'merged');
  assert.deepStrictEqual(summary.info.by_source, { google: 1, microsoft: 1 });
  assert.strictEqual(summary.jobs.length, 2);
}

{
  const summary = summarizeSupplementalLaneForMerge(
    'custom',
    [{ id: 'google-1', source: 'Google' }],
    {
      generated_at: '2026-06-17T06:49:17.331Z',
      lane_name: 'custom',
      jobs_fetched: 1,
      publish_contract: { max_staleness_minutes: 30 },
      sources: { google: 1 },
    },
    new Date('2026-06-17T08:00:00.000Z').getTime()
  );
  assert.strictEqual(summary.info.status, 'skipped_stale');
  assert.strictEqual(summary.info.skip_reason, 'stale_artifact');
  assert.strictEqual(summary.jobs.length, 0);
}

{
  const summary = summarizeSupplementalLaneForMerge(
    'custom',
    [{ id: 'google-1', source: 'Google' }],
    {
      generated_at: '2026-06-17T06:49:17.331Z',
      lane_name: 'custom',
      jobs_fetched: 1,
      publish_contract: { max_staleness_minutes: 90 },
      sources: { google: 1, tiktok: 0 },
    },
    new Date('2026-06-17T07:00:00.000Z').getTime()
  );
  assert.strictEqual(summary.info.status, 'skipped_invalid');
  assert.strictEqual(summary.info.skip_reason, 'source_set_mismatch');
}

console.log('PASS supplemental merge normalization');
