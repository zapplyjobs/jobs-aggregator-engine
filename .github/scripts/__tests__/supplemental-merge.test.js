#!/usr/bin/env node
'use strict';

const assert = require('assert');
const { normalizeSupplementalJobForMerge } = require('../index');

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

console.log('PASS supplemental merge normalization');
