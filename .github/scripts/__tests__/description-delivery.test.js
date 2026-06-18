#!/usr/bin/env node
'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { buildDescriptionDeliverySummary } = require('../index');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'desc-delivery-'));
fs.writeFileSync(path.join(dir, 'descriptions-google.jsonl'), [
  JSON.stringify({ id: 'google-1', description_text: 'Alpha' }),
  JSON.stringify({ id: 'google-2', description_text: 'Beta' }),
].join('\n') + '\n');
fs.writeFileSync(path.join(dir, 'descriptions-oracle.jsonl'), [
  JSON.stringify({ id: 'oracle-1', description_text: 'Gamma' }),
  JSON.stringify({ id: 'oracle-3', description_text: 'Delta' }),
].join('\n') + '\n');

const summary = buildDescriptionDeliverySummary([
  { id: 'google-1', source: 'google' },
  { id: 'google-2', source: 'google' },
  { id: 'oracle-1', source: 'oracle' },
  { id: 'oracle-2', source: 'oracle' },
  { id: 'apple-1', source: 'apple' },
  { id: 'workday-1', source: 'workday' },
], dir);

assert.strictEqual(summary.sources.google.mode, 'sidecar_only');
assert.strictEqual(summary.sources.google.final_rows_with_sidecar_match, 2);
assert.strictEqual(summary.sources.google.coverage_pct, 100);

assert.strictEqual(summary.sources.oracle.mode, 'partial_sidecar_coverage');
assert.strictEqual(summary.sources.oracle.final_rows_with_sidecar_match, 1);
assert.strictEqual(summary.sources.oracle.coverage_pct, 50);

assert.strictEqual(summary.sources.apple.mode, 'none_visible');
assert.strictEqual(summary.sources.apple.sidecar_rows, 0);
assert.strictEqual(summary.sources.apple.final_rows, 1);

assert.strictEqual(summary.sources.workday, undefined, 'workday should stay outside AGG sidecar truth because ENR owns its descriptions');

fs.rmSync(dir, { recursive: true, force: true });
console.log('PASS description delivery summary');
