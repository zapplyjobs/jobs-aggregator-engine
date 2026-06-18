#!/usr/bin/env node
'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { writeSidecar } = require('../fetch-supplemental-custom');

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'icims-sidecar-'));
const out = path.join(tmp, 'descriptions-icims.jsonl');
const rows = writeSidecar(out, [
  { id: 'icims-1', description: 'x'.repeat(100) },
  { id: 'icims-2', description: '' },
  { id: 'icims-3', description: '  ' },
  { id: 'icims-4', description: 'y'.repeat(60) },
]);
assert.strictEqual(rows, 2);
const lines = fs.readFileSync(out, 'utf8').trim().split('\n').map(JSON.parse);
assert.deepStrictEqual(lines.map(r => r.id), ['icims-1', 'icims-4']);
assert.strictEqual(lines[0].description_text.length, 100);
assert.strictEqual(lines[1].description_text.length, 60);
fs.rmSync(tmp, { recursive: true, force: true });
console.log('PASS icims sidecar writer');
