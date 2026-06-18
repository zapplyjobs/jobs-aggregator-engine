#!/usr/bin/env node
'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { parseJsonOrNdjson, isCurrentUsTechOracle, loadCurrentBoardPriorityIds } = require('../fetch-supplemental-oracle');

assert.deepStrictEqual(parseJsonOrNdjson(JSON.stringify([{ id: 'a' }])), [{ id: 'a' }]);
assert.deepStrictEqual(parseJsonOrNdjson('{"id":"a"}\n{"id":"b"}\n'), [{ id: 'a' }, { id: 'b' }]);

assert.strictEqual(isCurrentUsTechOracle({ source: 'oracle', tags: { locations: ['us'], domains: ['software'] } }), true);
assert.strictEqual(isCurrentUsTechOracle({ source: 'oracle', tags: { locations: ['us'], domains: ['general'] } }), false);
assert.strictEqual(isCurrentUsTechOracle({ source: 'workday', tags: { locations: ['us'], domains: ['software'] } }), false);

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'oracle-priority-'));
const allJobsPath = path.join(tmp, 'all_jobs.json');
fs.writeFileSync(allJobsPath, JSON.stringify([
  { id: 'oracle-a', source: 'oracle', tags: { locations: ['us'], domains: ['software'] } },
  { id: 'oracle-b', source: 'oracle', tags: { locations: ['us'], domains: ['general'] } },
  { id: 'oracle-c', source: 'oracle', tags: { locations: ['us'], domains: ['ai'] } },
]));

const cachedIds = new Set(['oracle-c']);
const existingPriorityIds = new Set(['oracle-short']);
const out = loadCurrentBoardPriorityIds(cachedIds, existingPriorityIds, allJobsPath);
assert.deepStrictEqual([...out].sort(), ['oracle-a', 'oracle-short']);

fs.rmSync(tmp, { recursive: true, force: true });
console.log('PASS oracle board priority');
