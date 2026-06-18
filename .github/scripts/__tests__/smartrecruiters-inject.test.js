#!/usr/bin/env node
'use strict';

const assert = require('assert');
const { injectDescriptions } = require('../index');

const jobs = [
  { id: 'sr-1', source: 'smartrecruiters', description: null },
  { id: 'sr-2', source: 'smartrecruiters', description: 'already here' },
  { id: 'sr-3', source: 'smartrecruiters', description: null },
];
const map = new Map([
  ['sr-1', 'desc one'],
  ['sr-2', 'desc two should not overwrite'],
]);

const injected = injectDescriptions(jobs, map, 'SR');
assert.strictEqual(injected, 1);
assert.strictEqual(jobs[0].description, 'desc one');
assert.strictEqual(jobs[1].description, 'already here');
assert.strictEqual(jobs[2].description, null);
console.log('PASS smartrecruiters description injection');
