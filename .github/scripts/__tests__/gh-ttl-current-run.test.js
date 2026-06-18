#!/usr/bin/env node
'use strict';

const assert = require('assert');
const { activePublicWindowTs } = require('../index');

const now = new Date('2026-06-18T05:00:00.000Z').getTime();
const point72 = {
  id: 'greenhouse-point72-7586061002',
  source: 'greenhouse',
  posted_at: '2024-08-15T17:34:49-04:00',
  source_updated_at: '2026-06-12T13:40:11-04:00',
  tags: { employment: 'internship', locations: ['us'], domains: ['ai', 'data_science'] },
};

assert.strictEqual(
  new Date(activePublicWindowTs(point72, now)).toISOString(),
  '2026-06-12T17:40:11.000Z',
  'current-run Point72 internship should anchor to source_updated_at'
);

assert.strictEqual(
  activePublicWindowTs({ ...point72, source: 'icims' }, now),
  new Date(point72.posted_at).getTime(),
  'exception must stay greenhouse-only'
);

assert.strictEqual(
  activePublicWindowTs({ ...point72, tags: { employment: 'entry_level' } }, now),
  new Date(point72.posted_at).getTime(),
  'exception must stay internship-only'
);

console.log('PASS GH TTL current-run anchor');
