#!/usr/bin/env node
'use strict';

const assert = require('assert');
const { buildUsSnapshotJobs } = require('../index');

const jobs = [
  {
    id: 'us-tagged',
    title: 'Software Engineer',
    tags: { locations: ['us'], employment: 'entry_level', domains: ['software'] },
  },
  {
    id: 'remote-us-plus-other-tags',
    title: 'Hardware Engineer',
    tags: { locations: ['remote', 'us'], employment: 'entry_level', domains: ['hardware'] },
  },
  {
    id: 'missing-tags-location-text-us',
    title: 'Support Engineer',
    location: 'Remote, United States',
    tags: { employment: 'entry_level', domains: ['software'] },
  },
  {
    id: 'state-code-false-positive',
    title: 'Product Manager',
    location: 'La Ciotat',
    job_state: 'LA',
    tags: { locations: [], employment: 'entry_level', domains: ['product'] },
  },
  {
    id: 'canada-tagged',
    title: 'Data Analyst',
    tags: { locations: ['canada'], employment: 'entry_level', domains: ['data_science'] },
  },
];

const snapshot = buildUsSnapshotJobs(jobs);

assert.deepStrictEqual(
  snapshot.map((job) => job.id),
  ['us-tagged', 'remote-us-plus-other-tags'],
  'US snapshot must use the producer-owned tags.locations contract only'
);

console.log('PASS us snapshot tag contract');
