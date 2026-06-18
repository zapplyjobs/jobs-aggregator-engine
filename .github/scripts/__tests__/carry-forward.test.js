#!/usr/bin/env node
'use strict';

const assert = require('assert');
const { mergeCarryForward, RETIRED_CARRY_FORWARD_SOURCES } = require('../index');

function line(job) {
  return JSON.stringify(job);
}

const now = new Date().toISOString();

assert.ok(RETIRED_CARRY_FORWARD_SOURCES.has('jsearch'), 'jsearch must be retired');

{
  const publicJobs = [];
  mergeCarryForward(
    publicJobs,
    [line({
      id: 'js-stale-internship',
      source: 'jsearch',
      title: 'Software Engineering Intern',
      company_name: 'Dead Source',
      posted_at: now,
      tags: { employment: 'internship', domains: ['software'], locations: ['us'] },
    })],
    new Set(),
    new Set(),
    [],
    new Set()
  );
  assert.strictEqual(publicJobs.length, 0, 'retired jsearch jobs must not carry forward');
}

{
  const publicJobs = [];
  mergeCarryForward(
    publicJobs,
    [line({
      id: 'greenhouse-live-carry',
      source: 'greenhouse',
      title: 'Software Engineer',
      company_name: 'Active Source',
      posted_at: now,
      tags: { employment: 'entry_level', domains: ['software'], locations: ['us'] },
    })],
    new Set(),
    new Set(),
    [],
    new Set()
  );
  assert.strictEqual(publicJobs.length, 1, 'active non-fetched sources still carry forward within TTL');
}

{
  const publicJobs = [];
  mergeCarryForward(
    publicJobs,
    [line({
      id: 'greenhouse-point72-7586061002',
      source: 'greenhouse',
      title: 'Quantitative Researcher Intern',
      company_name: 'Point72',
      posted_at: '2024-08-15T17:34:49-04:00',
      tags: { employment: 'internship', domains: ['software'], locations: ['us'] },
    })],
    new Set(),
    new Set(),
    [],
    new Set(['greenhouse'])
  );
  assert.strictEqual(publicJobs.length, 0, 'current-run missing greenhouse jobs must not carry forward when greenhouse fetched successfully');
}


{
  const publicJobs = [];
  mergeCarryForward(
    publicJobs,
    [line({
      id: 'workday-canada-carry',
      source: 'workday',
      title: 'Manufacturing Engineer',
      company_name: 'Magna',
      company_slug: 'magna',
      location: 'Mississauga, Ontario, CA',
      job_city: 'Mississauga',
      job_state: 'CA',
      posted_at: now,
      tags: { employment: 'entry_level', domains: ['manufacturing'], locations: ['us'], tag_engine_version: 30 },
    })],
    new Set(),
    new Set(),
    [],
    new Set()
  );
  assert.strictEqual(publicJobs.length, 1, 'carry-forward Canada row should remain in rolling pool');
  assert.deepStrictEqual(publicJobs[0].tags.locations, ['canada'], 'carry-forward location tags must be recomputed from current tagLocations logic');
}

{
  const publicJobs = [];
  mergeCarryForward(
    publicJobs,
    [line({
      id: 'workday-india-carry',
      source: 'workday',
      title: 'Memory Circuit Design Engineer',
      company_name: 'Micron',
      company_slug: 'micron',
      location: 'Hyderabad - Phoenix Aquila, India',
      job_city: 'Hyderabad - Phoenix Aquila',
      job_state: '',
      posted_at: now,
      tags: { employment: 'entry_level', domains: ['hardware'], locations: ['us'], tag_engine_version: 56 },
    })],
    new Set(),
    new Set(),
    [],
    new Set()
  );
  assert.strictEqual(publicJobs.length, 1, 'carry-forward India row should remain in rolling pool');
  assert.deepStrictEqual(publicJobs[0].tags.locations, [], 'carry-forward India row must drop stale us tag');
}

{
  const publicJobs = [];
  mergeCarryForward(
    publicJobs,
    [line({
      id: 'workday-heredia-carry',
      source: 'workday',
      title: 'Junior Analyst, Channel Operations',
      company_name: 'Baxter International',
      company_slug: 'baxter-international',
      location: 'La Aurora, Heredia',
      job_city: 'La Aurora',
      job_state: '',
      posted_at: now,
      tags: { employment: 'entry_level', domains: ['operations'], locations: ['us'], tag_engine_version: 56 },
    })],
    new Set(),
    new Set(),
    [],
    new Set()
  );
  assert.strictEqual(publicJobs.length, 1, 'carry-forward Heredia row should remain in rolling pool');
  assert.deepStrictEqual(publicJobs[0].tags.locations, [], 'carry-forward Heredia row must drop stale us tag');
}

console.log('PASS carry-forward retired source guard');
