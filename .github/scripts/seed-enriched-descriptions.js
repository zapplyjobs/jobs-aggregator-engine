#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');

const DATA_DIR = '.github/data';
const CHUNK_COUNT = 30;
const CONCURRENCY = 2;   // Bounded, conservative concurrency after A177a showed higher fan-out could stall R2 seeding in workflow

function fetchGitHubFile(url, target) {
  return new Promise((resolve) => {
    const file = fs.createWriteStream(target);
    https.get(url, (res) => {
      if (res.statusCode !== 200) {
        file.close();
        fs.unlink(target, () => resolve(false));
        return;
      }
      res.pipe(file);
      file.on('finish', () => file.close(() => resolve(true)));
    }).on('error', () => {
      file.close();
      fs.unlink(target, () => resolve(false));
    });
  });
}

async function parallelMap(items, concurrency, worker) {
  const results = [];
  let index = 0;
  async function run() {
    while (index < items.length) {
      const current = items[index++];
      results.push(await worker(current));
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => run()));
  return results;
}

async function seedChunk(r2, chunk) {
  const fileName = `descriptions-enriched-${chunk}.jsonl`;
  const target = path.join(DATA_DIR, fileName);
  const r2Result = await r2.downloadToFile(fileName, target).catch(() => null);
  if (r2Result) {
    const lines = fs.readFileSync(target, 'utf8').trim().split('\n').filter(Boolean).length;
    return { chunk, source: 'R2', lines };
  }
  const ghUrl = `https://raw.githubusercontent.com/zapplyjobs/jobs-data-2026/main/.github/data/${fileName}?t=${Date.now()}`;
  const ok = await fetchGitHubFile(ghUrl, target);
  if (!ok) return { chunk, source: 'missing', lines: 0 };
  const lines = fs.readFileSync(target, 'utf8').trim().split('\n').filter(Boolean).length;
  return { chunk, source: 'GitHub', lines };
}

async function main() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const { createR2Client } = require('./aggregator/lib/storage/r2-client');
  const r2 = createR2Client({ prefix: 'data/' });
  const chunks = Array.from({ length: CHUNK_COUNT }, (_, i) => i + 1);
  const results = await parallelMap(chunks, CONCURRENCY, (chunk) => seedChunk(r2, chunk));
  let total = 0;
  for (const row of results.sort((a, b) => a.chunk - b.chunk)) {
    if (row.source === 'missing') {
      console.log(`  chunk ${row.chunk}: not found (skipped)`);
      continue;
    }
    total += row.lines;
    console.log(`  chunk ${row.chunk}: ${row.lines} entries (${row.source})`);
  }
  if (total > 0) console.log(`✅ Seeded ${total} enriched descriptions total`);
  else console.log('⚠️ No descriptions-enriched chunks found — description fallback will be inactive');
}

main().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
