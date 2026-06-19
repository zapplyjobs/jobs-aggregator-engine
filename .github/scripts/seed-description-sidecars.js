#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');

const DATA_DIR = '.github/data';
const CONCURRENCY = 4;
const GH_HEADERS = {
  'User-Agent': 'ZJP-Sidecar-Seed',
  'Authorization': `Bearer ${process.env.GH_PAT || process.env.GITHUB_TOKEN}`,
  'Accept': 'application/vnd.github.v3+json',
};

function getJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: GH_HEADERS }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}

function download(url, target) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(target);
    https.get(url, { headers: GH_HEADERS }, (res) => {
      if (res.statusCode !== 200) {
        file.close();
        fs.unlink(target, () => resolve(false));
        return;
      }
      res.pipe(file);
      file.on('finish', () => file.close(() => resolve(true)));
    }).on('error', err => {
      file.close();
      fs.unlink(target, () => reject(err));
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
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length || 1) }, () => run()));
  return results;
}

async function main() {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  const { createR2Client } = require('./aggregator/lib/storage/r2-client');
  const r2 = createR2Client({ prefix: 'data/' });
  let seeded = 0;
  try {
    const objects = await r2.list('descriptions-');
    const names = objects
      .map(obj => path.basename(obj.key || ''))
      .filter(name => /^descriptions-.*\.jsonl$/.test(name))
      .filter(name => !fs.existsSync(path.join(DATA_DIR, name)));
    const results = await parallelMap(names, CONCURRENCY, async (name) => {
      const ok = await r2.downloadToFile(name, path.join(DATA_DIR, name)).catch(() => null);
      return ok ? 1 : 0;
    });
    seeded += results.reduce((a, b) => a + b, 0);
  } catch {}

  let ghSeeded = 0;
  try {
    const listing = await getJson('https://api.github.com/repos/zapplyjobs/jobs-data-2026/contents/.github/data');
    const names = listing
      .filter(item => item && /^descriptions-.*\.jsonl$/.test(item.name || ''))
      .filter(item => !fs.existsSync(path.join(DATA_DIR, item.name)));
    const results = await parallelMap(names, CONCURRENCY, async (item) => {
      const ok = await download(item.download_url, path.join(DATA_DIR, item.name)).catch(() => false);
      return ok ? 1 : 0;
    });
    ghSeeded += results.reduce((a, b) => a + b, 0);
  } catch {}

  console.log(`SEEDED_R2:${seeded}`);
  console.log(`SEEDED_GH:${ghSeeded}`);
}

main().catch(err => {
  console.error(err.message || err);
  process.exit(1);
});
