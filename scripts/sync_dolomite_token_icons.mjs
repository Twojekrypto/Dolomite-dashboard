#!/usr/bin/env node

import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '..');
const OUTPUT_PATH = path.join(ROOT, 'dolomite-token-icons.generated.js');
const OFFICIAL_APP_ORIGIN = 'https://app.dolomite.io';
const OFFICIAL_MANIFEST_URL = `${OFFICIAL_APP_ORIGIN}/asset-manifest.json`;
const REQUEST_TIMEOUT_MS = 25_000;

export function buildOfficialIconRegistry(manifest) {
  const registry = {};
  const files = manifest && typeof manifest.files === 'object' ? manifest.files : {};

  for (const [logicalPath, emittedPath] of Object.entries(files)) {
    const match = String(logicalPath).match(/^static\/media\/(.+)\.(svg|png|webp)$/i);
    if (!match || typeof emittedPath !== 'string') continue;
    const url = new URL(emittedPath, `${OFFICIAL_APP_ORIGIN}/`).href;
    if (!url.startsWith(`${OFFICIAL_APP_ORIGIN}/static/media/`)) continue;
    registry[match[1]] = url;
  }

  return Object.fromEntries(
    Object.entries(registry).sort(([left], [right]) => left.localeCompare(right, 'en', { sensitivity: 'base' })),
  );
}

export function renderOfficialIconRegistry(registry) {
  const serialized = JSON.stringify(registry, null, 2);
  return `/* Generated from ${OFFICIAL_MANIFEST_URL}. Do not edit manually. */
(function (root) {
  const exact = ${serialized};
  const lookup = Object.assign({}, exact);
  Object.entries(exact).forEach(function (entry) {
    const normalized = String(entry[0]).trim().toUpperCase();
    if (normalized && !lookup[normalized]) lookup[normalized] = entry[1];
  });
  root.DOLOMITE_TOKEN_ICONS = Object.freeze(lookup);
  root.getDolomiteOfficialTokenIcon = function (symbol) {
    const key = String(symbol || '').trim();
    return exact[key] || lookup[key.toUpperCase()] || '';
  };
})(typeof window !== 'undefined' ? window : globalThis);
`;
}

async function fetchOfficialManifest() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(OFFICIAL_MANIFEST_URL, {
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`HTTP ${response.status} for ${OFFICIAL_MANIFEST_URL}`);
    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function main() {
  const manifest = await fetchOfficialManifest();
  const registry = buildOfficialIconRegistry(manifest);
  if (Object.keys(registry).length < 50) {
    throw new Error(`Expected at least 50 official Dolomite media icons, received ${Object.keys(registry).length}`);
  }
  await writeFile(OUTPUT_PATH, renderOfficialIconRegistry(registry));
  console.log(`Wrote ${Object.keys(registry).length} official Dolomite icons to ${path.relative(ROOT, OUTPUT_PATH)}`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === __filename) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
