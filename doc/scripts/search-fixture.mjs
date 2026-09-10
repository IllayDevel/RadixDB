import { createIndex, close } from 'pagefind';
import { mkdir, writeFile } from 'node:fs/promises';
import assert from 'node:assert/strict';

// A real second search artifact, kept outside the production build.
const html = '<!doctype html><html lang="en"><head><title>Archive test fixture</title></head><body><main data-pagefind-body>archivalonlysentinel</main></body></html>';
await mkdir('.search-fixture/en', { recursive: true });
await writeFile('.search-fixture/en/index.html', html);
try {
  const { index, errors } = await createIndex();
  assert.deepEqual(errors, []);
  assert(index);
  assert.deepEqual((await index.addHTMLFile({ sourcePath: 'en/index.html', content: html })).errors, []);
  assert.deepEqual((await index.writeFiles({ outputPath: '.search-fixture/pagefind' })).errors, []);
} finally { await close(); }
