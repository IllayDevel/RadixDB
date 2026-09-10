import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { parse } from 'smol-toml';

const root = new URL('../', import.meta.url);
export function validatePublication(publication, applicationVersion) {
  assert(/^\d+\.\d+(?:\.\d+)?$/.test(publication.version), 'Invalid target version');
  assert(['development', 'release'].includes(publication.channel), 'Invalid publication channel');
  const normalize = v => /^\d+\.\d+$/.test(v) ? `${v}.0` : v;
  if (publication.channel === 'release') assert.equal(normalize(publication.version), normalize(applicationVersion), 'Release version mismatch');
}

export function readPublication() {
  const publication = parse(readFileSync(new URL('_meta/publication.toml', root), 'utf8'));
  const manifest = parse(readFileSync(new URL(publication.application_manifest, root), 'utf8'));
  validatePublication(publication, manifest.workspace.package.version);
  const revision = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: new URL('../', root), encoding: 'utf8' }).trim();
  const dirty = execFileSync('git', ['status', '--porcelain', '--untracked-files=normal'], { cwd: new URL('../', root), encoding: 'utf8' }).length > 0;
  if (publication.channel === 'release' && process.env.DOCS_ENFORCE_CLEAN === '1') {
    assert(!dirty, 'Release publication requires a clean source tree');
  }
  return { target: publication.version, channel: publication.channel, application: manifest.workspace.package.version, revision, dirty };
}

export function chapterRoute(chapter) {
  return chapter.path.replace(/\.(md|mdx)$/, '').replace(/(^|\/)index$/, '').replace(/\/$/, '');
}

export function publicationManifest() {
  const metadata = readPublication();
  const registry = parse(readFileSync(new URL('_meta/chapters.toml', root), 'utf8'));
  return { ...metadata, locales: registry.locales,
    chapters: registry.chapters.filter(c => c.status !== 'planned').map(chapterRoute),
    unstable_chapters: registry.chapters.filter(c => c.unstable_api).map(chapterRoute),
  };
}

export { chapterDestination } from '../src/lib/version-routing.mjs';
