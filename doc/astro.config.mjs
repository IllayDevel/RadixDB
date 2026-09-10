import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { satteri } from '@astrojs/markdown-satteri';
import { readFileSync } from 'node:fs';
import { parse } from 'smol-toml';
import { publicationManifest } from './scripts/publication.mjs';
import { writeFile } from 'node:fs/promises';
import satteriRadixDbSql from './src/syntax/satteri-radixdb-sql.mjs';

const publication = parse(readFileSync(new URL('./_meta/publication.toml', import.meta.url), 'utf8'));
const buildMetadata = publicationManifest();
const base = process.env.DOCS_BASE || `/manual/${publication.version}/`;
const site = process.env.DOCS_SITE || 'https://radixdb.org';
const outDir = process.env.DOCS_OUT_DIR;
const yandexMetrika = `
(function(m,e,t,r,i,k,a){
    m[i]=m[i]||function(){(m[i].a=m[i].a||[]).push(arguments)};
    m[i].l=1*new Date();
    for (var j=0; j<document.scripts.length; j++) {
        if (document.scripts[j].src === r) return;
    }
    k=e.createElement(t),a=e.getElementsByTagName(t)[0],k.async=1,k.src=r,a.parentNode.insertBefore(k,a);
})(window, document, 'script', 'https://mc.yandex.ru/metrika/tag.js?id=112445210', 'ym');

ym(112445210, 'init', {
    ssr: true,
    webvisor: true,
    clickmap: true,
    ecommerce: 'dataLayer',
    referrer: document.referrer,
    url: location.href,
    accurateTrackBounce: true,
    trackLinks: true
});
`;

export default defineConfig({
  site,
  base,
  ...(outDir ? { outDir } : {}),
  markdown: { processor: satteri({ mdastPlugins: [satteriRadixDbSql] }) },
  redirects: { '/': `${base}en/` },
  vite: { define: { 'import.meta.env.PUBLIC_DOCS_BUILD': JSON.stringify(JSON.stringify(buildMetadata)) } },
  integrations: [{
    name: 'radixdb-build-metadata',
    hooks: { 'astro:build:done': async ({ dir }) => {
      await writeFile(new URL('build-manifest.json', dir), JSON.stringify(buildMetadata, null, 2) + '\n');
      await writeFile(new URL('versions.json', dir), JSON.stringify([{ ...buildMetadata, base }], null, 2) + '\n');
    } },
  }, starlight({
    title: `RadixDB ${publication.version} ${publication.channel}`,
    head: [{ tag: 'script', attrs: { type: 'text/javascript' }, content: yandexMetrika }],
    defaultLocale: 'en',
    locales: {
      en: { label: 'English', lang: 'en' },
      ru: { label: 'Русский', lang: 'ru' },
    },
    sidebar: [
      { slug: 'index' },
      { label: 'Introduction', translations: { ru: 'Введение' }, items: [{ slug: 'preface/what-is-radixdb' }, { slug: 'preface/conventions' }] },
      { label: 'Tutorial', translations: { ru: 'Учебник' }, items: ['getting-started', 'first-database', 'relationships', 'transactions', 'server-connection'].map(name => ({ slug: `tutorial/${name}` })) },
      { label: 'SQL language', translations: { ru: 'Язык SQL' }, items: ['syntax', 'types', 'expressions', 'ddl', 'dml', 'indexes', 'queries', 'transactions', 'navigable-references'].map(name => ({ slug: `sql/${name}` })) },
      { label: 'Administration', translations: { ru: 'Администрирование' }, items: ['installation', 'server', 'configuration', 'extensions', 'storage', 'memory', 'backup-restore', 'upgrading', 'monitoring', 'troubleshooting', 'authentication', 'access-control'].map(name => ({ slug: `administration/${name}` })) },
      { label: 'Client interfaces', translations: { ru: 'Клиентские интерфейсы' }, items: ['overview', 'embedded-rust', 'rust-client', 'orm'].map(name => ({ slug: `clients/${name}` })) },
      { label: 'Server programming', translations: { ru: 'Серверное программирование' }, items: ['pl-sql', 'routines', 'triggers', 'jobs', 'routine-security', 'native-extensions'].map(name => ({ slug: `programming/${name}` })) },
      { label: 'Reference', translations: { ru: 'Справочник' }, items: [
        { label: 'SQL commands', translations: { ru: 'Команды SQL' }, items: [
          { slug: 'reference/sql' },
          ...['select', 'insert', 'update', 'delete', 'create-table', 'alter-table', 'drop-table',
            'create-index', 'alter-index', 'drop-index', 'describe', 'show', 'begin', 'commit',
            'rollback', 'savepoint', 'release-savepoint', 'set', 'extensions']
            .map(name => ({ slug: `reference/sql/${name}` })),
        ] },
        { label: 'Programs', translations: { ru: 'Программы' }, items: ['cli', 'server', 'cargo-radixdb-plugin'].map(name => ({ slug: `reference/programs/${name}` })) },
        { slug: 'reference/configuration' },
      ] },
      { label: 'Internals', translations: { ru: 'Внутреннее устройство' }, items: ['overview', 'storage', 'protocol'].map(name => ({ slug: `internals/${name}` })) },
      { label: 'Appendices', translations: { ru: 'Приложения' }, items: [{ slug: 'appendices/compatibility' }, { slug: 'appendices/limits' }, { slug: 'appendices/benchmarks' }, { slug: 'appendices/evidence' }, { slug: 'appendices/release-notes' }, { slug: 'appendices/glossary' }] },
    ],
    customCss: ['./src/styles/manual.css'],
    components: { Footer: './src/components/ManualFooter.astro', MarkdownContent: './src/components/ManualContent.astro' },
  })],
});
