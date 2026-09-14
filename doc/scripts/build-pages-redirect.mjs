import assert from 'node:assert/strict';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repo = fileURLToPath(new URL('../../', import.meta.url));
const output = path.resolve(process.env.DOCS_ARTIFACT_DIR || path.join(repo, '_site'));
const base = process.env.DOCS_ROOT_BASE || './';
assert(base === './' || /^\/(?:[A-Za-z0-9_-]+\/)*$/.test(base), 'Invalid Pages root path');
const destination = 'https://radixdb.org/';
const delay = 5;
const logo = readFileSync(path.join(repo, 'logo-light.png')).toString('base64');

const locales = {
  en: {
    description: 'Official website and documentation',
    open: 'Open radixdb.org',
    before: 'Redirecting in',
    after: 'seconds.',
    language: 'Русский',
    languageCode: 'ru',
    languageFile: 'index.ru.html',
    languageLabel: 'Language',
    support: 'Support',
    sponsor: 'Development supported by',
  },
  ru: {
    description: 'Официальный сайт и документация',
    open: 'Перейти на radixdb.org',
    before: 'Переход через',
    after: 'сек.',
    language: 'English',
    languageCode: 'en',
    languageFile: 'index.html',
    languageLabel: 'Язык',
    support: 'Поддержка',
    sponsor: 'Разработку поддерживает',
  },
};

function render(locale) {
  const text = locales[locale];
  return `<!doctype html>
<html lang="${locale}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex, follow">
  <meta http-equiv="refresh" content="${delay}; url=${destination}">
  <link rel="canonical" href="${destination}">
  <title>RadixDB</title>
  <style>
    :root { color-scheme: dark; font-family: system-ui, sans-serif; letter-spacing: 0; }
    * { box-sizing: border-box; }
    body { margin: 0; min-height: 100svh; display: grid; grid-template-rows: auto 1fr auto;
      background: #17191b; color: #f0f5f2; }
    a { color: #a8d9c1; text-underline-offset: 4px; }
    a:hover { color: #fff; }
    a:focus-visible { outline: 2px solid #a8d9c1; outline-offset: 6px; }
    nav { padding: 24px 32px; text-align: right; font-size: 14px; }
    main { width: min(100%, 720px); margin: auto; padding: 40px 24px 64px; text-align: center; }
    h1 { margin: 0 auto 24px; max-width: 600px; font-size: 32px; }
    img { display: block; width: 100%; height: auto; aspect-ratio: 3; }
    .description { margin: 0 0 32px; color: #c5cdc8; font-size: 20px; line-height: 1.5; }
    .open { display: inline-block; max-width: 100%; padding: 13px 22px; border-radius: 4px;
      background: #a8d9c1; color: #15231c; font-weight: 600; text-decoration: none; line-height: 1.5; }
    .open:hover { background: #c5ebd9; color: #15231c; }
    .countdown { margin: 24px 0 0; color: #a7b0aa; font-size: 14px; line-height: 1.6; }
    #seconds { display: inline-block; min-width: 1ch; font-variant-numeric: tabular-nums; }
    footer { display: flex; justify-content: space-between; flex-wrap: wrap; gap: 12px 24px;
      padding: 24px 32px; border-top: 1px solid #303735; font-size: 13px; color: #a7b0aa; }
    footer p { margin: 0; line-height: 1.6; }
    @media (max-width: 480px) {
      nav { padding: 20px; }
      main { padding: 24px 20px 40px; }
      .description { font-size: 18px; }
      footer { padding: 20px; justify-content: center; text-align: center; }
    }
  </style>
</head>
<body>
  <nav aria-label="${text.languageLabel}"><a href="${base}${text.languageFile}" lang="${text.languageCode}" hreflang="${text.languageCode}">${text.language}</a></nav>
  <main>
    <h1><img src="data:image/png;base64,${logo}" width="2172" height="724" alt="RadixDB"></h1>
    <p class="description">${text.description}</p>
    <a class="open" href="${destination}">${text.open}</a>
    <p class="countdown">${text.before} <span id="seconds">${delay}</span> ${text.after}</p>
  </main>
  <footer>
    <p>${text.sponsor} <a href="http://light-soft.info/">Light Soft</a></p>
    <p><a href="mailto:dev@radixdb.org">${text.support}</a></p>
  </footer>
  <script>
    const deadline = Date.now() + ${delay * 1000};
    const timer = setInterval(() => {
      const remaining = Math.max(0, Math.ceil((deadline - Date.now()) / 1000));
      document.getElementById('seconds').textContent = String(remaining);
      if (remaining === 0) {
        clearInterval(timer);
        location.replace('${destination}');
      }
    }, 200);
  </script>
</body>
</html>
`;
}

mkdirSync(output, { recursive: true });
writeFileSync(path.join(output, 'index.html'), render('en'));
writeFileSync(path.join(output, 'index.ru.html'), render('ru'));
writeFileSync(path.join(output, '404.html'), render('en'));
writeFileSync(path.join(output, '.nojekyll'), '');
console.log(`Pages redirect: ${output} -> ${destination} (${delay}s, en/ru)`);
