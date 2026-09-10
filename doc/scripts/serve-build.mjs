import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';

const root = path.resolve('dist');
const base = process.env.DOCS_BASE || '/manual/1.2/';
const types = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.svg': 'image/svg+xml', '.wasm': 'application/wasm', '.woff2': 'font/woff2' };

async function notFound(res, servedRoot) {
  try {
    const body = await readFile(path.join(servedRoot, '404.html'));
    res.writeHead(404, { 'Content-Type': 'text/html' }).end(body);
  } catch {
    res.writeHead(404).end();
  }
}

createServer(async (req, res) => {
  let servedRoot = root;
  try {
    const url = new URL(req.url, 'http://localhost');
    const archive = url.pathname.startsWith('/manual/1.0/');
    servedRoot = archive ? path.resolve('.search-fixture') : root;
    const servedBase = archive ? '/manual/1.0/' : base;
    if (!url.pathname.startsWith(servedBase)) { await notFound(res, servedRoot); return; }
    let file = path.resolve(servedRoot, decodeURIComponent(url.pathname.slice(servedBase.length)) || '.');
    if (file !== servedRoot && !file.startsWith(servedRoot + path.sep)) { res.writeHead(403).end(); return; }
    if ((await stat(file)).isDirectory()) file = path.join(file, 'index.html');
    const body = await readFile(file);
    res.writeHead(200, { 'Content-Type': types[path.extname(file)] || 'application/octet-stream' }).end(body);
  } catch { await notFound(res, servedRoot); }
}).listen(4326, '127.0.0.1', () => console.log('Built manual: http://127.0.0.1:4326' + base));
