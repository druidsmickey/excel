// Node.js server: serves the page and a small JSON API, saving to MongoDB (or data.json).
// Every create/update of a row stamps its updatedAt on the server.
const http = require('http');
const fs = require('fs');
const path = require('path');

// Load settings (PORT, MONGODB_URI) from .env if it exists; real environment variables win.
try { process.loadEnvFile(path.join(__dirname, '.env')); } catch {}

const PORT = process.env.PORT || 3000;
const DATA = path.join(__dirname, 'data.json');
const PUBLIC = path.join(__dirname, 'public');

const DEFAULT_STATE = () => {
  const now = new Date().toISOString();
  return {
    columns: ['Value 1', 'Value 2', 'Value 3', 'Value 4'],
    rows: [
      { id: 1, code: 'AB101', values: [10, 20, 30, 40], updatedAt: now },
      { id: 2, code: 'XY202', values: [15.5, 25, 35, 45], updatedAt: now },
      { id: 3, code: 'C303', values: [5, 10, 15, 20], updatedAt: now },
    ],
  };
};

// Storage: MongoDB when MONGODB_URI is set, otherwise the local data.json file.
// The whole table lives in one document ({ _id: 'main', columns, rows }).
const MONGODB_URI = process.env.MONGODB_URI;
let collection = null;
let state;
let nextId = 1;
let writeQueue = Promise.resolve();

async function loadState() {
  if (MONGODB_URI) {
    const { MongoClient } = require('mongodb');
    const client = new MongoClient(MONGODB_URI, { serverSelectionTimeoutMS: 8000 });
    await client.connect();
    collection = client.db(process.env.MONGODB_DB || 'rowtracker').collection('tracker');
    const doc = await collection.findOne({ _id: 'main' });
    if (doc) return { columns: doc.columns, rows: doc.rows };
    // First run on a fresh database: import data.json if there is one, else start with samples.
    const first = readFile() || DEFAULT_STATE();
    await collection.insertOne({ _id: 'main', ...first });
    return first;
  }
  return readFile() || DEFAULT_STATE();
}

function readFile() {
  try {
    const s = JSON.parse(fs.readFileSync(DATA, 'utf8'));
    // older format: rows only, four fixed columns
    return Array.isArray(s) ? { columns: ['Value 1', 'Value 2', 'Value 3', 'Value 4'], rows: s } : s;
  } catch {
    return null;
  }
}

// Writes are queued so they land in order.
const save = () => {
  const snapshot = JSON.stringify(state);
  writeQueue = writeQueue.then(() => collection
    ? collection.replaceOne({ _id: 'main' }, { _id: 'main', ...JSON.parse(snapshot) }, { upsert: true })
    : fs.promises.writeFile(DATA, JSON.stringify(JSON.parse(snapshot), null, 2))
  ).catch((e) => console.error('Save failed:', e.message));
};

function clean(body) {
  const code = String(body.code ?? '').trim().slice(0, 50);
  const values = state.columns.map((_, i) => {
    const n = parseFloat(body.values?.[i]);
    return Number.isFinite(n) ? n : 0;
  });
  return { code, values };
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let s = '';
    req.on('data', (c) => { s += c; if (s.length > 1e5) req.destroy(); });
    req.on('end', () => { try { resolve(JSON.parse(s || '{}')); } catch (e) { reject(e); } });
  });
}

const send = (res, status, obj) => {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
};

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const rowMatch = url.pathname.match(/^\/api\/rows(?:\/(\d+))?$/);
  const colMatch = url.pathname.match(/^\/api\/columns(?:\/(\d+))?$/);
  try {
    // With MongoDB, re-read the table on every API call so several servers sharing one database
    // never overwrite each other with a stale copy.
    if (collection && url.pathname.startsWith('/api/')) {
      await writeQueue;
      const doc = await collection.findOne({ _id: 'main' });
      if (doc) {
        state = { columns: doc.columns, rows: doc.rows };
        nextId = Math.max(nextId, state.rows.reduce((m, r) => Math.max(m, r.id), 0) + 1);
      }
    }
    if (url.pathname === '/api/data' && req.method === 'GET') return send(res, 200, state);

    // Bulk actions on a selection: { rows: [row ids], columns: [column indexes] }
    if ((url.pathname === '/api/clear' || url.pathname === '/api/delete') && req.method === 'POST') {
      const body = await readBody(req);
      const ids = new Set((body.rows || []).map(Number));
      const cols = [...new Set((body.columns || []).map(Number))].filter((i) => i >= 0 && i < state.columns.length);
      const now = new Date().toISOString();
      if (url.pathname === '/api/clear') {
        // Zero the numbers in selected rows (all columns) and selected columns (all rows); IDs are kept.
        state.rows.forEach((r) => {
          if (!ids.has(r.id) && !cols.length) return;
          r.values = r.values.map((v, i) => (ids.has(r.id) || cols.includes(i) ? 0 : v));
          r.updatedAt = now;
        });
        // Individual cells: { id, col } where col is a column index, or 'code' for the ID cell.
        for (const { id, col } of body.cells || []) {
          const r = state.rows.find((x) => x.id === Number(id));
          if (!r) continue;
          if (col === 'code') r.code = '';
          else if (Number.isInteger(col) && col >= 0 && col < r.values.length) r.values[col] = 0;
          else continue;
          r.updatedAt = now;
        }
      } else {
        state.rows = state.rows.filter((r) => !ids.has(r.id));
        if (cols.length) {
          state.columns = state.columns.filter((_, i) => !cols.includes(i));
          state.rows.forEach((r) => { r.values = r.values.filter((_, i) => !cols.includes(i)); r.updatedAt = now; });
        }
      }
      save();
      return send(res, 200, state);
    }

    if (colMatch) {
      const idx = colMatch[1] === undefined ? null : Number(colMatch[1]);
      if (req.method === 'POST' && idx === null) {
        const body = await readBody(req);
        state.columns.push(String(body.name || `Value ${state.columns.length + 1}`).trim().slice(0, 40));
        state.rows.forEach((r) => r.values.push(0));
        save();
        return send(res, 201, state);
      }
      if (idx === null || idx >= state.columns.length) return send(res, 404, { error: 'Not found' });
      if (req.method === 'PUT') {
        const body = await readBody(req);
        state.columns[idx] = String(body.name ?? '').trim().slice(0, 40) || state.columns[idx];
        save();
        return send(res, 200, state);
      }
      if (req.method === 'DELETE') {
        const now = new Date().toISOString();
        state.columns.splice(idx, 1);
        state.rows.forEach((r) => { r.values.splice(idx, 1); r.updatedAt = now; });
        save();
        return send(res, 200, state);
      }
      return send(res, 405, { error: 'Method not allowed' });
    }

    if (rowMatch) {
      const id = rowMatch[1] && Number(rowMatch[1]);
      if (req.method === 'POST' && !id) {
        const row = { id: nextId++, ...clean(await readBody(req)), updatedAt: new Date().toISOString() };
        state.rows.push(row); save();
        return send(res, 201, row);
      }
      const row = state.rows.find((r) => r.id === id);
      if (!row) return send(res, 404, { error: 'Not found' });
      if (req.method === 'PUT') {
        Object.assign(row, clean(await readBody(req)), { updatedAt: new Date().toISOString() });
        save();
        return send(res, 200, row);
      }
      if (req.method === 'DELETE') {
        state.rows = state.rows.filter((r) => r.id !== id); save();
        return send(res, 200, { ok: true });
      }
      return send(res, 405, { error: 'Method not allowed' });
    }

    // Static files
    const file = path.join(PUBLIC, url.pathname === '/' ? 'index.html' : url.pathname);
    if (!file.startsWith(PUBLIC) || !fs.existsSync(file)) { res.writeHead(404); return res.end('Not found'); }
    const types = { '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript' };
    res.writeHead(200, { 'Content-Type': types[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  } catch (e) {
    send(res, 400, { error: e.message });
  }
});

loadState().then((s) => {
  state = s;
  nextId = state.rows.reduce((m, r) => Math.max(m, r.id), 0) + 1;
  server.listen(PORT, () => console.log(`Row tracker running at http://localhost:${PORT} (storage: ${collection ? 'MongoDB' : 'data.json'})`));
}).catch((e) => { console.error('Could not start:', e.message); process.exit(1); });
