// Node.js server: serves the page and a small JSON API, saving to MongoDB (or data.json).
// Every create/update of a row stamps its updatedAt on the server.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

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

// Writes are queued so they land in order. save() resolves once this write is stored, and rejects if it
// could not be, so the request that changed the data can tell the browser.
const save = () => {
  const snapshot = JSON.stringify(state);
  const job = writeQueue.then(() => collection
    ? collection.replaceOne({ _id: 'main' }, { _id: 'main', ...JSON.parse(snapshot) }, { upsert: true })
    : fs.promises.writeFile(DATA, JSON.stringify(JSON.parse(snapshot), null, 2))
  );
  writeQueue = job.catch((e) => console.error('Save failed:', e.message));
  return job;
};

// API requests run one at a time (see exclusive below), so a request never sees another one half done.
let lock = Promise.resolve();
const exclusive = (fn) => {
  const run = lock.then(fn);
  lock = run.catch(() => {});
  return run;
};

const MAX_VALUE = 10000000;   // largest number allowed in a value cell

function clean(body) {
  const code = String(body.code ?? '').trim().slice(0, 50);
  const values = state.columns.map((_, i) => {
    const n = parseFloat(body.values?.[i]);
    return Number.isFinite(n) ? n : 0;
  });
  if (values.some((v) => Math.abs(v) > MAX_VALUE)) throw new Error('Values cannot be larger than 10,000,000');
  return { code, values };
}

function readBody(req) {
  if (req.parsedBody) return Promise.resolve(req.parsedBody);
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

// ---- Password protection -------------------------------------------------------------------
// The password is stored in MongoDB (document _id 'auth') as a salted scrypt hash, never as text.
// On the very first start with an empty database it is seeded from APP_PASSWORD in .env; after
// that the database is the source of truth and APP_PASSWORD can be removed from .env.
// Without MongoDB (data.json mode) APP_PASSWORD from .env is used directly.
// A correct login sets a signed, HttpOnly cookie. The signing key is derived from the stored hash,
// so servers sharing the database accept each other's cookies and changing the password signs
// everyone else out.
const APP_PASSWORD = process.env.APP_PASSWORD;
// A login lasts SESSION_MINUTES (default 5) and is renewed by every API call, so it ends after that
// much inactivity.
const SESSION_MIN = Number(process.env.SESSION_MINUTES) || 5;
// When served under a sub-path (e.g. /rowtracker behind nginx) set COOKIE_PATH to that path.
const COOKIE_PATH = process.env.COOKIE_PATH || '/';
const COOKIE_NAME = 'rowtracker_session';
const scrypt = (pw, salt) => new Promise((resolve, reject) =>
  crypto.scrypt(String(pw), salt, 64, (e, key) => (e ? reject(e) : resolve(key))));

let auth = null;       // { salt: hex, hash: hex }
let authLoadedAt = 0;

async function makeAuth(password) {
  const salt = crypto.randomBytes(16);
  return { salt: salt.toString('hex'), hash: (await scrypt(password, salt)).toString('hex') };
}

async function loadAuth() {
  if (!collection) {
    if (!APP_PASSWORD) throw new Error('Set APP_PASSWORD in .env.');
    auth = auth || await makeAuth(APP_PASSWORD);
    return auth;
  }
  if (auth && Date.now() - authLoadedAt < 5000) return auth;   // re-check the database every few seconds
  let doc = await collection.findOne({ _id: 'auth' });
  if (!doc) {
    if (!APP_PASSWORD) throw new Error('No password stored yet. Set APP_PASSWORD in .env for the first start.');
    doc = { _id: 'auth', ...(await makeAuth(APP_PASSWORD)) };
    await collection.insertOne(doc);
    console.log('Password saved to the database (hashed). You can remove APP_PASSWORD from .env now.');
  }
  auth = { salt: doc.salt, hash: doc.hash };
  authLoadedAt = Date.now();
  return auth;
}

async function checkPassword(pw) {
  const a = await loadAuth();
  const key = await scrypt(pw, Buffer.from(a.salt, 'hex'));
  return crypto.timingSafeEqual(key, Buffer.from(a.hash, 'hex'));
}

const sign = (s) => crypto.createHmac('sha256', crypto.createHash('sha256').update(auth.hash).digest()).update(s).digest('base64url');
const same = (a, b) => {
  const x = crypto.createHash('sha256').update(String(a)).digest();
  const y = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(x, y);
};
const newToken = () => { const exp = String(Date.now() + SESSION_MIN * 60e3); return `${exp}.${sign(exp)}`; };
async function isAuthed(req) {
  await loadAuth();
  const m = (req.headers.cookie || '').match(new RegExp(`(?:^|;\\s*)${COOKIE_NAME}=([^;]+)`));
  if (!m) return false;
  const [exp, sig] = m[1].split('.');
  return Boolean(exp && sig) && same(sig, sign(exp)) && Number(exp) > Date.now();
}
const cookie = (req, value, maxAge) =>
  `${COOKIE_NAME}=${value}; HttpOnly; SameSite=Strict; Path=${COOKIE_PATH}; Max-Age=${maxAge}` +
  (req.socket.encrypted || req.headers['x-forwarded-proto'] === 'https' ? '; Secure' : '');

// Lock an address out for 15 minutes after 5 wrong passwords.
const attempts = new Map();
const MAX_FAILS = 5, LOCK_MS = 15 * 60 * 1000;

async function handleApi(req, res, url, rowMatch, colMatch) {
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
  if (url.pathname === '/api/ping') return send(res, 200, { ok: true });
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
    await save();
    return send(res, 200, state);
  }

  if (colMatch) {
    const idx = colMatch[1] === undefined ? null : Number(colMatch[1]);
    if (req.method === 'POST' && idx === null) {
      const body = await readBody(req);
      state.columns.push(String(body.name || `Value ${state.columns.length + 1}`).trim().slice(0, 40));
      state.rows.forEach((r) => r.values.push(0));
      await save();
      return send(res, 201, state);
    }
    if (idx === null || idx >= state.columns.length) return send(res, 404, { error: 'Not found' });
    if (req.method === 'PUT') {
      const body = await readBody(req);
      state.columns[idx] = String(body.name ?? '').trim().slice(0, 40) || state.columns[idx];
      await save();
      return send(res, 200, state);
    }
    if (req.method === 'DELETE') {
      const now = new Date().toISOString();
      state.columns.splice(idx, 1);
      state.rows.forEach((r) => { r.values.splice(idx, 1); r.updatedAt = now; });
      await save();
      return send(res, 200, state);
    }
    return send(res, 405, { error: 'Method not allowed' });
  }

  if (rowMatch) {
    const id = rowMatch[1] && Number(rowMatch[1]);
    if (req.method === 'POST' && !id) {
      const row = { id: nextId++, ...clean(await readBody(req)), updatedAt: new Date().toISOString() };
      state.rows.push(row); await save();
      return send(res, 201, row);
    }
    const row = state.rows.find((r) => r.id === id);
    if (!row) return send(res, 404, { error: 'Not found' });
    if (req.method === 'PUT') {
      Object.assign(row, clean(await readBody(req)), { updatedAt: new Date().toISOString() });
      await save();
      return send(res, 200, row);
    }
    if (req.method === 'DELETE') {
      state.rows = state.rows.filter((r) => r.id !== id); await save();
      return send(res, 200, { ok: true });
    }
    return send(res, 405, { error: 'Method not allowed' });
  }

  return send(res, 404, { error: 'Not found' });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url.replace(/^\/{2,}/, '/'), 'http://localhost');
  const rowMatch = url.pathname.match(/^\/api\/rows(?:\/(\d+))?$/);
  const colMatch = url.pathname.match(/^\/api\/columns(?:\/(\d+))?$/);
  try {
    if (url.pathname === '/login' && req.method === 'POST') {
      const ip = req.socket.remoteAddress;
      const a = attempts.get(ip) || { fails: 0, until: 0 };
      if (a.until > Date.now()) return send(res, 429, { error: 'Too many attempts. Try again in a few minutes.' });
      const { password } = await readBody(req);
      if (typeof password === 'string' && await checkPassword(password)) {
        attempts.delete(ip);
        res.setHeader('Set-Cookie', cookie(req, newToken(), SESSION_MIN * 60));
        return send(res, 200, { ok: true });
      }
      a.fails += 1;
      if (a.fails >= MAX_FAILS) { a.fails = 0; a.until = Date.now() + LOCK_MS; }
      attempts.set(ip, a);
      return send(res, 401, { error: 'Wrong password' });
    }
    if (url.pathname === '/logout' && req.method === 'POST') {
      res.setHeader('Set-Cookie', cookie(req, '', 0));
      return send(res, 200, { ok: true });
    }
    if (url.pathname === '/api/password' && req.method === 'POST' && (await isAuthed(req))) {
      if (!collection) return send(res, 400, { error: 'Password is set by APP_PASSWORD in .env when MongoDB is not used.' });
      const { current, next } = await readBody(req);
      if (!(await checkPassword(String(current ?? '')))) return send(res, 401, { error: 'Current password is wrong' });
      if (typeof next !== 'string' || !next) return send(res, 400, { error: 'New password cannot be empty' });
      const fresh = await makeAuth(next);
      await collection.replaceOne({ _id: 'auth' }, { _id: 'auth', ...fresh, changedAt: new Date().toISOString() }, { upsert: true });
      auth = fresh; authLoadedAt = Date.now();
      res.setHeader('Set-Cookie', cookie(req, newToken(), SESSION_MIN * 60));   // keep this browser logged in
      return send(res, 200, { ok: true });
    }
    if (!(await isAuthed(req))) {
      if (url.pathname.startsWith('/api/')) return send(res, 401, { error: 'Not logged in' });
      res.writeHead(200, { 'Content-Type': 'text/html', 'Cache-Control': 'no-store' });
      return fs.createReadStream(path.join(PUBLIC, 'login.html')).pipe(res);   // login page for every other URL
    }

    if (url.pathname.startsWith('/api/')) {
      res.setHeader('Set-Cookie', cookie(req, newToken(), SESSION_MIN * 60));   // activity keeps the login alive
      if (req.method === 'POST' || req.method === 'PUT') req.parsedBody = await readBody(req);   // read before taking the lock
      return await exclusive(() => handleApi(req, res, url, rowMatch, colMatch));
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

loadState().then(async (s) => {
  state = s;
  await loadAuth();
  nextId = state.rows.reduce((m, r) => Math.max(m, r.id), 0) + 1;
  server.listen(PORT, () => console.log(`Row tracker running at http://localhost:${PORT} (storage: ${collection ? 'MongoDB' : 'data.json'})`));
}).catch((e) => { console.error('Could not start:', e.message); process.exit(1); });
