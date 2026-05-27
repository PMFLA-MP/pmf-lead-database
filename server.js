require('dotenv').config();
// ============================================================
// PMF Lead Database — Backend API (v5: typed arrays + batched streaming)
// Node.js + Express + PostgreSQL
// ============================================================

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;
const AUTH_TOKEN = process.env.AUTH_TOKEN || 'change-me-in-production';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  max: 10
});

app.use(cors({ origin: '*', exposedHeaders: ['x-auth-token'] }));
app.use(express.json({ limit: '100mb' }));

function requireAuth(req, res, next) {
  const token = req.headers['x-auth-token'] || req.query.token;
  if (!AUTH_TOKEN || token !== AUTH_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ============================================================
// SANITIZATION
// ============================================================
const SENTINEL_VALUES = new Set([
  '', 'not found', 'n/a', 'na', 'none', 'null', 'unknown',
  '-', '--', '#n/a', '#null', 'nil', 'no data', 'tbd', 'pending'
]);
function blank(v) {
  if (v === null || v === undefined) return '';
  const s = String(v).trim();
  if (!s) return '';
  if (SENTINEL_VALUES.has(s.toLowerCase())) return '';
  return s;
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS batches (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      lead_info_api TEXT,
      uploaded_at TIMESTAMPTZ DEFAULT NOW(),
      lead_count INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS leads (
      id SERIAL PRIMARY KEY,
      batch_id INTEGER REFERENCES batches(id) ON DELETE CASCADE,
      date_received TIMESTAMPTZ,
      source TEXT,
      lead_info_api TEXT,
      lead_date_api TEXT,
      first_name TEXT,
      last_name TEXT,
      full_name TEXT,
      email TEXT,
      phone TEXT,
      normalized_phone TEXT,
      company TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email) WHERE email <> '';
    CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(normalized_phone) WHERE normalized_phone <> '';
    CREATE INDEX IF NOT EXISTS idx_leads_full_name ON leads(full_name) WHERE full_name <> '';
    CREATE INDEX IF NOT EXISTS idx_leads_batch_id ON leads(batch_id);
  `);
  console.log('[init] Database schema ready');
}

// ============================================================
// HELPERS
// ============================================================
function normalizePhone(phone) {
  if (!phone) return '';
  return String(phone).replace(/\D/g, '');
}

function stringSimilarity(s1, s2) {
  if (!s1 || !s2) return 0;
  s1 = s1.toLowerCase().trim();
  s2 = s2.toLowerCase().trim();
  if (s1 === s2) return 1;
  const m = [];
  for (let i = 0; i <= s2.length; i++) m[i] = [i];
  for (let j = 0; j <= s1.length; j++) m[0][j] = j;
  for (let i = 1; i <= s2.length; i++) {
    for (let j = 1; j <= s1.length; j++) {
      m[i][j] = s2[i-1] === s1[j-1]
        ? m[i-1][j-1]
        : Math.min(m[i-1][j-1]+1, m[i][j-1]+1, m[i-1][j]+1);
    }
  }
  return 1 - (m[s2.length][s1.length] / Math.max(s1.length, s2.length));
}

function getPriorityLevel(days) {
  if (days <= 30) return 'critical';
  if (days <= 60) return 'high';
  if (days <= 90) return 'medium';
  if (days <= 365) return 'regular';
  return 'none';
}

function determineMatchType(lead, group) {
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName && lead.email && lead.email === o.email) return 'Name + Email';
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName && lead.normalizedPhone && lead.normalizedPhone === o.normalizedPhone) return 'Name + Phone';
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName && lead.company && o.company) {
      const sim = stringSimilarity(lead.company, o.company);
      if (sim >= 0.85) return `Name + Company (${Math.round(sim*100)}%)`;
    }
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.company && o.company && lead.company.toLowerCase().trim() === o.company.toLowerCase().trim()
        && lead.email && lead.email === o.email) return 'Company + Email';
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.company && o.company && lead.company.toLowerCase().trim() === o.company.toLowerCase().trim()
        && lead.normalizedPhone && lead.normalizedPhone === o.normalizedPhone) return 'Company + Phone';
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.email && lead.email === o.email) return 'Email only';
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.normalizedPhone && lead.normalizedPhone === o.normalizedPhone) return 'Phone only';
  }
  return 'Linked via chain';
}

// ============================================================
// DEDUP — typed arrays + batched fetching + streaming
//
// Designed to handle 500k+ candidate leads in <200MB RAM.
// Key tricks:
//   - Union-Find uses Int32Array (2MB for 500k IDs instead of 40MB+ Map)
//   - Companies fetched only for small name groups (not all candidates)
//   - Full lead data fetched lazily in batches of 100 groups at a time
//   - Each batch emitted to the response stream then freed
// ============================================================
async function computeDuplicatesStreaming(client, emit) {
  console.log('[dedup] starting');
  const t0 = Date.now();

  // ----- Step 1: Get signal groups (just ID arrays, no lead data) -----
  const [emailGroups, phoneGroups, nameGroups] = await Promise.all([
    client.query(`
      SELECT array_agg(id ORDER BY id) AS lead_ids
      FROM leads WHERE email <> ''
      GROUP BY email HAVING COUNT(*) > 1
    `),
    client.query(`
      SELECT array_agg(id ORDER BY id) AS lead_ids
      FROM leads WHERE normalized_phone <> ''
      GROUP BY normalized_phone HAVING COUNT(*) > 1
    `),
    client.query(`
      SELECT array_agg(id ORDER BY id) AS lead_ids
      FROM leads WHERE full_name <> ''
      GROUP BY full_name HAVING COUNT(*) > 1
    `)
  ]);
  console.log(`[dedup] signals: ${emailGroups.rows.length} email / ${phoneGroups.rows.length} phone / ${nameGroups.rows.length} name (${Date.now() - t0}ms)`);

  // Extract just the ID arrays — drop the row wrapper objects to save memory
  const emailArrays = emailGroups.rows.map(r => r.lead_ids);
  const phoneArrays = phoneGroups.rows.map(r => r.lead_ids);
  // For names, filter to small groups only (skip pathologically common names)
  const nameArrays = [];
  for (const row of nameGroups.rows) {
    if (row.lead_ids.length >= 2 && row.lead_ids.length <= 100) {
      nameArrays.push(row.lead_ids);
    }
  }
  // Free row wrappers
  emailGroups.rows.length = 0;
  phoneGroups.rows.length = 0;
  nameGroups.rows.length = 0;

  // ----- Step 2: Assign positions for typed-array union-find -----
  const idToIdx = new Map();
  const idxToId = [];
  function getIdx(id) {
    let idx = idToIdx.get(id);
    if (idx === undefined) {
      idx = idxToId.length;
      idToIdx.set(id, idx);
      idxToId.push(id);
    }
    return idx;
  }
  for (const arr of emailArrays) for (const id of arr) getIdx(id);
  for (const arr of phoneArrays) for (const id of arr) getIdx(id);
  for (const arr of nameArrays) for (const id of arr) getIdx(id);

  const N = idxToId.length;
  if (N === 0) {
    console.log('[dedup] no candidates');
    return 0;
  }
  console.log(`[dedup] ${N} candidates`);

  // ----- Step 3: Union-Find with typed arrays (compact memory) -----
  const parent = new Int32Array(N);
  const rank = new Int8Array(N);
  for (let i = 0; i < N; i++) parent[i] = i;

  function find(i) {
    let root = i;
    while (parent[root] !== root) root = parent[root];
    let cur = i;
    while (parent[cur] !== root) {
      const next = parent[cur];
      parent[cur] = root;
      cur = next;
    }
    return root;
  }
  function union(a, b) {
    const ra = find(a), rb = find(b);
    if (ra === rb) return;
    if (rank[ra] < rank[rb]) parent[ra] = rb;
    else if (rank[ra] > rank[rb]) parent[rb] = ra;
    else { parent[rb] = ra; rank[ra]++; }
  }

  for (const arr of emailArrays) {
    const i0 = idToIdx.get(arr[0]);
    for (let k = 1; k < arr.length; k++) union(i0, idToIdx.get(arr[k]));
  }
  emailArrays.length = 0; // free

  for (const arr of phoneArrays) {
    const i0 = idToIdx.get(arr[0]);
    for (let k = 1; k < arr.length; k++) union(i0, idToIdx.get(arr[k]));
  }
  phoneArrays.length = 0; // free

  // ----- Step 4: Name + fuzzy company (needs companies for small name groups) -----
  if (nameArrays.length > 0) {
    const nameGroupIds = new Set();
    for (const arr of nameArrays) for (const id of arr) nameGroupIds.add(id);

    const companyById = new Map();
    const idsToFetch = Array.from(nameGroupIds);
    const FETCH_CHUNK = 5000;
    for (let i = 0; i < idsToFetch.length; i += FETCH_CHUNK) {
      const slice = idsToFetch.slice(i, i + FETCH_CHUNK);
      const r = await client.query(
        `SELECT id, company FROM leads WHERE id = ANY($1::int[]) AND company <> ''`,
        [slice]
      );
      for (const row of r.rows) companyById.set(row.id, row.company);
    }

    for (const arr of nameArrays) {
      for (let i = 0; i < arr.length - 1; i++) {
        const c1 = companyById.get(arr[i]);
        if (!c1) continue;
        const idx1 = idToIdx.get(arr[i]);
        for (let j = i + 1; j < arr.length; j++) {
          const c2 = companyById.get(arr[j]);
          if (!c2) continue;
          if (stringSimilarity(c1, c2) >= 0.85) {
            union(idx1, idToIdx.get(arr[j]));
          }
        }
      }
    }
    companyById.clear();
  }
  nameArrays.length = 0; // free

  console.log(`[dedup] union-find done (${Date.now() - t0}ms)`);

  // ----- Step 5: Collect groups by root, keep only size >= 2 -----
  const rootToGroup = new Map();
  for (let i = 0; i < N; i++) {
    const r = find(i);
    if (!rootToGroup.has(r)) rootToGroup.set(r, []);
    rootToGroup.get(r).push(idxToId[i]);
  }

  const finalGroups = [];
  for (const [, ids] of rootToGroup) {
    if (ids.length >= 2) finalGroups.push(ids);
  }

  // Free union-find structures
  idToIdx.clear();
  idxToId.length = 0;
  rootToGroup.clear();
  console.log(`[dedup] ${finalGroups.length} groups identified`);

  if (finalGroups.length === 0) return 0;

  // ----- Step 6: Fetch full data + emit groups in batches -----
  const priOrder = ['critical', 'high', 'medium', 'regular', 'none'];
  const GROUPS_PER_BATCH = 100;
  let emitted = 0;

  for (let b = 0; b < finalGroups.length; b += GROUPS_PER_BATCH) {
    const batch = finalGroups.slice(b, b + GROUPS_PER_BATCH);
    const allIds = [];
    for (const g of batch) for (const id of g) allIds.push(id);

    const r = await client.query(`
      SELECT l.id, l.batch_id,
             b.name AS batch_name, b.lead_info_api AS batch_lead_info_api,
             l.date_received, l.source, l.lead_info_api, l.lead_date_api,
             l.first_name, l.last_name, l.full_name, l.email, l.phone,
             l.normalized_phone, l.company
      FROM leads l JOIN batches b ON b.id = l.batch_id
      WHERE l.id = ANY($1::int[])
    `, [allIds]);

    const fullById = new Map();
    for (const row of r.rows) {
      fullById.set(row.id, {
        id: row.id,
        batchId: row.batch_id,
        batchName: row.batch_name,
        batchLeadInfoApi: row.batch_lead_info_api,
        dateStr: row.date_received ? row.date_received.toISOString() : null,
        displayDate: row.date_received ? new Date(row.date_received).toLocaleDateString() : '',
        source: row.source,
        leadInfoApi: row.lead_info_api,
        leadDateApi: row.lead_date_api,
        firstName: row.first_name,
        lastName: row.last_name,
        fullName: row.full_name,
        email: row.email,
        phone: row.phone,
        normalizedPhone: row.normalized_phone,
        company: row.company
      });
    }
    r.rows.length = 0; // free pg rows

    for (const ids of batch) {
      const gl = [];
      for (const id of ids) {
        const l = fullById.get(id);
        if (l) gl.push(l);
      }
      if (gl.length < 2) continue;
      gl.sort((a, c) => new Date(a.dateStr) - new Date(c.dateStr));

      gl[0].matchType = 'Original';
      for (let i = 1; i < gl.length; i++) gl[i].matchType = determineMatchType(gl[i], gl);

      let priority = 'none';
      for (let i = 0; i < gl.length - 1; i++) {
        for (let j = i + 1; j < gl.length; j++) {
          const days = (new Date(gl[j].dateStr) - new Date(gl[i].dateStr)) / 86400000;
          const p = getPriorityLevel(days);
          if (priOrder.indexOf(p) < priOrder.indexOf(priority)) priority = p;
        }
      }

      const batchIds = new Set(gl.map(l => l.batchId));
      emit({
        leads: gl,
        priority,
        isCrossBatch: batchIds.size > 1,
        batchCount: batchIds.size,
        occurrenceCount: gl.length
      });
      emitted++;
    }

    fullById.clear();
  }

  console.log(`[dedup] emitted ${emitted} groups in ${Date.now() - t0}ms`);
  return emitted;
}

// ============================================================
// ROUTES
// ============================================================
app.get('/health', (req, res) => res.json({ ok: true, time: new Date().toISOString() }));

app.get('/api/stats', requireAuth, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM batches) AS total_batches,
        (SELECT COUNT(*)::int FROM leads) AS total_leads,
        (SELECT MAX(uploaded_at) FROM batches) AS last_update
    `);
    res.json(r.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/batches', requireAuth, async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT b.id, b.name, b.lead_info_api, b.uploaded_at, b.lead_count,
             COUNT(l.id)::int AS actual_lead_count
      FROM batches b
      LEFT JOIN leads l ON l.batch_id = b.id
      GROUP BY b.id
      ORDER BY b.uploaded_at DESC
    `);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/batches/:id', requireAuth, async (req, res) => {
  try {
    await pool.query('DELETE FROM batches WHERE id = $1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/clear', requireAuth, async (req, res) => {
  try {
    await pool.query('TRUNCATE TABLE batches RESTART IDENTITY CASCADE');
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/batches', requireAuth, async (req, res) => {
  const client = await pool.connect();
  try {
    const { name, leadInfoApi, leads } = req.body;
    if (!Array.isArray(leads) || leads.length === 0) {
      return res.status(400).json({ error: 'No leads provided' });
    }

    await client.query('BEGIN');

    const batchResult = await client.query(
      'INSERT INTO batches (name, lead_info_api, lead_count) VALUES ($1, $2, $3) RETURNING id, uploaded_at',
      [blank(name) || 'Untitled', blank(leadInfoApi), leads.length]
    );
    const batchId = batchResult.rows[0].id;

    const CHUNK = 500;
    for (let i = 0; i < leads.length; i += CHUNK) {
      const slice = leads.slice(i, i + CHUNK);
      const values = [];
      const placeholders = [];
      let pi = 1;
      for (const l of slice) {
        const cFirst = blank(l.firstName);
        const cLast = blank(l.lastName);
        const cCompany = blank(l.company);
        const cEmail = blank(l.email).toLowerCase();
        const cPhone = blank(l.phone);
        const fullName = (cFirst + ' ' + cLast).trim().toLowerCase();
        const normPhone = normalizePhone(cPhone);
        placeholders.push(`($${pi++},$${pi++},$${pi++},$${pi++},$${pi++},$${pi++},$${pi++},$${pi++},$${pi++},$${pi++},$${pi++},$${pi++})`);
        values.push(
          batchId,
          l.dateStr || null,
          blank(l.source),
          blank(l.leadInfoApi),
          blank(l.leadDateApi),
          cFirst, cLast, fullName, cEmail, cPhone, normPhone, cCompany
        );
      }
      await client.query(
        `INSERT INTO leads (batch_id, date_received, source, lead_info_api, lead_date_api,
                            first_name, last_name, full_name, email, phone, normalized_phone, company)
         VALUES ${placeholders.join(',')}`,
        values
      );
    }

    await client.query('COMMIT');
    res.json({ ok: true, batchId, leadCount: leads.length, uploadedAt: batchResult.rows[0].uploaded_at });
  } catch (e) {
    await client.query('ROLLBACK');
    console.error('[POST /api/batches]', e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

app.get('/api/duplicates', requireAuth, async (req, res) => {
  const client = await pool.connect();
  let headersSent = false;
  try {
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store');
    res.write('{"groups":[');
    headersSent = true;

    let first = true;
    const totalGroups = await computeDuplicatesStreaming(client, (group) => {
      if (!first) res.write(',');
      res.write(JSON.stringify(group));
      first = false;
    });

    res.write(`],"totalGroups":${totalGroups}}`);
    res.end();
  } catch (e) {
    console.error('[GET /api/duplicates]', e);
    if (!headersSent) res.status(500).json({ error: e.message });
    else {
      try { res.write(`],"totalGroups":0,"error":${JSON.stringify(e.message)}}`); } catch (_) {}
      res.end();
    }
  } finally {
    client.release();
  }
});

initDb()
  .then(() => app.listen(PORT, () => console.log(`[ready] server on port ${PORT}`)))
  .catch(err => { console.error('[fatal] DB init failed:', err); process.exit(1); });