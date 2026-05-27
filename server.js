require('dotenv').config();
// ============================================================
// PMF Lead Database — Backend API (v3: SQL-driven dedup)
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
// SQL-DRIVEN DEDUP
// Pushes the heavy lifting into Postgres so memory stays bounded.
// ============================================================
async function computeDuplicates(client) {
  console.log('[dedup] starting');
  const t0 = Date.now();

  // Step 1: SQL identifies all duplicate signals — returns just lead IDs
  // grouped by each matching key. Tiny payload, fast with indexes.
  const [emailGroups, phoneGroups, nameGroups] = await Promise.all([
    client.query(`
      SELECT array_agg(id ORDER BY id) AS lead_ids
      FROM leads
      WHERE email <> ''
      GROUP BY email
      HAVING COUNT(*) > 1
    `),
    client.query(`
      SELECT array_agg(id ORDER BY id) AS lead_ids
      FROM leads
      WHERE normalized_phone <> ''
      GROUP BY normalized_phone
      HAVING COUNT(*) > 1
    `),
    client.query(`
      SELECT array_agg(id ORDER BY id) AS lead_ids
      FROM leads
      WHERE full_name <> ''
      GROUP BY full_name
      HAVING COUNT(*) > 1
    `)
  ]);

  console.log(`[dedup] signals: ${emailGroups.rows.length} email, ${phoneGroups.rows.length} phone, ${nameGroups.rows.length} name (${Date.now() - t0}ms)`);

  // Step 2: Collect candidate IDs (only leads with at least one duplicate signal)
  const candidateIds = new Set();
  for (const row of emailGroups.rows) {
    for (const id of row.lead_ids) candidateIds.add(id);
  }
  for (const row of phoneGroups.rows) {
    for (const id of row.lead_ids) candidateIds.add(id);
  }
  // Skip pathologically large name groups (common names like "John Smith")
  // since they're almost always false-positive matches anyway
  for (const row of nameGroups.rows) {
    if (row.lead_ids.length <= 100) {
      for (const id of row.lead_ids) candidateIds.add(id);
    }
  }

  if (candidateIds.size === 0) {
    console.log('[dedup] no candidates found');
    return [];
  }

  console.log(`[dedup] ${candidateIds.size} candidate leads`);

  // Step 3: Fetch full data for candidates only
  const ids = Array.from(candidateIds);
  const leadsResult = await client.query(`
    SELECT l.id, l.batch_id,
           b.name AS batch_name, b.lead_info_api AS batch_lead_info_api,
           l.date_received, l.source, l.lead_info_api, l.lead_date_api,
           l.first_name, l.last_name, l.full_name, l.email, l.phone,
           l.normalized_phone, l.company
    FROM leads l
    JOIN batches b ON b.id = l.batch_id
    WHERE l.id = ANY($1::int[])
  `, [ids]);

  const leadById = new Map();
  for (const row of leadsResult.rows) {
    leadById.set(row.id, {
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

  // Step 4: Union-Find on lead IDs only (tiny memory footprint)
  const parent = new Map();
  const rank = new Map();
  for (const id of ids) parent.set(id, id);

  function find(id) {
    let root = id;
    while (parent.get(root) !== root) root = parent.get(root);
    let cur = id;
    while (parent.get(cur) !== root) {
      const next = parent.get(cur);
      parent.set(cur, root);
      cur = next;
    }
    return root;
  }
  function union(a, b) {
    const ra = find(a), rb = find(b);
    if (ra === rb) return;
    const rankA = rank.get(ra) || 0;
    const rankB = rank.get(rb) || 0;
    if (rankA < rankB) parent.set(ra, rb);
    else if (rankA > rankB) parent.set(rb, ra);
    else { parent.set(rb, ra); rank.set(ra, rankA + 1); }
  }

  // Union by email
  for (const row of emailGroups.rows) {
    const arr = row.lead_ids;
    for (let i = 1; i < arr.length; i++) union(arr[0], arr[i]);
  }
  // Union by phone
  for (const row of phoneGroups.rows) {
    const arr = row.lead_ids;
    for (let i = 1; i < arr.length; i++) union(arr[0], arr[i]);
  }
  // Union by name + similar company (the fuzzy step — only runs on small name groups)
  for (const row of nameGroups.rows) {
    const arr = row.lead_ids;
    if (arr.length < 2 || arr.length > 100) continue;
    for (let i = 0; i < arr.length - 1; i++) {
      const l1 = leadById.get(arr[i]);
      if (!l1 || !l1.company) continue;
      for (let j = i + 1; j < arr.length; j++) {
        const l2 = leadById.get(arr[j]);
        if (!l2 || !l2.company) continue;
        if (stringSimilarity(l1.company, l2.company) >= 0.85) {
          union(arr[i], arr[j]);
        }
      }
    }
  }

  // Step 5: Group leads by their root
  const groupMap = new Map();
  for (const id of ids) {
    const root = find(id);
    if (!groupMap.has(root)) groupMap.set(root, []);
    groupMap.get(root).push(id);
  }

  // Step 6: Build final group objects
  const groups = [];
  const priOrder = ['critical', 'high', 'medium', 'regular', 'none'];

  for (const [, groupIds] of groupMap) {
    if (groupIds.length < 2) continue;
    const gl = groupIds.map(id => leadById.get(id)).filter(Boolean);
    if (gl.length < 2) continue;
    gl.sort((a, b) => new Date(a.dateStr) - new Date(b.dateStr));

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
    groups.push({
      leads: gl,
      priority,
      isCrossBatch: batchIds.size > 1,
      batchCount: batchIds.size,
      occurrenceCount: gl.length
    });
  }

  console.log(`[dedup] done: ${groups.length} groups in ${Date.now() - t0}ms`);
  return groups;
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
          cFirst,
          cLast,
          fullName,
          cEmail,
          cPhone,
          normPhone,
          cCompany
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
  try {
    const groups = await computeDuplicates(client);
    res.json({ totalGroups: groups.length, groups });
  } catch (e) {
    console.error('[GET /api/duplicates]', e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

initDb()
  .then(() => app.listen(PORT, () => console.log(`[ready] server on port ${PORT}`)))
  .catch(err => { console.error('[fatal] DB init failed:', err); process.exit(1); });