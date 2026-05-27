require('dotenv').config();
// ============================================================
// PMF Lead Database — Backend API (v7: tier-based dedup)
// Node.js + Express + PostgreSQL
//
// CHANGES from v6:
//   - Match results classified into 4 tiers: exact | strong | probable | fuzzy.
//     * exact:    all four fields (name/email/phone/company) byte-identical
//     * strong:   v6's "strong" groups (name+contact+company≥85%)
//     * probable: v6's "weak" pairs (name+contact, companies differ/blank)
//     * fuzzy:    name+company 75–84% similar OR same-company different-names
//   - Added company signal-group processing for "different contacts at same business"
//   - Added PATCH /api/batches/:id for inline batch renaming
//   - Removed the priority field on emitted groups (frontend no longer uses it)
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

function normalizeCompany(c) {
  return (c || '').toLowerCase().trim();
}

function stringSimilarity(s1, s2) {
  if (!s1 || !s2) return 0;
  s1 = s1.toLowerCase().trim();
  s2 = s2.toLowerCase().trim();
  if (s1 === s2) return 1;
  const maxLen = Math.max(s1.length, s2.length);
  if (maxLen === 0) return 0;
  // Fast-reject: edit distance >= |len(s1) - len(s2)|; if the length gap alone
  // pushes max similarity below 50%, no chance of clearing any threshold we use (≥75%).
  if (Math.abs(s1.length - s2.length) > maxLen * 0.5) return 0;
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

// Thresholds
const COMPANY_SIM_STRONG = 0.85;        // strong tier requires this
const COMPANY_SIM_FUZZY  = 0.75;        // fuzzy tier accepts down to this
const MIN_COMPANY_LEN_FOR_FUZZY = 5;    // skip super-short company strings for fuzzy
const MAX_SIGNAL_GROUP_SIZE = 100;      // skip pathological email/phone/name groups
const MAX_COMPANY_GROUP_SIZE = 50;      // tighter cap for company groups (more noise)

function companiesStrong(c1, c2) {
  if (!c1 || !c2) return false;
  return stringSimilarity(c1, c2) >= COMPANY_SIM_STRONG;
}

// Tier classifier — applied at emit time once full lead data is available.
function classifyTier(leads, source) {
  if (source === 'weak-pair') return 'probable';
  if (source === 'fuzzy-pair' || source === 'fuzzy-cluster') return 'fuzzy';
  // source === 'strong-group' — could be exact or strong
  const f = leads[0];
  const refName = f.fullName || '';
  const refEmail = f.email || '';
  const refPhone = f.normalizedPhone || '';
  const refCompany = normalizeCompany(f.company);
  for (let i = 1; i < leads.length; i++) {
    const l = leads[i];
    if ((l.fullName || '') !== refName) return 'strong';
    if ((l.email || '') !== refEmail) return 'strong';
    if ((l.normalizedPhone || '') !== refPhone) return 'strong';
    if (normalizeCompany(l.company) !== refCompany) return 'strong';
  }
  return 'exact';
}

// determineMatchType — per-lead badge. Tier-aware so weak/fuzzy pairs are tagged clearly.
function determineMatchType(lead, group, source) {
  if (source === 'fuzzy-cluster') {
    return 'Same Company';
  }
  if (source === 'fuzzy-pair') {
    // Name match + company 75-84% similar
    for (const o of group) {
      if (o === lead) continue;
      if (lead.fullName && lead.fullName === o.fullName && lead.company && o.company) {
        const sim = stringSimilarity(lead.company, o.company);
        if (sim >= COMPANY_SIM_FUZZY && sim < COMPANY_SIM_STRONG) {
          return `Name + Company (${Math.round(sim*100)}%)`;
        }
      }
    }
    return 'Fuzzy match';
  }
  if (source === 'weak-pair') {
    for (const o of group) {
      if (o === lead) continue;
      if (lead.fullName && lead.fullName === o.fullName
          && lead.email && lead.email === o.email) {
        return 'Name + Email (unverified)';
      }
    }
    for (const o of group) {
      if (o === lead) continue;
      if (lead.fullName && lead.fullName === o.fullName
          && lead.normalizedPhone && lead.normalizedPhone === o.normalizedPhone) {
        return 'Name + Phone (unverified)';
      }
    }
    return 'Weak match';
  }
  // source === 'strong-group'
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName
        && lead.email && lead.email === o.email
        && companiesStrong(lead.company, o.company)) {
      return 'Name + Email';
    }
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName
        && lead.normalizedPhone && lead.normalizedPhone === o.normalizedPhone
        && companiesStrong(lead.company, o.company)) {
      return 'Name + Phone';
    }
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName && lead.company && o.company) {
      const sim = stringSimilarity(lead.company, o.company);
      if (sim >= COMPANY_SIM_STRONG) return `Name + Company (${Math.round(sim*100)}%)`;
    }
  }
  return 'Linked via chain';
}

// ============================================================
// DEDUP — tier-based emission
// ============================================================
async function computeDuplicatesStreaming(client, emit) {
  console.log('[dedup] starting');
  const t0 = Date.now();

  // ----- Step 1: Get all four signal groups -----
  const [emailGroups, phoneGroups, nameGroups, companyGroups] = await Promise.all([
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
    `),
    client.query(`
      SELECT array_agg(id ORDER BY id) AS lead_ids
      FROM leads
      WHERE company <> '' AND LENGTH(TRIM(company)) >= ${MIN_COMPANY_LEN_FOR_FUZZY}
      GROUP BY LOWER(TRIM(company)) HAVING COUNT(*) > 1
    `)
  ]);

  const filterGroups = (rows, cap) => {
    const out = [];
    for (const row of rows) {
      if (row.lead_ids.length >= 2 && row.lead_ids.length <= cap) out.push(row.lead_ids);
    }
    return out;
  };
  const emailArrays   = filterGroups(emailGroups.rows,   MAX_SIGNAL_GROUP_SIZE);
  const phoneArrays   = filterGroups(phoneGroups.rows,   MAX_SIGNAL_GROUP_SIZE);
  const nameArrays    = filterGroups(nameGroups.rows,    MAX_SIGNAL_GROUP_SIZE);
  const companyArrays = filterGroups(companyGroups.rows, MAX_COMPANY_GROUP_SIZE);
  emailGroups.rows.length = 0;
  phoneGroups.rows.length = 0;
  nameGroups.rows.length = 0;
  companyGroups.rows.length = 0;

  console.log(`[dedup] signals: ${emailArrays.length} email / ${phoneArrays.length} phone / ${nameArrays.length} name / ${companyArrays.length} company (${Date.now() - t0}ms)`);

  // ----- Step 2: Collect all candidate IDs -----
  const allCandidateIds = new Set();
  for (const arr of emailArrays)   for (const id of arr) allCandidateIds.add(id);
  for (const arr of phoneArrays)   for (const id of arr) allCandidateIds.add(id);
  for (const arr of nameArrays)    for (const id of arr) allCandidateIds.add(id);
  for (const arr of companyArrays) for (const id of arr) allCandidateIds.add(id);

  if (allCandidateIds.size === 0) {
    console.log('[dedup] no candidates');
    return 0;
  }

  // ----- Step 3: Fetch (name, company) for all candidates -----
  const meta = new Map();
  const candidateIdArr = Array.from(allCandidateIds);
  allCandidateIds.clear();
  const FETCH_CHUNK = 5000;
  for (let i = 0; i < candidateIdArr.length; i += FETCH_CHUNK) {
    const slice = candidateIdArr.slice(i, i + FETCH_CHUNK);
    const r = await client.query(
      `SELECT id, full_name, company FROM leads WHERE id = ANY($1::int[])`,
      [slice]
    );
    for (const row of r.rows) {
      meta.set(row.id, { name: row.full_name || '', company: row.company || '' });
    }
    r.rows.length = 0;
  }
  console.log(`[dedup] meta fetched for ${meta.size} candidates (${Date.now() - t0}ms)`);

  // ----- Step 4: Typed-array union-find -----
  const N = candidateIdArr.length;
  const idToIdx = new Map();
  for (let i = 0; i < N; i++) idToIdx.set(candidateIdArr[i], i);
  const idxToId = candidateIdArr;

  const parent = new Int32Array(N);
  const rankArr = new Int8Array(N);
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
    if (rankArr[ra] < rankArr[rb]) parent[ra] = rb;
    else if (rankArr[ra] > rankArr[rb]) parent[rb] = ra;
    else { parent[rb] = ra; rankArr[ra]++; }
  }

  // Pair stores — keyed by "minId-maxId" to dedupe across signal groups.
  const weakPairs = new Map();  // Tier 3 candidates (name+contact, company unverified)
  const fuzzyPairs = new Map(); // Tier 4 candidates (name + 75-84% company)

  function pairKey(idA, idB) {
    return idA < idB ? `${idA}-${idB}` : `${idB}-${idA}`;
  }
  function addWeakPair(idA, idB, signal) {
    const k = pairKey(idA, idB);
    if (weakPairs.has(k)) return;
    weakPairs.set(k, [Math.min(idA, idB), Math.max(idA, idB), signal]);
  }
  function addFuzzyPair(idA, idB, signal) {
    const k = pairKey(idA, idB);
    if (weakPairs.has(k) || fuzzyPairs.has(k)) return;
    fuzzyPairs.set(k, [Math.min(idA, idB), Math.max(idA, idB), signal]);
  }

  // ----- Step 5: Email signal groups -----
  // same email + same name + companies≥85% → STRONG
  // same email + same name + companies differ/blank → PROBABLE
  // same email + different name → NO EDGE
  for (const arr of emailArrays) {
    for (let i = 0; i < arr.length - 1; i++) {
      const a = meta.get(arr[i]);
      if (!a || !a.name) continue;
      for (let j = i + 1; j < arr.length; j++) {
        const b = meta.get(arr[j]);
        if (!b || !b.name) continue;
        if (a.name !== b.name) continue;
        if (companiesStrong(a.company, b.company)) {
          union(idToIdx.get(arr[i]), idToIdx.get(arr[j]));
        } else {
          addWeakPair(arr[i], arr[j], 'email');
        }
      }
    }
  }
  emailArrays.length = 0;

  // ----- Step 6: Phone signal groups (same rules) -----
  for (const arr of phoneArrays) {
    for (let i = 0; i < arr.length - 1; i++) {
      const a = meta.get(arr[i]);
      if (!a || !a.name) continue;
      for (let j = i + 1; j < arr.length; j++) {
        const b = meta.get(arr[j]);
        if (!b || !b.name) continue;
        if (a.name !== b.name) continue;
        if (companiesStrong(a.company, b.company)) {
          union(idToIdx.get(arr[i]), idToIdx.get(arr[j]));
        } else {
          addWeakPair(arr[i], arr[j], 'phone');
        }
      }
    }
  }
  phoneArrays.length = 0;

  // ----- Step 7: Name signal groups -----
  // companies ≥85% → STRONG
  // companies 75-84% → FUZZY pair (NEW in v7)
  for (const arr of nameArrays) {
    for (let i = 0; i < arr.length - 1; i++) {
      const a = meta.get(arr[i]);
      if (!a || !a.company || a.company.length < MIN_COMPANY_LEN_FOR_FUZZY) continue;
      for (let j = i + 1; j < arr.length; j++) {
        const b = meta.get(arr[j]);
        if (!b || !b.company || b.company.length < MIN_COMPANY_LEN_FOR_FUZZY) continue;
        const sim = stringSimilarity(a.company, b.company);
        if (sim >= COMPANY_SIM_STRONG) {
          union(idToIdx.get(arr[i]), idToIdx.get(arr[j]));
        } else if (sim >= COMPANY_SIM_FUZZY) {
          addFuzzyPair(arr[i], arr[j], 'name+company-fuzzy');
        }
      }
    }
  }
  nameArrays.length = 0;

  console.log(`[dedup] union-find done, ${weakPairs.size} probable, ${fuzzyPairs.size} fuzzy-pair (${Date.now() - t0}ms)`);

  // ----- Step 8: Company signal groups → fuzzy CLUSTERS (NEW in v7) -----
  // "Same business, different contacts." For each company group, pick one representative
  // per (strong-component-root, name) combo. Emit only if ≥2 distinct names remain.
  const fuzzyClusters = []; // arrays of representative IDs
  for (const arr of companyArrays) {
    const seen = new Map(); // "root|name" -> first id
    const distinctNames = new Set();
    for (const id of arr) {
      const m = meta.get(id);
      if (!m || !m.name) continue;
      const root = find(idToIdx.get(id));
      const key = `${root}|${m.name}`;
      if (!seen.has(key)) {
        seen.set(key, id);
        distinctNames.add(m.name);
      }
    }
    if (distinctNames.size >= 2) {
      fuzzyClusters.push(Array.from(seen.values()));
    }
  }
  companyArrays.length = 0;
  console.log(`[dedup] ${fuzzyClusters.length} fuzzy clusters (${Date.now() - t0}ms)`);

  // ----- Step 9: Collect strong groups -----
  const rootToGroup = new Map();
  for (let i = 0; i < N; i++) {
    const r = find(i);
    if (!rootToGroup.has(r)) rootToGroup.set(r, []);
    rootToGroup.get(r).push(idxToId[i]);
  }
  const strongGroups = [];
  for (const [, ids] of rootToGroup) {
    if (ids.length >= 2) strongGroups.push(ids);
  }
  rootToGroup.clear();

  // ----- Step 10: Filter pairs — drop those already inside a strong group -----
  const weakPairList = [];
  for (const [, pair] of weakPairs) {
    const ra = find(idToIdx.get(pair[0]));
    const rb = find(idToIdx.get(pair[1]));
    if (ra === rb) continue;
    weakPairList.push(pair);
  }
  weakPairs.clear();

  const fuzzyPairList = [];
  for (const [, pair] of fuzzyPairs) {
    const ra = find(idToIdx.get(pair[0]));
    const rb = find(idToIdx.get(pair[1]));
    if (ra === rb) continue;
    fuzzyPairList.push(pair);
  }
  fuzzyPairs.clear();

  console.log(`[dedup] final: ${strongGroups.length} strong, ${weakPairList.length} probable, ${fuzzyPairList.length} fuzzy-pair, ${fuzzyClusters.length} fuzzy-cluster`);

  meta.clear();
  idToIdx.clear();

  const totalGroups = strongGroups.length + weakPairList.length + fuzzyPairList.length + fuzzyClusters.length;
  if (totalGroups === 0) return 0;

  // ----- Step 11: Build emission queue -----
  const queue = [];
  for (const ids of strongGroups)   queue.push({ source: 'strong-group',  ids });
  for (const p of weakPairList)     queue.push({ source: 'weak-pair',     ids: [p[0], p[1]], signal: p[2] });
  for (const p of fuzzyPairList)    queue.push({ source: 'fuzzy-pair',    ids: [p[0], p[1]], signal: p[2] });
  for (const ids of fuzzyClusters)  queue.push({ source: 'fuzzy-cluster', ids, signal: 'same-company' });
  strongGroups.length = 0;
  weakPairList.length = 0;
  fuzzyPairList.length = 0;
  fuzzyClusters.length = 0;

  // ----- Step 12: Fetch full lead data and emit in batches -----
  const GROUPS_PER_BATCH = 100;
  let emitted = 0;

  for (let b = 0; b < queue.length; b += GROUPS_PER_BATCH) {
    const batch = queue.slice(b, b + GROUPS_PER_BATCH);
    const allIds = [];
    for (const g of batch) for (const id of g.ids) allIds.push(id);

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
    r.rows.length = 0;

    for (const g of batch) {
      const gl = [];
      for (const id of g.ids) {
        const l = fullById.get(id);
        if (l) gl.push(l);
      }
      if (gl.length < 2) continue;
      gl.sort((a, c) => new Date(a.dateStr) - new Date(c.dateStr));

      gl[0].matchType = 'Original';
      for (let i = 1; i < gl.length; i++) {
        gl[i].matchType = determineMatchType(gl[i], gl, g.source);
      }

      const matchTier = classifyTier(gl, g.source);
      const batchIds = new Set(gl.map(l => l.batchId));
      const sources = new Set(gl.map(l => l.source).filter(Boolean));

      emit({
        leads: gl,
        matchTier,                                  // 'exact' | 'strong' | 'probable' | 'fuzzy'
        isCrossBatch: batchIds.size > 1,
        batchCount: batchIds.size,
        sourceCount: sources.size,
        occurrenceCount: gl.length,
        signal: g.signal || null                    // 'email' | 'phone' | 'name+company-fuzzy' | 'same-company' | null
      });
      emitted++;
    }
    fullById.clear();
  }

  queue.length = 0;
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

// NEW in v7: rename a batch's name and/or lead_info_api.
app.patch('/api/batches/:id', requireAuth, async (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    if (isNaN(id)) return res.status(400).json({ error: 'Invalid batch id' });

    const updates = [];
    const values = [];
    let p = 1;
    if (typeof req.body.name === 'string') {
      const n = blank(req.body.name);
      if (!n) return res.status(400).json({ error: 'Name cannot be blank' });
      updates.push(`name = $${p++}`);
      values.push(n);
    }
    if (typeof req.body.lead_info_api === 'string') {
      updates.push(`lead_info_api = $${p++}`);
      values.push(blank(req.body.lead_info_api));
    }
    if (updates.length === 0) {
      return res.status(400).json({ error: 'No updatable fields provided' });
    }
    values.push(id);

    const r = await pool.query(
      `UPDATE batches SET ${updates.join(', ')} WHERE id = $${p} RETURNING id, name, lead_info_api, uploaded_at, lead_count`,
      values
    );
    if (r.rows.length === 0) return res.status(404).json({ error: 'Batch not found' });
    res.json(r.rows[0]);
  } catch (e) {
    console.error('[PATCH /api/batches/:id]', e);
    res.status(500).json({ error: e.message });
  }
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

// ============================================================
// CLEAN EXPORT — one CSV ready for Salesforce upload
// ============================================================
// Rules:
//   Exact + Strong groups → merge to one row:
//     - name fields: from the oldest record (they match by definition)
//     - email + phone: most recent non-blank value
//     - company: longest version (usually the most complete)
//     - date received: earliest
//     - source + lead_info_api: unique values joined with ' | '
//   Probable + Fuzzy-pair groups → keep one row, the winner by most-non-blank-fields
//   Fuzzy-cluster groups → keep ALL rows (these are co-workers at the same business,
//     not duplicates of each other); each gets a "Co-worker at: X" note
//   Leads in no group → pass through as Unique
// ============================================================

function csvField(s) {
  if (s == null) return '';
  const str = String(s);
  if (str.indexOf('"') >= 0 || str.indexOf(',') >= 0 || str.indexOf('\n') >= 0 || str.indexOf('\r') >= 0) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function csvLine(fields) {
  return fields.map(csvField).join(',') + '\r\n';
}

const CLEAN_HEADER = csvLine([
  'First Name', 'Last Name', 'Company', 'Email', 'Phone',
  'Lead Source', 'Lead Info API', 'Date Received',
  'Match Quality', 'Duplicate Count', 'Notes'
]);

function dateMs(l) {
  return l.dateStr ? new Date(l.dateStr).getTime() : 0;
}

function mergeRow(leads, quality) {
  const oldest = [...leads].sort((a, b) => dateMs(a) - dateMs(b))[0];
  const newestFirst = [...leads].sort((a, b) => dateMs(b) - dateMs(a));

  const mostRecentNonBlank = (field) => {
    for (const l of newestFirst) {
      const v = (l[field] || '').toString().trim();
      if (v) return v;
    }
    return '';
  };
  const longest = (field) => {
    let best = '';
    for (const l of leads) {
      const v = (l[field] || '').toString().trim();
      if (v.length > best.length) best = v;
    }
    return best;
  };
  const uniqueConcat = (field) => {
    const seen = new Set();
    const out = [];
    for (const l of leads) {
      const v = (l[field] || '').toString().trim();
      if (v && !seen.has(v.toLowerCase())) {
        seen.add(v.toLowerCase());
        out.push(v);
      }
    }
    return out.join(' | ');
  };

  return [
    oldest.firstName || '',
    oldest.lastName || '',
    longest('company'),
    mostRecentNonBlank('email'),
    mostRecentNonBlank('phone'),
    uniqueConcat('source'),
    uniqueConcat('batchLeadInfoApi'),
    oldest.displayDate || '',
    quality,
    leads.length,
    ''
  ];
}

function winnerRow(leads, quality) {
  const score = (l) => {
    let s = 0;
    if ((l.firstName || '').toString().trim()) s++;
    if ((l.lastName  || '').toString().trim()) s++;
    if ((l.email     || '').toString().trim()) s++;
    if ((l.phone     || '').toString().trim()) s++;
    if ((l.company   || '').toString().trim()) s++;
    if ((l.source    || '').toString().trim()) s++;
    return s;
  };
  const sorted = [...leads].sort((a, b) => {
    const ds = score(b) - score(a);
    if (ds !== 0) return ds;
    return dateMs(a) - dateMs(b); // older wins ties (preserves first-seen date)
  });
  const w = sorted[0];
  return [
    w.firstName || '',
    w.lastName || '',
    w.company || '',
    w.email || '',
    w.phone || '',
    w.source || '',
    w.batchLeadInfoApi || '',
    w.displayDate || '',
    quality,
    leads.length,
    ''
  ];
}

function passThroughRow(l, quality, notes) {
  return [
    l.firstName || '',
    l.lastName || '',
    l.company || '',
    l.email || '',
    l.phone || '',
    l.source || '',
    l.batchLeadInfoApi || '',
    l.displayDate || '',
    quality,
    1,
    notes || ''
  ];
}

app.get('/api/export/clean', requireAuth, async (req, res) => {
  const client = await pool.connect();
  let headersSent = false;
  const memberIds = new Set();
  let groupsWritten = 0;
  let uniquesWritten = 0;

  try {
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="leads_clean_for_salesforce.csv"');
    res.setHeader('Cache-Control', 'no-store');
    res.write('\uFEFF');      // BOM for Excel
    res.write(CLEAN_HEADER);
    headersSent = true;

    // Step 1: run dedup, emit one row per group (or N rows for fuzzy-clusters)
    await computeDuplicatesStreaming(client, (group) => {
      for (const l of group.leads) memberIds.add(l.id);

      if (group.matchTier === 'exact') {
        res.write(csvLine(mergeRow(group.leads, 'Exact')));
        groupsWritten++;
      } else if (group.matchTier === 'strong') {
        res.write(csvLine(mergeRow(group.leads, 'Strong')));
        groupsWritten++;
      } else if (group.matchTier === 'probable') {
        res.write(csvLine(winnerRow(group.leads, 'Probable')));
        groupsWritten++;
      } else if (group.matchTier === 'fuzzy') {
        if (group.signal === 'same-company') {
          // Co-workers at the same business — keep every record
          const company = group.leads[0].company || '';
          for (const l of group.leads) {
            res.write(csvLine(passThroughRow(l, 'Unique', `Co-worker at: ${company}`)));
            groupsWritten++;
          }
        } else {
          res.write(csvLine(winnerRow(group.leads, 'Fuzzy')));
          groupsWritten++;
        }
      }
    });

    console.log(`[clean-export] ${groupsWritten} rows from groups, ${memberIds.size} member ids tracked`);

    // Step 2: stream all leads NOT in any duplicate group as Unique
    let lastId = 0;
    const CHUNK = 5000;
    while (true) {
      const r = await client.query(`
        SELECT l.id, b.lead_info_api AS batch_lead_info_api,
               l.date_received, l.source, l.first_name, l.last_name,
               l.email, l.phone, l.company
        FROM leads l JOIN batches b ON b.id = l.batch_id
        WHERE l.id > $1 ORDER BY l.id LIMIT $2
      `, [lastId, CHUNK]);
      if (r.rows.length === 0) break;
      for (const row of r.rows) {
        lastId = row.id;
        if (memberIds.has(row.id)) continue;
        const lead = {
          firstName: row.first_name,
          lastName: row.last_name,
          company: row.company,
          email: row.email,
          phone: row.phone,
          source: row.source,
          batchLeadInfoApi: row.batch_lead_info_api,
          displayDate: row.date_received ? new Date(row.date_received).toLocaleDateString() : ''
        };
        res.write(csvLine(passThroughRow(lead, 'Unique', '')));
        uniquesWritten++;
      }
      r.rows.length = 0;
    }

    console.log(`[clean-export] total: ${groupsWritten} group rows + ${uniquesWritten} unique rows = ${groupsWritten + uniquesWritten}`);
    res.end();
  } catch (e) {
    console.error('[GET /api/export/clean]', e);
    if (!headersSent) {
      res.status(500).json({ error: e.message });
    } else {
      try { res.write(`\r\n# ERROR mid-stream: ${e.message}\r\n`); } catch (_) {}
      res.end();
    }
  } finally {
    client.release();
  }
});

initDb()
  .then(() => app.listen(PORT, () => console.log(`[ready] server on port ${PORT}`)))
  .catch(err => { console.error('[fatal] DB init failed:', err); process.exit(1); });