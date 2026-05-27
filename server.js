require('dotenv').config();
// ============================================================
// PMF Lead Database — Backend API (v6: strong/weak edge dedup)
// Node.js + Express + PostgreSQL
//
// CHANGES from v5:
//   - Email-shared and phone-shared records no longer union by default.
//     They now require name match AND company similarity ≥85% (strong),
//     OR they become a "weak pair" that doesn't chain transitively.
//   - Different-name records sharing only an email or phone are no longer
//     linked at all (was the source of cross-business chaining).
//   - Each emitted group has matchStrength: 'strong' | 'weak'.
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
  const maxLen = Math.max(s1.length, s2.length);
  if (maxLen === 0) return 0;
  // Fast-reject: edit distance >= |len(s1) - len(s2)|, so similarity cannot
  // exceed 1 - lenDiff/maxLen. We only ever call with threshold >= 0.85, so a
  // length gap above 50% guarantees rejection without running Levenshtein.
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

function getPriorityLevel(days) {
  if (days <= 30) return 'critical';
  if (days <= 60) return 'high';
  if (days <= 90) return 'medium';
  if (days <= 365) return 'regular';
  return 'none';
}

// Strict company-match gate: both sides must be present, and similar enough.
const COMPANY_SIM_THRESHOLD = 0.85;
function companiesMatch(c1, c2) {
  if (!c1 || !c2) return false; // blank treated as "not enough info to match"
  return stringSimilarity(c1, c2) >= COMPANY_SIM_THRESHOLD;
}

// determineMatchType — runs after groups are formed, picks the badge for each lead.
// For weak pair groups (size 2, contact-only match), labels are tagged "(unverified)"
// so the UI can distinguish them from strong groups where company was verified.
function determineMatchType(lead, group, isWeakPair) {
  if (isWeakPair) {
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

  // Strong group — every union had companies ≥85% similar.
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName
        && lead.email && lead.email === o.email
        && companiesMatch(lead.company, o.company)) {
      return 'Name + Email';
    }
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName
        && lead.normalizedPhone && lead.normalizedPhone === o.normalizedPhone
        && companiesMatch(lead.company, o.company)) {
      return 'Name + Phone';
    }
  }
  for (const o of group) {
    if (o === lead) continue;
    if (lead.fullName && lead.fullName === o.fullName && lead.company && o.company) {
      const sim = stringSimilarity(lead.company, o.company);
      if (sim >= COMPANY_SIM_THRESHOLD) return `Name + Company (${Math.round(sim*100)}%)`;
    }
  }
  return 'Linked via chain';
}

// ============================================================
// DEDUP — strong/weak edges, typed arrays, batched streaming
// ============================================================
async function computeDuplicatesStreaming(client, emit) {
  console.log('[dedup] starting');
  const t0 = Date.now();
  const MAX_GROUP_SIZE = 100; // skip pathological signal groups (e.g. info@gmail.com)

  // ----- Step 1: Get signal groups (ID arrays only) -----
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

  const filterGroups = (rows) => {
    const out = [];
    for (const row of rows) {
      if (row.lead_ids.length >= 2 && row.lead_ids.length <= MAX_GROUP_SIZE) {
        out.push(row.lead_ids);
      }
    }
    return out;
  };
  const emailArrays = filterGroups(emailGroups.rows);
  const phoneArrays = filterGroups(phoneGroups.rows);
  const nameArrays = filterGroups(nameGroups.rows);
  emailGroups.rows.length = 0;
  phoneGroups.rows.length = 0;
  nameGroups.rows.length = 0;

  console.log(`[dedup] signals: ${emailArrays.length} email / ${phoneArrays.length} phone / ${nameArrays.length} name (${Date.now() - t0}ms)`);

  // ----- Step 2: Collect all candidate IDs -----
  const allCandidateIds = new Set();
  for (const arr of emailArrays) for (const id of arr) allCandidateIds.add(id);
  for (const arr of phoneArrays) for (const id of arr) allCandidateIds.add(id);
  for (const arr of nameArrays) for (const id of arr) allCandidateIds.add(id);

  if (allCandidateIds.size === 0) {
    console.log('[dedup] no candidates');
    return 0;
  }

  // ----- Step 3: Fetch (name, company) for all candidates -----
  // We need both to pairwise verify matches inside each signal group.
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
  const idxToId = candidateIdArr; // alias — same array

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

  // Weak pair store. Keyed by "minId-maxId" to dedupe across signal groups.
  // These will NOT participate in union-find — they're emitted as standalone pair groups.
  const weakPairs = new Map(); // key -> [minId, maxId, signal]
  function addWeakPair(idA, idB, signal) {
    const min = idA < idB ? idA : idB;
    const max = idA < idB ? idB : idA;
    const k = `${min}-${max}`;
    if (weakPairs.has(k)) return;
    weakPairs.set(k, [min, max, signal]);
  }

  // ----- Step 5: Process email signal groups -----
  // Rule: same email + same name + companies match ≥85%  →  STRONG (union)
  //       same email + same name + companies differ/blank →  WEAK PAIR
  //       same email + different name                     →  NO EDGE (was the bridge bug)
  for (const arr of emailArrays) {
    for (let i = 0; i < arr.length - 1; i++) {
      const a = meta.get(arr[i]);
      if (!a || !a.name) continue;
      for (let j = i + 1; j < arr.length; j++) {
        const b = meta.get(arr[j]);
        if (!b || !b.name) continue;
        if (a.name !== b.name) continue;
        if (companiesMatch(a.company, b.company)) {
          union(idToIdx.get(arr[i]), idToIdx.get(arr[j]));
        } else {
          addWeakPair(arr[i], arr[j], 'email');
        }
      }
    }
  }
  emailArrays.length = 0;

  // ----- Step 6: Process phone signal groups (same rules) -----
  for (const arr of phoneArrays) {
    for (let i = 0; i < arr.length - 1; i++) {
      const a = meta.get(arr[i]);
      if (!a || !a.name) continue;
      for (let j = i + 1; j < arr.length; j++) {
        const b = meta.get(arr[j]);
        if (!b || !b.name) continue;
        if (a.name !== b.name) continue;
        if (companiesMatch(a.company, b.company)) {
          union(idToIdx.get(arr[i]), idToIdx.get(arr[j]));
        } else {
          addWeakPair(arr[i], arr[j], 'phone');
        }
      }
    }
  }
  phoneArrays.length = 0;

  // ----- Step 7: Process name signal groups (Name + Company≥85%) -----
  // Unchanged from v5. This rule already required company similarity, so it stays strong.
  for (const arr of nameArrays) {
    for (let i = 0; i < arr.length - 1; i++) {
      const a = meta.get(arr[i]);
      if (!a || !a.company) continue;
      for (let j = i + 1; j < arr.length; j++) {
        const b = meta.get(arr[j]);
        if (!b || !b.company) continue;
        if (companiesMatch(a.company, b.company)) {
          union(idToIdx.get(arr[i]), idToIdx.get(arr[j]));
        }
      }
    }
  }
  nameArrays.length = 0;

  console.log(`[dedup] union-find done, ${weakPairs.size} candidate weak pairs (${Date.now() - t0}ms)`);

  // ----- Step 8: Collect strong groups (size ≥ 2) -----
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

  // ----- Step 9: Filter weak pairs — drop ones already inside a strong group -----
  // If both members of a weak pair landed in the same strong component (via some
  // other path), the pair is redundant — they're already shown together.
  const weakPairList = [];
  for (const [, pair] of weakPairs) {
    const ra = find(idToIdx.get(pair[0]));
    const rb = find(idToIdx.get(pair[1]));
    if (ra === rb) continue;
    weakPairList.push(pair);
  }
  weakPairs.clear();

  console.log(`[dedup] ${strongGroups.length} strong groups, ${weakPairList.length} weak pair groups`);

  meta.clear();
  idToIdx.clear();

  if (strongGroups.length === 0 && weakPairList.length === 0) return 0;

  // ----- Step 10: Combined emit list (strong groups + weak pairs) -----
  const allGroupsToEmit = [];
  for (const ids of strongGroups) allGroupsToEmit.push({ type: 'strong', ids });
  for (const pair of weakPairList) {
    allGroupsToEmit.push({ type: 'weak', ids: [pair[0], pair[1]], weakSignal: pair[2] });
  }
  strongGroups.length = 0;
  weakPairList.length = 0;

  // ----- Step 11: Fetch full lead data + emit in batches -----
  const priOrder = ['critical', 'high', 'medium', 'regular', 'none'];
  const GROUPS_PER_BATCH = 100;
  let emitted = 0;

  for (let b = 0; b < allGroupsToEmit.length; b += GROUPS_PER_BATCH) {
    const batch = allGroupsToEmit.slice(b, b + GROUPS_PER_BATCH);
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

      const isWeak = g.type === 'weak';
      gl[0].matchType = 'Original';
      for (let i = 1; i < gl.length; i++) {
        gl[i].matchType = determineMatchType(gl[i], gl, isWeak);
      }

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
        occurrenceCount: gl.length,
        matchStrength: isWeak ? 'weak' : 'strong',
        weakSignal: g.weakSignal || null
      });
      emitted++;
    }
    fullById.clear();
  }

  allGroupsToEmit.length = 0;
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