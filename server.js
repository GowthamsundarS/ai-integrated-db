require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const path = require('path');
const { Pool } = require('pg');
const mysql = require('mysql2/promise');

const PORT = process.env.PORT || 3000;
const ENV_MODEL = process.env.NVIDIA_MODEL || '';
const NVIDIA_BASE_URL = (process.env.NVIDIA_BASE_URL || 'https://integrate.api.nvidia.com/v1').replace(/\/$/, '');
const MAX_ROWS = parseInt(process.env.MAX_ROWS || '200', 10);
const SESSION_TTL_MS = 30 * 60 * 1000;

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public')));

/* ---------------- Sessions (in memory only) ---------------- */
const sessions = new Map(); // token -> { adapter, schema, allowWrites, proposals, last }

setInterval(() => {
  const now = Date.now();
  for (const [t, s] of sessions) {
    if (now - s.last > SESSION_TTL_MS) {
      s.adapter.close().catch(() => {});
      sessions.delete(t);
    }
  }
}, 60 * 1000).unref();

function getSession(req, res) {
  const s = sessions.get(req.header('x-session'));
  if (!s) { res.status(401).json({ error: 'Not connected or session expired. Please reconnect.' }); return null; }
  s.last = Date.now();
  return s;
}

/* ---------------- DB adapters ---------------- */
function trimResult(columns, rows) {
  const truncated = rows.length > MAX_ROWS;
  return {
    columns,
    rows: rows.slice(0, MAX_ROWS).map(r => columns.map(c => r[c])),
    truncated,
    totalRows: rows.length
  };
}

async function createAdapter(cfg) {
  const common = {
    host: cfg.host, port: Number(cfg.port), user: cfg.user,
    password: cfg.password, database: cfg.database
  };

  if (cfg.type === 'postgres') {
    const pool = new Pool({
      ...common,
      ssl: cfg.ssl ? { rejectUnauthorized: false } : false,
      max: 3, connectionTimeoutMillis: 8000, statement_timeout: 20000
    });
    await pool.query('select 1');
    return {
      type: 'postgres',
      async schema() {
        const cols = (await pool.query(`
          select c.table_schema s, c.table_name t, c.column_name c, c.data_type d, c.is_nullable n
          from information_schema.columns c
          join information_schema.tables t on t.table_schema=c.table_schema and t.table_name=c.table_name
          where t.table_type='BASE TABLE' and c.table_schema not in ('pg_catalog','information_schema')
          order by c.table_schema, c.table_name, c.ordinal_position`)).rows;
        const keys = (await pool.query(`
          select tc.constraint_type ct, tc.table_schema s, tc.table_name t, kcu.column_name c,
                 ccu.table_schema fs, ccu.table_name ft, ccu.column_name fc
          from information_schema.table_constraints tc
          join information_schema.key_column_usage kcu
            on tc.constraint_name=kcu.constraint_name and tc.table_schema=kcu.table_schema
          left join information_schema.constraint_column_usage ccu
            on tc.constraint_type='FOREIGN KEY' and ccu.constraint_name=tc.constraint_name and ccu.table_schema=tc.table_schema
          where tc.constraint_type in ('PRIMARY KEY','FOREIGN KEY')
            and tc.table_schema not in ('pg_catalog','information_schema')`)).rows;
        const counts = (await pool.query(
          `select schemaname s, relname t, n_live_tup n from pg_stat_user_tables`)).rows;
        const tables = new Map();
        for (const r of cols) {
          const k = `${r.s}.${r.t}`;
          if (!tables.has(k)) tables.set(k, { name: r.s === 'public' ? r.t : k, rows: null, columns: [], fks: [] });
          tables.get(k).columns.push({ name: r.c, type: r.d, nullable: r.n === 'YES', pk: false });
        }
        for (const r of keys) {
          const t = tables.get(`${r.s}.${r.t}`); if (!t) continue;
          if (r.ct === 'PRIMARY KEY') { const col = t.columns.find(x => x.name === r.c); if (col) col.pk = true; }
          else t.fks.push({ column: r.c, ref: `${r.ft}.${r.fc}` });
        }
        for (const r of counts) { const t = tables.get(`${r.s}.${r.t}`); if (t) t.rows = Number(r.n); }
        return { tables: [...tables.values()] };
      },
      async run(sql, readOnly) {
        const c = await pool.connect();
        try {
          await c.query(readOnly ? 'BEGIN READ ONLY' : 'BEGIN');
          let r = await c.query(sql);
          await c.query('COMMIT');
          if (Array.isArray(r)) r = r[r.length - 1];
          if (r.fields && r.fields.length) return trimResult(r.fields.map(f => f.name), r.rows);
          return { columns: [], rows: [], message: `${r.command} OK, ${r.rowCount ?? 0} row(s) affected` };
        } catch (e) {
          await c.query('ROLLBACK').catch(() => {});
          throw e;
        } finally { c.release(); }
      },
      close: () => pool.end()
    };
  }

  if (cfg.type === 'mysql') {
    const pool = mysql.createPool({
      ...common,
      ssl: cfg.ssl ? { rejectUnauthorized: false } : undefined,
      connectionLimit: 3, connectTimeout: 8000
    });
    await pool.query('select 1');
    return {
      type: 'mysql',
      async schema() {
        const [cols] = await pool.query(`
          select c.table_name t, c.column_name c, c.data_type d, c.is_nullable n, c.column_key k
          from information_schema.columns c
          join information_schema.tables t on t.table_schema=c.table_schema and t.table_name=c.table_name
          where c.table_schema=database() and t.table_type='BASE TABLE'
          order by c.table_name, c.ordinal_position`);
        const [fks] = await pool.query(`
          select table_name t, column_name c, referenced_table_name ft, referenced_column_name fc
          from information_schema.key_column_usage
          where table_schema=database() and referenced_table_name is not null`);
        const [counts] = await pool.query(
          `select table_name t, table_rows n from information_schema.tables where table_schema=database()`);
        const tables = new Map();
        for (const r of cols) {
          if (!tables.has(r.t)) tables.set(r.t, { name: r.t, rows: null, columns: [], fks: [] });
          tables.get(r.t).columns.push({ name: r.c, type: r.d, nullable: r.n === 'YES', pk: r.k === 'PRI' });
        }
        for (const r of fks) tables.get(r.t)?.fks.push({ column: r.c, ref: `${r.ft}.${r.fc}` });
        for (const r of counts) { const t = tables.get(r.t); if (t) t.rows = Number(r.n); }
        return { tables: [...tables.values()] };
      },
      async run(sql, readOnly) {
        const c = await pool.getConnection();
        try {
          await c.query(readOnly ? 'START TRANSACTION READ ONLY' : 'START TRANSACTION');
          const [rows, fields] = await c.query({ sql, timeout: 20000 });
          await c.query('COMMIT');
          if (Array.isArray(rows) && fields) return trimResult(fields.map(f => f.name), rows);
          return { columns: [], rows: [], message: `OK, ${rows.affectedRows ?? 0} row(s) affected` };
        } catch (e) {
          await c.query('ROLLBACK').catch(() => {});
          throw e;
        } finally { c.release(); }
      },
      close: () => pool.end()
    };
  }

  throw new Error('Unsupported database type');
}

/* ---------------- Helpers ---------------- */
function schemaToText(schema) {
  return schema.tables.map(t => {
    const cols = t.columns.map(c => `${c.name} ${c.type}${c.pk ? ' PK' : ''}${c.nullable ? '' : ' NOT NULL'}`).join(', ');
    const fks = t.fks.length ? ` | FKs: ${t.fks.map(f => `${f.column} -> ${f.ref}`).join(', ')}` : '';
    const n = t.rows != null ? ` (~${t.rows} rows)` : '';
    return `- ${t.name}${n}: ${cols}${fks}`;
  }).join('\n');
}

// A statement is "read" if it is a single SELECT-like statement. Reads always run inside
// a READ ONLY transaction, so the database itself enforces that nothing is modified.
function classifySql(sql) {
  const body = sql.trim().replace(/;+\s*$/, '');
  const single = !body.includes(';');
  const readLike = /^\s*(select|with|show|explain|describe|desc)\b/i.test(body);
  return single && readLike ? 'read' : 'write';
}

async function askLLM(system, messages, model) {
  if (!process.env.NVIDIA_API_KEY) throw new Error('NVIDIA_API_KEY is not set on the server (.env).');
  const r = await fetch(`${NVIDIA_BASE_URL}/chat/completions`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'accept': 'application/json',
      'authorization': `Bearer ${process.env.NVIDIA_API_KEY}`
    },
    body: JSON.stringify({
      model,
      max_tokens: 1500,
      temperature: 0,
      stream: false,
      messages: [{ role: 'system', content: system }, ...messages]
    })
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`Model "${model}" failed: ` + (data?.detail || data?.error?.message || data?.title || `HTTP ${r.status}`) + ' (pick another model from the dropdown)');
  const text = data?.choices?.[0]?.message?.content;
  if (!text) throw new Error('NVIDIA API returned an empty response.');
  return text.replace(/<think>[\s\S]*?<\/think>/g, '').trim(); // strip reasoning blocks if a reasoning model is used
}

const NON_CHAT = /embed|rerank|guard|safety|reward|retriev|parse|clip|vila|-vl|vision|riva|translate|bge|whisper|tts|asr|stable|sdxl|flux|cosmos|molmim|diffusion|nv-/i;
const PREFERRED = [/nemotron-3-super/, /nemotron-super/, /llama-4-maverick/, /gpt-oss-120b/, /llama-3\.3-70b/, /qwen.*(72b|235b|coder)/, /deepseek/, /mistral.*large/];
let modelCache = { at: 0, ids: [] };

async function listModels() {
  if (!process.env.NVIDIA_API_KEY) throw new Error('NVIDIA_API_KEY is not set on the server (.env).');
  if (Date.now() - modelCache.at < 10 * 60 * 1000 && modelCache.ids.length) return modelCache.ids;
  const r = await fetch(`${NVIDIA_BASE_URL}/models`, { headers: { authorization: `Bearer ${process.env.NVIDIA_API_KEY}` } });
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d?.detail || d?.error?.message || `Could not list models (HTTP ${r.status})`);
  const ids = (d.data || []).map(m => m.id).filter(id => !NON_CHAT.test(id)).sort();
  modelCache = { at: Date.now(), ids };
  return ids;
}

function pickDefault(ids) {
  if (ENV_MODEL && ids.includes(ENV_MODEL)) return ENV_MODEL;
  for (const re of PREFERRED) { const m = ids.find(id => re.test(id)); if (m) return m; }
  return ids[0] || ENV_MODEL || '';
}

function parseModelJson(text) {
  const cleaned = text.replace(/```json|```/g, '').trim();
  const start = cleaned.indexOf('{'), end = cleaned.lastIndexOf('}');
  if (start !== -1 && end > start) {
    try { return JSON.parse(cleaned.slice(start, end + 1)); } catch (_) {}
  }
  return { reply: text, sql: null };
}

/* ---------------- Routes ---------------- */
app.post('/api/connect', async (req, res) => {
  const cfg = req.body || {};
  if (!cfg.host || !cfg.user || !cfg.database) return res.status(400).json({ error: 'Host, user and database are required.' });
  try {
    const adapter = await createAdapter(cfg);
    const schema = await adapter.schema();
    const token = crypto.randomBytes(24).toString('hex');
    sessions.set(token, {
      adapter, schema, allowWrites: !!cfg.allowWrites,
      proposals: new Map(), last: Date.now()
    });
    // credentials are NOT stored anywhere beyond the live connection pool
    res.json({ token, type: adapter.type, allowWrites: !!cfg.allowWrites, tables: schema.tables });
  } catch (e) {
    res.status(400).json({ error: 'Connection failed: ' + e.message });
  }
});

app.post('/api/disconnect', async (req, res) => {
  const t = req.header('x-session');
  const s = sessions.get(t);
  if (s) { await s.adapter.close().catch(() => {}); sessions.delete(t); }
  res.json({ ok: true });
});

app.get('/api/models', async (req, res) => {
  const s = getSession(req, res); if (!s) return;
  try {
    const ids = await listModels();
    res.json({ models: ids, default: pickDefault(ids) });
  } catch (e) {
    // fall back to the env model so the app still works if listing is unavailable
    res.json({ models: ENV_MODEL ? [ENV_MODEL] : [], default: ENV_MODEL, warning: e.message });
  }
});

app.post('/api/chat', async (req, res) => {
  const s = getSession(req, res); if (!s) return;
  const messages = (req.body.messages || []).slice(-20)
    .filter(m => (m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string' && m.content.trim());
  if (!messages.length || messages[messages.length - 1].role !== 'user')
    return res.status(400).json({ error: 'Send at least one user message.' });

  const system = `You are a database assistant connected to a ${s.adapter.type === 'postgres' ? 'PostgreSQL' : 'MySQL'} database.
You can see ONLY the schema below, never the data. You cannot execute anything yourself; any SQL you write is shown to the user, who must approve it before it runs.

SCHEMA:
${schemaToText(s.schema)}

RULES:
- Answer questions about the structure/design of the database directly from the schema (no SQL needed).
- When the user wants data or a change, propose exactly ONE SQL statement in the correct dialect, using only tables/columns from the schema.
- Prefer SELECT with a sensible LIMIT. Only propose INSERT/UPDATE/DELETE/DDL if explicitly requested, and always include a WHERE clause for UPDATE/DELETE.
- ${s.allowWrites ? 'Write statements are enabled for this session.' : 'Write statements are DISABLED for this session; if asked to modify data, explain that and offer a SELECT alternative.'}
- Never say you have run a query. Say what the query does and ask the user to approve it.
- Respond with ONLY a JSON object: {"reply": "<short explanation in markdown-free plain text>", "sql": "<single statement or null>"}`;

  try {
    const model = (typeof req.body.model === 'string' && req.body.model.trim()) || ENV_MODEL;
    if (!model) return res.status(400).json({ error: 'No model selected.' });
    const parsed = parseModelJson(await askLLM(system, messages, model));
    let proposal = null;
    if (parsed.sql && typeof parsed.sql === 'string' && parsed.sql.trim()) {
      const sql = parsed.sql.trim();
      proposal = { id: crypto.randomBytes(8).toString('hex'), sql, kind: classifySql(sql) };
      s.proposals.set(proposal.id, { used: false });
    }
    res.json({ reply: parsed.reply || '', proposal });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Executes ONLY a proposal that the user explicitly approved in the UI (one-time use).
app.post('/api/execute', async (req, res) => {
  const s = getSession(req, res); if (!s) return;
  const { proposalId, sql } = req.body || {};
  const p = s.proposals.get(proposalId);
  if (!p || p.used) return res.status(400).json({ error: 'Unknown or already-used proposal.' });
  if (!sql || !sql.trim()) return res.status(400).json({ error: 'Empty SQL.' });

  const kind = classifySql(sql); // re-classified, since the user may have edited the SQL
  if (kind === 'write' && !s.allowWrites)
    return res.status(403).json({ error: 'Write statements are disabled for this session (reconnect with "Allow writes" enabled).' });

  p.used = true;
  try {
    const result = await s.adapter.run(sql.trim(), kind === 'read');
    res.json({ kind, ...result });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`DB Chat running at http://localhost:${PORT}`));
