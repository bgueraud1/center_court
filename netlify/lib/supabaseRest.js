// netlify/lib/supabaseRest.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

function assertConfig() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
}

function baseHeaders(extra = {}) {
  assertConfig();
  return {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    Accept: "application/json",
    "Content-Type": "application/json",
    ...extra,
  };
}

function buildUrl(table, params = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  }
  return url.toString();
}

async function restGet(table, params = {}) {
  const r = await fetch(buildUrl(table, params), {
    method: "GET",
    headers: baseHeaders(),
  });
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!r.ok) {
    throw new Error(`Supabase GET ${table} failed: ${r.status} ${text}`);
  }
  return data;
}

async function restPost(table, payload) {
  const r = await fetch(buildUrl(table), {
    method: "POST",
    headers: baseHeaders({ Prefer: "return=representation" }),
    body: JSON.stringify(payload),
  });
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!r.ok) {
    throw new Error(`Supabase POST ${table} failed: ${r.status} ${text}`);
  }
  return data;
}

async function restPatch(table, filters, payload) {
  const query = {};
  for (const [k, v] of Object.entries(filters || {})) {
    query[k] = `eq.${v}`;
  }

  const r = await fetch(buildUrl(table, query), {
    method: "PATCH",
    headers: baseHeaders({ Prefer: "return=representation" }),
    body: JSON.stringify(payload),
  });
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!r.ok) {
    throw new Error(`Supabase PATCH ${table} failed: ${r.status} ${text}`);
  }
  return data;
}

async function restUpsert(table, payload, onConflict) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  if (onConflict) url.searchParams.set("on_conflict", onConflict);

  const r = await fetch(url.toString(), {
    method: "POST",
    headers: baseHeaders({
      Prefer: "resolution=merge-duplicates,return=representation",
    }),
    body: JSON.stringify(payload),
  });

  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!r.ok) {
    throw new Error(`Supabase UPSERT ${table} failed: ${r.status} ${text}`);
  }
  return data;
}

module.exports = {
  restGet,
  restPost,
  restPatch,
  restUpsert,
};