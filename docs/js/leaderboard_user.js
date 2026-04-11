const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const USERS_TABLE = process.env.USERS_TABLE || "users";
const SCORES_TABLE = process.env.SCORES_TABLE || "scores";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "OPTIONS, GET"
};

function jsonResponse(status, body) {
  return {
    statusCode: status,
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS),
    body: JSON.stringify(body)
  };
}

function parseJsonMaybe(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try { return JSON.parse(value); } catch { return null; }
}

async function supabaseRequest(table, { method = "GET", query = {} } = {}) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }

  const headers = {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    Accept: "application/json"
  };

  const res = await fetch(url.toString(), { method, headers });
  const text = await res.text().catch(() => null);
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!res.ok) {
    throw new Error(`Supabase ${method} ${table} failed: ${res.status} ${text}`);
  }

  return data;
}

async function supabaseSelectAll(table, query = {}) {
  const out = [];
  const pageSize = 1000;
  let offset = 0;

  while (true) {
    const batch = await supabaseRequest(table, {
      method: "GET",
      query: Object.assign({}, query, {
        limit: pageSize,
        offset
      })
    });

    if (!Array.isArray(batch)) return [];
    out.push(...batch);

    if (batch.length < pageSize) break;
    offset += pageSize;
  }

  return out;
}

function normalizeScore(row) {
  return {
    id: row?.id ?? null,
    user_id: row?.user_id ?? null,
    pseudo: row?.pseudo ?? null,
    game_id: row?.game_id ?? null,
    points: Number(row?.points ?? 0),
    meta: row?.meta ?? null,
    created_at: row?.created_at ?? null,
    mode: row?.mode ?? null,
    created_day: row?.created_day ?? null,
    anon_id: row?.anon_id ?? null
  };
}

function normalizeUser(row) {
  return {
    id: row?.id ?? null,
    pseudo: row?.pseudo ?? null,
    country: row?.country ?? null,
    league: row?.league ?? null,
    tour: row?.tour ?? null,
    league_id: row?.league_id ?? null,
    created_at: row?.created_at ?? null
  };
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 204, headers: CORS_HEADERS, body: "" };
    }

    if ((event.httpMethod || "GET").toUpperCase() !== "GET") {
      return jsonResponse(405, { ok: false, error: "Method not allowed" });
    }

    const [scoresRows, usersRows] = await Promise.all([
      supabaseSelectAll(SCORES_TABLE, {
        select: "id,user_id,pseudo,game_id,points,meta,created_at,mode,created_day,anon_id",
        order: "created_at.desc"
      }),
      supabaseSelectAll(USERS_TABLE, {
        select: "id,pseudo,country,league,tour,league_id,created_at",
        order: "created_at.asc"
      })
    ]);

    return jsonResponse(200, {
      ok: true,
      scores: scoresRows.map(normalizeScore),
      users: usersRows.map(normalizeUser)
    });
  } catch (err) {
    console.error("[leaderboard] fatal", err);
    return jsonResponse(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error."
    });
  }
};