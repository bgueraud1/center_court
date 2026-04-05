const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const BRACKET_TABLE = process.env.BRACKET_TABLE || "bracket";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, x-user-id, x-user-name, x-user-tour, x-user-country, x-user-league, x-user-rank, x-user-token",
  "Access-Control-Allow-Methods": "OPTIONS, GET"
};

function jsonResponse(status, body) {
  return {
    statusCode: status,
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS),
    body: JSON.stringify(body)
  };
}

function getHeader(headers, name) {
  const key = Object.keys(headers || {}).find(k => k.toLowerCase() === name.toLowerCase());
  return key ? headers[key] : null;
}

async function supabaseRequest(table, { method = "GET", query = {} } = {}) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }

  const res = await fetch(url.toString(), {
    method,
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      Accept: "application/json"
    }
  });

  const text = await res.text().catch(() => null);
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!res.ok) {
    throw new Error(`Supabase ${method} ${table} failed: ${res.status} ${text}`);
  }

  return data;
}

async function supabaseSelect(table, query = {}) {
  return await supabaseRequest(table, { method: "GET", query });
}

function normalizeRow(row) {
  return {
    id: row?.id ?? null,
    user_id: row?.user_id ?? null,
    user_name: row?.user_name ?? row?.pseudo ?? "—",
    user_tour: String(row?.user_tour ?? row?.tour ?? "").toUpperCase() || "—",
    user_country: row?.user_country ?? row?.country ?? null,
    user_world_rank: row?.user_world_rank ?? row?.world_ranking ?? row?.world_rank ?? null,
    user_rank_points: row?.user_rank_points ?? row?.rank_points ?? null,
    user_tournaments_won: row?.user_tournaments_won ?? 0,
    current_tournament_bracket_name: row?.current_tournament_bracket_name ?? null,
    updated_at: row?.updated_at ?? null
  };
}

function dedupeLatest(rows) {
  const seen = new Map();
  for (const raw of rows || []) {
    const row = normalizeRow(raw);
    const key = row.user_id || `${row.user_name}|${row.user_tour}`;
    if (!seen.has(key)) {
      seen.set(key, row);
    }
  }

  return Array.from(seen.values())
    .filter(r => Number.isFinite(Number(r.user_world_rank)))
    .sort((a, b) => {
      const ra = Number(a.user_world_rank);
      const rb = Number(b.user_world_rank);
      if (ra !== rb) return ra - rb;
      return String(a.user_name || "").localeCompare(String(b.user_name || ""));
    });
}

async function loadByTour(tour) {
  const rows = await supabaseSelect(BRACKET_TABLE, {
    select: "id,user_id,user_name,user_tour,user_country,user_world_rank,user_rank_points,user_tournaments_won,current_tournament_bracket_name,updated_at",
    user_tour: `eq.${tour}`,
    order: "updated_at.desc",
    limit: "5000"
  }).catch(() => []);
  return dedupeLatest(rows);
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 204, headers: CORS_HEADERS, body: "" };
    }

    if ((event.httpMethod || "GET").toUpperCase() !== "GET") {
      return jsonResponse(405, { ok: false, error: "Method not allowed" });
    }

    const headers = event.headers || {};
    const auth = {
      user_id: getHeader(headers, "x-user-id") || null,
      user_name: getHeader(headers, "x-user-name") || null
    };

    const atp = await loadByTour("ATP");
    const wta = await loadByTour("WTA");

    return jsonResponse(200, {
      ok: true,
      authenticated: Boolean(auth.user_id || auth.user_name),
      source: "bracket",
      atp,
      wta
    });
  } catch (err) {
    console.error("[leaderboard] fatal", err);
    return jsonResponse(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error."
    });
  }
};
