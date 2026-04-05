const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const USERS_TABLE = process.env.USERS_TABLE || "users";
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

function parseJsonMaybe(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try { return JSON.parse(value); } catch { return null; }
}

async function supabaseRequest(table, { method = "GET", query = {}, payload = null } = {}) {
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

  if (method !== "GET") {
    headers["Content-Type"] = "application/json";
    headers["Prefer"] = "return=representation";
  }

  const res = await fetch(url.toString(), {
    method,
    headers,
    body: method === "GET" ? undefined : JSON.stringify(payload)
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

async function resolveUser(ctx) {
  const base = {
    user_id: ctx.user_id,
    user_name: ctx.user_name,
    user_tour: ctx.user_tour,
    user_country: ctx.user_country,
    user_league: ctx.user_league,
    user_world_rank: Number.parseInt(String(ctx.user_world_rank || ""), 10),
    source: "headers"
  };

  if (base.user_id) {
    const rows = await supabaseSelect(USERS_TABLE, {
      select: "id,pseudo,created_at,league,country,tour,league_id",
      id: `eq.${base.user_id}`,
      limit: "1"
    }).catch(() => []);

    if (Array.isArray(rows) && rows.length) {
      const u = rows[0];
      return {
        id: u.id || null,
        pseudo: u.pseudo || base.user_name || null,
        created_at: u.created_at || null,
        league: u.league || base.user_league || null,
        country: u.country || base.user_country || null,
        tour: u.tour || base.user_tour || null,
        league_id: u.league_id ?? null,
        source: "users.id"
      };
    }
  }

  if (base.user_name) {
    const rows = await supabaseSelect(USERS_TABLE, {
      select: "id,pseudo,created_at,league,country,tour,league_id",
      pseudo: `eq.${base.user_name}`,
      limit: "1"
    }).catch(() => []);

    if (Array.isArray(rows) && rows.length) {
      const u = rows[0];
      return {
        id: u.id || null,
        pseudo: u.pseudo || base.user_name || null,
        created_at: u.created_at || null,
        league: u.league || base.user_league || null,
        country: u.country || base.user_country || null,
        tour: u.tour || base.user_tour || null,
        league_id: u.league_id ?? null,
        source: "users.pseudo"
      };
    }
  }

  return base.user_id || base.user_name ? {
    id: base.user_id || null,
    pseudo: base.user_name || null,
    created_at: null,
    league: base.user_league || null,
    country: base.user_country || null,
    tour: base.user_tour || null,
    league_id: null,
    source: "fallback.headers"
  } : null;
}

function normalizeBracket(row) {
  return {
    id: row?.id ?? null,
    user_id: row?.user_id ?? null,
    user_name: row?.user_name ?? null,
    user_tournaments_won: row?.user_tournaments_won ?? 0,
    user_world_rank: row?.user_world_rank ?? null,
    user_rank_points: row?.user_rank_points ?? null,
    user_tour: row?.user_tour ?? null,
    user_country: row?.user_country ?? null,
    user_current_tournament_bracket_id: row?.user_current_tournament_bracket_id ?? null,
    user_current_tournament_bracket_name: row?.user_current_tournament_bracket_name ?? null,
    user_current_tournament_bracket_proposition: row?.user_current_tournament_bracket_proposition ?? null,
    user_performances_this_year: row?.user_performances_this_year ?? null,
    current_tournament_bracket_id: row?.current_tournament_bracket_id ?? null,
    current_tournament_bracket_name: row?.current_tournament_bracket_name ?? null,
    current_tournament_bracket: row?.current_tournament_bracket ?? null,
    created_at: row?.created_at ?? null,
    updated_at: row?.updated_at ?? null
  };
}

function computeLatestBracket(rows) {
  const normalized = (rows || []).map(normalizeBracket);
  if (!normalized.length) return null;
  return normalized.sort((a, b) => new Date(b.updated_at || b.created_at || 0) - new Date(a.updated_at || a.created_at || 0))[0];
}

function extractWonTournaments(rows) {
  const seen = new Map();
  for (const raw of rows || []) {
    const row = normalizeBracket(raw);
    const winsCount = Number(row.user_tournaments_won);
    const tournamentName = row.current_tournament_bracket_name || row.user_current_tournament_bracket_name || null;
    if (Number.isFinite(winsCount) && winsCount > 0 && tournamentName) {
      const key = `${row.current_tournament_bracket_id || tournamentName}`;
      if (!seen.has(key)) {
        seen.set(key, {
          name: tournamentName,
          id: row.current_tournament_bracket_id || row.user_current_tournament_bracket_id || null,
          date: row.updated_at || row.created_at || null,
          wins: winsCount
        });
      }
    }
  }
  return Array.from(seen.values()).sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
}

function normalizePerformances(value) {
  if (!value) return [];
  const arr = Array.isArray(value) ? value : parseJsonMaybe(value);
  if (!Array.isArray(arr)) return [];
  return arr.map((item, idx) => {
    if (typeof item === "string") {
      return { label: item, value: null, date: null, index: idx + 1 };
    }
    if (item && typeof item === "object") {
      return {
        label: item.label || item.name || item.tournament_name || `Performance ${idx + 1}`,
        value: item.value || item.result || item.score || null,
        date: item.date || item.created_at || null,
        index: idx + 1
      };
    }
    return { label: `Performance ${idx + 1}`, value: String(item), date: null, index: idx + 1 };
  });
}

async function loadBracketRows(user) {
  const select = "id,user_id,user_name,user_tournaments_won,user_world_rank,user_rank_points,user_tour,user_country,user_current_tournament_bracket_id,user_current_tournament_bracket_name,user_current_tournament_bracket_proposition,user_performances_this_year,current_tournament_bracket_id,current_tournament_bracket_name,current_tournament_bracket,created_at,updated_at";

  if (user?.id) {
    const rowsById = await supabaseSelect(BRACKET_TABLE, {
      select,
      user_id: `eq.${user.id}`,
      order: "updated_at.desc"
    }).catch(() => []);
    if (Array.isArray(rowsById) && rowsById.length) return rowsById;
  }

  if (user?.pseudo) {
    const rowsByName = await supabaseSelect(BRACKET_TABLE, {
      select,
      user_name: `eq.${user.pseudo}`,
      order: "updated_at.desc"
    }).catch(() => []);
    if (Array.isArray(rowsByName) && rowsByName.length) return rowsByName;
  }

  return [];
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
    const ctx = {
      user_id: getHeader(headers, "x-user-id") || null,
      user_name: getHeader(headers, "x-user-name") || null,
      user_tour: getHeader(headers, "x-user-tour") || null,
      user_country: getHeader(headers, "x-user-country") || null,
      user_league: getHeader(headers, "x-user-league") || null,
      user_world_rank: getHeader(headers, "x-user-rank") || null
    };

    const user = await resolveUser(ctx);
    if (!user) {
      return jsonResponse(200, {
        ok: true,
        authenticated: false,
        user: null,
        latest_bracket: null,
        bracket_rows: [],
        won_tournaments: [],
        user_source: "none"
      });
    }

    const bracketRows = await loadBracketRows(user);
    const latestBracket = computeLatestBracket(bracketRows);
    const wonTournaments = extractWonTournaments(bracketRows);

    return jsonResponse(200, {
      ok: true,
      authenticated: true,
      user,
      user_source: user.source || "users",
      latest_bracket: latestBracket,
      bracket_rows: bracketRows.map(normalizeBracket),
      won_tournaments: wonTournaments,
      performances_this_year: normalizePerformances(latestBracket?.user_performances_this_year || null)
    });
  } catch (err) {
    console.error("[profile] fatal", err);
    return jsonResponse(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error."
    });
  }
};
