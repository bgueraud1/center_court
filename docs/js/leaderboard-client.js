// netlify/functions/leaderboard.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "OPTIONS, GET"
};

function jsonResponse(status, body) {
  return {
    statusCode: status,
    headers: {
      "Content-Type": "application/json",
      ...CORS_HEADERS
    },
    body: JSON.stringify(body)
  };
}

function buildHeaders() {
  return {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    Accept: "application/json"
  };
}

async function fetchAllRows(table, select, order = "created_at.desc") {
  const allRows = [];
  const pageSize = 1000;
  let offset = 0;

  while (true) {
    const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
    url.searchParams.set("select", select);
    if (order) url.searchParams.set("order", order);
    url.searchParams.set("limit", String(pageSize));
    url.searchParams.set("offset", String(offset));

    const res = await fetch(url.toString(), {
      method: "GET",
      headers: buildHeaders()
    });

    const text = await res.text();
    let data;
    try {
      data = text ? JSON.parse(text) : [];
    } catch (e) {
      throw new Error(`Invalid JSON returned by Supabase for table ${table}: ${text}`);
    }

    if (!res.ok) {
      throw new Error(`Supabase read failed for ${table}: ${res.status} ${text}`);
    }

    if (!Array.isArray(data)) {
      throw new Error(`Unexpected response format for table ${table}`);
    }

    allRows.push(...data);

    if (data.length < pageSize) break;
    offset += pageSize;
  }

  return allRows;
}

function normalizeString(value) {
  return value === null || value === undefined ? "" : String(value);
}

function buildLeaderboard(users, scores) {
  const usersById = new Map();
  const usersByPseudo = new Map();

  for (const user of users) {
    const id = normalizeString(user.id || user.user_id);
    const pseudo = normalizeString(user.pseudo).trim().toLowerCase();

    if (id) usersById.set(id, user);
    if (pseudo && !usersByPseudo.has(pseudo)) usersByPseudo.set(pseudo, user);
  }

  const map = new Map();

  for (const row of scores) {
    const rowUserId = normalizeString(row.user_id);
    const rowPseudo = normalizeString(row.pseudo).trim().toLowerCase();

    const matchedUser =
      (rowUserId && usersById.get(rowUserId)) ||
      (rowPseudo && usersByPseudo.get(rowPseudo)) ||
      null;

    const key = matchedUser
      ? `u:${normalizeString(matchedUser.id || matchedUser.user_id)}`
      : rowUserId
        ? `u:${rowUserId}`
        : `p:${rowPseudo || "anonymous"}`;

    if (!map.has(key)) {
      map.set(key, {
        user_id: matchedUser ? normalizeString(matchedUser.id || matchedUser.user_id) : (row.user_id ? String(row.user_id) : null),
        pseudo: matchedUser?.pseudo || row.pseudo || null,
        league: matchedUser?.league || "—",
        league_id: matchedUser?.league_id !== undefined && matchedUser?.league_id !== null ? String(matchedUser.league_id) : "",
        tour: normalizeString(matchedUser?.tour).toUpperCase() || "",
        total: 0,
        scores: 0,
        breakdown: {}
      });
    }

    const entry = map.get(key);
    const points = Number(row.points || 0);
    const mode = normalizeString(row.mode) || "default";

    entry.total += Number.isFinite(points) ? points : 0;
    entry.scores += 1;
    entry.breakdown[mode] = (entry.breakdown[mode] || 0) + (Number.isFinite(points) ? points : 0);

    if (!entry.tour) {
      const inferredTour = mode.toUpperCase().startsWith("WTA") ? "WTA" : mode.toUpperCase().startsWith("ATP") ? "ATP" : "";
      if (inferredTour) entry.tour = inferredTour;
    }

    if (entry.league === "—" && matchedUser?.league) {
      entry.league = matchedUser.league;
    }
  }

  return Array.from(map.values())
    .sort((a, b) => (b.total - a.total) || String(a.pseudo || "").localeCompare(String(b.pseudo || "")));
}

exports.handler = async function (event) {
  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return jsonResponse(500, {
        ok: false,
        error: "Server misconfigured",
        detail: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
      });
    }

    if (event.httpMethod === "OPTIONS") {
      return {
        statusCode: 204,
        headers: CORS_HEADERS,
        body: ""
      };
    }

    if (event.httpMethod !== "GET") {
      return jsonResponse(405, { ok: false, error: "Method Not Allowed" });
    }

    const usersSelect = "id,pseudo,league,league_id,tour,country,created_at";
    const scoresSelect = "id,user_id,pseudo,game_id,points,meta,created_at,mode,created_day,anon_id";

    const [users, scores] = await Promise.all([
      fetchAllRows("users", usersSelect, "created_at.desc"),
      fetchAllRows("scores", scoresSelect, "created_at.desc")
    ]);

    const leaderboard = buildLeaderboard(users, scores);

    return jsonResponse(200, {
      ok: true,
      users,
      scores,
      leaderboard
    });
  } catch (err) {
    console.error("leaderboard error:", err);
    return jsonResponse(500, {
      ok: false,
      error: "Server error",
      detail: String(err && err.message ? err.message : err)
    });
  }
};