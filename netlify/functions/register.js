// netlify/functions/register.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "OPTIONS, POST"
};

function jsonResponse(status, body) {
  return {
    statusCode: status,
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS),
    body: JSON.stringify(body)
  };
}

async function supabaseRequest(table, {
  method = "GET",
  query = {},
  payload = null,
  select = "*",
  prefer = null
} = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);

  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null && v !== "") {
      url.searchParams.set(k, String(v));
    }
  }

  if (select) {
    url.searchParams.set("select", select);
  }

  const headers = {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    Accept: "application/json"
  };

  if (method !== "GET") {
    headers["Content-Type"] = "application/json";
    headers["Prefer"] = prefer || "return=representation";
  }

  const res = await fetch(url.toString(), {
    method,
    headers,
    body: method === "GET" ? undefined : JSON.stringify(payload)
  });

  const text = await res.text().catch(() => null);
  let data;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!res.ok) {
    const err = new Error(`Supabase ${method} ${table} failed: ${res.status}`);
    err.status = res.status;
    err.detail = data;
    throw err;
  }

  return data;
}

async function supabaseSelect(table, query = {}) {
  return await supabaseRequest(table, { method: "GET", query });
}

async function supabaseInsert(table, payload, select = "*") {
  const data = await supabaseRequest(table, {
    method: "POST",
    payload: [payload],
    select,
    prefer: "return=representation"
  });
  return Array.isArray(data) ? data[0] : data;
}

async function getNextWorldRank() {
  const rows = await supabaseSelect("bracket", {
    select: "user_world_rank",
    order: "user_world_rank.desc",
    limit: "1"
  });

  const maxRank = Number(rows?.[0]?.user_world_rank || 0);
  return Number.isFinite(maxRank) ? maxRank + 1 : 1;
}

module.exports.handler = async function(event) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return jsonResponse(500, {
      error: "Server misconfigured",
      detail: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
    });
  }

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }

  if (event.httpMethod !== "POST") {
    return jsonResponse(405, { error: "Method Not Allowed" });
  }

  let body;
  try {
    body = event.body ? JSON.parse(event.body) : {};
  } catch {
    return jsonResponse(400, { error: "Invalid JSON" });
  }

  const pseudo = body.pseudo ? String(body.pseudo).trim() : null;
  const password_hash = body.password_hash ? String(body.password_hash) : null;
  const tour = body.tour ? String(body.tour).trim().toUpperCase() : null;
  const country = body.country ? String(body.country).trim() : null;

  if (!pseudo || !password_hash) {
    return jsonResponse(400, { error: "Missing pseudo or password_hash" });
  }

  if (!tour || (tour !== "ATP" && tour !== "WTA")) {
    return jsonResponse(400, {
      error: "Invalid tour",
      detail: "tour must be 'ATP' or 'WTA'"
    });
  }

  if (!country || country.length < 2) {
    return jsonResponse(400, {
      error: "Invalid country",
      detail: "country must be provided"
    });
  }

  try {
    const existing = await supabaseSelect("users", {
      select: "id,pseudo",
      pseudo: `eq.${pseudo}`,
      limit: "1"
    });

    if (Array.isArray(existing) && existing.length > 0) {
      return jsonResponse(409, { error: "User already exists", detail: { pseudo } });
    }
  } catch (err) {
    return jsonResponse(500, {
      error: "User lookup failed",
      detail: String(err?.detail || err)
    });
  }

  try {
    const league = "Future F15";

    const insertedUser = await supabaseInsert("users", {
      pseudo,
      password_hash,
      league,
      tour,
      country,
      created_at: new Date().toISOString()
    }, "id,pseudo,tour,country,league");

    const newWorldRank = await getNextWorldRank();

    await supabaseInsert("bracket", {
      user_id: insertedUser.id,
      user_name: insertedUser.pseudo,
      user_tour: insertedUser.tour,
      user_country: insertedUser.country,
      user_world_rank: newWorldRank
    }, "id,user_id,user_name,user_tour,user_country,user_world_rank");

    return jsonResponse(200, {
      ok: true,
      user: {
        id: insertedUser.id,
        pseudo: insertedUser.pseudo,
        tour: insertedUser.tour,
        country: insertedUser.country,
        league: insertedUser.league
      },
      bracket: {
        user_id: insertedUser.id,
        user_name: insertedUser.pseudo,
        user_tour: insertedUser.tour,
        user_country: insertedUser.country,
        user_world_rank: newWorldRank
      }
    });
  } catch (err) {
    return jsonResponse(500, {
      error: "Server error",
      detail: String(err?.detail || err)
    });
  }
};