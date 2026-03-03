// netlify/functions/create-user.js
// POST-only Netlify Function: create a new user in Supabase "users" table
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

module.exports.handler = async function(event) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return jsonResponse(500, { error: "Server misconfigured", detail: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" });
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
  } catch (e) {
    return jsonResponse(400, { error: "Invalid JSON" });
  }

  const pseudo = body.pseudo ? String(body.pseudo).trim() : null;
  const password_hash = body.password_hash ? String(body.password_hash) : null;

  if (!pseudo || !password_hash) {
    return jsonResponse(400, { error: "Missing pseudo or password_hash" });
  }

  // 1) check existing pseudo
  try {
    const q = new URL(`${SUPABASE_URL}/rest/v1/users`);
    q.searchParams.set('select', 'id,pseudo');
    q.searchParams.set('pseudo', `eq.${encodeURIComponent(pseudo)}`);

    const r = await fetch(q.toString(), {
      method: 'GET',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Accept': 'application/json'
      }
    });

    if (!r.ok) {
      const txt = await r.text().catch(()=>null);
      console.warn("User lookup failed", r.status, txt);
      // Non fatal - but safer to abort
      return jsonResponse(500, { error: "User lookup failed", status: r.status, detail: txt });
    }

    const arr = await r.json();
    if (Array.isArray(arr) && arr.length > 0) {
      return jsonResponse(409, { error: "User already exists", detail: { pseudo } });
    }
  } catch (e) {
    console.error("user lookup error", String(e));
    return jsonResponse(500, { error: "Server error", detail: String(e) });
  }

  // 2) insert user
  try {
    // league must be "Future F15" for new users
    const league = "Future F15";

    const insertObj = {
      pseudo: pseudo,
      password_hash: password_hash,
      league: league,
      created_at: new Date().toISOString()
    };

    const url = `${SUPABASE_URL}/rest/v1/users`;
    const r2 = await fetch(url, {
      method: 'POST',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
      },
      body: JSON.stringify([insertObj]) // array insert for PostgREST
    });

    const text = await r2.text();
    let data;
    try { data = text ? JSON.parse(text) : null; } catch(e) { data = text; }

    if (!r2.ok) {
      return jsonResponse(500, { error: "Supabase insert failed", status: r2.status, detail: data });
    }

    // return the inserted user (array)
    return jsonResponse(200, { ok: true, inserted: data });
  } catch (err) {
    console.error("create-user error", String(err));
    return jsonResponse(500, { error: "Server error", detail: String(err) });
  }
};