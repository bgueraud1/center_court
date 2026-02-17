// netlify/functions/check-user.js
// POST-only Netlify Function: verify pseudo + password_hash against Supabase "users" table
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
  try { body = event.body ? JSON.parse(event.body) : {}; } catch(e) { return jsonResponse(400, { error: "Invalid JSON" }); }

  const pseudo = body.pseudo ? String(body.pseudo).trim() : null;
  const password_hash = body.password_hash ? String(body.password_hash) : null;

  if (!pseudo || !password_hash) {
    return jsonResponse(400, { error: "Missing pseudo or password_hash" });
  }

  try {
    const q = new URL(`${SUPABASE_URL}/rest/v1/users`);
    q.searchParams.set('select', 'id,pseudo');
    q.searchParams.set('pseudo', `eq.${encodeURIComponent(pseudo)}`);
    q.searchParams.set('password_hash', `eq.${encodeURIComponent(password_hash)}`);

    const r = await fetch(q.toString(), {
      method: 'GET',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Accept': 'application/json'
      }
    });

    const text = await r.text().catch(()=>null);
    let arr;
    try { arr = text ? JSON.parse(text) : null; } catch(e){ arr = text; }

    if (!r.ok) {
      return jsonResponse(500, { error: "User lookup failed", status: r.status, detail: arr });
    }

    if (Array.isArray(arr) && arr.length > 0) {
      // found
      const user = arr[0];
      return jsonResponse(200, { ok: true, user: { id: user.id, pseudo: user.pseudo } });
    } else {
      return jsonResponse(401, { ok: false, error: "Invalid credentials" });
    }
  } catch (e) {
    console.error("check-user error", String(e));
    return jsonResponse(500, { error: "Server error", detail: String(e) });
  }
};
