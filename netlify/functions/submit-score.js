// netlify/functions/submit-score.js
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

  const game_id = body.game_id;
  const points = Number(body.points);
  if (!game_id || isNaN(points)) {
    return jsonResponse(400, { error: "Missing or invalid game_id/points" });
  }

  const pseudoFromClient = body.pseudo ? String(body.pseudo).trim() : null;
  const password_hash = body.password_hash ? String(body.password_hash) : null;
  const mode = body.mode ? String(body.mode).slice(0,50) : null;
  const meta = body.meta ? (typeof body.meta === "string" ? body.meta : JSON.stringify(body.meta)) : null;

  // NEW: accept user_id from client (but verify it)
  const clientUserId = body.user_id ? String(body.user_id) : null;

  // try to authenticate local user (if pseudo+password_hash provided)
  let user_id = null;
  let pseudoToStore = pseudoFromClient || null;

  try {
    // If client provided user_id, attempt to verify and fetch pseudo
    if (clientUserId) {
      try {
        const q = new URL(`${SUPABASE_URL}/rest/v1/users`);
        q.searchParams.set('select', 'id,pseudo');
        q.searchParams.set('id', `eq.${encodeURIComponent(clientUserId)}`);

        const r = await fetch(q.toString(), {
          method: 'GET',
          headers: {
            'apikey': SUPABASE_KEY,
            'Authorization': `Bearer ${SUPABASE_KEY}`,
            'Accept': 'application/json'
          }
        });

        if (r.ok) {
          const arr = await r.json();
          if (Array.isArray(arr) && arr.length > 0) {
            // trust this user_id because it exists in the users table
            user_id = arr[0].id || null;
            pseudoToStore = arr[0].pseudo || pseudoToStore;
          } else {
            // not found -> ignore clientUserId
            console.warn('client-provided user_id not found in users table', clientUserId);
          }
        } else {
          console.warn('user lookup by id failed', await r.text().catch(()=>null));
        }
      } catch (e) {
        console.warn('user lookup by id error', String(e));
      }
    }

    // If we still have no user_id, attempt pseudo+password_hash authentication (existing behaviour)
    if (!user_id && pseudoFromClient && password_hash) {
      const q2 = new URL(`${SUPABASE_URL}/rest/v1/users`);
      q2.searchParams.set('select', 'id,pseudo,password_hash');
      q2.searchParams.set('pseudo', `eq.${encodeURIComponent(pseudoFromClient)}`);
      q2.searchParams.set('password_hash', `eq.${encodeURIComponent(password_hash)}`);

      const r2 = await fetch(q2.toString(), {
        method: 'GET',
        headers: {
          'apikey': SUPABASE_KEY,
          'Authorization': `Bearer ${SUPABASE_KEY}`,
          'Accept': 'application/json'
        }
      });

      if (r2.ok) {
        const arr2 = await r2.json();
        if (Array.isArray(arr2) && arr2.length > 0) {
          user_id = arr2[0].id || null;
          pseudoToStore = arr2[0].pseudo || pseudoToStore;
        }
      } else {
        console.warn("User lookup failed (pseudo+hash)", await r2.text().catch(()=>null));
      }
    }
  } catch (e) {
    console.warn("user lookup error", String(e));
    // continue as anonymous if error
  }

  const insertObj = {
    user_id: user_id, // null allowed
    pseudo: pseudoToStore || null,
    game_id: game_id,
    points: points,
    meta: meta,
    mode: mode,
    created_at: new Date().toISOString()
  };

  // remove undefined keys (so PostgREST won't choke)
  Object.keys(insertObj).forEach(k => { if (insertObj[k] === undefined) delete insertObj[k]; });

  try {
    const url = `${SUPABASE_URL}/rest/v1/scores`;
    const r = await fetch(url, {
      method: 'POST',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
      },
      body: JSON.stringify([insertObj]) // array insert
    });

    const text = await r.text();
    let data;
    try { data = text ? JSON.parse(text) : null; } catch(e) { data = text; }

    if (!r.ok) {
      return jsonResponse(500, { error: "Supabase insert failed", status: r.status, detail: data });
    }

    return jsonResponse(200, { ok: true, inserted: data });
  } catch (err) {
    console.error("submit-score error", String(err));
    return jsonResponse(500, { error: "Server error", detail: String(err) });
  }
};