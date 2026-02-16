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
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS),
    body: JSON.stringify(body)
  };
}

function isoDateStart(dateStr) {
  return `${dateStr}T00:00:00Z`;
}
function isoDateNext(dateStr) {
  const d = new Date(dateStr + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate()+1);
  return d.toISOString();
}

module.exports.handler = async function(event) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return jsonResponse(500, { error: "Server misconfigured", detail: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" });
  }

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }
  if (event.httpMethod !== "GET") return jsonResponse(405, { error: "Method Not Allowed" });

  const q = event.queryStringParameters || {};
  const date = q.date || null; // YYYY-MM-DD
  const game_id = q.game_id || null;
  const limit = Number(q.limit || 50);

  // build supabase REST URL filter
  try {
    const url = new URL(`${SUPABASE_URL}/rest/v1/scores`);
    // select minimal fields we need
    url.searchParams.set('select', 'user_id,pseudo,points,mode,created_at,game_id');
    if (game_id) url.searchParams.append('game_id', `eq.${encodeURIComponent(game_id)}`);

    // if date provided, filter between date start (inclusive) and next day (exclusive)
    if (date) {
      const start = isoDateStart(date);
      const next = isoDateNext(date);
      url.searchParams.append('created_at', `gte.${encodeURIComponent(start)}`);
      url.searchParams.append('created_at', `lt.${encodeURIComponent(next)}`);
    }

    // allow many rows then aggregate server side
    url.searchParams.set('limit', '1000');

    const r = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Accept': 'application/json'
      }
    });

    if (!r.ok) {
      const txt = await r.text();
      return jsonResponse(500, { error: "Supabase read failed", status: r.status, detail: txt });
    }

    const rows = await r.json();
    // aggregate by (user_id or pseudo)
    const map = new Map();
    for (const row of rows) {
      const key = row.user_id ? `u:${row.user_id}` : `p:${row.pseudo || 'anonymous'}`;
      if (!map.has(key)) {
        map.set(key, { user_id: row.user_id || null, pseudo: row.pseudo || null, total: 0, breakdown: {} });
      }
      const o = map.get(key);
      const pts = Number(row.points) || 0;
      o.total += pts;
      const m = row.mode || 'default';
      o.breakdown[m] = (o.breakdown[m] || 0) + pts;
    }

    // convert to array and sort
    const arr = Array.from(map.values()).sort((a,b)=>b.total - a.total).slice(0, limit);

    return jsonResponse(200, { ok: true, leaderboard: arr, raw_count: rows.length });

  } catch (err) {
    console.error("leaderboard error", String(err));
    return jsonResponse(500, { error: "Server error", detail: String(err) });
  }
};
