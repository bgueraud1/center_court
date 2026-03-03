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
    headers: { "Content-Type": "application/json", ...CORS_HEADERS },
    body: JSON.stringify(body)
  };
}

function isoDateStart(dateStr) {
  return `${dateStr}T00:00:00Z`;
}

function isoDateNext(dateStr) {
  const d = new Date(dateStr + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString();
}

exports.handler = async function(event) {

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return jsonResponse(500, {
      error: "Server misconfigured",
      detail: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
    });
  }

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS };
  }

  if (event.httpMethod !== "GET") {
    return jsonResponse(405, { error: "Method Not Allowed" });
  }

  const q = event.queryStringParameters || {};
  const date = q.date || null;
  const game_id = q.game_id || null;
  const limit = Number(q.limit || 50);

  // new param: check_existing=1 -> return whether a given user/pseudo already has rows for mode/date
  const checkExisting = q.check_existing === '1' || q.check_existing === 'true';

  try {

    // if checkExisting requested, expect mode and user_id or pseudo
    if (checkExisting) {
      const mode = q.mode || null;
      const user_id = q.user_id || null;
      const pseudo = q.pseudo || null;

      if (!mode || (!user_id && !pseudo)) {
        return jsonResponse(400, { ok: false, error: 'need mode and user_id or pseudo for check' });
      }

      // build Supabase REST query
      // base select
      let query = `${SUPABASE_URL}/rest/v1/scores?select=user_id,pseudo,points,mode,created_at,game_id`;

      // filters
      if (game_id) {
        query += `&game_id=eq.${encodeURIComponent(game_id)}`;
      }
      // filter by mode
      query += `&mode=eq.${encodeURIComponent(mode)}`;

      // by date if given
      if (date) {
        const start = isoDateStart(date);
        const next = isoDateNext(date);
        query += `&created_at=gte.${encodeURIComponent(start)}&created_at=lt.${encodeURIComponent(next)}`;
      }

      // filter by user_id or pseudo
      if (user_id) {
        query += `&user_id=eq.${encodeURIComponent(user_id)}`;
      } else if (pseudo) {
        // search by pseudo exact
        query += `&pseudo=eq.${encodeURIComponent(pseudo)}`;
      }

      query += `&limit=1`; // we only need existence

      const r = await fetch(query, {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
          Accept: "application/json"
        }
      });

      if (!r.ok) {
        const txt = await r.text();
        return jsonResponse(500, {
          ok: false,
          error: "Supabase read failed (check_existing)",
          status: r.status,
          detail: txt
        });
      }

      const rows = await r.json();
      const exists = Array.isArray(rows) && rows.length > 0;

      return jsonResponse(200, { ok: true, exists: exists, rows: rows });

    } // end checkExisting branch

    // --- default: fetch all rows (as before) and aggregate leaderboard ---

    // ⚠️ Construction MANUELLE de l'URL (PAS URLSearchParams)
    let query = `${SUPABASE_URL}/rest/v1/scores?select=user_id,pseudo,points,mode,created_at`;

    if (date) {
      const start = isoDateStart(date);
      const next = isoDateNext(date);
      query += `&created_at=gte.${start}&created_at=lt.${next}`;
    }

    if (game_id) {
      query += `&game_id=eq.${game_id}`;
    }

    query += `&limit=1000`;

    const r = await fetch(query, {
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        Accept: "application/json"
      }
    });

    if (!r.ok) {
      const txt = await r.text();
      return jsonResponse(500, {
        error: "Supabase read failed",
        status: r.status,
        detail: txt
      });
    }

    const rows = await r.json();

    // Aggregation
    const map = new Map();

    for (const row of rows) {
      const key = row.user_id
        ? `u:${row.user_id}`
        : `p:${row.pseudo || "anonymous"}`;

      if (!map.has(key)) {
        map.set(key, {
          user_id: row.user_id || null,
          pseudo: row.pseudo || null,
          total: 0,
          breakdown: {}
        });
      }

      const entry = map.get(key);
      const pts = Number(row.points) || 0;

      entry.total += pts;

      const mode = row.mode || "default";
      entry.breakdown[mode] =
        (entry.breakdown[mode] || 0) + pts;
    }

    const leaderboard = Array.from(map.values())
      .sort((a, b) => b.total - a.total)
      .slice(0, limit);

    return jsonResponse(200, {
      ok: true,
      leaderboard
    });

  } catch (err) {
    return jsonResponse(500, {
      error: "Server error",
      detail: String(err)
    });
  }
};