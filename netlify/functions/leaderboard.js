// netlify/functions/leaderboard.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'GET, OPTIONS'
};

exports.handler = async function(event) {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  }
  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing Supabase env vars' }) };
  }

  let fetchImpl = global.fetch;
  if (!fetchImpl) {
    const nf = await import('node-fetch');
    fetchImpl = nf.default || nf;
  }

  try {
    const q = event.queryStringParameters || {};
    const date = q.date || (new Date()).toISOString().slice(0,10);
    const period = (q.period || 'daily').toLowerCase();
    const game_id = q.game_id || null;
    const mode = q.mode || null;
    const limit = q.limit ? Number(q.limit) : 50;

    // compute start date/time
    const startDateIso = (() => {
      if (period === 'weekly') {
        const d = new Date(date + 'T00:00:00Z');
        d.setUTCDate(d.getUTCDate() - 6); // last 7 days inclusive
        return d.toISOString().slice(0,19) + 'Z';
      } else if (period === 'all') {
        return '1970-01-01T00:00:00Z';
      } else {
        // daily
        return date + 'T00:00:00Z';
      }
    })();

    // build select for aggregated rows grouped by user_id + pseudo + game_id + mode
    // PostgREST aggregate: select=user_id,pseudo,game_id,mode,total:sum(points)&group=user_id,pseudo,game_id,mode
    let baseUrl = `${SUPABASE_URL}/rest/v1/scores`;
    const params = new URLSearchParams();
    const select = 'user_id,pseudo,game_id,mode,total:sum(points)';
    params.set('select', select);
    params.set('created_at', `gte.${startDateIso}`);
    if (game_id) params.set('game_id', `eq.${game_id}`);
    if (mode) params.set('mode', `eq.${mode}`);
    params.set('group', 'user_id,pseudo,game_id,mode');
    params.set('order', 'total.desc');
    params.set('limit', String(limit * 5)); // fetch more rows to be safe for breakdown aggregation

    const url = `${baseUrl}?${params.toString()}`;

    const r = await fetchImpl(url, { headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` } });
    const rows = await r.json();
    if (!r.ok) {
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase query failed', detail: rows }) };
    }

    // aggregate server-side: build per-user totals and per-game breakdown
    const users = {}; // user_id_or_pseudo -> { user_id, pseudo, total, breakdown: { game_id: { mode: points, total } } }
    for (const rrow of (Array.isArray(rows) ? rows : [])) {
      const uid = rrow.user_id || `anon:${rrow.pseudo || 'unknown'}`;
      if (!users[uid]) users[uid] = { user_id: rrow.user_id || null, pseudo: rrow.pseudo || (rrow.user_id ? String(rrow.user_id) : 'anon'), total: 0, breakdown: {} };
      const u = users[uid];
      const gid = rrow.game_id || 'unknown';
      const md = rrow.mode || 'default';
      const pts = Number(rrow.total || 0);
      u.total += pts;
      if (!u.breakdown[gid]) u.breakdown[gid] = {};
      u.breakdown[gid][md] = (u.breakdown[gid][md] || 0) + pts;
    }

    // produce sorted leaderboard array by total desc
    const leaderboard = Object.values(users).sort((a,b) => b.total - a.total).slice(0, limit);

    return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ ok: true, leaderboard }) };
  } catch (err) {
    console.error('leaderboard error', err && err.stack ? err.stack : err);
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
