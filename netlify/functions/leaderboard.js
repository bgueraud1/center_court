// netlify/functions/leaderboard.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;

exports.handler = async function(event) {
  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY' }) };
  }

  try {
    // Query params: date=YYYY-MM-DD optional, game_id optional, limit optional
    const qp = event.queryStringParameters || {};
    const game_id = qp.game_id;
    const dateParam = qp.date; // YYYY-MM-DD expected (optional)
    const limit = qp.limit ? Number(qp.limit) : 200;

    // compute start/end UTC from dateParam or today
    let day;
    if (dateParam) {
      // naive parse YYYY-MM-DD (tolerance)
      day = new Date(dateParam + 'T00:00:00Z');
      if (isNaN(day)) day = new Date(); // fallback
    } else {
      day = new Date();
    }
    const start = new Date(Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate(), 0,0,0)).toISOString();
    const end = new Date(Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate(), 0,0,0) + 24*3600*1000).toISOString();

    // build URL
    let q = `${SUPABASE_URL.replace(/\/$/, '')}/rest/v1/scores?created_at=gte.${encodeURIComponent(start)}&created_at=lt.${encodeURIComponent(end)}`;

    if (game_id) q += `&game_id=eq.${encodeURIComponent(game_id)}`;
    q += `&select=*,id&order=points.desc&limit=${encodeURIComponent(limit)}`;

    const headers = {
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'apikey': SUPABASE_KEY,
      'Accept': 'application/json'
    };

    const res = await fetch(q, { headers });
    const text = await res.text();
    if (!res.ok) {
      let detail = text;
      try { detail = JSON.parse(text); } catch(e) {}
      return { statusCode: 500, body: JSON.stringify({ error: 'Supabase query failed', status: res.status, detail }) };
    }
    const rows = JSON.parse(text);
    return { statusCode: 200, body: JSON.stringify({ ok:true, leaderboard: rows }) };

  } catch (err) {
    console.error('leaderboard exception', err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
