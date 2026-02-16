// netlify/functions/leaderboard.js
// GET endpoint to fetch today's leaderboard rows (raw). Query params: ?date=YYYY-MM-DD&game_id=guess_player&limit=50

exports.handler = async function(event, context) {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: { 'Access-Control-Allow-Origin':'*', 'Access-Control-Allow-Methods':'GET,OPTIONS', 'Access-Control-Allow-Headers':'*' }, body: '' };
  }
  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, headers: {'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Method Not Allowed' }) };
  }

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_KEY;
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Server misconfigured' }) };
  }

  const params = event.queryStringParameters || {};
  const dateISO = params.date || (new Date()).toISOString().slice(0,10);
  const game_id = params.game_id || null;
  const limit = params.limit ? Number(params.limit) : 200;

  // build date window
  const d0 = new Date(dateISO + 'T00:00:00Z');
  const d1 = new Date(d0);
  d1.setUTCDate(d1.getUTCDate() + 1);
  const start = d0.toISOString();
  const end = d1.toISOString();

  let qUrl = `${SUPABASE_URL}/rest/v1/scores?created_at=gte.${encodeURIComponent(start)}&created_at=lt.${encodeURIComponent(end)}&select=id,created_at,user_id,pseudo,points,game_id,meta&limit=${limit}`;
  if (game_id) qUrl += `&game_id=eq.${encodeURIComponent(game_id)}`;

  try {
    const r = await fetch(qUrl, {
      headers: {
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'apikey': SUPABASE_KEY,
        'Accept': 'application/json'
      }
    });
    if (!r.ok) {
      const txt = await r.text();
      return { statusCode: 500, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Supabase query failed', detail: txt }) };
    }
    const rows = await r.json();
    return { statusCode: 200, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ ok:true, leaderboard: rows }) };
  } catch (err) {
    return { statusCode: 500, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Server error', detail: String(err) }) };
  }
};
