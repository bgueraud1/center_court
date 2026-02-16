// netlify/functions/submit-score.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// util: headers CORS à renvoyer sur TOUTES les réponses
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*', // ou ton domaine si tu veux restreindre
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS, POST'
};

exports.handler = async function(event) {
  // handle preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  }

  // require POST
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing Supabase env vars' }) };
  }

  // lazy fetch implementation for Netlify Node env
  let fetchImpl = global.fetch;
  if (!fetchImpl) {
    const nf = await import('node-fetch');
    fetchImpl = nf.default || nf;
  }

  try {
    const body = event.body ? JSON.parse(event.body) : {};
    const { game_id, points, pseudo, password_hash, anon_id, meta } = body;

    if (!game_id || typeof points === 'undefined') {
      return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing game_id or points' }) };
    }

    // --- user handling (same logic que précédemment) ---
    let user_id = null;
    if (pseudo && password_hash) {
      const findUserUrl = `${SUPABASE_URL}/rest/v1/users?select=id,password_hash&pseudo=eq.${encodeURIComponent(pseudo)}&limit=1`;
      const rFind = await fetchImpl(findUserUrl, { headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` } });
      const users = await rFind.json();
      if (Array.isArray(users) && users.length > 0) {
        if (users[0].password_hash !== password_hash) {
          return { statusCode: 403, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Invalid password' }) };
        }
        user_id = users[0].id;
      } else {
        // create user
        const createUrl = `${SUPABASE_URL}/rest/v1/users`;
        const rc = await fetchImpl(createUrl, {
          method: 'POST',
          headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=representation' },
          body: JSON.stringify([{ pseudo, password_hash }])
        });
        const j = await rc.json();
        if (rc.ok && Array.isArray(j) && j.length) user_id = j[0].id;
      }
    }

    // --- duplicate for same day check ---
    const today = new Date().toISOString().slice(0,10);
    const checkUrl = `${SUPABASE_URL}/rest/v1/scores?select=id,user_id,anon_id,pseudo&game_id=eq.${encodeURIComponent(game_id)}&created_at=gte.${today}`;
    const rq = await fetchImpl(checkUrl, { headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` } });
    const existing = await rq.json();

    const already = (existing || []).some(r => {
      if (user_id && r.user_id === user_id) return true;
      if (anon_id && r.anon_id && r.anon_id === anon_id) return true;
      if (!user_id && !anon_id && pseudo && r.pseudo === pseudo) return true;
      return false;
    });
    if (already) return { statusCode: 403, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Already submitted today for this game' }) };

    // --- insert score ---
    const insertUrl = `${SUPABASE_URL}/rest/v1/scores`;
    const record = {
      user_id: user_id || null,
      pseudo: pseudo || (anon_id ? `anon_${String(anon_id).slice(0,8)}` : 'anonymous'),
      anon_id: anon_id || null,
      game_id,
      points: Number(points),
      meta: meta ? (typeof meta === 'string' ? JSON.parse(meta) : meta) : null
    };
    const rInsert = await fetchImpl(insertUrl, {
      method: 'POST',
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', Prefer: 'return=representation' },
      body: JSON.stringify([record])
    });
    const jInsert = await rInsert.json();
    if (!rInsert.ok) {
      return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Insert failed', detail: jInsert }) };
    }

    return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ ok:true, record: jInsert[0] }) };

  } catch(err) {
    console.error('submit-score error', err);
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
