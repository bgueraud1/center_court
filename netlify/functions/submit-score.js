// netlify/functions/submit-score.js
// Robust supabase REST usage + CORS + defensive parsing

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*', // en prod: remplace par ton domaine
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS, POST'
};

function jsonSafeParse(text) {
  try { return JSON.parse(text); } catch (e) { return null; }
}

exports.handler = async function(event) {
  // Preflight CORS
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  }

  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing Supabase env vars' }) };
  }

  // lazy import fetch for Netlify Node
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

    // ----- 1) user handling (create or fetch) -----
    let user_id = null;
    if (pseudo && password_hash) {
      // try find user by pseudo
      const findUserUrl = `${SUPABASE_URL}/rest/v1/users?select=id,password_hash&pseudo=eq.${encodeURIComponent(pseudo)}&limit=1`;
      const rFind = await fetchImpl(findUserUrl, {
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
      });

      let findJson = null;
      try { findJson = await rFind.json(); } catch(e) { findJson = null; }

      if (!rFind.ok) {
        // log and continue: do not crash, but return an informative error
        const text = JSON.stringify(findJson || { status: rFind.status, statusText: rFind.statusText });
        console.error('supabase find user error', text);
        return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase find user failed', detail: findJson }) };
      }

      if (Array.isArray(findJson) && findJson.length > 0) {
        // check password hash match (we compare hash directly for prototype)
        if (findJson[0].password_hash !== password_hash) {
          return { statusCode: 403, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Invalid password' }) };
        }
        user_id = findJson[0].id;
      } else {
        // create user (representation return)
        const createUrl = `${SUPABASE_URL}/rest/v1/users`;
        const rCreate = await fetchImpl(createUrl, {
          method: 'POST',
          headers: {
            apikey: SUPABASE_KEY,
            Authorization: `Bearer ${SUPABASE_KEY}`,
            'Content-Type': 'application/json',
            Prefer: 'return=representation'
          },
          body: JSON.stringify([{ pseudo, password_hash }])
        });

        let createJson = null;
        try { createJson = await rCreate.json(); } catch(e) { createJson = null; }

        if (!rCreate.ok || !Array.isArray(createJson) || createJson.length === 0) {
          console.error('supabase create user failed', createJson);
          return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase create user failed', detail: createJson }) };
        }
        user_id = createJson[0].id;
      }
    }

    // ----- 2) duplicate same-day check -----
    // Use full ISO-day start to be precise
    const targetDate = (new Date()).toISOString().slice(0,10);
    const isoStart = `${targetDate}T00:00:00Z`;

    const checkUrl = `${SUPABASE_URL}/rest/v1/scores?select=id,user_id,anon_id,pseudo,created_at&game_id=eq.${encodeURIComponent(game_id)}&created_at=gte.${encodeURIComponent(isoStart)}`;
    const rCheck = await fetchImpl(checkUrl, {
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
    });

    let existing = null;
    // Defensive parsing: if supabase returns an error object, handle gracefully
    try { existing = await rCheck.json(); } catch (e) { existing = null; }

    if (!rCheck.ok) {
      // If Supabase returned error, log and fail early (no silent crash)
      console.error('supabase check existing returned non-ok', { status: rCheck.status, body: existing });
      // to be permissive you could set existing = [], but better return informative error
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase check failed', detail: existing }) };
    }

    // Ensure existing is an array (some endpoints may return an object on error)
    if (!Array.isArray(existing)) {
      console.warn('supabase check returned unexpected payload (not array)', existing);
      // defensive: treat as no existing rows to allow insertion OR return error
      existing = [];
    }

    const already = existing.some(r => {
      if (user_id && r.user_id && String(r.user_id) === String(user_id)) return true;
      if (anon_id && r.anon_id && String(r.anon_id) === String(anon_id)) return true;
      if (!user_id && !anon_id && pseudo && r.pseudo && r.pseudo === pseudo) return true;
      return false;
    });

    if (already) {
      return { statusCode: 403, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Already submitted today for this game' }) };
    }

    // ----- 3) insert score -----
    const insertUrl = `${SUPABASE_URL}/rest/v1/scores`;
    const record = {
      user_id: user_id || null,
      pseudo: pseudo || (anon_id ? `anon_${String(anon_id).slice(0,8)}` : 'anonymous'),
      anon_id: anon_id || null,
      game_id,
      points: Number(points),
      meta: meta || null
    };

    const rInsert = await fetchImpl(insertUrl, {
      method: 'POST',
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        Prefer: 'return=representation'
      },
      body: JSON.stringify([record])
    });

    let insertJson = null;
    try { insertJson = await rInsert.json(); } catch(e) { insertJson = null; }

    if (!rInsert.ok) {
      console.error('supabase insert error', insertJson);
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Insert failed', detail: insertJson }) };
    }

    // success
    return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ ok: true, record: Array.isArray(insertJson)?insertJson[0]:insertJson }) };

  } catch (err) {
    console.error('submit-score handler error', err && err.stack ? err.stack : err);
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
