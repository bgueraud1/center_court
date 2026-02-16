// netlify/functions/submit-score.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS, POST'
};

exports.handler = async function(event) {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  }
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing Supabase env vars' }) };
  }

  // fetch shim (Netlify Node)
  let fetchImpl = global.fetch;
  if (!fetchImpl) {
    const nf = await import('node-fetch');
    fetchImpl = nf.default || nf;
  }

  try {
    const body = event.body ? JSON.parse(event.body) : {};
    const { game_id, points, pseudo: providedPseudo, password_hash, anon_id, meta, mode } = body;

    if (!game_id || typeof points === 'undefined') {
      return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing game_id or points' }) };
    }

    // normalize inputs
    const effectiveMode = mode ? String(mode).trim() : 'default';
    let effectivePseudo = providedPseudo ? String(providedPseudo).trim() : null;
    let user_id = null;

    // If credentials provided, try to find/create user (simple local users table)
    if (password_hash && effectivePseudo) {
      // find user by pseudo
      const findUrl = `${SUPABASE_URL}/rest/v1/users?select=id,password_hash&pseudo=eq.${encodeURIComponent(effectivePseudo)}&limit=1`;
      const rFind = await fetchImpl(findUrl, { headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` } });
      const findJson = await rFind.json();
      if (!rFind.ok) {
        // don't block: return an error
        return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase find user failed', detail: findJson }) };
      }
      if (Array.isArray(findJson) && findJson.length > 0) {
        if (findJson[0].password_hash !== password_hash) {
          return { statusCode: 403, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Invalid password' }) };
        }
        user_id = findJson[0].id;
      } else {
        // create user
        const createUrl = `${SUPABASE_URL}/rest/v1/users`;
        const rCreate = await fetchImpl(createUrl, {
          method: 'POST',
          headers: {
            apikey: SUPABASE_KEY,
            Authorization: `Bearer ${SUPABASE_KEY}`,
            'Content-Type': 'application/json',
            Prefer: 'return=representation'
          },
          body: JSON.stringify([{ pseudo: effectivePseudo, password_hash }])
        });
        const createJson = await rCreate.json();
        if (!rCreate.ok || !Array.isArray(createJson) || createJson.length === 0) {
          return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase create user failed', detail: createJson }) };
        }
        user_id = createJson[0].id;
      }
    }

    // if no pseudo provided, fallback to anon id or generated
    if (!effectivePseudo) {
      if (anon_id) effectivePseudo = `anon_${String(anon_id).slice(0,8)}`;
      else effectivePseudo = `anon_${Math.random().toString(36).slice(2,9)}`;
    }

    // meta normalization
    let metaOut = meta || null;
    try { if (metaOut && typeof metaOut === 'object') metaOut = JSON.stringify(metaOut); } catch(e) { metaOut = String(metaOut); }
    if (anon_id) {
      try {
        const parsed = metaOut ? JSON.parse(metaOut) : {};
        if (typeof parsed === 'object' && !Array.isArray(parsed)) { parsed.anon_id = anon_id; metaOut = JSON.stringify(parsed); }
        else { metaOut = JSON.stringify({ original_meta: metaOut, anon_id }); }
      } catch(e) { metaOut = JSON.stringify({ original_meta: metaOut, anon_id }); }
    }

    // build record to insert
    const record = {
      user_id: user_id || null,
      pseudo: effectivePseudo,
      game_id,
      mode: effectiveMode,
      points: Number(points),
      meta: metaOut
    };

    const insertUrl = `${SUPABASE_URL}/rest/v1/scores`;
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

    const insertJson = await rInsert.json();

    // If conflict due to unique constraint we created, PostgREST returns 409
    if (!rInsert.ok) {
      if (rInsert.status === 409) {
        return { statusCode: 409, headers: CORS_HEADERS, body: JSON.stringify({ ok:false, error: 'Already submitted today for this game/mode' }) };
      }
      // Some other DB error
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Insert failed', detail: insertJson }) };
    }

    // success
    return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ ok: true, record: Array.isArray(insertJson) ? insertJson[0] : insertJson }) };

  } catch (err) {
    console.error('submit-score error', err && err.stack ? err.stack : err);
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
