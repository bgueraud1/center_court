// netlify/functions/submit-score.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // ou SERVICE_ROLE_KEY
const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Allow-Methods': 'OPTIONS, POST'
};

exports.handler = async function (event) {
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  }
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing Supabase env vars' }) };
  }

  // fetch shim for Netlify Node
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

    // Normalize mode: if absent, fallback to generic
    const effectiveMode = mode ? String(mode).trim() : 'default';

    // 1) User handling: if pseudo+password_hash => find or create in users table
    let user_id = null;
    let effectivePseudo = providedPseudo ? String(providedPseudo).trim() : null;

    if (password_hash && effectivePseudo) {
      // find existing user by pseudo
      const findUserUrl = `${SUPABASE_URL}/rest/v1/users?select=id,password_hash&pseudo=eq.${encodeURIComponent(effectivePseudo)}&limit=1`;
      const rFind = await fetchImpl(findUserUrl, {
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
      });
      const findJson = await rFind.json();

      if (!rFind.ok) {
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

    // anonymous pseudo fallback
    if (!effectivePseudo) {
      if (anon_id) {
        effectivePseudo = `anon_${String(anon_id).slice(0,8)}`;
      } else {
        effectivePseudo = `anon_${Math.random().toString(36).slice(2,9)}`;
      }
    }

    // meta: keep it stringified and include anon_id if present
    let metaOut = meta || null;
    try {
      if (metaOut && typeof metaOut === 'object') metaOut = JSON.stringify(metaOut);
    } catch (e) { metaOut = String(metaOut); }
    if (anon_id) {
      try {
        let mObj = metaOut ? JSON.parse(metaOut) : {};
        if (typeof mObj !== 'object' || Array.isArray(mObj)) mObj = { meta: metaOut };
        mObj.anon_id = anon_id;
        metaOut = JSON.stringify(mObj);
      } catch (e) {
        metaOut = JSON.stringify({ original_meta: String(metaOut || ''), anon_id });
      }
    }

    // 2) Duplicate check: same day AND same game_id AND same mode
    const today = (new Date()).toISOString().slice(0,10);
    const isoStart = `${today}T00:00:00Z`;

    // Build check URL to include game_id + mode and either user_id or pseudo (or both)
    let checkUrl = `${SUPABASE_URL}/rest/v1/scores?select=id,user_id,pseudo,created_at&game_id=eq.${encodeURIComponent(game_id)}&mode=eq.${encodeURIComponent(effectiveMode)}&created_at=gte.${encodeURIComponent(isoStart)}`;

    if (user_id && effectivePseudo) {
      const orParam = encodeURIComponent(`(user_id.eq.${user_id},pseudo.eq.${effectivePseudo})`);
      checkUrl += `&or=${orParam}`;
    } else if (user_id) {
      checkUrl += `&user_id=eq.${encodeURIComponent(user_id)}`;
    } else if (effectivePseudo) {
      checkUrl += `&pseudo=eq.${encodeURIComponent(effectivePseudo)}`;
    }

    const rCheck = await fetchImpl(checkUrl, {
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
    });
    const existing = await rCheck.json();
    if (!rCheck.ok) {
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase check failed', detail: existing }) };
    }

    const exists = Array.isArray(existing) && existing.some(r => {
      if (user_id && r.user_id && String(r.user_id) === String(user_id)) return true;
      if (effectivePseudo && r.pseudo && String(r.pseudo) === String(effectivePseudo)) return true;
      return false;
    });

    if (exists) {
      return { statusCode: 403, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Already submitted today for this game/mode' }) };
    }

    // 3) Insert
    const insertUrl = `${SUPABASE_URL}/rest/v1/scores`;
    const record = {
      user_id: user_id || null,
      pseudo: effectivePseudo,
      game_id,
      mode: effectiveMode,
      points: Number(points),
      meta: metaOut
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

    const insertJson = await rInsert.json();
    if (!rInsert.ok) {
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Insert failed', detail: insertJson }) };
    }

    return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ ok: true, record: Array.isArray(insertJson) ? insertJson[0] : insertJson }) };
  } catch (err) {
    console.error('submit-score error', err && err.stack ? err.stack : err);
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
