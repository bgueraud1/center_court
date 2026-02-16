// netlify/functions/submit-score.js
// Version corrigée : ne dépend pas d'une colonne `anon_id` (utilise pseudo à la place)

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

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

  // lazy import fetch for Netlify Node if needed
  let fetchImpl = global.fetch;
  if (!fetchImpl) {
    const nf = await import('node-fetch');
    fetchImpl = nf.default || nf;
  }

  try {
    const body = event.body ? JSON.parse(event.body) : {};
    const { game_id, points, pseudo: providedPseudo, password_hash, anon_id, meta } = body;

    if (!game_id || typeof points === 'undefined') {
      return { statusCode: 400, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Missing game_id or points' }) };
    }

    // 1) User handling: if pseudo+password_hash => find or create in users table
    let user_id = null;
    let effectivePseudo = providedPseudo ? String(providedPseudo).trim() : null;

    if (password_hash && effectivePseudo) {
      // find existing user
      const findUserUrl = `${SUPABASE_URL}/rest/v1/users?select=id,password_hash&pseudo=eq.${encodeURIComponent(effectivePseudo)}&limit=1`;
      const rFind = await fetchImpl(findUserUrl, {
        headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
      });
      let findJson = null;
      try { findJson = await rFind.json(); } catch (e) { findJson = null; }

      if (!rFind.ok) {
        console.error('supabase find user error', { status: rFind.status, body: findJson });
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
        let createJson = null;
        try { createJson = await rCreate.json(); } catch(e){ createJson = null; }
        if (!rCreate.ok || !Array.isArray(createJson) || createJson.length === 0) {
          console.error('supabase create user failed', createJson);
          return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase create user failed', detail: createJson }) };
        }
        user_id = createJson[0].id;
      }
    }

    // 2) Determine effectivePseudo if anonymous: use anon_id to create a pseudo placeholder
    if (!effectivePseudo) {
      if (anon_id) {
        effectivePseudo = `anon_${String(anon_id).slice(0,8)}`;
      } else {
        // ultimate fallback: random anon
        effectivePseudo = `anon_${Math.random().toString(36).slice(2,9)}`;
      }
    }

    // If we have anon_id and meta is not a string, try to embed it in meta JSON for audit
    let metaOut = meta || null;
    try {
      // if meta is string containing JSON, keep; if it's object, stringify
      if (metaOut && typeof metaOut === 'object') metaOut = JSON.stringify(metaOut);
    } catch(e) { metaOut = String(metaOut); }
    if (anon_id) {
      // ensure metaOut contains anon info (string)
      try {
        let mObj = metaOut ? JSON.parse(metaOut) : {};
        if (typeof mObj !== 'object' || Array.isArray(mObj)) mObj = { meta: metaOut };
        mObj.anon_id = anon_id;
        metaOut = JSON.stringify(mObj);
      } catch(e) {
        // metaOut not JSON => wrap
        metaOut = JSON.stringify({ original_meta: String(metaOut || ''), anon_id });
      }
    }

    // 3) Duplicate today-check:
    const today = (new Date()).toISOString().slice(0,10);
    const isoStart = `${today}T00:00:00Z`;

    // Build check URL with either:
    // - if user_id and effectivePseudo -> use or=(user_id.eq.X,pseudo.eq.Y)
    // - else if user_id -> user_id=eq.X
    // - else -> pseudo=eq.Y
    let checkUrlBase = `${SUPABASE_URL}/rest/v1/scores?select=id,user_id,pseudo,created_at&game_id=eq.${encodeURIComponent(game_id)}&created_at=gte.${encodeURIComponent(isoStart)}`;

    if (user_id && effectivePseudo) {
      const orParam = encodeURIComponent(`(user_id.eq.${user_id},pseudo.eq.${effectivePseudo})`);
      checkUrlBase += `&or=${orParam}`;
    } else if (user_id) {
      checkUrlBase += `&user_id=eq.${encodeURIComponent(user_id)}`;
    } else if (effectivePseudo) {
      checkUrlBase += `&pseudo=eq.${encodeURIComponent(effectivePseudo)}`;
    }

    const rCheck = await fetchImpl(checkUrlBase, {
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
    });

    let existing = null;
    try { existing = await rCheck.json(); } catch(e) { existing = null; }

    if (!rCheck.ok) {
      console.error('supabase check existing non-ok', { status: rCheck.status, body: existing });
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Supabase check failed', detail: existing }) };
    }

    if (!Array.isArray(existing)) {
      console.warn('supabase check returned unexpected payload (not array)', existing);
      existing = [];
    }

    const already = existing.some(r => {
      if (user_id && r.user_id && String(r.user_id) === String(user_id)) return true;
      if (effectivePseudo && r.pseudo && String(r.pseudo) === String(effectivePseudo)) return true;
      return false;
    });

    if (already) {
      return { statusCode: 403, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Already submitted today for this game' }) };
    }

    // 4) Insert
    const insertUrl = `${SUPABASE_URL}/rest/v1/scores`;
    const record = {
      user_id: user_id || null,
      pseudo: effectivePseudo,
      game_id,
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

    let insertJson = null;
    try { insertJson = await rInsert.json(); } catch(e){ insertJson = null; }

    if (!rInsert.ok) {
      console.error('supabase insert error', insertJson);
      return { statusCode: 502, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Insert failed', detail: insertJson }) };
    }

    return { statusCode: 200, headers: CORS_HEADERS, body: JSON.stringify({ ok: true, record: Array.isArray(insertJson)?insertJson[0]:insertJson }) };

  } catch (err) {
    console.error('submit-score handler error', err && err.stack ? err.stack : err);
    return { statusCode: 500, headers: CORS_HEADERS, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
