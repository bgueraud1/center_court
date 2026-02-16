// netlify/functions/submit-score.js
// POST endpoint to accept score submissions and insert into Supabase (PostgREST).
// Expects JSON body: { game_id, points, pseudo?, password_hash?, user_id? , anon_id?, meta?, mode? }

exports.handler = async function(event, context) {
  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization'
      },
      body: ''
    };
  }

  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ error: 'Method Not Allowed' })
    };
  }

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_KEY;
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, headers: {'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Server misconfigured (missing SUPABASE env vars)' }) };
  }

  let payload;
  try {
    payload = JSON.parse(event.body || '{}');
  } catch (e) {
    return { statusCode: 400, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error: 'invalid_json' }) };
  }

  const { game_id, points } = payload;
  if (!game_id || typeof points !== 'number') {
    return { statusCode: 400, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'missing game_id or points must be number' }) };
  }

  // optional fields
  const user_id = payload.user_id || null;
  const pseudo = payload.pseudo || null;
  const anon_id = payload.anon_id || null;
  const meta = payload.meta || null;
  const mode = payload.mode || null; // e.g. "ATP_top20" or "WTA_by_country"

  // compute today's UTC window (use ISO date string)
  const today = new Date();
  const yyyy = today.getUTCFullYear();
  const mm = String(today.getUTCMonth()+1).padStart(2,'0');
  const dd = String(today.getUTCDate()).padStart(2,'0');
  const todayStart = `${yyyy}-${mm}-${dd}T00:00:00Z`;
  // tomorrow start for < filter
  const tomorrow = new Date(today);
  tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
  const yyyy2 = tomorrow.getUTCFullYear();
  const mm2 = String(tomorrow.getUTCMonth()+1).padStart(2,'0');
  const dd2 = String(tomorrow.getUTCDate()).padStart(2,'0');
  const tomorrowStart = `${yyyy2}-${mm2}-${dd2}T00:00:00Z`;

  // Build fetch to Supabase REST to get today's rows for this game_id (limit 200)
  const selectCols = encodeURIComponent('id,created_at,user_id,pseudo,meta,points,game_id,mode');
  const query = `${SUPABASE_URL}/rest/v1/scores?game_id=eq.${encodeURIComponent(game_id)}&created_at=gte.${encodeURIComponent(todayStart)}&created_at=lt.${encodeURIComponent(tomorrowStart)}&select=${selectCols}&limit=200`;
  try {
    const resp = await fetch(query, {
      headers: {
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'apikey': SUPABASE_KEY,
        'Accept': 'application/json'
      }
    });
    if (!resp.ok) {
      const txt = await resp.text();
      return { statusCode: 500, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Supabase check failed', detail: txt }) };
    }
    const rows = await resp.json();

    // check duplicates:
    // - if user_id provided -> if any row.user_id === user_id => already submitted
    // - else if anon_id provided -> if any row.meta contains anon_id substring => already submitted
    // - else if pseudo provided -> if any row.pseudo === pseudo AND row.user_id is null -> treat as duplicate (best-effort)
    let already = false;
    if (user_id) {
      already = rows.some(r => r.user_id && String(r.user_id) === String(user_id));
    } else if (anon_id) {
      already = rows.some(r => {
        if (!r.meta) return false;
        try {
          // try JSON parse meta if possible
          const m = typeof r.meta === 'string' ? r.meta : JSON.stringify(r.meta);
          return String(m).includes(String(anon_id));
        } catch(e) {
          return String(r.meta).includes(String(anon_id));
        }
      });
    } else if (pseudo) {
      already = rows.some(r => (!r.user_id || r.user_id===null) && r.pseudo && String(r.pseudo) === String(pseudo));
    }

    if (already) {
      return { statusCode: 409, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ ok:false, error:'Already submitted today for this game' }) };
    }

    // prepare insert payload
    const insertObj = {
      game_id: game_id,
      points: points,
      created_at: new Date().toISOString()
    };
    if (user_id) insertObj.user_id = user_id;
    if (pseudo) insertObj.pseudo = pseudo;
    // ensure meta contains anon_id if anon_id provided (and meta not already including)
    let metaToSend = meta;
    if (anon_id) {
      try {
        // try merge if meta JSON
        let metaObj = {};
        if (meta) {
          try { metaObj = JSON.parse(meta); } catch(e){ metaObj = { raw_meta: meta }; }
        }
        metaObj = Object.assign({}, metaObj, { anon_id });
        metaToSend = JSON.stringify(metaObj);
      } catch(e) {
        metaToSend = JSON.stringify({ anon_id, meta: meta || null });
      }
    } else if (meta && typeof meta === 'object') {
      metaToSend = JSON.stringify(meta);
    } else if (meta && typeof meta === 'string') {
      metaToSend = meta;
    }
    if (metaToSend) insertObj.meta = metaToSend;
    if (mode) insertObj.mode = mode; // if you have mode column; if not present it will be ignored by PostgREST? (it will fail if column absent)

    // Insert row
    const insertResp = await fetch(`${SUPABASE_URL}/rest/v1/scores`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'apikey': SUPABASE_KEY,
        'Content-Type': 'application/json',
        'Prefer': 'return=representation'
      },
      body: JSON.stringify(insertObj)
    });

    const insertText = await insertResp.text();
    if (!insertResp.ok) {
      return { statusCode: 500, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Supabase insert failed', detail: insertText }) };
    }
    // success -> return inserted object
    let inserted = [];
    try { inserted = JSON.parse(insertText); } catch(e){ inserted = insertText; }

    return { statusCode: 200, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ ok:true, inserted }) };

  } catch (err) {
    return { statusCode: 500, headers:{'Access-Control-Allow-Origin':'*'}, body: JSON.stringify({ error:'Server error', detail: String(err) }) };
  }
};
