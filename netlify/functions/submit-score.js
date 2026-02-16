// netlify/functions/submit-score.js
// CommonJS module for Netlify functions — no top-level await.
const { createClient } = require('@supabase/supabase-js');

exports.handler = async function(event, context) {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Missing Supabase env vars' }) };
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
    auth: { persistSession: false }
  });

  let payload;
  try {
    payload = JSON.parse(event.body || '{}');
  } catch (err) {
    return { statusCode: 400, body: JSON.stringify({ error: 'bad_json', detail: err.message }) };
  }

  const {
    game_id,
    points,
    user_id,     // optional (UUID or string)
    pseudo,      // optional
    anon_id,     // optional (if you still use anon_id)
    mode,        // optional (top20/top100/...); use only if you have that column
    meta         // optional json/string
  } = payload;

  if (!game_id || typeof points !== 'number') {
    return { statusCode: 400, body: JSON.stringify({ error: 'missing_fields', message: 'game_id and numeric points required' }) };
  }

  // date for daily uniqueness (format YYYY-MM-DD)
  const today = (new Date()).toISOString().slice(0,10);

  try {
    // 1) check existing submission for *today*.
    //    We MUST filter by created_day and either user_id (preferred) OR pseudo OR anon_id.
    let query = supabase
      .from('scores')
      .select('id, points')
      .eq('game_id', game_id)
      .eq('created_day', today)
      .limit(1);

    if (user_id) {
      query = query.eq('user_id', user_id);
    } else if (anon_id) {
      query = query.eq('anon_id', anon_id);
    } else if (pseudo) {
      query = query.eq('pseudo', pseudo);
    } else {
      // if no identifier at all, treat as anonymous but we cannot dedupe → allow insert.
      query = null;
    }

    if (query) {
      const { data: existing, error: fetchErr } = await query;
      if (fetchErr) {
        console.error('Supabase select error', fetchErr);
        return { statusCode: 500, body: JSON.stringify({ error: 'Supabase check failed', detail: fetchErr.message || fetchErr }) };
      }
      if (existing && existing.length > 0) {
        return { statusCode: 409, body: JSON.stringify({ ok:false, error: 'Already submitted today for this game' }) };
      }
    }

    // 2) insert new row
    const row = {
      game_id,
      points,
      meta: meta || null,
      created_at: (new Date()).toISOString()
    };
    if (user_id) row.user_id = user_id;
    if (pseudo) row.pseudo = pseudo;
    if (anon_id) row.anon_id = anon_id;
    if (mode) row.mode = mode; // set only if your table has a mode column

    const { data: inserted, error: insertErr } = await supabase
      .from('scores')
      .insert(row)
      .select()   // return the created row
      .single();

    if (insertErr) {
      // if unique constraint fires we return 409
      console.error('Supabase insert error', insertErr);
      if (insertErr.code === '23505' || (insertErr.details && insertErr.details.includes('unique'))) {
        return { statusCode: 409, body: JSON.stringify({ ok:false, error: 'Already submitted today (unique constraint)' }) };
      }
      return { statusCode: 500, body: JSON.stringify({ error: 'Supabase insert failed', detail: insertErr }) };
    }

    return { statusCode: 200, body: JSON.stringify({ ok:true, row: inserted }) };
  } catch (e) {
    console.error('submit-score handler exception', e);
    return { statusCode: 500, body: JSON.stringify({ error: 'Server error', detail: String(e) }) };
  }
};
