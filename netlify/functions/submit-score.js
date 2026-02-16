// netlify/functions/submit-score.js
// Utilise uniquement fetch (Node 18+ sur Netlify) — pas de dépendances externes.

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY;

exports.handler = async function(event) {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method Not Allowed' }) };
  }

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY' }) };
  }

  let payload;
  try {
    payload = JSON.parse(event.body || '{}');
  } catch (e) {
    return { statusCode: 400, body: JSON.stringify({ error: 'bad_json', detail: e.message }) };
  }

  const { game_id, points, user_id, anon_id, pseudo, meta, mode } = payload;

  if (!game_id || typeof points !== 'number') {
    return { statusCode: 400, body: JSON.stringify({ error: 'missing_fields', message: 'game_id and numeric points required' }) };
  }

  try {
    // calculer plage "aujourd'hui" en UTC (00:00:00 UTC -> 24h)
    const now = new Date();
    const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 0, 0, 0));
    const end = new Date(start.getTime() + 24 * 3600 * 1000);

    const startISO = start.toISOString();
    const endISO = end.toISOString();

    // Construire l'URL PostgREST de recherche
    // /rest/v1/scores?game_id=eq.<..>&created_at=gte.<start>&created_at=lt.<end>&user_id=eq.<id>
    const params = new URLSearchParams();
    params.set('game_id', `eq.${game_id}`);
    params.set('created_at', `gte.${startISO}`); // fera 2 fois la clé created_at ci-dessous - on forma en string finale
    // NOTE: URLSearchParams ne permet pas 2x le même nom facilement; on construira manuellement.

    // build query base
    let q = `${SUPABASE_URL.replace(/\/$/, '')}/rest/v1/scores?game_id=eq.${encodeURIComponent(game_id)}&created_at=gte.${encodeURIComponent(startISO)}&created_at=lt.${encodeURIComponent(endISO)}`;

    if (user_id) {
      q += `&user_id=eq.${encodeURIComponent(user_id)}`;
    } else if (anon_id) {
      q += `&anon_id=eq.${encodeURIComponent(anon_id)}`;
    } else if (pseudo) {
      q += `&pseudo=eq.${encodeURIComponent(pseudo)}`;
    } else {
      // pas d'identifiant — nous laissons q tel quel pour detecter d'éventuelles soumissions sans id
    }

    const headers = {
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'apikey': SUPABASE_KEY,
      'Accept': 'application/json'
    };

    // 1) Check existing submissions for today
    const resCheck = await fetch(q, { headers });
    if (!resCheck.ok) {
      const text = await resCheck.text().catch(()=>null);
      return { statusCode: 500, body: JSON.stringify({ error: 'Supabase check failed', status: resCheck.status, detail: text }) };
    }
    const existing = await resCheck.json();
    if (Array.isArray(existing) && existing.length > 0) {
      return { statusCode: 409, body: JSON.stringify({ ok:false, error: 'Already submitted today for this game' }) };
    }

    // 2) Insert new row
    const insertRow = {
      game_id,
      points,
      meta: meta || null,
      created_at: (new Date()).toISOString()
    };
    if (user_id) insertRow.user_id = user_id;
    if (pseudo) insertRow.pseudo = pseudo;
    if (anon_id) insertRow.anon_id = anon_id;
    if (mode) insertRow.mode = mode;

    const insertUrl = `${SUPABASE_URL.replace(/\/$/, '')}/rest/v1/scores`;
    const resInsert = await fetch(insertUrl, {
      method: 'POST',
      headers: Object.assign({
        'Content-Type': 'application/json',
        'Prefer': 'return=representation',
      }, headers),
      body: JSON.stringify(insertRow)
    });

    const insertText = await resInsert.text();
    if (!resInsert.ok) {
      // renvoyer le message renvoyé par Supabase pour debugging
      let detail = insertText;
      try { detail = JSON.parse(insertText); } catch(e) {}
      return { statusCode: 500, body: JSON.stringify({ error: 'Supabase insert failed', status: resInsert.status, detail }) };
    }

    // resInsert ok -> body JSON du row inséré (return=representation)
    let inserted;
    try { inserted = JSON.parse(insertText); } catch(e){ inserted = insertText; }

    return { statusCode: 200, body: JSON.stringify({ ok:true, row: inserted }) };

  } catch (err) {
    console.error('submit-score exception', err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
