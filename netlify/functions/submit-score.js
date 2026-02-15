// netlify/functions/submit-score.js
const AIRTABLE_BASE = process.env.AIRTABLE_BASE_ID;
const AIRTABLE_PAT  = process.env.AIRTABLE_PAT;
const TABLE_USERS = process.env.AIRTABLE_TABLE_USERS || 'Users';
const TABLE_SCORES = process.env.AIRTABLE_TABLE_SCORES || 'Scores';

const fetchImpl = global.fetch || (await import('node-fetch')).default;

function airtableFetch(path, opts = {}) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE}/${encodeURIComponent(path)}`;
  const headers = Object.assign({}, opts.headers || {}, {
    Authorization: `Bearer ${AIRTABLE_PAT}`,
    'Content-Type': 'application/json'
  });
  return fetchImpl(url, Object.assign({}, opts, { headers }));
}

exports.handler = async function (event) {
  if (event.httpMethod !== 'POST') return { statusCode: 405, body: JSON.stringify({ error: 'Method Not Allowed' }) };

  try {
    const body = JSON.parse(event.body || '{}');
    // required
    const { game_id, points } = body;
    // identity possibilities:
    // - registered: { pseudo, password_hash }
    // - anonymous: { anon_id } (pseudo optional for display)
    const pseudoIn = body.pseudo ? String(body.pseudo).trim().slice(0,50) : null;
    const password_hash = body.password_hash ? String(body.password_hash) : null;
    const anon_id = body.anon_id ? String(body.anon_id) : null;
    const meta = body.meta ? String(body.meta).slice(0,1000) : '';

    if (!game_id || typeof points === 'undefined') {
      return { statusCode: 400, body: JSON.stringify({ error: 'Missing game_id or points' }) };
    }
    const cleanGame = String(game_id).trim().slice(0,50);
    const cleanPoints = Number(points);
    if (!Number.isFinite(cleanPoints) || Number.isNaN(cleanPoints)) {
      return { statusCode: 400, body: JSON.stringify({ error: 'Invalid points' }) };
    }
    // optional scalar guard
    if (Math.abs(cleanPoints) > 1000000) return { statusCode: 400, body: JSON.stringify({ error: 'Points out of range' }) };

    // Determine identity mode
    const isAnon = !!anon_id;
    let pseudoToStore = pseudoIn || (isAnon ? `anon_${anon_id.slice(0,8)}` : null);

    // If registered path: check/create user
    if (!isAnon) {
      if (!pseudoIn || !password_hash) {
        return { statusCode: 400, body: JSON.stringify({ error: 'Registered users must provide pseudo and password_hash' }) };
      }
      // lookup user
      const filter = `?filterByFormula=({pseudo} = "${pseudoIn.replace(/"/g,'\\"')}")&maxRecords=1`;
      const r = await airtableFetch(TABLE_USERS + filter);
      if (!r.ok) {
        const txt = await r.text();
        return { statusCode: 500, body: JSON.stringify({ error: 'Airtable users lookup failed', detail: txt }) };
      }
      const ud = await r.json();
      if (ud.records && ud.records.length > 0) {
        const user = ud.records[0];
        const existingHash = (user.fields && user.fields.password_hash) || '';
        if (existingHash !== String(password_hash)) {
          return { statusCode: 403, body: JSON.stringify({ error: 'Pseudo already taken with different password' }) };
        }
        // ok
      } else {
        // create user
        const createBody = { records: [{ fields: { pseudo: pseudoIn, password_hash } }] };
        const rc = await airtableFetch(TABLE_USERS, { method: 'POST', body: JSON.stringify(createBody) });
        if (!rc.ok) {
          const txt = await rc.text();
          return { statusCode: 500, body: JSON.stringify({ error: 'Airtable create user failed', detail: txt }) };
        }
      }
      pseudoToStore = pseudoIn;
    } else {
      // anon path: ensure anon_id is short enough
      if (!anon_id || anon_id.length < 6) return { statusCode: 400, body: JSON.stringify({ error: 'Invalid anon_id' }) };
      // pseudoToStore already set above
    }

    // Prevent multiple submissions same day for this identity (UTC date)
    const todayUTC = new Date().toISOString().slice(0,10);
    // Build filter: same date & same game & (same pseudo OR same anon_id)
    let identityClause = isAnon
      ? `{anon_id} = "${anon_id.replace(/"/g,'\\"')}"`
      : `{pseudo} = "${pseudoToStore.replace(/"/g,'\\"')}"`;
    const formula = `AND(DATETIME_FORMAT({created_at}, 'YYYY-MM-DD') = "${todayUTC}", {game_id} = "${cleanGame}", ${identityClause})`;

    const rCheck = await airtableFetch(`${TABLE_SCORES}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`);
    if (!rCheck.ok) {
      const txt = await rCheck.text();
      return { statusCode: 500, body: JSON.stringify({ error: 'Airtable read failed', detail: txt }) };
    }
    const existing = await rCheck.json();
    if (existing.records && existing.records.length > 0) {
      return { statusCode: 403, body: JSON.stringify({ error: 'Already submitted today for this game' }) };
    }

    // Insert the score row into Scores
    const fields = {
      pseudo: pseudoToStore || 'anonymous',
      game_id: cleanGame,
      points: cleanPoints,
      created_at: new Date().toISOString(),
      meta
    };
    if (isAnon) fields.anon_id = anon_id;

    const insertBody = { records: [{ fields }] };
    const rInsert = await airtableFetch(TABLE_SCORES, { method: 'POST', body: JSON.stringify(insertBody) });
    if (!rInsert.ok) {
      const txt = await rInsert.text();
      return { statusCode: 500, body: JSON.stringify({ error: 'Airtable insert failed', detail: txt }) };
    }
    const inserted = await rInsert.json();
    return { statusCode: 200, body: JSON.stringify({ ok: true, record: inserted.records && inserted.records[0] }) };

  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
