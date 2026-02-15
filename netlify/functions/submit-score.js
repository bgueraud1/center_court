// netlify/functions/submit-score.js
const AIRTABLE_BASE = process.env.AIRTABLE_BASE_ID;
const AIRTABLE_PAT  = process.env.AIRTABLE_PAT;
const TABLE_USERS   = process.env.AIRTABLE_TABLE_USERS || 'Users';
const TABLE_SCORES  = process.env.AIRTABLE_TABLE_SCORES  || 'Scores';

exports.handler = async function (event) {
  // quick environment checks
  if (!AIRTABLE_BASE || !AIRTABLE_PAT) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Missing AIRTABLE_BASE_ID or AIRTABLE_PAT in environment' }) };
  }

  // lazy fetch implementation
  let fetchImpl = globalThis.fetch;
  if (!fetchImpl) {
    try {
      const nf = await import('node-fetch');
      fetchImpl = nf.default || nf;
    } catch (e) {
      console.error('Could not load fetch implementation', e);
      return { statusCode: 500, body: JSON.stringify({ error: 'Server fetch unavailable', detail: String(e) }) };
    }
  }

  function airtableFetch(path, opts = {}) {
    const url = `https://api.airtable.com/v0/${AIRTABLE_BASE}/${encodeURIComponent(path)}`;
    const headers = Object.assign({}, opts.headers || {}, {
      Authorization: `Bearer ${AIRTABLE_PAT}`,
      'Content-Type': 'application/json'
    });
    return fetchImpl(url, Object.assign({}, opts, { headers }));
  }

  if (event.httpMethod !== 'POST') return { statusCode: 405, body: JSON.stringify({ error: 'Method Not Allowed' }) };

  try {
    const body = JSON.parse(event.body || '{}');
    const { game_id, points } = body;
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
    if (Math.abs(cleanPoints) > 1000000) return { statusCode: 400, body: JSON.stringify({ error: 'Points out of range' }) };

    const isAnon = !!anon_id;
    let pseudoToStore = pseudoIn || (isAnon ? `anon_${anon_id.slice(0,8)}` : null);

    // Registered path: create/check user
    if (!isAnon) {
      if (!pseudoIn || !password_hash) {
        return { statusCode: 400, body: JSON.stringify({ error: 'Registered users must provide pseudo and password_hash' }) };
      }
      const filter = `?filterByFormula=({pseudo} = "${pseudoIn.replace(/"/g,'\\"')}")&maxRecords=1`;
      const r = await airtableFetch(TABLE_USERS + filter);
      const txt = await r.text();
      if (!r.ok) {
        return { statusCode: 500, body: JSON.stringify({ error: 'Airtable users lookup failed', status: r.status, detail: txt }) };
      }
      const ud = JSON.parse(txt);
      if (ud.records && ud.records.length > 0) {
        const user = ud.records[0];
        const existingHash = (user.fields && user.fields.password_hash) || '';
        if (existingHash !== String(password_hash)) {
          return { statusCode: 403, body: JSON.stringify({ error: 'Pseudo already taken with different password' }) };
        }
      } else {
        // create user
        const createBody = { records: [{ fields: { pseudo: pseudoIn, password_hash } }] };
        const rc = await airtableFetch(TABLE_USERS, { method: 'POST', body: JSON.stringify(createBody) });
        const txtc = await rc.text();
        if (!rc.ok) {
          return { statusCode: 500, body: JSON.stringify({ error: 'Airtable create user failed', status: rc.status, detail: txtc }) };
        }
      }
      pseudoToStore = pseudoIn;
    } else {
      if (!anon_id || anon_id.length < 6) return { statusCode: 400, body: JSON.stringify({ error: 'Invalid anon_id' }) };
    }

    // Prevent duplicate submission same day (UTC)
    const todayUTC = new Date().toISOString().slice(0,10);
    const identityClause = isAnon
      ? `{anon_id} = "${anon_id.replace(/"/g,'\\"')}"`
      : `{pseudo} = "${pseudoToStore.replace(/"/g,'\\"')}"`;
    const formula = `AND(DATETIME_FORMAT({created_at}, 'YYYY-MM-DD') = "${todayUTC}", {game_id} = "${cleanGame}", ${identityClause})`;
    const checkUrl = `${TABLE_SCORES}?maxRecords=1&filterByFormula=${encodeURIComponent(formula)}`;
    const rCheck = await airtableFetch(checkUrl);
    const txtCheck = await rCheck.text();
    if (!rCheck.ok) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Airtable read failed', status: rCheck.status, detail: txtCheck }) };
    }
    const existing = JSON.parse(txtCheck);
    if (existing.records && existing.records.length > 0) {
      return { statusCode: 403, body: JSON.stringify({ error: 'Already submitted today for this game' }) };
    }

    // Insert score
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
    const txtInsert = await rInsert.text();
    if (!rInsert.ok) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Airtable insert failed', status: rInsert.status, detail: txtInsert }) };
    }
    const inserted = JSON.parse(txtInsert);
    return { statusCode: 200, body: JSON.stringify({ ok: true, record: inserted.records && inserted.records[0] }) };

  } catch (err) {
    console.error('submit-score error', err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
