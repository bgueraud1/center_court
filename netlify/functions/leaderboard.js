// netlify/functions/leaderboard.js
const AIRTABLE_BASE = process.env.AIRTABLE_BASE_ID;
const AIRTABLE_PAT  = process.env.AIRTABLE_PAT;
const TABLE_SCORES = process.env.AIRTABLE_TABLE_SCORES || 'Scores';

exports.handler = async function(event) {
  let fetchImpl = globalThis.fetch;
  if (!fetchImpl) {
    try {
      const nf = await import('node-fetch');
      fetchImpl = nf.default || nf;
    } catch (e) {
      console.error('Could not load fetch implementation', e);
      return { statusCode: 500, body: JSON.stringify({ error: 'Server fetch unavailable' }) };
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

  if (event.httpMethod !== 'GET') return { statusCode: 405, body: 'Method Not Allowed' };

  try {
    const q = event.queryStringParameters || {};
    const date = q.date || (new Date().toISOString().slice(0,10));
    const game_id = q.game_id || null;
    const limit = Number(q.limit || 50);

    let formula = `DATETIME_FORMAT({created_at}, 'YYYY-MM-DD') = "${date}"`;
    if (game_id) {
      const gEsc = game_id.replace(/"/g,'\\"');
      formula = `AND(${formula}, {game_id} = "${gEsc}")`;
    }

    const records = [];
    let offset = null;
    do {
      let url = `${TABLE_SCORES}?pageSize=100&filterByFormula=${encodeURIComponent(formula)}` + (offset ? `&offset=${offset}` : '');
      const r = await airtableFetch(url);
      if (!r.ok) {
        const txt = await r.text();
        return { statusCode: 500, body: JSON.stringify({ error: 'Airtable read failed', detail: txt }) };
      }
      const data = await r.json();
      if (data.records && data.records.length) records.push(...data.records);
      offset = data.offset;
    } while (offset);

    const agg = {};
    records.forEach(rec => {
      const f = rec.fields || {};
      const p = (f.pseudo || 'anonymous').trim();
      const pts = Number(f.points) || 0;
      const gid = f.game_id || 'unknown';
      if (!agg[p]) agg[p] = { pseudo: p, total: 0, games: {} };
      agg[p].total += pts;
      agg[p].games[gid] = (agg[p].games[gid] || 0) + pts;
    });

    const list = Object.values(agg).sort((a,b) => b.total - a.total).slice(0, limit);
    return { statusCode: 200, body: JSON.stringify({ ok: true, date, game_id: game_id || 'all', totalRecords: records.length, leaderboard: list }) };

  } catch (err) {
    console.error(err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Server error', detail: String(err) }) };
  }
};
