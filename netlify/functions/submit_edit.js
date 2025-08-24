// netlify/functions/submit_edit.js
// CommonJS; requires node-fetch@2 and papaparse installed at repo root.
const fetch = require('node-fetch');
const Papa = require('papaparse');

const OWNER = process.env.GITHUB_OWNER;
const REPO = process.env.GITHUB_REPO;
const BRANCH = process.env.GITHUB_BRANCH || 'main';
const CSV_PATH = process.env.CSV_PATH || 'player_data_wta.csv';
const GITHUB_PAT = process.env.GITHUB_PAT_FOR_NETLIFY;
const ADMIN_CODE = process.env.ADMIN_CODE || null;
const COMMITTER_NAME = process.env.GITHUB_COMMITTER_NAME || 'center-court bot';
const COMMITTER_EMAIL = process.env.GITHUB_COMMITTER_EMAIL || 'bot@center-court.net';
const SUGGESTION_LABELS = (process.env.SUGGESTION_LABELS || 'suggestion,from-website').split(',').map(s => s.trim()).filter(Boolean);
const SITE_BASE_URL = process.env.SITE_BASE_URL || 'https://www.center-court.net';

function safeLog(...args) {
  // DO NOT print secrets
  try { console.log(...args); } catch(e){ /* ignore */ }
}

function buildGithubHeaders() {
  if (!GITHUB_PAT) return { 'Accept': 'application/vnd.github+json' };
  return {
    'Accept': 'application/vnd.github+json',
    'Authorization': `token ${GITHUB_PAT}`
  };
}

// Github helpers (unchanged)
async function githubGetFile(path) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/contents/${encodeURIComponent(path)}?ref=${encodeURIComponent(BRANCH)}`;
  const res = await fetch(url, { headers: buildGithubHeaders() });
  if (!res.ok) {
    const body = await res.text();
    throw { code: res.status, message: 'GitHub GET contents failed', body };
  }
  return await res.json();
}

async function githubPutFile(path, message, contentBase64, sha) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/contents/${encodeURIComponent(path)}`;
  const payload = { message, content: contentBase64, branch: BRANCH, committer: { name: COMMITTER_NAME, email: COMMITTER_EMAIL }, sha };
  const res = await fetch(url, { method: 'PUT', headers: buildGithubHeaders(), body: JSON.stringify(payload) });
  const text = await res.text();
  if (!res.ok) {
    let parsed; try { parsed = JSON.parse(text); } catch(e){ parsed = { body: text }; }
    throw { code: res.status, message: 'GitHub PUT contents failed', body: parsed };
  }
  return JSON.parse(text);
}

async function githubCreateIssue(title, body, labels) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/issues`;
  const payload = { title, body, labels };
  const res = await fetch(url, { method: 'POST', headers: buildGithubHeaders(), body: JSON.stringify(payload) });
  const text = await res.text();
  if (!res.ok) {
    let parsed; try { parsed = JSON.parse(text); } catch(e){ parsed = { body: text }; }
    throw { code: res.status, message: 'GitHub create issue failed', body: parsed };
  }
  return JSON.parse(text);
}

// CSV helpers
function detectKeyColumn(fields) {
  const candidates = ['player_key','player','slug','id','player_id','player_slug','url'];
  for (const c of candidates) if (fields.includes(c)) return c;
  return fields[0];
}

function tryMatchRow(rows, keyCol, playerVal) {
  if (!playerVal) return null;
  const sPlayer = String(playerVal);
  // exact match
  const exact = rows.find(r => String(r[keyCol]) === sPlayer);
  if (exact) return exact;
  // numeric prefix from slug
  const numeric = sPlayer.match(/^(\d+)/);
  if (numeric) {
    const num = numeric[1];
    const altCols = ['player_id','id'];
    for (const col of altCols) {
      const f = rows.find(r => String(r[col]) === String(num));
      if (f) return f;
    }
    const found2 = rows.find(r => {
      const v = ''+r[keyCol];
      const m = v.match(/^(\d+)/);
      return m && m[1] === num;
    });
    if (found2) return found2;
  }
  // case-insensitive
  const lower = rows.find(r => (''+r[keyCol]).toLowerCase() === sPlayer.toLowerCase());
  if (lower) return lower;
  return null;
}

function sanitizeEdits(edits, allowedFields) {
  const keys = Object.keys(edits || {});
  if (keys.length === 0) throw { code:400, message: 'edits empty' };
  const sanitized = {};
  for (const k of keys) {
    if (!allowedFields.includes(k)) throw { code:400, message: `Invalid field in edits: ${k}` };
    sanitized[k] = edits[k] === null || edits[k] === undefined ? '' : String(edits[k]);
  }
  return sanitized;
}

// Robust body parser:
// - try JSON
// - if not JSON and content-type urlencoded -> parse URLSearchParams
// - else if body string contains "=" -> parse URLSearchParams
// - else fallback to event.queryStringParameters (GET/form)
function parseRequestBody(event) {
  let raw = null;
  if (!event.body) return null;
  raw = event.isBase64Encoded ? Buffer.from(event.body,'base64').toString('utf8') : event.body;
  // try JSON
  try {
    const j = JSON.parse(raw);
    return j;
  } catch (err) {
    // not JSON
  }
  // try urlencoded like a=b&c=d
  const contentType = (event.headers && (event.headers['content-type'] || event.headers['Content-Type'] || '')).toLowerCase();
  if (contentType.includes('application/x-www-form-urlencoded') || contentType.includes('text/plain') || raw.includes('=')) {
    try {
      const params = new URLSearchParams(raw);
      const obj = {};
      for (const [k,v] of params) {
        // If keys like edits[field]=value, handle nested edits
        const m = k.match(/^edits\[(.+)\]$/);
        if (m) {
          obj.edits = obj.edits || {};
          obj.edits[m[1]] = v;
        } else {
          // simple keys: player, name, admin_code etc.
          if (obj[k] === undefined) obj[k] = v;
          else {
            // already exists -> convert to array
            if (!Array.isArray(obj[k])) obj[k] = [obj[k]];
            obj[k].push(v);
          }
        }
      }
      return obj;
    } catch(e) {
      // fallthrough
    }
  }
  // if nothing, return raw string as fallback
  return raw;
}

exports.handler = async function(event, context) {
  try {
    safeLog("=== submit_edit invoked ===");
    safeLog("Method:", event.httpMethod);
    safeLog("Path:", event.path || '(none)');
    safeLog("Headers keys:", Object.keys(event.headers || {}).join(", "));
    safeLog("QueryStringParameters:", event.queryStringParameters ? JSON.stringify(event.queryStringParameters) : '{}');
    // parse body robustly
    let bodyParsed = null;
    if (event.body) {
      bodyParsed = parseRequestBody(event);
      safeLog("Parsed body type:", typeof bodyParsed);
      // For debugging: do not log whole content if big
      if (typeof bodyParsed === 'object') safeLog("Parsed JSON body keys:", Object.keys(bodyParsed).join(", "));
      else safeLog("Parsed raw body length:", String(bodyParsed).length);
    } else {
      safeLog("No body in request");
    }

    // If bodyParsed is null or a string, try fallback to query params
    if ((!bodyParsed || typeof bodyParsed === 'string') && event.queryStringParameters && Object.keys(event.queryStringParameters).length) {
      // prefer query params when body is not parseable
      const q = event.queryStringParameters;
      const fallback = {};
      // Any param that is not standard becomes edits
      for (const k of Object.keys(q)) {
        if (['player','name','admin_code','reported_via'].includes(k)) fallback[k] = q[k];
        else {
          fallback.edits = fallback.edits || {};
          fallback.edits[k] = q[k];
        }
      }
      bodyParsed = Object.assign({}, (typeof bodyParsed === 'object' ? bodyParsed : {}), fallback);
      safeLog("Used query-string fallback, keys:", Object.keys(bodyParsed).join(", "));
    }

    const envChecks = {
      ADMIN_CODE_PRESENT: !!ADMIN_CODE,
      GITHUB_PAT_PRESENT: !!GITHUB_PAT,
      CSV_PATH: !!CSV_PATH
    };
    safeLog("Env presence:", envChecks);

    // validate
    // ---------- Normalisation des variantes côté client ----------
    let body = bodyParsed || {};
      
    // Si front envoie player_slug / player_id / player_name, mappe-les sur les clés attendues
    if (body && typeof body === 'object') {
      // map player_name -> name
      if (!body.name && body.player_name) body.name = body.player_name;
    
      // map player_slug / player_id -> player (valeur utilisée pour le matching)
      if (!body.player) {
        if (body.player_slug) body.player = body.player_slug;
        else if (body.player_id) body.player = String(body.player_id);
      }
    
      // Si l'appel n'envoie pas d'"edits" structuré, construis-le automatiquement :
      // on considère comme "meta" : player/player_slug/player_id/player_name/name/admin_code/reported_via/source/notes
      if (!body.edits || typeof body.edits !== 'object') {
        const metaKeys = new Set(['player','player_slug','player_id','player_name','name','admin_code','reported_via','source','notes']);
        const edits = {};
        for (const k of Object.keys(body)) {
          if (!metaKeys.has(k)) {
            edits[k] = body[k];
          }
        }
        // si on a trouvé des clés à modifier, on les place dans body.edits
        if (Object.keys(edits).length > 0) body.edits = edits;
      }
    }
// ---------- Fin normalisation ----------
    const player = body.player;
    const editsRaw = body.edits;

    if (event.httpMethod !== 'POST') {
      return { statusCode: 405, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Method Not Allowed - use POST' }) };
    }

    if (!player) {
      return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'player (slug/id) is required' }) };
    }
    if (!editsRaw || typeof editsRaw !== 'object') {
      return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'edits object is required' }) };
    }

    // --- normalize and debug-admin-check (safe: logs short hashes, not secrets) ---
    const crypto = require('crypto');
      
    function shortHash(s){
      try { return crypto.createHash('sha256').update(String(s||'')).digest('hex').slice(0,8); }
      catch(e){ return '(hash-fail)'; }
    }
    
    const providedAdminRaw = (body.admin_code || body.admin || '').toString();
    const providedAdmin = providedAdminRaw.trim(); // remove accidental leading/trailing spaces
    const ADMIN_CODE_NORMALIZED = (ADMIN_CODE || '').toString().trim();
    
    safeLog('Admin provided? ', providedAdmin.length > 0, 'len=', providedAdmin.length);
    safeLog('Admin hashes (short): provided=', shortHash(providedAdmin), 'env=', ADMIN_CODE_NORMALIZED ? shortHash(ADMIN_CODE_NORMALIZED) : '(no-env)');
    
    const isAdmin = (ADMIN_CODE_NORMALIZED && providedAdmin && providedAdmin === ADMIN_CODE_NORMALIZED && !!GITHUB_PAT);
    if (providedAdmin && !ADMIN_CODE_NORMALIZED) safeLog('ADMIN_CODE not configured but admin_code provided (ignored)');
    
    // ADMIN path: update CSV
    if (isAdmin) {
      if (!OWNER || !REPO || !GITHUB_PAT) {
        return { statusCode: 500, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Server misconfigured: missing GITHUB_OWNER/GITHUB_REPO/GITHUB_PAT_FOR_NETLIFY' }) };
      }
      // fetch CSV
      let fileJson;
      try { fileJson = await githubGetFile(CSV_PATH); } catch (err) {
        safeLog('Error fetching CSV from GitHub:', err);
        return { statusCode: 502, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Failed to fetch CSV from GitHub', detail: err }) };
      }
      const sha = fileJson.sha;
      const csvRaw = Buffer.from(fileJson.content, 'base64').toString('utf8');
      const parsed = Papa.parse(csvRaw, { header: true, skipEmptyLines: false });
      const rows = parsed.data;
      const fields = parsed.meta && parsed.meta.fields ? parsed.meta.fields : Object.keys(rows[0] || {});
      const keyCol = detectKeyColumn(fields);
      const existingRow = tryMatchRow(rows, keyCol, player);
      if (!existingRow) {
        return { statusCode: 404, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: `Player not found for '${player}'` }) };
      }
      let sanitizedEdits;
      try { sanitizedEdits = sanitizeEdits(editsRaw, fields); } catch (e) {
        return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: e.message || String(e) }) };
      }
      const updatedRows = rows.map(r => {
        if ((String(r[keyCol]) === String(existingRow[keyCol])) || ((''+r[keyCol]).toLowerCase() === (''+existingRow[keyCol]).toLowerCase())) {
          return Object.assign({}, r, sanitizedEdits);
        }
        return r;
      });
      const newCsv = Papa.unparse(updatedRows, { header: true });
      const changeSummary = Object.keys(sanitizedEdits).map(k => `${k} -> ${sanitizedEdits[k]}`).join('; ');
      const message = `Update ${player} via site (admin). ${changeSummary}`;
      const contentNewB64 = Buffer.from(newCsv, 'utf8').toString('base64');
      try {
        const putRes = await githubPutFile(CSV_PATH, message, contentNewB64, sha);
        const commitUrl = putRes.commit && putRes.commit.html_url ? putRes.commit.html_url : null;
        return { statusCode: 200, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:true, committed:true, commit: putRes.commit, commit_url: commitUrl }) };
      } catch (err) {
        safeLog('Commit failed, attempting retry if possible:', err);
        if (err && (err.code === 409 || err.code === 422 || String(err.message || '').toLowerCase().includes('sha'))) {
          try {
            const latest = await githubGetFile(CSV_PATH);
            const latestCsv = Buffer.from(latest.content, 'base64').toString('utf8');
            const parsedLatest = Papa.parse(latestCsv, { header: true, skipEmptyLines: false });
            const rowsLatest = parsedLatest.data;
            const fieldsLatest = parsedLatest.meta && parsedLatest.meta.fields ? parsedLatest.meta.fields : Object.keys(rowsLatest[0] || {});
            const keyColLatest = detectKeyColumn(fieldsLatest);
            const existingLatest = tryMatchRow(rowsLatest, keyColLatest, player);
            if (!existingLatest) return { statusCode: 409, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error:'Conflict: player missing in latest CSV' }) };
            const updatedLatestRows = rowsLatest.map(r => {
              if ((String(r[keyColLatest]) === String(existingLatest[keyColLatest])) || ((''+r[keyColLatest]).toLowerCase() === (''+existingLatest[keyColLatest]).toLowerCase())) {
                return Object.assign({}, r, sanitizeEdits(editsRaw, fieldsLatest));
              }
              return r;
            });
            const newCsv2 = Papa.unparse(updatedLatestRows, { header: true });
            const contentNewB642 = Buffer.from(newCsv2, 'utf8').toString('base64');
            const putRes2 = await githubPutFile(CSV_PATH, message + ' (retry)', contentNewB642, latest.sha);
            return { statusCode:200, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ok:true, committed:true, commit: putRes2.commit }) };
          } catch (err2) {
            safeLog('Retry failed:', err2);
            return { statusCode:502, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error:'Commit failed after retry', detail: err2 }) };
          }
        }
        return { statusCode:502, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error:'Commit failed', detail: err }) };
      }
    } // end admin path

    // NON-ADMIN path -> create issue
    if (!OWNER || !REPO || !GITHUB_PAT) {
      return { statusCode:500, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error:'Server misconfigured: missing GITHUB_OWNER/GITHUB_REPO/GITHUB_PAT_FOR_NETLIFY' }) };
    }

    // Try to fetch a snapshot of current values (best-effort)
    let currentSnap = null;
    try {
      const fileJson = await githubGetFile(CSV_PATH);
      const csvRaw = Buffer.from(fileJson.content, 'base64').toString('utf8');
      const parsed = Papa.parse(csvRaw, { header: true, skipEmptyLines: false });
      const fields = parsed.meta && parsed.meta.fields ? parsed.meta.fields : Object.keys(parsed.data[0] || {});
      const keyCol = detectKeyColumn(fields);
      const existing = tryMatchRow(parsed.data, keyCol, player);
      currentSnap = { row: existing, fields, keyCol };
    } catch (err) {
      safeLog('Could not fetch CSV snapshot (continuing):', err);
    }

    // Build issue body
    let tableRows = '';
    if (currentSnap && currentSnap.row) {
      tableRows = currentSnap.fields
        .filter(f => body.edits && body.edits.hasOwnProperty(f))
        .map(f => `| ${f} | ${currentSnap.row[f] || ''} | ${body.edits[f] || ''} |`).join('\n');
    } else {
      tableRows = Object.keys(body.edits || {}).map(k => `| ${k} |  | ${body.edits[k] || ''} |`).join('\n');
    }

    const pageLink = `${SITE_BASE_URL}/players/${player}`;
    const ts = (new Date()).toISOString();
    const issueTitle = `Suggestion: correction pour ${body.name || player} (${player})`;
    const issueBodyLines = [
      'Suggestion envoyée depuis le formulaire d\'édition du site.',
      '',
      `**Joueuse**: ${body.name || '(nom non fourni)'}`,
      `**Slug / clé**: ${player}`,
      `**Page**: ${pageLink}`,
      `**Envoyé le**: ${ts}`,
      '',
      '## Modifications proposées',
      '| Champ | Valeur actuelle | Valeur proposée |',
      '| --- | --- | --- |',
      tableRows || '| (aucunes modifications valides détectées) | | |',
      '',
      '## Payload JSON',
      '```json',
      JSON.stringify(body, null, 2),
      '```',
      ''
    ];
    const issueBody = issueBodyLines.join('\n');

    try {
      const created = await githubCreateIssue(issueTitle, issueBody, SUGGESTION_LABELS);
      return { statusCode:200, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ok:true, suggestion:true, issue_url: created.html_url, issue_number: created.number }) };
    } catch (err) {
      safeLog('Failed to create issue:', err);
      return { statusCode:502, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error:'Failed to create suggestion issue', detail: err }) };
    }

  } catch (err) {
    console.error('submit_edit unexpected ERROR:', err && err.stack ? err.stack : err);
    return { statusCode:500, headers:{'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: String(err && err.stack ? err.stack : err) }) };
  }
};
