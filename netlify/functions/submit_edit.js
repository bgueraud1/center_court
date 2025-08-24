// netlify/functions/submit_edit.js
// CommonJS module (Netlify Functions). Uses node-fetch v2 and papaparse.
// package.json below lists dependencies.

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
  // Avoid logging secrets
  console.log(...args);
}

function buildGithubHeaders() {
  if (!GITHUB_PAT) return { 'Accept': 'application/vnd.github+json' };
  return {
    'Accept': 'application/vnd.github+json',
    'Authorization': `token ${GITHUB_PAT}`
  };
}

async function githubGetFile(path) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/contents/${encodeURIComponent(path)}?ref=${encodeURIComponent(BRANCH)}`;
  const res = await fetch(url, { headers: buildGithubHeaders() });
  if (res.status === 404) {
    const text = await res.text();
    throw { code: 404, message: `File not found: ${path}`, body: text };
  }
  if (!res.ok) {
    const body = await res.text();
    throw { code: res.status, message: `GitHub GET contents failed`, body };
  }
  const json = await res.json();
  return json; // includes .content (base64), .sha
}

async function githubPutFile(path, message, contentBase64, sha) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/contents/${encodeURIComponent(path)}`;
  const payload = {
    message,
    content: contentBase64,
    branch: BRANCH,
    committer: { name: COMMITTER_NAME, email: COMMITTER_EMAIL },
    sha
  };
  const res = await fetch(url, {
    method: 'PUT',
    headers: buildGithubHeaders(),
    body: JSON.stringify(payload)
  });
  const body = await res.text();
  if (!res.ok) {
    let parsed;
    try { parsed = JSON.parse(body); } catch(e){ parsed = { body }; }
    throw { code: res.status, message: 'GitHub PUT contents failed', body: parsed };
  }
  return JSON.parse(body);
}

async function githubCreateIssue(title, body, labels) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}/issues`;
  const payload = { title, body, labels };
  const res = await fetch(url, {
    method: 'POST',
    headers: buildGithubHeaders(),
    body: JSON.stringify(payload)
  });
  const text = await res.text();
  if (!res.ok) {
    let parsed;
    try { parsed = JSON.parse(text); } catch(e){ parsed = { body: text }; }
    throw { code: res.status, message: 'GitHub create issue failed', body: parsed };
  }
  return JSON.parse(text);
}

function detectKeyColumn(fields) {
  const candidates = ['player_key','player','slug','id','player_id','player_slug','url'];
  for (const c of candidates) {
    if (fields.includes(c)) return c;
  }
  // fallback to first column
  return fields[0];
}

function tryMatchRow(rows, keyCol, playerVal) {
  if (!playerVal) return null;
  // try exact
  const exact = rows.find(r => String(r[keyCol]) === String(playerVal));
  if (exact) return exact;
  // try numeric id extraction from slug: e.g., "313112-xxx"
  const numeric = (''+playerVal).match(/^(\d+)/);
  if (numeric) {
    const num = numeric[1];
    // try columns id/player_id
    const altCols = ['id','player_id'];
    for (const col of altCols) {
      const found = rows.find(r => String(r[col]) === String(num));
      if (found) return found;
    }
    // also try keyCol numeric part
    const found2 = rows.find(r => {
      const v = ''+r[keyCol];
      const m = v.match(/^(\d+)/);
      return m && m[1] === num;
    });
    if (found2) return found2;
  }
  // case-insensitive slug match
  const lower = rows.find(r => (''+r[keyCol]).toLowerCase() === (''+playerVal).toLowerCase());
  if (lower) return lower;
  return null;
}

function sanitizeEdits(edits, allowedFields) {
  const keys = Object.keys(edits || {});
  if (keys.length === 0) throw { code:400, message: 'edits empty' };
  // Only keep edits where key in allowedFields
  const sanitized = {};
  for (const k of keys) {
    if (!allowedFields.includes(k)) {
      throw { code:400, message: `Invalid field in edits: ${k}` };
    }
    // values should be string (or convertible)
    sanitized[k] = edits[k] === null || edits[k] === undefined ? '' : String(edits[k]);
  }
  return sanitized;
}

// main handler
exports.handler = async function(event, context) {
  try {
    safeLog("=== submit_edit invoked ===");
    safeLog("Method:", event.httpMethod);
    if (event.httpMethod !== 'POST') {
      return { statusCode: 405, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Method Not Allowed - use POST' }) };
    }

    let body = null;
    if (!event.body) {
      return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Empty request body' }) };
    }
    try {
      body = JSON.parse(event.isBase64Encoded ? Buffer.from(event.body,'base64').toString('utf8') : event.body);
    } catch (err) {
      return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Request body must be JSON' }) };
    }

    const player = body.player;
    const editsRaw = body.edits;
    const providedAdminCode = body.admin_code || null;
    const reporter = body.reporter || null; // optional metadata (user name/email), we won't trust for auth

    if (!player) {
      return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'player (slug/id) is required' }) };
    }
    if (!editsRaw || typeof editsRaw !== 'object') {
      return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'edits object is required' }) };
    }

    // Determine admin mode
    const isAdmin = (ADMIN_CODE && providedAdminCode && providedAdminCode === ADMIN_CODE && !!GITHUB_PAT);
    if (providedAdminCode && !ADMIN_CODE) {
      safeLog('ADMIN_CODE not configured on server but client sent admin_code - ignoring');
    }

    // If admin mode -> commit
    if (isAdmin) {
      // Validate repo env
      if (!OWNER || !REPO || !GITHUB_PAT) {
        return { statusCode: 500, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Server misconfigured: missing GITHUB_OWNER/GITHUB_REPO/GITHUB_PAT_FOR_NETLIFY' }) };
      }

      // Step 1: get file current content
      let fileJson;
      try {
        fileJson = await githubGetFile(CSV_PATH);
      } catch (err) {
        safeLog('Error fetching CSV from GitHub:', err);
        return { statusCode: 502, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Failed to fetch CSV from GitHub', detail: err }) };
      }

      const sha = fileJson.sha;
      const contentB64 = fileJson.content;
      const csvRaw = Buffer.from(contentB64, 'base64').toString('utf8');

      // parse CSV
      const parsed = Papa.parse(csvRaw, { header: true, skipEmptyLines: false });
      if (parsed.errors && parsed.errors.length) {
        safeLog('Warning: CSV parse errors', parsed.errors);
      }
      const rows = parsed.data;
      const fields = parsed.meta && parsed.meta.fields ? parsed.meta.fields : Object.keys(rows[0] || {});
      const keyCol = detectKeyColumn(fields);

      const existingRow = tryMatchRow(rows, keyCol, player);
      if (!existingRow) {
        return { statusCode: 404, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: `Player not found for '${player}'` }) };
      }

      // sanitize edits
      let sanitizedEdits;
      try {
        sanitizedEdits = sanitizeEdits(editsRaw, fields);
      } catch (err) {
        return { statusCode: 400, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: err.message || String(err) }) };
      }

      // apply merge
      const updatedRows = rows.map(r => {
        if (r[keyCol] === existingRow[keyCol] || ((''+r[keyCol]).toLowerCase() === (''+existingRow[keyCol]).toLowerCase())) {
          return Object.assign({}, r, sanitizedEdits);
        }
        return r;
      });

      // serialize
      const newCsv = Papa.unparse(updatedRows, { header: true });

      // commit message
      const changeSummary = Object.keys(sanitizedEdits).map(k => `${k} -> ${sanitizedEdits[k]}`).join('; ');
      const message = `Update ${player} via site (admin). ${changeSummary}`;

      const contentNewB64 = Buffer.from(newCsv, 'utf8').toString('base64');

      // attempt commit (with one retry in case SHA changed)
      try {
        const putRes = await githubPutFile(CSV_PATH, message, contentNewB64, sha);
        const commitUrl = putRes.commit && putRes.commit.html_url ? putRes.commit.html_url : null;
        return {
          statusCode: 200,
          headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ ok:true, committed:true, commit: putRes.commit, content: putRes.content, commit_url: commitUrl })
        };
      } catch (err) {
        safeLog('First commit attempt failed:', err);
        // if conflict due to outdated sha, try one retry: fetch latest, reapply, commit with new sha
        if (err && err.code && (err.code === 409 || err.code === 422 || String(err.message).toLowerCase().includes('sha'))) {
          safeLog('Retrying commit after re-fetching latest file...');
          try {
            const latest = await githubGetFile(CSV_PATH);
            const latestCsv = Buffer.from(latest.content, 'base64').toString('utf8');
            const parsedLatest = Papa.parse(latestCsv, { header: true, skipEmptyLines: false });
            const rowsLatest = parsedLatest.data;
            const fieldsLatest = parsedLatest.meta && parsedLatest.meta.fields ? parsedLatest.meta.fields : Object.keys(rowsLatest[0] || {});
            const keyColLatest = detectKeyColumn(fieldsLatest);
            const existingLatest = tryMatchRow(rowsLatest, keyColLatest, player);
            if (!existingLatest) {
              return { statusCode: 409, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error:'Conflict: player row disappeared in latest CSV' }) };
            }
            const updatedLatestRows = rowsLatest.map(r => {
              if ((''+r[keyColLatest]) === (''+existingLatest[keyColLatest]) || ((''+r[keyColLatest]).toLowerCase() === (''+existingLatest[keyColLatest]).toLowerCase())) {
                return Object.assign({}, r, sanitizedEdits);
              }
              return r;
            });
            const newCsv2 = Papa.unparse(updatedLatestRows, { header: true });
            const contentNewB642 = Buffer.from(newCsv2, 'utf8').toString('base64');
            const putRes2 = await githubPutFile(CSV_PATH, message + ' (retry)', contentNewB642, latest.sha);
            return {
              statusCode: 200,
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify({ ok:true, committed:true, commit: putRes2.commit, content: putRes2.content })
            };
          } catch (err2) {
            safeLog('Retry commit failed:', err2);
            return { statusCode: 502, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Commit failed after retry', detail: err2 }) };
          }
        }
        return { statusCode: 502, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Commit failed', detail: err }) };
      }
    } // end admin path

    // Non-admin path: create GitHub issue with suggestion
    // Validate owner/repo exist
    if (!OWNER || !REPO || !GITHUB_PAT) {
      // If PAT missing, still we could create issue anonymously? No - GitHub API requires auth for creating issues in private repo.
      return { statusCode: 500, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Server misconfigured: missing GITHUB_OWNER/GITHUB_REPO/GITHUB_PAT_FOR_NETLIFY' }) };
    }

    // Build issue body - try to include current vs proposed by fetching CSV (best effort)
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
      safeLog('Could not fetch CSV to include current values in issue (continuing):', err);
    }

    // Build table of changes
    let tableRows = '';
    if (currentSnap && currentSnap.row) {
      tableRows = currentSnap.fields
        .filter(f => body.edits.hasOwnProperty(f))
        .map(f => `| ${f} | ${currentSnap.row[f] || ''} | ${body.edits[f] || ''} |`).join('\n');
    } else {
      // we don't have current snapshot; just list proposed edits
      tableRows = Object.keys(body.edits).map(k => `| ${k} |  | ${body.edits[k] || ''} |`).join('\n');
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
      return {
        statusCode: 200,
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ ok:true, suggestion:true, issue_url: created.html_url, issue_number: created.number })
      };
    } catch (err) {
      safeLog('Failed to create issue:', err);
      return { statusCode: 502, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: 'Failed to create suggestion issue', detail: err }) };
    }

  } catch (err) {
    console.error('submit_edit unexpected ERROR:', err && err.stack ? err.stack : err);
    return { statusCode: 500, headers: {'Content-Type':'application/json'}, body: JSON.stringify({ ok:false, error: String(err && err.stack ? err.stack : err) }) };
  }
};
