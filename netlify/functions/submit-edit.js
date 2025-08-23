// netlify/functions/submit-edit.js
const fetch = globalThis.fetch || require('node-fetch');

const GITHUB_API = 'https://api.github.com';

// Fetch helper
async function gh(path, method='GET', body=null, token) {
  const opts = { method, headers: { 'Accept': 'application/vnd.github.v3+json', 'Authorization': `token ${token}` } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(`${GITHUB_API}${path}`, opts);
  const json = await r.json();
  if (!r.ok) {
    const err = new Error('GitHub API error');
    err.status = r.status;
    err.body = json;
    throw err;
  }
  return json;
}

function updateCsvContent(csvText, playerId, newFields) {
  const lines = csvText.split(/\r?\n/);
  const header = lines[0];
  const cols = header.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/);
  const colIndex = {};
  cols.forEach((c,i)=> colIndex[c.trim()] = i);

  if (!('player_id' in colIndex)) throw new Error('player_id column missing');

  let found = false;
  const out = lines.map((line, i) => {
    if (i === 0) return line;
    if (!line) return line;
    const parts = line.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/);
    if (parts[colIndex['player_id']].trim() == String(playerId)) {
      found = true;
      // update only known columns
      for (const [k,v] of Object.entries(newFields)) {
        if (k in colIndex) {
          let val = String(v).replace(/\n/g,' ').replace(/\r/g,'');
          // quote if comma or quote present
          if (val.includes('"')) val = val.replace(/"/g,'""');
          if (val.includes(',') || val.includes('"')) val = `"${val}"`;
          parts[colIndex[k]] = val;
        }
      }
      return parts.join(',');
    }
    return line;
  });

  if (!found) throw new Error('player_id not found in CSV');
  return out.join('\n');
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod !== 'POST') return { statusCode: 405, body: JSON.stringify({ message: 'Method not allowed' })};
    const body = JSON.parse(event.body || '{}');

    const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
    const ADMIN_CODE = process.env.ADMIN_CODE || '';
    const OWNER = process.env.GITHUB_OWNER;
    const REPO = process.env.GITHUB_REPO;
    const CSV_PATH = process.env.CSV_PATH || 'player_base_and_maps/player_data_wta.csv';

    if (!GITHUB_TOKEN || !OWNER || !REPO) {
      return { statusCode: 500, body: JSON.stringify({ message: 'Server misconfiguration (missing env vars)' })};
    }

    const player_id = body.player_id;
    if (!player_id) return { statusCode: 400, body: JSON.stringify({ message: 'Missing player_id' })};

    // allowed fields only
    const allowed = ['height_inches','height_cm','plays','birth_date','birthplace','represented_country','full_name','note'];
    const safe = {};
    for (const k of Object.keys(body)) {
      if (allowed.includes(k) && body[k] !== undefined && String(body[k]).trim() !== '') {
        safe[k] = String(body[k]).trim();
      }
    }

    // Admin path: create branch, commit, open PR
    if (body.admin_code && body.admin_code === ADMIN_CODE) {
      // 1. get file
      const file = await gh(`/repos/${OWNER}/${REPO}/contents/${CSV_PATH}`, 'GET', null, GITHUB_TOKEN);
      const sha = file.sha;
      const csvText = Buffer.from(file.content, 'base64').toString('utf8');

      // 2. update CSV
      let newCsv;
      try {
        newCsv = updateCsvContent(csvText, player_id, safe);
      } catch (err) {
        return { statusCode: 400, body: JSON.stringify({ message: 'CSV update error: ' + err.message })};
      }

      // 3. create branch
      const mainRef = await gh(`/repos/${OWNER}/${REPO}/git/ref/heads/main`, 'GET', null, GITHUB_TOKEN);
      const baseSha = mainRef.object.sha;
      const branch = `auto/update-player-${player_id}-${Date.now()}`;
      await gh(`/repos/${OWNER}/${REPO}/git/refs`, 'POST', { ref: `refs/heads/${branch}`, sha: baseSha }, GITHUB_TOKEN);

      // 4. commit file on branch
      const encoded = Buffer.from(newCsv, 'utf8').toString('base64');
      await gh(`/repos/${OWNER}/${REPO}/contents/${CSV_PATH}`, 'PUT', {
        message: `Auto-update player ${player_id} via web form`,
        content: encoded,
        sha: sha,
        branch: branch
      }, GITHUB_TOKEN);

      // 5. create PR
      const pr = await gh(`/repos/${OWNER}/${REPO}/pulls`, 'POST', {
        title: `Auto-update player ${player_id}`,
        head: branch,
        base: 'main',
        body: `Automatic suggestion applied by admin. Please review.`
      }, GITHUB_TOKEN);

      return { statusCode: 200, body: JSON.stringify({ message: 'PR created', pr_url: pr.html_url })};
    }

    // Non-admin path: create GitHub issue
    let bodyText = `Suggestion for player ${player_id}\n\n`;
    for (const [k,v] of Object.entries(safe)) bodyText += `- ${k}: ${v}\n`;
    if (body.note) bodyText += `\nNote: ${body.note}\n`;

    const issue = await gh(`/repos/${OWNER}/${REPO}/issues`, 'POST', {
      title: `Suggestion: update player ${player_id}`,
      body: bodyText,
      labels: ['suggestion']
    }, GITHUB_TOKEN);

    return { statusCode: 200, body: JSON.stringify({ message: 'Suggestion recorded as issue', issue_url: issue.html_url })};

  } catch (err) {
    console.error('submit-edit error', err);
    const msg = (err && err.message) ? err.message : String(err);
    return { statusCode: 500, body: JSON.stringify({ message: 'Server error: ' + msg })};
  }
};
