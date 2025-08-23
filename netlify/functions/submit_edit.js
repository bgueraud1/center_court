// netlify/functions/submit_edit.js
const fetch = require('node-fetch');

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') return { statusCode: 405, body: 'Method Not Allowed' };
  const body = event.body && event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString() : event.body;
  const params = new URLSearchParams(body);

  const player_id = params.get('player_id') || 'unknown';
  const field = params.get('field') || '';
  const value = params.get('value') || '';
  const sender = params.get('sender') || 'anonymous';
  const email = params.get('email') || '';
  const admin_code = params.get('admin_code') || '';

  // If admin_code == process.env.ADMIN_CODE -> mark as admin_proposal
  const isAdmin = (process.env.ADMIN_CODE && admin_code === process.env.ADMIN_CODE);

  // Create a Github issue with the suggested change
  const repo = process.env.TARGET_REPO || 'YOUR_USER/YOUR_REPO'; // set in Netlify env
  const token = process.env.GITHUB_PAT_FOR_NETLIFY; // set in Netlify env

  const title = `[Suggested edit] player ${player_id} - ${field}`;
  const bodyText = `Proposition par: ${sender} (${email})\n\nPlayer: ${player_id}\nField: ${field}\nNew value: ${value}\nAdmin?: ${isAdmin}\n\n(Envoyer sur la branche de modération)`;

  try {
    const resp = await fetch(`https://api.github.com/repos/${repo}/issues`, {
      method: 'POST',
      headers: {
        Authorization: `token ${token}`,
        'Content-Type': 'application/json',
        'User-Agent': 'center-court-bot'
      },
      body: JSON.stringify({ title, body: bodyText })
    });
    if (!resp.ok) {
      const txt = await resp.text();
      return { statusCode: 500, body: `GitHub API error: ${resp.status} - ${txt}` };
    }
    const issue = await resp.json();
    return {
      statusCode: 200,
      body: `Merci — proposition créée: ${issue.html_url}`
    };
  } catch (e) {
    return { statusCode: 500, body: `Erreur interne: ${String(e)}` };
  }
};
