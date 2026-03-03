// netlify/functions/logout.js
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "OPTIONS, POST"
};

function jsonResponse(status, body, headers = {}) {
  return {
    statusCode: status,
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS, headers),
    body: JSON.stringify(body)
  };
}

module.exports.handler = async function(event) {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers: CORS_HEADERS, body: '' };
  if (event.httpMethod !== 'POST') return jsonResponse(405, { error: 'Method Not Allowed' });

  // expire a generic 'auth' cookie (adjust cookie name/path/flags if you used others)
  const cookie = 'auth=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; SameSite=Lax';

  return jsonResponse(200, { ok: true }, { 'Set-Cookie': cookie });
};