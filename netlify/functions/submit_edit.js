// netlify/functions/submit_edit.js
exports.handler = async function(event, context) {
  try {
    console.log("=== submit_edit invoked ===");
    console.log("Method:", event.httpMethod);
    console.log("Path:", event.path);
    console.log("QueryStringParameters:", event.queryStringParameters);
    // log headers presence (do NOT log sensitive headers like Authorization if present)
    console.log("Headers keys:", Object.keys(event.headers || {}).join(", "));

    let bodyParsed = null;
    // Try parse body as JSON, fall back to text
    if (event.body) {
      try {
        bodyParsed = JSON.parse(event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString('utf8') : event.body);
        console.log("Parsed JSON body:", bodyParsed);
      } catch (err) {
        console.log("Body is not JSON, raw body length:", event.body.length);
        bodyParsed = event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString('utf8') : event.body;
      }
    } else {
      console.log("No body in request");
    }

    // Log important env var presence (but NOT their values)
    const envChecks = {
      ADMIN_CODE_PRESENT: !!process.env.ADMIN_CODE,
      GITHUB_PAT_PRESENT: !!process.env.GITHUB_PAT_FOR_NETLIFY || !!process.env.GITHUB_PAT,
      CSV_PATH: !!process.env.CSV_PATH
    };
    console.log("Env presence:", envChecks);

    // Echo back for client debugging
    const responsePayload = {
      ok: true,
      received: {
        method: event.httpMethod,
        path: event.path,
        query: event.queryStringParameters,
        body: bodyParsed
      },
      envChecks
    };

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(responsePayload)
    };

  } catch (err) {
    console.error("submit_edit unexpected ERROR:", err && err.stack ? err.stack : err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ok: false, error: String(err && err.stack ? err.stack : err) })
    };
  }
};
