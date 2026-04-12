const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const USERS_TABLE = process.env.USERS_TABLE || "users";
const QUESTION_TABLE = process.env.QUESTION_GAME_TABLE || "question_game";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, x-user-id, x-user-name, x-user-tour, x-user-country, x-user-league, x-user-rank",
  "Access-Control-Allow-Methods": "OPTIONS, GET, POST"
};

function jsonResponse(status, body) {
  return {
    statusCode: status,
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS),
    body: JSON.stringify(body)
  };
}

function getHeader(headers, name) {
  const key = Object.keys(headers || {}).find((k) => k.toLowerCase() === name.toLowerCase());
  return key ? headers[key] : null;
}

function parseJsonMaybe(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try { return JSON.parse(value); } catch { return null; }
}

function normalizeTags(tags) {
  if (!tags) return [];
  if (Array.isArray(tags)) return tags.map((t) => String(t).trim()).filter(Boolean);
  return String(tags)
    .split(/[,;\n]+/g)
    .map((t) => t.trim())
    .filter(Boolean);
}

function hasATPOrWTA(tags) {
  const upper = normalizeTags(tags).map((t) => t.toUpperCase());
  return upper.includes("ATP") || upper.includes("WTA");
}

function makeId(prefix = "qg") {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 8)}`;
}

async function supabaseRequest(table, { method = "GET", query = {}, payload = null } = {}) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }

  const headers = {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    Accept: "application/json"
  };

  if (method !== "GET") {
    headers["Content-Type"] = "application/json";
    headers["Prefer"] = "return=representation";
  }

  const res = await fetch(url.toString(), {
    method,
    headers,
    body: method === "GET" ? undefined : JSON.stringify(payload)
  });

  const text = await res.text().catch(() => null);
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!res.ok) {
    throw new Error(`Supabase ${method} ${table} failed: ${res.status} ${text}`);
  }

  return data;
}

async function supabaseSelect(table, query = {}) {
  return await supabaseRequest(table, { method: "GET", query });
}

async function resolveUser(ctx) {
  const base = {
    user_id: ctx.user_id,
    user_name: ctx.user_name,
    user_tour: ctx.user_tour,
    user_country: ctx.user_country,
    user_league: ctx.user_league,
    user_world_rank: Number.parseInt(String(ctx.user_world_rank || ""), 10),
    source: "headers"
  };

  if (base.user_id) {
    const rows = await supabaseSelect(USERS_TABLE, {
      select: "id,pseudo,created_at,league,country,tour,league_id",
      id: `eq.${base.user_id}`,
      limit: "1"
    }).catch(() => []);
    if (Array.isArray(rows) && rows.length) return rows[0];
  }

  if (base.user_name) {
    const rows = await supabaseSelect(USERS_TABLE, {
      select: "id,pseudo,created_at,league,country,tour,league_id",
      pseudo: `eq.${base.user_name}`,
      limit: "1"
    }).catch(() => []);
    if (Array.isArray(rows) && rows.length) return rows[0];
  }

  return null;
}

function normalizeQuestion(row) {
  return {
    id: row?.id ?? null,
    user_id: row?.user_id ?? null,
    question_type: row?.question_type ?? null,
    question_corps: row?.question_corps ?? null,
    qcm_a: row?.qcm_a ?? null,
    qcm_b: row?.qcm_b ?? null,
    qcm_c: row?.qcm_c ?? null,
    qcm_d: row?.qcm_d ?? null,
    qcm_answer: row?.qcm_answer ?? null,
    answer: row?.answer ?? null,
    true_false: row?.true_false ?? null,
    difficulty: row?.difficulty ?? null,
    open_player: row?.open_player ?? null,
    true_false_additional: row?.true_false_additional ?? null,
    tags: row?.tags ?? null
  };
}

function validateQuestion(row, index) {
  if (!row.question_type || !["open", "qcm", "tf"].includes(row.question_type)) {
    return `Question #${index + 1}: invalid question type.`;
  }

  if (!row.question_corps || !String(row.question_corps).trim()) {
    return `Question #${index + 1}: question text is required.`;
  }

  const diff = Number(row.difficulty);
  if (!Number.isInteger(diff) || diff < 1 || diff > 4) {
    return `Question #${index + 1}: difficulty must be between 1 and 4.`;
  }

  if (!hasATPOrWTA(row.tags)) {
    return `Question #${index + 1}: tags must include ATP, WTA, or both.`;
  }

  if (row.question_type === "open") {
    if (!row.answer || !String(row.answer).trim()) {
      return `Question #${index + 1}: open questions need an answer.`;
    }
  }

  if (row.question_type === "qcm") {
    if (![row.qcm_a, row.qcm_b, row.qcm_c, row.qcm_d].every((v) => String(v || "").trim())) {
      return `Question #${index + 1}: QCM questions need all four options.`;
    }
    const letter = String(row.qcm_answer || "").toLowerCase().trim();
    if (!["a", "b", "c", "d"].includes(letter)) {
      return `Question #${index + 1}: QCM correct answer must be a, b, c, or d.`;
    }
  }

  if (row.question_type === "tf") {
    if (typeof row.true_false !== "boolean") {
      return `Question #${index + 1}: True / False questions need a boolean answer.`;
    }
  }

  return null;
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 204, headers: CORS_HEADERS, body: "" };
    }

    if ((event.httpMethod || "GET").toUpperCase() !== "POST") {
      return jsonResponse(405, { ok: false, error: "Method not allowed" });
    }

    const headers = event.headers || {};
    const ctx = {
      user_id: getHeader(headers, "x-user-id") || null,
      user_name: getHeader(headers, "x-user-name") || null,
      user_tour: getHeader(headers, "x-user-tour") || null,
      user_country: getHeader(headers, "x-user-country") || null,
      user_league: getHeader(headers, "x-user-league") || null,
      user_world_rank: getHeader(headers, "x-user-rank") || null
    };

    const user = await resolveUser(ctx);
    if (!user) {
      return jsonResponse(401, { ok: false, error: "Authentication required." });
    }

    const body = parseJsonMaybe(event.body) || {};
    const incoming = Array.isArray(body.questions) ? body.questions : [];
    if (!incoming.length) {
      return jsonResponse(400, { ok: false, error: "No questions provided." });
    }

    const rows = incoming.map((raw, index) => {
      const id = String(raw?.id || "").trim() || makeId(`q${index + 1}`);
      const question_type = String(raw?.question_type || "").trim().toLowerCase();
      const question_corps = String(raw?.question_corps || "").trim();
      const difficulty = Number(raw?.difficulty || 1);
      const tags = normalizeTags(raw?.tags).join(", ");

      const row = {
        id,
        user_id: user.id || ctx.user_id || null,
        question_type,
        question_corps,
        qcm_a: null,
        qcm_b: null,
        qcm_c: null,
        qcm_d: null,
        qcm_answer: null,
        answer: null,
        true_false: null,
        difficulty,
        open_player: null,
        true_false_additional: null,
        tags
      };

      if (question_type === "open") {
        row.answer = String(raw?.answer || "").trim();
        row.open_player = Boolean(raw?.open_player);
      } else if (question_type === "qcm") {
        row.qcm_a = String(raw?.qcm_a || "").trim();
        row.qcm_b = String(raw?.qcm_b || "").trim();
        row.qcm_c = String(raw?.qcm_c || "").trim();
        row.qcm_d = String(raw?.qcm_d || "").trim();
        row.qcm_answer = String(raw?.qcm_answer || "").trim().toLowerCase();
      } else if (question_type === "tf") {
        row.true_false = typeof raw?.true_false === "boolean" ? raw.true_false : null;
        row.true_false_additional = raw?.true_false_additional ? String(raw.true_false_additional).trim() : null;
      }

      return row;
    });

    for (let i = 0; i < rows.length; i++) {
      const error = validateQuestion(rows[i], i);
      if (error) {
        return jsonResponse(400, { ok: false, error });
      }
    }

    const inserted = await supabaseRequest(QUESTION_TABLE, {
      method: "POST",
      query: { select: "*" },
      payload: rows.map(normalizeQuestion)
    });

    return jsonResponse(200, {
      ok: true,
      inserted: Array.isArray(inserted) ? inserted.map(normalizeQuestion) : []
    });
  } catch (err) {
    console.error("[question_game_submit] fatal", err);
    return jsonResponse(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error."
    });
  }
};