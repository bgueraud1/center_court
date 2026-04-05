const fs = require("fs");
const path = require("path");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const USERS_TABLE = process.env.USERS_TABLE || "users";
const NEXT_INSCRIPTIONS_TABLE = process.env.NEXT_INSCRIPTIONS_TABLE || "next_inscriptions";
const BRACKET_TABLE = process.env.BRACKET_TABLE || "bracket";
const OPEN_JSON_PATH = path.join(process.cwd(), "docs/bracket/open_inscriptions.json");
const BRACKET_TOURNAMENTS_DIR = process.env.BRACKET_TOURNAMENTS_DIR || "docs/bracket/tournaments";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization, x-user-id, x-user-name, x-user-tour, x-user-country, x-user-league, x-user-rank, x-user-token",
  "Access-Control-Allow-Methods": "OPTIONS, GET, POST, PATCH"
};

function jsonResponse(status, body) {
  return {
    statusCode: status,
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS),
    body: JSON.stringify(body)
  };
}

function readBody(event) {
  try {
    return event.body ? JSON.parse(event.body) : {};
  } catch {
    return {};
  }
}

function getHeader(headers, name) {
  const key = Object.keys(headers || {}).find(k => k.toLowerCase() === name.toLowerCase());
  return key ? headers[key] : null;
}

function getCallerContext(event, body = {}) {
  const headers = event.headers || {};
  const user_id = getHeader(headers, "x-user-id") || body.user_id || null;
  const user_name = getHeader(headers, "x-user-name") || body.user_name || null;
  const user_tour = getHeader(headers, "x-user-tour") || body.user_tour || null;
  const user_country = getHeader(headers, "x-user-country") || body.user_country || null;
  const user_league = getHeader(headers, "x-user-league") || body.user_league || null;
  const user_world_rank_raw = getHeader(headers, "x-user-rank") || body.user_world_rank || null;
  const user_world_rank = Number.parseInt(String(user_world_rank_raw || ""), 10);

  return {
    user_id,
    user_name,
    user_tour,
    user_country,
    user_league,
    user_world_rank: Number.isFinite(user_world_rank) ? user_world_rank : null,
    raw_headers: headers
  };
}

function parseJsonMaybe(value) {
  if (!value) return null;
  if (typeof value === "object") return value;
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
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

async function supabasePatch(table, query, payload) {
  return await supabaseRequest(table, { method: "PATCH", query, payload });
}

async function supabaseInsert(table, payload) {
  const data = await supabaseRequest(table, { method: "POST", query: { select: "*" }, payload: [payload] });
  return Array.isArray(data) ? data[0] : data;
}

async function supabaseUpsert(table, payload, onConflict) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  if (onConflict) url.searchParams.set("on_conflict", onConflict);
  url.searchParams.set("select", "*");

  const res = await fetch(url.toString(), {
    method: "POST",
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=representation"
    },
    body: JSON.stringify([payload])
  });

  const text = await res.text().catch(() => null);
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!res.ok) {
    throw new Error(`Supabase UPSERT ${table} failed: ${res.status} ${text}`);
  }

  return Array.isArray(data) ? data[0] : data;
}

function readOpenPayload() {
  if (!fs.existsSync(OPEN_JSON_PATH)) return null;
  try {
    return JSON.parse(fs.readFileSync(OPEN_JSON_PATH, "utf-8"));
  } catch {
    return null;
  }
}

function extractOpenWindow(openPayload) {
  const windowObj = openPayload?.window || openPayload?.registration_window || {};
  return {
    is_open_today: Boolean(windowObj.is_open_today ?? windowObj.is_open ?? false),
    phase: String(openPayload?.window?.phase || windowObj.phase || (windowObj.is_open_today ? "open" : "closed")).toLowerCase(),
    open_date: windowObj.open_date || windowObj.window_start_date || openPayload?.current_paris_date || null,
    close_date: windowObj.close_date || null,
    target_start_date: windowObj.target_start_date || null,
    count: Number(windowObj.count ?? openPayload?.count ?? 0) || 0,
    current_paris_date: openPayload?.current_paris_date || null
  };
}

function normalizeCategory(category) {
  return String(category || "").trim().toUpperCase();
}

function isAtpRestricted(category, rank) {
  const c = normalizeCategory(category);
  const isChallenger = c === "CH" || c.includes("CHALLENGER");
  const isFuture = c === "FU" || c.includes("FUTURE");

  if (!Number.isFinite(rank)) return { ok: true, reason: null };

  if (isChallenger && rank <= 50) {
    return { ok: false, reason: "ATP top 50 players cannot enter Challenger events." };
  }
  if (isFuture && rank <= 200) {
    return { ok: false, reason: "ATP top 200 players cannot enter Future events." };
  }
  return { ok: true, reason: null };
}

async function resolveUser(ctx) {
  const base = {
    user_id: ctx.user_id,
    user_name: ctx.user_name,
    user_tour: ctx.user_tour,
    user_country: ctx.user_country,
    user_league: ctx.user_league,
    user_world_rank: ctx.user_world_rank,
    source: "headers"
  };

  if (base.user_id) {
    const rows = await supabaseSelect(USERS_TABLE, {
      select: "id,pseudo,tour,country,league,league_id",
      id: `eq.${base.user_id}`,
      limit: "1"
    }).catch(() => []);

    if (Array.isArray(rows) && rows.length) {
      const u = rows[0];
      return {
        user_id: u.id,
        user_name: u.pseudo || base.user_name || null,
        user_tour: u.tour || base.user_tour || null,
        user_country: u.country || base.user_country || null,
        user_league: u.league || base.user_league || null,
        user_world_rank: Number.isFinite(base.user_world_rank) ? base.user_world_rank : null,
        league_id: u.league_id || null,
        source: "users.id"
      };
    }
  }

  if (base.user_name) {
    const rows = await supabaseSelect(USERS_TABLE, {
      select: "id,pseudo,tour,country,league,league_id",
      pseudo: `eq.${base.user_name}`,
      limit: "1"
    }).catch(() => []);

    if (Array.isArray(rows) && rows.length) {
      const u = rows[0];
      return {
        user_id: u.id,
        user_name: u.pseudo || base.user_name || null,
        user_tour: u.tour || base.user_tour || null,
        user_country: u.country || base.user_country || null,
        user_league: u.league || base.user_league || null,
        user_world_rank: Number.isFinite(base.user_world_rank) ? base.user_world_rank : null,
        league_id: u.league_id || null,
        source: "users.pseudo"
      };
    }

    return {
      ...base,
      user_id: base.user_id || null,
      source: "fallback.headers"
    };
  }

  return null;
}

async function loadBracketRowsForUser(userId) {
  return await supabaseSelect(BRACKET_TABLE, {
    select: "*",
    user_id: `eq.${userId}`,
    order: "updated_at.desc"
  }).catch(() => []);
}

function chooseCurrentBracketRow(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;
  return rows.find(r => r.user_current_tournament_bracket_proposition || r.current_tournament_bracket || r.current_tournament_bracket_id || r.user_current_tournament_bracket_id) || rows[0] || null;
}

async function loadNextInscriptionRows(userId) {
  return await supabaseSelect(NEXT_INSCRIPTIONS_TABLE, {
    select: "*",
    user_id: `eq.${userId}`,
    order: "window_start_date.desc"
  }).catch(() => []);
}

function chooseNextInscriptionRow(rows, preferredWindowStart) {
  if (!Array.isArray(rows) || !rows.length) return null;
  if (preferredWindowStart) {
    const exact = rows.find(r => String(r.window_start_date || "") === String(preferredWindowStart));
    if (exact) return exact;
  }
  return rows[0] || null;
}

function loadTournamentJson(tour, tournamentId, year) {
  if (!tour || !tournamentId || !year) return null;
  const fileName = `${String(tour).toLowerCase()}_${String(tournamentId)}_${String(year)}_temporary.json`;
  const filePath = path.join(process.cwd(), BRACKET_TOURNAMENTS_DIR, fileName);
  if (!fs.existsSync(filePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf-8"));
  } catch {
    return null;
  }
}

function normalizeCurrentRow(row) {
  if (!row) return null;
  const currentBracket = parseJsonMaybe(row.current_tournament_bracket);
  const proposal = parseJsonMaybe(row.user_current_tournament_bracket_proposition);
  return {
    id: row.id ?? null,
    user_id: row.user_id ?? null,
    user_name: row.user_name ?? null,
    user_tour: row.user_tour ?? null,
    user_country: row.user_country ?? null,
    user_world_rank: row.user_world_rank ?? null,
    current_tournament_bracket_id: row.current_tournament_bracket_id ?? null,
    current_tournament_bracket_name: row.current_tournament_bracket_name ?? null,
    current_tournament_bracket: currentBracket,
    user_current_tournament_bracket_id: row.user_current_tournament_bracket_id ?? null,
    user_current_tournament_bracket_name: row.user_current_tournament_bracket_name ?? null,
    user_current_tournament_bracket_proposition: row.user_current_tournament_bracket_proposition ?? null,
    user_current_tournament_bracket_proposition_json: proposal,
    has_played: Boolean(row.user_current_tournament_bracket_proposition),
    locked: true
  };
}

function normalizeNextRow(row, templateJson) {
  if (!row) return null;
  const proposal = parseJsonMaybe(row.user_proposition_next_week);
  return {
    id: row.id ?? null,
    window_start_date: row.window_start_date ?? null,
    target_start_date: row.target_start_date ?? null,
    user_id: row.user_id ?? null,
    user_name: row.user_name ?? null,
    user_world_rank: row.user_world_rank ?? null,
    tour: row.tour ?? null,
    tournament_id: row.tournament_id ?? null,
    tournament_name: row.tournament_name ?? null,
    tournament_category: row.tournament_category ?? null,
    tournament_num_players: row.tournament_num_players ?? null,
    assigned_preference_rank: row.assigned_preference_rank ?? null,
    assigned_at: row.assigned_at ?? null,
    user_proposition_next_week: row.user_proposition_next_week ?? null,
    user_proposition_next_week_json: proposal,
    template_json: templateJson,
    template_mode: templateJson ? (isAllMode(templateJson) ? "all" : "first_round") : "missing",
    editable: Boolean(templateJson && !row.user_proposition_next_week),
    locked: Boolean(row.user_proposition_next_week),
    has_registration: true
  };
}

function buildCurrentStatus(current) {
  if (!current) return "NO_CURRENT_PLAY";
  return current.has_played ? "CURRENT_PLAYED" : "NO_CURRENT_PLAY";
}

function buildNextStatus(next) {
  if (!next) return "NOT_REGISTERED";
  if (next.locked) return "LOCKED";
  if (!next.template_json) return "BRACKET_NOT_READY";
  return "READY";
}

async function loadStateForUser(user) {
  const openPayload = readOpenPayload();
  const openWindow = extractOpenWindow(openPayload);

  const bracketRows = await loadBracketRowsForUser(user.user_id);
  const currentRow = chooseCurrentBracketRow(bracketRows);
  const current = normalizeCurrentRow(currentRow);

  const nextRows = await loadNextInscriptionRows(user.user_id);
  const nextRow = chooseNextInscriptionRow(nextRows, openWindow.open_date);

  let templateJson = null;
  if (nextRow) {
    const tour = String(nextRow.tour || user.user_tour || "").toLowerCase();
    const tournamentId = String(nextRow.tournament_id || "").trim();
    const year = nextRow.target_start_date
      ? Number(String(nextRow.target_start_date).slice(0, 4))
      : (openWindow.target_start_date ? Number(String(openWindow.target_start_date).slice(0, 4)) : new Date().getFullYear());
    templateJson = loadTournamentJson(tour, tournamentId, year);
  }

  const next = normalizeNextRow(nextRow, templateJson);
  return { openPayload, openWindow, current, next };
}

function isAllMode(json) {
  const matches = Array.isArray(json?.matches) ? json.matches : [];
  return matches.some(m => String(m.match_id || "").toUpperCase() === "MS001" || String(m.match_id || "").toUpperCase() === "LS001");
}

function buildResponse({ user, current, next, message = "" }) {
  const currentStatus = buildCurrentStatus(current);
  const nextStatus = buildNextStatus(next);
  const status = nextStatus !== "NOT_REGISTERED" ? nextStatus : currentStatus;

  return {
    ok: true,
    authenticated: true,
    status,
    current_status: currentStatus,
    next_status: nextStatus,
    user,
    current,
    next,
    bracket: current,
    registration: next,
    message
  };
}

function validateProposalObject(input) {
  const parsed = parseJsonMaybe(input);
  if (!parsed) return { ok: false, reason: "Invalid bracket JSON." };
  if (!Array.isArray(parsed.matches) || !parsed.matches.length) return { ok: false, reason: "Bracket JSON has no matches." };

  for (const m of parsed.matches) {
    if (!m || !m.match_id) return { ok: false, reason: "A match is missing match_id." };
    if (!m.winner_player_id || !m.winner_player_name) return { ok: false, reason: `Winner is missing for ${m.match_id}.` };
  }

  return { ok: true };
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 204, headers: CORS_HEADERS, body: "" };
    }

    const method = (event.httpMethod || "GET").toUpperCase();
    const body = method === "POST" ? readBody(event) : {};
    const ctx = getCallerContext(event, body);

    console.log("[bracket] incoming headers", {
      user_id: ctx.user_id,
      user_name: ctx.user_name,
      user_tour: ctx.user_tour,
      user_country: ctx.user_country,
      user_league: ctx.user_league,
      user_world_rank: ctx.user_world_rank
    });

    const user = await resolveUser(ctx);
    console.log("[bracket] resolved user", user);

    if (!user) {
      return jsonResponse(200, {
        ok: true,
        authenticated: false,
        status: "NOT_AUTHENTICATED",
        current_status: "NO_CURRENT_PLAY",
        next_status: "NOT_REGISTERED",
        user: null,
        current: null,
        next: null,
        bracket: null,
        registration: null,
        message: "You must log in to play."
      });
    }

    if (method === "POST" && String(body.action || "").toLowerCase() === "save_next_week_proposition") {
      const state = await loadStateForUser(user);
      const next = state.next;

      if (!next) {
        return jsonResponse(400, { ok: false, error: "You are not registered for next week." });
      }
      if (next.locked) {
        return jsonResponse(400, { ok: false, error: "Your next-week proposal is already locked." });
      }

      const proposalSource = body.user_proposition_next_week_json || body.proposition_json || body.bracket || body.user_proposition_next_week;
      const validation = validateProposalObject(proposalSource);
      if (!validation.ok) {
        return jsonResponse(400, { ok: false, error: validation.reason });
      }

      const proposalObject = parseJsonMaybe(proposalSource);
      const proposalText = typeof body.user_proposition_next_week === "string"
        ? body.user_proposition_next_week
        : JSON.stringify(proposalObject, null, 2);

      const savedRows = await supabasePatch(
        NEXT_INSCRIPTIONS_TABLE,
        { id: `eq.${next.id}` },
        { user_proposition_next_week: proposalText }
      ).catch(async () => {
        return await supabasePatch(
          NEXT_INSCRIPTIONS_TABLE,
          {
            user_id: `eq.${user.user_id}`,
            window_start_date: `eq.${next.window_start_date}`
          },
          { user_proposition_next_week: proposalText }
        );
      });

      return jsonResponse(200, {
        ok: true,
        authenticated: true,
        status: "LOCKED",
        current_status: state.current ? buildCurrentStatus(state.current) : "NO_CURRENT_PLAY",
        next_status: "LOCKED",
        user,
        current: state.current,
        next: {
          ...next,
          user_proposition_next_week: proposalText,
          user_proposition_next_week_json: proposalObject,
          locked: true,
          editable: false
        },
        bracket: state.current,
        registration: {
          ...next,
          user_proposition_next_week: proposalText,
          user_proposition_next_week_json: proposalObject,
          locked: true,
          editable: false
        },
        saved: savedRows,
        message: "Bracket submitted successfully."
      });
    }

    const state = await loadStateForUser(user);

    return jsonResponse(200, buildResponse({
      user,
      current: state.current,
      next: state.next,
      message: state.next?.locked
        ? "Your next-week proposal is locked."
        : (state.next?.editable
          ? "Bracket ready."
          : (state.next ? "You are registered for next week." : (state.current ? "Current bracket loaded." : "No bracket available.")))
    }));
  } catch (err) {
    console.error("[bracket] fatal", err);
    return jsonResponse(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error."
    });
  }
};