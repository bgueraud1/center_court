// netlify/functions/bracket.js
const fs = require("fs");
const path = require("path");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const USERS_TABLE = process.env.USERS_TABLE || "users";
const INSCRIPTIONS_TABLE = process.env.INSCRIPTIONS_TABLE || "inscriptions";
const BRACKET_TABLE = process.env.BRACKET_TABLE || "bracket";
const OPEN_JSON_PATH = path.join(process.cwd(), "docs/bracket/open_inscriptions.json");
const BRACKET_TOURNAMENTS_DIR = process.env.BRACKET_TOURNAMENTS_DIR || "docs/bracket/tournaments";

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
  const user_world_rank_raw = getHeader(headers, "x-user-rank") || body.user_world_rank || null;
  const user_world_rank = Number.parseInt(String(user_world_rank_raw || ""), 10);

  return {
    user_id,
    user_name,
    user_tour,
    user_country,
    user_world_rank: Number.isFinite(user_world_rank) ? user_world_rank : null
  };
}

async function supabaseGet(table, queryParams) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  for (const [k, v] of Object.entries(queryParams || {})) {
    if (v !== undefined && v !== null) url.searchParams.set(k, v);
  }

  const r = await fetch(url.toString(), {
    method: "GET",
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      Accept: "application/json"
    }
  });

  const text = await r.text().catch(() => null);
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!r.ok) {
    throw new Error(`Supabase GET ${table} failed: ${r.status} ${text}`);
  }

  return data;
}

async function supabaseInsert(table, payload) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const r = await fetch(`${SUPABASE_URL}/rest/v1/${table}?select=*`, {
    method: "POST",
    headers: {
      apikey: SUPABASE_KEY,
      Authorization: `Bearer ${SUPABASE_KEY}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      Prefer: "return=representation"
    },
    body: JSON.stringify([payload])
  });

  const text = await r.text().catch(() => null);
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!r.ok) {
    throw new Error(`Supabase INSERT ${table} failed: ${r.status} ${text}`);
  }

  return Array.isArray(data) ? data[0] : data;
}

async function supabaseUpsert(table, payload, onConflict) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
  if (onConflict) url.searchParams.set("on_conflict", onConflict);

  const r = await fetch(url.toString(), {
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

  const text = await r.text().catch(() => null);
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }

  if (!r.ok) {
    throw new Error(`Supabase UPSERT ${table} failed: ${r.status} ${text}`);
  }

  return Array.isArray(data) ? data[0] : data;
}

function readOpenPayload() {
  if (!fs.existsSync(OPEN_JSON_PATH)) {
    return {
      version: 1,
      timezone: "Europe/Paris",
      generated_at: null,
      current_paris_date: null,
      registration_window: {
        is_open_today: false,
        open_date: null,
        close_date: null,
        target_start_date: null,
        count: 0
      },
      open_tournaments: []
    };
  }
  return JSON.parse(fs.readFileSync(OPEN_JSON_PATH, "utf-8"));
}

function toKey(tour, tournamentId) {
  return `${String(tour || "").toUpperCase()}::${String(tournamentId)}`;
}

function groupCountByTournament(rows) {
  const counts = new Map();
  for (const row of rows) {
    const key = toKey(row.tour || "", row.tournament_id);
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

function normalizeCategory(category) {
  return String(category || "").trim().toUpperCase();
}

function isAtpRestricted(category, rank) {
  const c = normalizeCategory(category);
  const isChallengerOrFuture = c === "CH" || c === "FU" || c.includes("CHALLENGER") || c.includes("FUTURE");
  const isFuture = c === "FU" || c.includes("FUTURE");

  if (!Number.isFinite(rank)) return { ok: true, reason: null };

  if (isChallengerOrFuture && rank <= 50) {
    return { ok: false, reason: "ATP top 50 players cannot enter Challenger/Future events." };
  }
  if (isFuture && rank <= 200) {
    return { ok: false, reason: "ATP top 200 players cannot enter Future events." };
  }
  return { ok: true, reason: null };
}

async function getDbUserProfile(userId) {
  if (!userId) return null;

  const bracketRows = await supabaseGet(BRACKET_TABLE, {
    select: "user_id,user_name,user_world_rank,user_tour,user_country",
    user_id: `eq.${userId}`,
    limit: "1"
  }).catch(() => []);

  if (Array.isArray(bracketRows) && bracketRows.length > 0) {
    return bracketRows[0];
  }

  const userRows = await supabaseGet(USERS_TABLE, {
    select: "id,pseudo,tour,country",
    id: `eq.${userId}`,
    limit: "1"
  }).catch(() => []);

  if (Array.isArray(userRows) && userRows.length > 0) {
    return {
      user_id: userRows[0].id,
      user_name: userRows[0].pseudo,
      user_tour: userRows[0].tour,
      user_country: userRows[0].country,
      user_world_rank: null
    };
  }

  return null;
}

async function loadWeekInscriptionRows(weekStart) {
  return await supabaseGet(INSCRIPTIONS_TABLE, {
    select: "tournament_id,user_id,user_name,user_world_rank,registration_week_start,tournament_name,tournament_num_players,tournament_start_date,tour,tournament_level",
    registration_week_start: `eq.${weekStart}`
  }).catch(() => []);
}

function loadTournamentJson(tour, tournamentId, year) {
  const fileName = `${String(tour).toLowerCase()}_${String(tournamentId)}_${String(year)}_temporary.json`;
  const filePath = path.join(process.cwd(), BRACKET_TOURNAMENTS_DIR, fileName);
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf-8"));
}

function buildTemplateFromFirstRound(tournamentInfo, firstRoundJson) {
  const leafMatches = (firstRoundJson.matches || []).map((m, idx) => ({
    match_id: String(m.match_id || `MS${String(idx + 1).padStart(3, "0")}`),
    round_number: 1,
    left_child_match_id: null,
    right_child_match_id: null,
    left_player: m.winner_player_name ? {
      player_id: String(m.winner_player_id || ""),
      player_name: String(m.winner_player_name || ""),
      player_country: m.winner_country ? String(m.winner_country) : null
    } : null,
    right_player: m.loser_player_name ? {
      player_id: String(m.loser_player_id || ""),
      player_name: String(m.loser_player_name || ""),
      player_country: m.loser_country || m.loser_wountry || null
    } : null,
    winner: null
  }));

  const rounds = [{ round_number: 1, matches: leafMatches }];
  let previousRound = leafMatches;
  let roundNumber = 2;

  while (previousRound.length > 1) {
    const roundMatches = [];
    for (let i = 0; i < previousRound.length; i += 2) {
      const leftChild = previousRound[i] || null;
      const rightChild = previousRound[i + 1] || null;
      roundMatches.push({
        match_id: `R${roundNumber}_M${String(roundMatches.length + 1).padStart(3, "0")}`,
        round_number: roundNumber,
        left_child_match_id: leftChild ? leftChild.match_id : null,
        right_child_match_id: rightChild ? rightChild.match_id : null,
        left_player: null,
        right_player: null,
        winner: null
      });
    }
    rounds.push({ round_number: roundNumber, matches: roundMatches });
    previousRound = roundMatches;
    roundNumber += 1;
  }

  return {
    version: 1,
    generated_at: new Date().toISOString(),
    tournament: tournamentInfo,
    rounds
  };
}

function buildMatchLookup(bracket) {
  const map = new Map();
  for (const round of bracket.rounds || []) {
    for (const match of round.matches || []) {
      map.set(match.match_id, match);
    }
  }
  return map;
}

function recomputeBracketFromWinners(bracket) {
  const lookup = buildMatchLookup(bracket);

  for (const round of bracket.rounds || []) {
    for (const match of round.matches || []) {
      if (match.round_number > 1) {
        const leftChild = match.left_child_match_id ? lookup.get(match.left_child_match_id) : null;
        const rightChild = match.right_child_match_id ? lookup.get(match.right_child_match_id) : null;
        match.left_player = leftChild && leftChild.winner ? JSON.parse(JSON.stringify(leftChild.winner)) : null;
        match.right_player = rightChild && rightChild.winner ? JSON.parse(JSON.stringify(rightChild.winner)) : null;
      }
      const players = [match.left_player, match.right_player].filter(Boolean);
      if (players.length === 1) {
        match.winner = JSON.parse(JSON.stringify(players[0]));
      }
    }
  }

  return bracket;
}

function flattenBracket(bracket) {
  const rows = [];
  for (const round of bracket.rounds || []) {
    for (const match of round.matches || []) {
      rows.push({
        match_id: match.match_id,
        round_number: round.round_number,
        left_player_id: match.left_player ? match.left_player.player_id : null,
        left_player_name: match.left_player ? match.left_player.player_name : null,
        right_player_id: match.right_player ? match.right_player.player_id : null,
        right_player_name: match.right_player ? match.right_player.player_name : null,
        winner_player_id: match.winner ? match.winner.player_id : null,
        winner_player_name: match.winner ? match.winner.player_name : null
      });
    }
  }
  return rows;
}

function buildStoredBracketPayload(templateBracket, user, tournament, locked = false) {
  const bracket = JSON.parse(JSON.stringify(templateBracket));
  recomputeBracketFromWinners(bracket);

  return {
    version: 1,
    locked,
    generated_at: new Date().toISOString(),
    user: {
      user_id: user.user_id,
      user_name: user.user_name,
      user_world_rank: user.user_world_rank,
      user_tour: user.user_tour,
      user_country: user.user_country
    },
    tournament: {
      current_tournament_bracket_id: tournament.current_tournament_bracket_id,
      current_tournament_bracket_name: tournament.current_tournament_bracket_name,
      tournament_id: tournament.current_tournament_bracket_id,
      tournament_name: tournament.current_tournament_bracket_name,
      tournament_start_date: tournament.tournament_start_date,
      tour: tournament.tour,
      tournament_level: tournament.tournament_level
    },
    rounds: bracket.rounds.map(round => ({
      round_number: round.round_number,
      matches: round.matches.map(m => ({
        match_id: m.match_id,
        round_number: m.round_number,
        left_child_match_id: m.left_child_match_id || null,
        right_child_match_id: m.right_child_match_id || null,
        left_player: m.left_player,
        right_player: m.right_player,
        winner_player_id: m.winner ? m.winner.player_id : null,
        winner_player_name: m.winner ? m.winner.player_name : null
      }))
    })),
    matches_flat: flattenBracket(bracket)
  };
}

async function getBracketRecord(userId, tournamentId) {
  const rows = await supabaseGet(BRACKET_TABLE, {
    select: "*",
    user_id: `eq.${userId}`,
    current_tournament_bracket_id: `eq.${tournamentId}`,
    limit: "1"
  }).catch(() => []);
  return Array.isArray(rows) && rows.length ? rows[0] : null;
}

async function loadCurrentBracketContext(ctx) {
  const openPayload = readOpenPayload();
  const weekStart = openPayload?.registration_window?.open_date || null;
  const targetStartDate = openPayload?.registration_window?.target_start_date || null;
  const isOpenToday = Boolean(openPayload?.registration_window?.is_open_today);

  const profile = await getDbUserProfile(ctx.user_id);
  const user = profile || {
    user_id: ctx.user_id,
    user_name: ctx.user_name,
    user_tour: ctx.user_tour,
    user_country: ctx.user_country,
    user_world_rank: ctx.user_world_rank
  };

  return { openPayload, weekStart, targetStartDate, isOpenToday, user };
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") {
      return { statusCode: 204, headers: CORS_HEADERS, body: "" };
    }

    const method = (event.httpMethod || "GET").toUpperCase();
    const body = method === "POST" ? readBody(event) : {};
    const ctx = getCallerContext(event, body);

    if (!ctx.user_id) {
      return jsonResponse(200, {
        ok: true,
        authenticated: false,
        status: "NOT_AUTHENTICATED",
        user: null,
        tournament: null,
        bracket: null,
        locked: false,
        message: "You must log in to play."
      });
    }

    const { openPayload, weekStart, targetStartDate, isOpenToday, user } = await loadCurrentBracketContext(ctx);

    // 1) Register
    if (method === "POST" && String(body.action || "").toLowerCase() === "register") {
      if (!isOpenToday) {
        return jsonResponse(400, { ok: false, error: "Registrations are closed today." });
      }

      const tournamentId = String(body.tournament_id || "").trim();
      const tournamentName = String(body.tournament_name || "").trim();

      const tournament = (openPayload.open_tournaments || []).find(t => String(t.tournament_id) === tournamentId);
      if (!tournament) {
        return jsonResponse(400, { ok: false, error: "This tournament is not open for registration." });
      }

      const allWeekRows = weekStart ? await loadWeekInscriptionRows(weekStart) : [];
      const existingForUser = allWeekRows.find(r => String(r.user_id) === String(user.user_id));
      if (existingForUser) {
        return jsonResponse(400, {
          ok: false,
          error: "You are already registered for one tournament this week.",
          already_registered: existingForUser
        });
      }

      const countForTournament = allWeekRows.filter(r => String(r.tournament_id) === tournamentId).length;
      if (countForTournament >= (Number(tournament.draw_size) || 0)) {
        return jsonResponse(400, { ok: false, error: "Tournament is full." });
      }

      const rank = Number.isFinite(user.user_world_rank) ? user.user_world_rank : null;
      if (String(tournament.tour || "").toUpperCase() === "ATP") {
        const rule = isAtpRestricted(tournament.category, rank);
        if (!rule.ok) return jsonResponse(403, { ok: false, error: rule.reason });
      }

      const insertPayload = {
        tournament_name: tournamentName || tournament.tournament_name,
        tournament_id: tournamentId,
        tournament_num_players: Number(tournament.draw_size) || null,
        user_id: user.user_id,
        user_name: user.user_name || null,
        user_world_rank: rank,
        registration_week_start: weekStart,
        tournament_start_date: targetStartDate,
        tour: tournament.tour,
        tournament_level: tournament.category
      };

      const inserted = await supabaseInsert(INSCRIPTIONS_TABLE, insertPayload);

      return jsonResponse(200, {
        ok: true,
        status: "REGISTERED",
        inserted,
        user,
        message: "Registration saved."
      });
    }

    // 2) GET / bracket page state
    const inscriptionRows = weekStart ? await loadWeekInscriptionRows(weekStart) : [];
    const userInscription = inscriptionRows.find(r => String(r.user_id) === String(user.user_id)) || null;

    if (!userInscription) {
      return jsonResponse(200, {
        ok: true,
        authenticated: true,
        status: "NOT_REGISTERED",
        user,
        tournament: null,
        bracket: null,
        locked: false,
        message: "You are not registered for this week."
      });
    }

    const tournamentId = String(userInscription.tournament_id || "");
    const tournamentName = String(userInscription.tournament_name || "");
    const tournamentStartDate = String(userInscription.tournament_start_date || "");
    const tour = String(userInscription.tour || user.user_tour || "").toUpperCase();
    const tournamentLevel = String(userInscription.tournament_level || "").toUpperCase();
    const year = tournamentStartDate ? Number(tournamentStartDate.slice(0, 4)) : new Date().getFullYear();

    const bracketJson = loadTournamentJson(tour, tournamentId, year);
    const tournamentInfo = {
      current_tournament_bracket_id: tournamentId,
      current_tournament_bracket_name: tournamentName,
      tournament_start_date: tournamentStartDate,
      tour,
      tournament_level: tournamentLevel
    };

    if (!bracketJson) {
      return jsonResponse(200, {
        ok: true,
        authenticated: true,
        status: "BRACKET_NOT_READY",
        user,
        tournament: tournamentInfo,
        bracket: null,
        locked: false,
        message: "The first-round JSON is not available yet."
      });
    }

    const templateBracket = buildTemplateFromFirstRound(
      {
        current_tournament_bracket_id: tournamentId,
        current_tournament_bracket_name: tournamentName,
        tournament_start_date: tournamentStartDate,
        tour,
        tournament_level: tournamentLevel,
        event_id: bracketJson.event_id || null,
        event_year: bracketJson.event_year || year
      },
      bracketJson
    );

    const record = await getBracketRecord(user.user_id, tournamentId);

    if (record && record.user_current_tournament_bracket_proposition) {
      let storedBracket = null;
      try {
        storedBracket = typeof record.current_tournament_bracket === "string"
          ? JSON.parse(record.current_tournament_bracket)
          : record.current_tournament_bracket || null;
      } catch {
        storedBracket = null;
      }

      if (!storedBracket) {
        storedBracket = buildStoredBracketPayload(templateBracket, user, tournamentInfo, true);
      }

      return jsonResponse(200, {
        ok: true,
        authenticated: true,
        status: "LOCKED",
        user,
        tournament: tournamentInfo,
        bracket: storedBracket,
        locked: true,
        message: "Your bracket is locked."
      });
    }

    if (method === "POST" && String(body.action || "").toLowerCase() === "submit") {
      const submittedBracket = body.bracket || null;
      if (!submittedBracket) {
        return jsonResponse(400, { ok: false, error: "Missing bracket payload." });
      }

      const appliedBracket = buildStoredBracketPayload(templateBracket, user, tournamentInfo, true);

      const row = {
        user_id: user.user_id,
        user_name: user.user_name || null,
        user_world_rank: user.user_world_rank,
        user_tour: tour,
        user_country: user.user_country || null,
        current_tournament_bracket_id: tournamentId,
        current_tournament_bracket_name: tournamentName,
        current_tournament_bracket: appliedBracket,
        user_current_tournament_bracket_proposition: JSON.stringify(submittedBracket, null, 2)
      };

      const saved = await supabaseUpsert(BRACKET_TABLE, row, "user_id,current_tournament_bracket_id");

      return jsonResponse(200, {
        ok: true,
        status: "LOCKED",
        user,
        tournament: tournamentInfo,
        bracket: appliedBracket,
        bracket_record: saved,
        locked: true,
        message: "Bracket submitted successfully."
      });
    }

    return jsonResponse(200, {
      ok: true,
      authenticated: true,
      status: "READY",
      user,
      tournament: tournamentInfo,
      bracket: buildStoredBracketPayload(templateBracket, user, tournamentInfo, false),
      locked: false,
      message: "Bracket ready."
    });

  } catch (err) {
    return jsonResponse(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error."
    });
  }
};