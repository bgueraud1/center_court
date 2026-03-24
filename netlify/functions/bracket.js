// netlify/functions/bracket.js
const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const USERS_TABLE = process.env.USERS_TABLE || "users";
const INSCRIPTIONS_TABLE = process.env.INSCRIPTIONS_TABLE || "inscriptions";
const BRACKET_TABLE = process.env.BRACKET_TABLE || "bracket";
const BRACKET_TOURNAMENTS_DIR = process.env.BRACKET_TOURNAMENTS_DIR || "docs/bracket/tournaments";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
});

function json(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
    body: JSON.stringify(body),
  };
}

function readBody(event) {
  if (!event.body) return {};
  try {
    return JSON.parse(event.body);
  } catch {
    return {};
  }
}

function getHeader(headers, name) {
  const key = Object.keys(headers || {}).find((k) => k.toLowerCase() === name.toLowerCase());
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
    user_world_rank: Number.isFinite(user_world_rank) ? user_world_rank : null,
  };
}

function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function safeString(v) {
  return v === undefined || v === null ? "" : String(v);
}

function asPlayer(id, name, country = null) {
  if (!id && !name) return null;
  return {
    player_id: safeString(id),
    player_name: safeString(name),
    player_country: country ? safeString(country) : null,
  };
}

function buildTemplateFromFirstRound(tournamentInfo, firstRoundJson) {
  const leafMatches = (firstRoundJson.matches || []).map((m, idx) => {
    const leftPlayer = asPlayer(m.winner_player_id, m.winner_player_name, m.winner_country);
    const rightPlayer = asPlayer(
      m.loser_player_id,
      m.loser_player_name,
      m.loser_country || m.loser_wountry || null
    );

    return {
      match_id: safeString(m.match_id || `MS${String(idx + 1).padStart(3, "0")}`),
      round_number: 1,
      left_child_match_id: null,
      right_child_match_id: null,
      left_player: leftPlayer,
      right_player: rightPlayer,
      winner: null,
      source: deepClone(m),
    };
  });

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
        winner: null,
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
    rounds,
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

        match.left_player = leftChild && leftChild.winner ? deepClone(leftChild.winner) : null;
        match.right_player = rightChild && rightChild.winner ? deepClone(rightChild.winner) : null;
      }

      const players = [match.left_player, match.right_player].filter(Boolean);

      if (players.length === 1) {
        match.winner = deepClone(players[0]);
      } else if (players.length === 2 && match.winner) {
        const ok = players.some((p) => String(p.player_id) === String(match.winner.player_id));
        if (!ok) match.winner = null;
      } else if (players.length === 0) {
        match.winner = null;
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
        winner_player_name: match.winner ? match.winner.player_name : null,
      });
    }
  }
  return rows;
}

function buildStoredBracketPayload(templateBracket, user, tournament, locked = false) {
  const bracket = deepClone(templateBracket);
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
      user_country: user.user_country,
    },
    tournament: {
      current_tournament_bracket_id: tournament.current_tournament_bracket_id,
      current_tournament_bracket_name: tournament.current_tournament_bracket_name,
      tournament_id: tournament.current_tournament_bracket_id,
      tournament_name: tournament.current_tournament_bracket_name,
      tournament_start_date: tournament.tournament_start_date,
      tour: tournament.tour,
      tournament_level: tournament.tournament_level,
    },
    rounds: bracket.rounds.map((round) => ({
      round_number: round.round_number,
      matches: round.matches.map((m) => ({
        match_id: m.match_id,
        round_number: m.round_number,
        left_child_match_id: m.left_child_match_id || null,
        right_child_match_id: m.right_child_match_id || null,
        left_player: m.left_player,
        right_player: m.right_player,
        winner_player_id: m.winner ? m.winner.player_id : null,
        winner_player_name: m.winner ? m.winner.player_name : null,
      })),
    })),
    matches_flat: flattenBracket(bracket),
  };
}

async function getDbUserProfile(userId) {
  if (!userId) return null;

  let { data: bracketRow, error: bracketErr } = await supabase
    .from(BRACKET_TABLE)
    .select("user_id,user_name,user_world_rank,user_tour,user_country")
    .eq("user_id", userId)
    .limit(1)
    .maybeSingle();

  if (!bracketErr && bracketRow) return bracketRow;

  const { data: userRow, error: userErr } = await supabase
    .from(USERS_TABLE)
    .select("id,pseudo,tour,country")
    .eq("id", userId)
    .limit(1)
    .maybeSingle();

  if (!userErr && userRow) {
    return {
      user_id: userRow.id,
      user_name: userRow.pseudo,
      user_world_rank: null,
      user_tour: userRow.tour,
      user_country: userRow.country,
    };
  }

  return null;
}

async function getLatestInscriptionForUser(userId) {
  const { data, error } = await supabase
    .from(INSCRIPTIONS_TABLE)
    .select("*")
    .eq("user_id", userId)
    .order("tournament_start_date", { ascending: false, nullsFirst: false })
    .order("registration_week_start", { ascending: false, nullsFirst: false })
    .limit(1);

  if (error) throw error;
  return Array.isArray(data) && data.length ? data[0] : null;
}

async function getBracketRecord(userId, tournamentId) {
  const { data, error } = await supabase
    .from(BRACKET_TABLE)
    .select("*")
    .eq("user_id", userId)
    .eq("current_tournament_bracket_id", String(tournamentId))
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

function tournamentFilePath(tour, tournamentId, year) {
  const fileName = `${String(tour).toLowerCase()}_${String(tournamentId)}_${String(year)}_temporary.json`;
  return path.join(process.cwd(), BRACKET_TOURNAMENTS_DIR, fileName);
}

function loadTournamentJson(tour, tournamentId, year) {
  const filePath = tournamentFilePath(tour, tournamentId, year);
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, "utf-8"));
}

function validateAndApplySubmission(templateBracket, submittedBracket) {
  const bracket = deepClone(templateBracket);
  const submittedMap = new Map();

  const submittedRounds = submittedBracket && Array.isArray(submittedBracket.rounds) ? submittedBracket.rounds : [];
  for (const round of submittedRounds) {
    for (const match of round.matches || []) {
      submittedMap.set(String(match.match_id), match);
    }
  }

  const lookup = buildMatchLookup(bracket);

  for (const round of bracket.rounds) {
    for (const match of round.matches) {
      if (match.round_number > 1) {
        const leftChild = match.left_child_match_id ? lookup.get(match.left_child_match_id) : null;
        const rightChild = match.right_child_match_id ? lookup.get(match.right_child_match_id) : null;
        match.left_player = leftChild && leftChild.winner ? deepClone(leftChild.winner) : null;
        match.right_player = rightChild && rightChild.winner ? deepClone(rightChild.winner) : null;
      }

      const posted = submittedMap.get(String(match.match_id));
      const players = [match.left_player, match.right_player].filter(Boolean);

      if (players.length === 2) {
        if (!posted || !posted.winner_player_id) {
          throw new Error(`Bracket incomplet: ${match.match_id} n'a pas de gagnant.`);
        }
        const wanted = String(posted.winner_player_id);
        const chosen = players.find((p) => String(p.player_id) === wanted);
        if (!chosen) {
          throw new Error(`Choix invalide sur ${match.match_id}.`);
        }
        match.winner = deepClone(chosen);
      } else if (players.length === 1) {
        match.winner = deepClone(players[0]);
      } else {
        match.winner = null;
      }
    }
  }

  const flat = flattenBracket(bracket);
  const incomplete = flat.filter((m) => {
    const hasTwoPlayers = Boolean(m.left_player_id && m.right_player_id);
    const hasWinner = Boolean(m.winner_player_id);
    return hasTwoPlayers && !hasWinner;
  });

  if (incomplete.length) {
    throw new Error("Le bracket n'est pas entièrement rempli.");
  }

  const finalMatch = flat[flat.length - 1];
  if (!finalMatch || !finalMatch.winner_player_id) {
    throw new Error("Le bracket final n'est pas complété.");
  }

  return bracket;
}

exports.handler = async (event) => {
  try {
    const method = (event.httpMethod || "GET").toUpperCase();
    const body = method === "POST" ? readBody(event) : {};
    const ctx = getCallerContext(event, body);

    if (!ctx.user_id) {
      return json(200, {
        ok: true,
        status: "NOT_AUTHENTICATED",
        authenticated: false,
        user: null,
        tournament: null,
        bracket: null,
        locked: false,
        message: "You must log in to play.",
      });
    }

    const profile = await getDbUserProfile(ctx.user_id);
    const user = profile || {
      user_id: ctx.user_id,
      user_name: ctx.user_name,
      user_world_rank: ctx.user_world_rank,
      user_tour: ctx.user_tour,
      user_country: ctx.user_country,
    };

    const inscription = await getLatestInscriptionForUser(ctx.user_id);
    if (!inscription) {
      return json(200, {
        ok: true,
        status: "NOT_REGISTERED",
        authenticated: true,
        user,
        tournament: null,
        bracket: null,
        locked: false,
        message: "You are not registered for any tournament this week.",
      });
    }

    const tournamentId = String(inscription.tournament_id || "");
    const tournamentName = String(inscription.tournament_name || "");
    const tournamentStartDate = String(inscription.tournament_start_date || "");
    const tour = String(inscription.tour || user.user_tour || "").toUpperCase();
    const tournamentLevel = String(inscription.tournament_level || "").toUpperCase();
    const year = tournamentStartDate ? Number(tournamentStartDate.slice(0, 4)) : new Date().getFullYear();

    const bracketJson = loadTournamentJson(tour, tournamentId, year);
    if (!bracketJson) {
      return json(200, {
        ok: true,
        status: "BRACKET_NOT_READY",
        authenticated: true,
        user,
        tournament: {
          current_tournament_bracket_id: tournamentId,
          current_tournament_bracket_name: tournamentName,
          tournament_start_date: tournamentStartDate,
          tour,
          tournament_level: tournamentLevel,
        },
        bracket: null,
        locked: false,
        message: "The first-round JSON is not available yet.",
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
        event_year: bracketJson.event_year || year,
      },
      bracketJson
    );

    const record = await getBracketRecord(ctx.user_id, tournamentId);

    if (record && record.user_current_tournament_bracket_proposition) {
      let storedBracket = null;
      try {
        storedBracket =
          typeof record.current_tournament_bracket === "string"
            ? JSON.parse(record.current_tournament_bracket)
            : record.current_tournament_bracket || null;
      } catch {
        storedBracket = null;
      }

      if (!storedBracket) {
        storedBracket = buildStoredBracketPayload(templateBracket, user, {
          current_tournament_bracket_id: tournamentId,
          current_tournament_bracket_name: tournamentName,
          tournament_start_date: tournamentStartDate,
          tour,
          tournament_level: tournamentLevel,
        }, true);
      }

      return json(200, {
        ok: true,
        status: "LOCKED",
        authenticated: true,
        user,
        tournament: {
          current_tournament_bracket_id: tournamentId,
          current_tournament_bracket_name: tournamentName,
          tournament_start_date: tournamentStartDate,
          tour,
          tournament_level: tournamentLevel,
        },
        bracket: storedBracket,
        locked: true,
        message: "Your bracket is locked.",
        bracket_record: record,
      });
    }

    if (method === "POST") {
      const submittedBracket = body.bracket || body.current_tournament_bracket || null;
      if (!submittedBracket) {
        return json(400, { ok: false, error: "Missing bracket payload." });
      }

      if (record && record.user_current_tournament_bracket_proposition) {
        return json(409, {
          ok: false,
          error: "This bracket is already locked and cannot be changed.",
        });
      }

      const appliedBracket = validateAndApplySubmission(templateBracket, submittedBracket);

      const storedPayload = buildStoredBracketPayload(appliedBracket, user, {
        current_tournament_bracket_id: tournamentId,
        current_tournament_bracket_name: tournamentName,
        tournament_start_date: tournamentStartDate,
        tour,
        tournament_level: tournamentLevel,
      }, true);

      const row = {
        user_id: user.user_id,
        user_name: user.user_name || null,
        user_world_rank: user.user_world_rank,
        user_tour: tour,
        user_country: user.user_country || null,
        current_tournament_bracket_id: tournamentId,
        current_tournament_bracket_name: tournamentName,
        current_tournament_bracket: storedPayload,
        user_current_tournament_bracket_proposition: JSON.stringify(storedPayload, null, 2),
      };

      const { data, error } = await supabase
        .from(BRACKET_TABLE)
        .upsert(row, { onConflict: "user_id,current_tournament_bracket_id" })
        .select("*")
        .single();

      if (error) {
        return json(500, {
          ok: false,
          error: error.message || "Failed to save bracket.",
        });
      }

      return json(200, {
        ok: true,
        status: "SAVED",
        bracket_record: data,
        bracket: storedPayload,
        locked: true,
      });
    }

    const existingBracket = record && record.current_tournament_bracket
      ? (typeof record.current_tournament_bracket === "string"
          ? JSON.parse(record.current_tournament_bracket)
          : record.current_tournament_bracket)
      : buildStoredBracketPayload(templateBracket, user, {
          current_tournament_bracket_id: tournamentId,
          current_tournament_bracket_name: tournamentName,
          tournament_start_date: tournamentStartDate,
          tour,
          tournament_level: tournamentLevel,
        }, false);

    return json(200, {
      ok: true,
      status: "READY",
      authenticated: true,
      user,
      tournament: {
        current_tournament_bracket_id: tournamentId,
        current_tournament_bracket_name: tournamentName,
        tournament_start_date: tournamentStartDate,
        tour,
        tournament_level: tournamentLevel,
      },
      bracket: existingBracket,
      locked: false,
      message: "Bracket ready.",
      bracket_record: record || null,
    });
  } catch (err) {
    return json(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error.",
    });
  }
};