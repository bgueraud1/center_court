// netlify/functions/inscriptions.js
const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPEN_JSON_PATH = path.join(process.cwd(), "docs/bracket/open_inscriptions.json");

// Adjust if your DB names differ
const USERS_TABLE = process.env.USERS_TABLE || "users";
const BRACKET_USERS_TABLE = process.env.BRACKET_USERS_TABLE || "bracket_users";
const INSCRIPTIONS_TABLE = process.env.INSCRIPTIONS_TABLE || "inscriptions";

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
        count: 0,
      },
      open_tournaments: [],
    };
  }
  return JSON.parse(fs.readFileSync(OPEN_JSON_PATH, "utf-8"));
}

function readBody(event) {
  if (!event.body) return {};
  try {
    return JSON.parse(event.body);
  } catch (_) {
    return {};
  }
}

function getHeader(headers, name) {
  const key = Object.keys(headers || {}).find((k) => k.toLowerCase() === name.toLowerCase());
  return key ? headers[key] : null;
}

/**
 * This function supports two ways to identify the user:
 * 1) x-user-id / x-user-name / x-user-tour / x-user-rank headers
 * 2) Authorization: Bearer <token> if you already have a verified auth flow
 *
 * For step 1, the headers path is enough and matches a "light integration" approach.
 */
function getCallerContext(event, body = {}) {
  const headers = event.headers || {};
  const user_id = getHeader(headers, "x-user-id") || body.user_id || null;
  const user_name = getHeader(headers, "x-user-name") || body.user_name || null;
  const user_tour = getHeader(headers, "x-user-tour") || body.user_tour || null;
  const user_country = getHeader(headers, "x-user-country") || body.user_country || null;
  const user_world_rank_raw = getHeader(headers, "x-user-rank") || body.user_world_rank || null;

  const user_world_rank = Number.parseInt(String(user_world_rank_raw || ""), 10);
  const world_rank = Number.isFinite(user_world_rank) ? user_world_rank : null;

  return {
    user_id,
    user_name,
    user_tour,
    user_country,
    user_world_rank: world_rank,
    access_token: null, // kept for later if you want JWT auth
  };
}

async function getDbUserProfile(userId) {
  if (!userId) return null;

  // Try bracket users first because that's where world rank lives in your description
  let { data: bracketRow, error: bracketErr } = await supabase
    .from(BRACKET_USERS_TABLE)
    .select("user_id,user_name,user_world_rank,user_tour,user_country")
    .eq("user_id", userId)
    .maybeSingle();

  if (!bracketErr && bracketRow) return bracketRow;

  // Fallback to users table
  const { data: userRow, error: userErr } = await supabase
    .from(USERS_TABLE)
    .select("id,pseudo,tour,country")
    .eq("id", userId)
    .maybeSingle();

  if (!userErr && userRow) {
    return {
      user_id: userRow.id,
      user_name: userRow.pseudo,
      user_tour: userRow.tour,
      user_country: userRow.country,
      user_world_rank: null,
    };
  }

  return null;
}

function normalizeCategory(category) {
  return String(category || "").trim().toUpperCase();
}

function isAtpRestricted(category, rank) {
  const c = normalizeCategory(category);
  const isChallengerOrFuture = c === "CH" || c === "FU" || c.includes("CHALLENGER") || c.includes("FUTURE");
  const isFuture = c === "FU" || c.includes("FUTURE");

  if (!Number.isFinite(rank)) {
    return { ok: true, reason: null };
  }

  if (isChallengerOrFuture && rank <= 50) {
    return { ok: false, reason: "ATP top 50 players cannot enter Challenger/Future events." };
  }
  if (isFuture && rank <= 200) {
    return { ok: false, reason: "ATP top 200 players cannot enter Future events." };
  }
  return { ok: true, reason: null };
}

async function loadWeekInscriptionRows(weekStart) {
  const { data, error } = await supabase
    .from(INSCRIPTIONS_TABLE)
    .select("tournament_id,user_id,user_name,user_world_rank,registration_week_start,tournament_name,tournament_num_players,tournament_start_date,tour,tournament_level")
    .eq("registration_week_start", weekStart);

  if (error) throw error;
  return Array.isArray(data) ? data : [];
}

function toKey(tour, tournamentId) {
  return `${String(tour || "").toUpperCase()}::${String(tournamentId)}`;
}

function buildCurrentWeekMap(rows) {
  const map = new Map();
  for (const row of rows) {
    map.set(toKey(row.tour || "", row.tournament_id), row);
  }
  return map;
}

function groupCountByTournament(rows) {
  const counts = new Map();
  for (const row of rows) {
    const key = toKey(row.tour || "", row.tournament_id);
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

exports.handler = async (event) => {
  try {
    const method = (event.httpMethod || "GET").toUpperCase();
    const openPayload = readOpenPayload();
    const body = method === "POST" ? readBody(event) : {};
    const ctx = getCallerContext(event, body);

    if (method === "GET") {
      if (!ctx.user_id) {
        return json(200, {
          ok: true,
          authenticated: false,
          open_payload: openPayload,
          user: null,
          already_registered: null,
          tournaments: (openPayload.open_tournaments || []).map((t) => ({
            ...t,
            registered_count: 0,
            slots_left: t.draw_size,
            eligible: false,
            eligibility_reason: "Not authenticated",
          })),
        });
      }

      const profile = await getDbUserProfile(ctx.user_id);
      const user = profile || {
        user_id: ctx.user_id,
        user_name: ctx.user_name,
        user_tour: ctx.user_tour,
        user_country: ctx.user_country,
        user_world_rank: ctx.user_world_rank,
      };

      const weekStart = openPayload?.registration_window?.open_date || null;
      const targetStartDate = openPayload?.registration_window?.target_start_date || null;
      const isOpenToday = Boolean(openPayload?.registration_window?.is_open_today);

      const allWeekRows = weekStart ? await loadWeekInscriptionRows(weekStart) : [];
      const counts = groupCountByTournament(allWeekRows);
      const userWeekRows = weekStart
        ? allWeekRows.filter((r) => String(r.user_id) === String(user.user_id))
        : [];

      const alreadyRegistered = userWeekRows[0] || null;
      const selectedTournamentKey = alreadyRegistered
        ? toKey(alreadyRegistered.tour || "", alreadyRegistered.tournament_id)
        : null;

      const tournaments = (openPayload.open_tournaments || [])
        .filter((t) => {
          // Only show tournaments that match the user's tour
          const userTour = String(user.user_tour || "").toUpperCase();
          return userTour ? String(t.tour || "").toUpperCase() === userTour : true;
        })
        .map((t) => {
          const key = toKey(t.tour, t.tournament_id);
          const registeredCount = counts.get(key) || 0;
          const slotsLeft = Math.max(0, (Number(t.draw_size) || 0) - registeredCount);

          let eligible = true;
          let eligibilityReason = null;

          const userTour = String(user.user_tour || "").toUpperCase();
          const category = normalizeCategory(t.category);
          const rank = Number.isFinite(user.user_world_rank) ? user.user_world_rank : null;

          if (alreadyRegistered && selectedTournamentKey !== key) {
            eligible = false;
            eligibilityReason = "You are already registered for this week.";
          } else if (slotsLeft <= 0) {
            eligible = false;
            eligibilityReason = "Tournament is full.";
          } else if (userTour && String(t.tour || "").toUpperCase() !== userTour) {
            eligible = false;
            eligibilityReason = "This tournament does not match your tour.";
          } else if (String(t.tour || "").toUpperCase() === "ATP") {
            const rule = isAtpRestricted(category, rank);
            eligible = rule.ok;
            eligibilityReason = rule.reason;
          }

          return {
            ...t,
            registered_count: registeredCount,
            slots_left: slotsLeft,
            eligible,
            eligibility_reason: eligibilityReason,
            selected: selectedTournamentKey === key,
          };
        });

      return json(200, {
        ok: true,
        authenticated: true,
        open_payload: openPayload,
        user,
        already_registered: alreadyRegistered,
        is_open_today: isOpenToday,
        target_start_date: targetStartDate,
        tournaments,
      });
    }

    if (method === "POST") {
      if (!ctx.user_id) {
        return json(401, { ok: false, error: "Not authenticated." });
      }

      const profile = await getDbUserProfile(ctx.user_id);
      const user = profile || {
        user_id: ctx.user_id,
        user_name: ctx.user_name,
        user_tour: ctx.user_tour,
        user_country: ctx.user_country,
        user_world_rank: ctx.user_world_rank,
      };

      const weekStart = openPayload?.registration_window?.open_date || null;
      const targetStartDate = openPayload?.registration_window?.target_start_date || null;
      const isOpenToday = Boolean(openPayload?.registration_window?.is_open_today);

      if (!isOpenToday) {
        return json(400, { ok: false, error: "Registrations are closed today." });
      }

      const tournamentId = String(body.tournament_id || "").trim();
      const tournamentTour = String(body.tour || "").trim().toUpperCase();
      const tournamentCategory = normalizeCategory(body.tournament_level || body.category);
      const tournamentName = String(body.tournament_name || "").trim();
      const tournamentNumPlayers = Number.parseInt(String(body.tournament_num_players || ""), 10) || null;
      const tournamentStartDate = String(body.tournament_start_date || targetStartDate || "").trim();

      const tournament = (openPayload.open_tournaments || []).find(
        (t) =>
          String(t.tournament_id) === tournamentId &&
          String(t.tour || "").toUpperCase() === tournamentTour
      );

      if (!tournament) {
        return json(400, { ok: false, error: "This tournament is not open for registration." });
      }

      const allWeekRows = weekStart ? await loadWeekInscriptionRows(weekStart) : [];
      const existingForUser = allWeekRows.find((r) => String(r.user_id) === String(user.user_id));
      if (existingForUser) {
        return json(400, {
          ok: false,
          error: "You are already registered for one tournament this week.",
          already_registered: existingForUser,
        });
      }

      const countForTournament = allWeekRows.filter(
        (r) => String(r.tournament_id) === tournamentId && String(r.tour || "").toUpperCase() === tournamentTour
      ).length;

      if (countForTournament >= (Number(tournament.draw_size) || 0)) {
        return json(400, { ok: false, error: "Tournament is full." });
      }

      const rank = Number.isFinite(user.user_world_rank) ? user.user_world_rank : null;
      if (tournamentTour === "ATP") {
        const rule = isAtpRestricted(tournament.category, rank);
        if (!rule.ok) {
          return json(403, { ok: false, error: rule.reason });
        }
      }

      const insertPayload = {
        tournament_name: tournamentName || tournament.tournament_name,
        tournament_id: tournamentId,
        tournament_num_players: tournamentNumPlayers || tournament.draw_size || null,
        user_id: user.user_id,
        user_name: user.user_name || ctx.user_name || null,
        user_world_rank: rank,
        registration_week_start: weekStart,
        tournament_start_date: tournamentStartDate,
        tour: tournamentTour,
        tournament_level: tournamentCategory,
      };

      const { data, error } = await supabase
        .from(INSCRIPTIONS_TABLE)
        .insert(insertPayload)
        .select("*")
        .single();

      if (error) {
        return json(500, {
          ok: false,
          error: error.message || "Database insert failed.",
        });
      }

      return json(200, {
        ok: true,
        inserted: data,
      });
    }

    return json(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    return json(500, {
      ok: false,
      error: err && err.message ? err.message : "Unexpected error.",
    });
  }
};