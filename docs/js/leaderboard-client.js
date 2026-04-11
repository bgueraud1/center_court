(() => {
  "use strict";

  const ENDPOINT = "/.netlify/functions/leaderboard_user";
  const TIME_ZONE = "Europe/Paris";

  const state = {
    loading: true,
    error: null,
    activeTour: "ATP",
    activeGame: "all",
    activePeriod: "week",
    scores: [],
    users: [],
    gameOptions: [],
    leaderboard: [],
    filteredScoreCount: 0
  };

  const $ = (id) => document.getElementById(id);

  function escapeHtml(value) {
    if (value === null || value === undefined) return "";
    return String(value).replace(/[&<>"']/g, (m) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      "\"": "&quot;",
      "'": "&#39;"
    })[m]);
  }

  function parseJsonMaybeDeep(value, depth = 4) {
    let current = value;
    for (let i = 0; i < depth; i++) {
      if (current === null || current === undefined) return current;
      if (typeof current === "object") return current;
      if (typeof current !== "string") return current;

      const trimmed = current.trim();
      if (!trimmed) return null;

      try {
        current = JSON.parse(trimmed);
      } catch {
        return current;
      }
    }
    return current;
  }

  function asArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function normalizeTour(value) {
    if (!value) return null;
    const s = String(value).toUpperCase();
    if (s.includes("ATP")) return "ATP";
    if (s.includes("WTA")) return "WTA";
    return null;
  }

  function prettyLabel(value) {
    if (!value) return "Unknown";
    return String(value)
      .split(/[_\-\s]+/)
      .filter(Boolean)
      .map((part) => {
        const upper = part.toUpperCase();
        if (["ATP", "WTA", "H2H", "ID", "API", "JSON", "URL"].includes(upper)) return upper;
        if (/^\d+$/.test(part)) return part;
        return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
      })
      .join(" ");
  }

  function parseDate(value) {
    if (!value) return null;
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  function getRowDate(row) {
    const d1 = parseDate(row?.created_at);
    if (d1) return d1;
    if (row?.created_day) {
      const d2 = parseDate(`${row.created_day}T00:00:00Z`);
      if (d2) return d2;
    }
    return null;
  }

  function getTimeZoneOffsetMs(date, timeZone = TIME_ZONE) {
    const dtf = new Intl.DateTimeFormat("en-US", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    });

    const parts = {};
    for (const p of dtf.formatToParts(date)) {
      if (p.type !== "literal") parts[p.type] = p.value;
    }

    const asUTC = Date.UTC(
      Number(parts.year),
      Number(parts.month) - 1,
      Number(parts.day),
      Number(parts.hour),
      Number(parts.minute),
      Number(parts.second)
    );

    return asUTC - date.getTime();
  }

  function zonedTimeToUtc(year, month, day, hour = 0, minute = 0, second = 0, timeZone = TIME_ZONE) {
    const utcGuess = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
    const offsetMs = getTimeZoneOffsetMs(utcGuess, timeZone);
    return new Date(utcGuess.getTime() - offsetMs);
  }

  function startOfWeekMonday(date = new Date(), timeZone = TIME_ZONE) {
    const dtf = new Intl.DateTimeFormat("en-GB", {
      timeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      weekday: "short"
    });

    const parts = {};
    for (const p of dtf.formatToParts(date)) {
      if (p.type !== "literal") parts[p.type] = p.value;
    }

    const weekdayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
    const dow = weekdayMap[String(parts.weekday || "").slice(0, 3)] ?? 0;

    const localDate = new Date(Date.UTC(
      Number(parts.year),
      Number(parts.month) - 1,
      Number(parts.day)
    ));

    localDate.setUTCDate(localDate.getUTCDate() - ((dow + 6) % 7));

    return zonedTimeToUtc(
      localDate.getUTCFullYear(),
      localDate.getUTCMonth() + 1,
      localDate.getUTCDate(),
      0,
      0,
      0,
      timeZone
    );
  }

  function getPeriodCutoff(period) {
    const now = new Date();
    if (period === "week") return startOfWeekMonday(now, TIME_ZONE);
    if (period === "52w") return new Date(now.getTime() - 52 * 7 * 24 * 60 * 60 * 1000);
    return null;
  }

  function codeToFlag(code) {
    const clean = String(code || "").trim().toUpperCase();
    if (!/^[A-Z]{2}$/.test(clean)) return "";
    return String.fromCodePoint(...clean.split("").map((c) => 127397 + c.charCodeAt(0)));
  }

  function countryCodeFromName(name) {
    const raw = String(name || "").trim();
    if (!raw) return "";

    const exact = {
      France: "FR",
      Russia: "RU",
      Spain: "ES",
      Italy: "IT",
      Germany: "DE",
      Belgium: "BE",
      Netherlands: "NL",
      Austria: "AT",
      Switzerland: "CH",
      Sweden: "SE",
      Norway: "NO",
      Denmark: "DK",
      Finland: "FI",
      Poland: "PL",
      Ukraine: "UA",
      Greece: "GR",
      Croatia: "HR",
      Serbia: "RS",
      Czechia: "CZ",
      "Czech Republic": "CZ",
      Portugal: "PT",
      Brazil: "BR",
      Argentina: "AR",
      Canada: "CA",
      Australia: "AU",
      Japan: "JP",
      China: "CN",
      "United States": "US",
      USA: "US",
      US: "US",
      "United Kingdom": "GB",
      UK: "GB",
      England: "GB"
    };

    if (exact[raw]) return exact[raw];
    if (/^[A-Z]{2}$/.test(raw)) return raw;
    return "";
  }

  function formatCountry(value) {
    if (!value) return "—";
    const code = countryCodeFromName(String(value));
    const flag = code ? codeToFlag(code) : "";
    return flag ? `${flag} ${value}` : value;
  }

  function buildUserIndex(users) {
    const map = new Map();

    for (const user of users || []) {
      if (!user) continue;
      if (user.id) map.set(`id:${user.id}`, user);
      if (user.pseudo) map.set(`pseudo:${String(user.pseudo).trim().toLowerCase()}`, user);
    }

    return map;
  }

  function resolveUserForScore(row, userIndex) {
    if (!row) return null;

    if (row.user_id) {
      const byId = userIndex.get(`id:${row.user_id}`);
      if (byId) return byId;
    }

    if (row.pseudo) {
      const byPseudo = userIndex.get(`pseudo:${String(row.pseudo).trim().toLowerCase()}`);
      if (byPseudo) return byPseudo;
    }

    return null;
  }

  function resolveRowTour(row, user) {
    const meta = parseJsonMaybeDeep(row?.meta);
    const fromMeta = normalizeTour(meta?.tour);
    if (fromMeta) return fromMeta;

    const fromMode = normalizeTour(row?.mode);
    if (fromMode) return fromMode;

    const fromUser = normalizeTour(user?.tour);
    if (fromUser) return fromUser;

    return null;
  }

  function rowPoints(row) {
    const n = Number(row?.points);
    return Number.isFinite(n) ? n : 0;
  }

  function scoreIdentity(row) {
    if (row?.user_id) return `id:${row.user_id}`;
    if (row?.pseudo) return `pseudo:${String(row.pseudo).trim().toLowerCase()}`;
    if (row?.anon_id) return `anon:${row.anon_id}`;
    return `row:${row?.id || `${Date.now()}-${Math.random().toString(16).slice(2)}`}`;
  }

  function computeLeaderboard(scores, users, tour, game, period) {
    const userIndex = buildUserIndex(users);
    const cutoff = getPeriodCutoff(period);
    const rows = [];

    for (const score of scores || []) {
      const user = resolveUserForScore(score, userIndex);
      const rowTour = resolveRowTour(score, user);

      if (tour && rowTour !== tour) continue;
      if (game !== "all" && String(score.game_id || "") !== String(game)) continue;

      const d = getRowDate(score);
      if (cutoff && d && d < cutoff) continue;
      if (cutoff && !d) continue;

      rows.push({
        score,
        user,
        tour: rowTour || "—",
        key: scoreIdentity(score)
      });
    }

    const groups = new Map();

    for (const item of rows) {
      const { score, user, tour: rowTour } = item;
      const userKey = user?.id ? `id:${user.id}` : item.key;

      if (!groups.has(userKey)) {
        groups.set(userKey, {
          user_id: user?.id || score.user_id || null,
          pseudo: user?.pseudo || score.pseudo || "Unknown",
          country: user?.country || "—",
          league: user?.league || "—",
          tour: user?.tour || rowTour || "—",
          points: 0,
          scores: 0
        });
      }

      const entry = groups.get(userKey);
      entry.points += rowPoints(score);
      entry.scores += 1;

      if ((!entry.country || entry.country === "—") && user?.country) entry.country = user.country;
      if ((!entry.league || entry.league === "—") && user?.league) entry.league = user.league;
      if ((!entry.tour || entry.tour === "—") && rowTour) entry.tour = rowTour;
    }

    return Array.from(groups.values())
      .sort((a, b) => {
        if (b.points !== a.points) return b.points - a.points;
        if (b.scores !== a.scores) return b.scores - a.scores;
        return String(a.pseudo || "").localeCompare(String(b.pseudo || ""));
      })
      .map((entry, index) => ({
        rank: index + 1,
        ...entry
      }));
  }

  function countMatchingScores(scores, users, tour, game, period) {
    const userIndex = buildUserIndex(users);
    const cutoff = getPeriodCutoff(period);
    let count = 0;

    for (const score of scores || []) {
      const user = resolveUserForScore(score, userIndex);
      const rowTour = resolveRowTour(score, user);
      if (tour && rowTour !== tour) continue;
      if (game !== "all" && String(score.game_id || "") !== String(game)) continue;

      const d = getRowDate(score);
      if (cutoff && d && d < cutoff) continue;
      if (cutoff && !d) continue;

      count += 1;
    }

    return count;
  }

  function renderGameOptions(scores) {
    const select = $("gameSelect");
    const options = new Map();

    for (const row of scores || []) {
      if (!row?.game_id) continue;
      const key = String(row.game_id);
      if (!options.has(key)) options.set(key, prettyLabel(key));
    }

    state.gameOptions = Array.from(options.entries())
      .sort((a, b) => a[1].localeCompare(b[1]))
      .map(([value, label]) => ({ value, label }));

    const current = state.activeGame;
    select.innerHTML = `<option value="all">All games</option>` +
      state.gameOptions.map((opt) => `<option value="${escapeHtml(opt.value)}">${escapeHtml(opt.label)}</option>`).join("");

    if ([...select.options].some((opt) => opt.value === current)) {
      select.value = current;
    } else {
      select.value = "all";
      state.activeGame = "all";
    }
  }

  function setStatus(text) {
    $("statusLabel").textContent = text;
  }

  function updateButtons() {
    $("btnATP").classList.toggle("active", state.activeTour === "ATP");
    $("btnWTA").classList.toggle("active", state.activeTour === "WTA");
    $("btnWeek").classList.toggle("active", state.activePeriod === "week");
    $("btn52w").classList.toggle("active", state.activePeriod === "52w");
    $("btnAll").classList.toggle("active", state.activePeriod === "all");
    $("gameSelect").value = state.activeGame;
  }

  function renderLeaderboard() {
    const body = $("tbl").querySelector("tbody");
    body.innerHTML = "";

    const rows = state.leaderboard;

    $("chipTour").textContent = state.activeTour;
    $("chipGame").textContent = state.activeGame === "all" ? "All games" : prettyLabel(state.activeGame);
    $("chipPeriod").textContent = state.activePeriod === "week" ? "Week" : state.activePeriod === "52w" ? "52 weeks" : "All time";

    $("titleView").textContent = `${state.activeTour} leaderboard`;
    $("subtitleView").textContent = `${$("chipPeriod").textContent} · ${$("chipGame").textContent} · Week starts on Monday at 00:00 (${TIME_ZONE})`;
    $("statusView").textContent = state.loading ? "Loading…" : `${rows.length} player${rows.length > 1 ? "s" : ""} ranked`;

    $("chipPlayers").textContent = String(rows.length);
    $("chipScores").textContent = String(state.filteredScoreCount || 0);

    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="7" class="empty">No scores match the current filters.</td></tr>`;
      return;
    }

    for (const row of rows) {
      const tr = document.createElement("tr");
      tr.className = state.activeTour === "ATP" ? "atp" : "wta";
      tr.innerHTML = `
        <td class="rank">${row.rank}</td>
        <td class="pseudo">${escapeHtml(row.pseudo)}</td>
        <td class="country">${escapeHtml(formatCountry(row.country))}</td>
        <td class="league">${escapeHtml(row.league || "—")}</td>
        <td class="tour">${escapeHtml(row.tour || "—")}</td>
        <td class="total">${escapeHtml(String(row.points))}</td>
        <td class="small">${escapeHtml(String(row.scores))}</td>
      `;
      body.appendChild(tr);
    }
  }

  function refresh() {
    state.leaderboard = computeLeaderboard(
      state.scores,
      state.users,
      state.activeTour,
      state.activeGame,
      state.activePeriod
    );

    state.filteredScoreCount = countMatchingScores(
      state.scores,
      state.users,
      state.activeTour,
      state.activeGame,
      state.activePeriod
    );

    updateButtons();
    renderLeaderboard();
  }

  async function loadData() {
    try {
      state.loading = true;
      setStatus("Loading…");
      $("statusView").textContent = "Reading Supabase tables…";

      const res = await fetch(ENDPOINT, { cache: "no-store" });
      const data = await res.json();

      if (!res.ok || !data || !data.ok) {
        throw new Error((data && (data.error || data.message)) || `HTTP ${res.status}`);
      }

      state.scores = asArray(data.scores);
      state.users = asArray(data.users);

      renderGameOptions(state.scores);
      state.loading = false;
      state.error = null;

      setStatus(`Loaded · ${state.scores.length} scores`);
      refresh();
    } catch (err) {
      console.error(err);
      state.loading = false;
      state.error = err;
      setStatus("Error");

      const body = $("tbl").querySelector("tbody");
      body.innerHTML = `<tr><td colspan="7" class="error">${escapeHtml(err.message || "Unable to load leaderboard.")}</td></tr>`;
      $("titleView").textContent = "Leaderboard unavailable";
      $("subtitleView").textContent = "Could not read Supabase tables.";
      $("statusView").textContent = "Error";
    }
  }

  function bindUi() {
    $("btnATP").addEventListener("click", () => {
      state.activeTour = "ATP";
      refresh();
    });

    $("btnWTA").addEventListener("click", () => {
      state.activeTour = "WTA";
      refresh();
    });

    $("btnWeek").addEventListener("click", () => {
      state.activePeriod = "week";
      refresh();
    });

    $("btn52w").addEventListener("click", () => {
      state.activePeriod = "52w";
      refresh();
    });

    $("btnAll").addEventListener("click", () => {
      state.activePeriod = "all";
      refresh();
    });

    $("gameSelect").addEventListener("change", (e) => {
      state.activeGame = e.target.value || "all";
      refresh();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    bindUi();
    loadData();
  });
})();