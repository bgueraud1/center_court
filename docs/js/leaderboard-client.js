/* /js/leaderboard-client.js
   Leaderboard client that fetches data from a server-side GET endpoint.
   It prefers raw data: { ok:true, users:[...], scores:[...] }.
   It can also fallback to { ok:true, leaderboard:[...] }.
*/
(() => {
  'use strict';

  const CFG = window.LEADERBOARD_CONFIG || {};
  const ENDPOINTS = Array.isArray(CFG.endpoints) && CFG.endpoints.length
    ? CFG.endpoints
    : [
        CFG.endpoint,
        '/.netlify/functions/leaderboard',
        '/.netlify/functions/leaderboard-data',
        '/.netlify/functions/users-leaderboard'
      ].filter(Boolean);

  const DEFAULT_SCOPE = 'league';
  const DEFAULT_PERIOD = 'week';
  const DEFAULT_TOUR = 'ATP';

  const els = {
    tbl: document.getElementById('tbl'),
    tbody: document.querySelector('#tbl tbody'),
    titleView: document.getElementById('titleView'),
    subtitleView: document.getElementById('subtitleView'),
    statusView: document.getElementById('statusView'),
    chipTour: document.getElementById('chipTour'),
    chipGame: document.getElementById('chipGame'),
    chipPeriod: document.getElementById('chipPeriod'),
    chipPlayers: document.getElementById('chipPlayers'),
    chipScores: document.getElementById('chipScores'),
    sessionLabel: document.getElementById('sessionLabel'),
    sessionChip: document.getElementById('sessionChip'),
    gameSelect: document.getElementById('gameSelect'),
    btnATP: document.getElementById('btnATP'),
    btnWTA: document.getElementById('btnWTA'),
    btnWeek: document.getElementById('btnWeek'),
    btn52w: document.getElementById('btn52w'),
    btnAll: document.getElementById('btnAll'),
    toast: document.getElementById('lb-toast')
  };

  let users = [];
  let scores = [];
  let serverLeaderboard = [];
  let selectedTour = DEFAULT_TOUR;
  let selectedGame = 'all';
  let selectedPeriod = DEFAULT_PERIOD;
  let selectedScope = DEFAULT_SCOPE;
  let gameList = [];
  let loading = false;

  const usersById = new Map();
  const usersByPseudo = new Map();

  function fmt(n) {
    const num = Number(n || 0);
    return Number.isFinite(num) ? num.toLocaleString() : '0';
  }

  function escapeHtml(s) {
    if (s === undefined || s === null) return '';
    return String(s).replace(/[&<>"']/g, m => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#39;'
    }[m]));
  }

  function showToast(msg, duration = 1800) {
    if (!els.toast) return;
    els.toast.textContent = String(msg);
    els.toast.style.display = 'block';
    els.toast.style.opacity = '1';
    clearTimeout(showToast._t1);
    clearTimeout(showToast._t2);
    showToast._t1 = setTimeout(() => {
      els.toast.style.transition = 'opacity .2s ease';
      els.toast.style.opacity = '0';
      showToast._t2 = setTimeout(() => {
        els.toast.style.display = 'none';
      }, 240);
    }, duration);
  }

  function getSession() {
    try {
      if (window.TA_AUTH && typeof window.TA_AUTH.getSession === 'function') {
        return window.TA_AUTH.getSession();
      }
    } catch (e) {}

    try {
      const raw = localStorage.getItem('ta_session_v1');
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      return null;
    }
  }

  function buildAuthHeaders() {
    const s = getSession();
    const headers = {};
    if (s && (s.id || s.pseudo || s.tour || s.country || s.league)) {
      if (s.id) headers['x-user-id'] = String(s.id);
      if (s.pseudo) headers['x-user-name'] = String(s.pseudo);
      if (s.tour) headers['x-user-tour'] = String(s.tour);
      if (s.country) headers['x-user-country'] = String(s.country);
      if (s.league) headers['x-user-league'] = String(s.league);
    }
    return headers;
  }

  function updateSessionChip() {
    if (!els.sessionLabel) return;
    const s = getSession();
    if (s && (s.pseudo || s.username || s.id)) {
      const name = s.pseudo || s.username || s.name || s.id;
      els.sessionLabel.textContent = `Connecté : ${name}`;
      if (els.sessionChip) els.sessionChip.title = `Utilisateur local : ${name}`;
    } else {
      els.sessionLabel.textContent = 'Global view';
      if (els.sessionChip) els.sessionChip.title = 'Aucun utilisateur local détecté';
    }
  }

  function setActiveButtons() {
    if (els.btnATP) els.btnATP.classList.toggle('active', selectedTour === 'ATP');
    if (els.btnWTA) els.btnWTA.classList.toggle('active', selectedTour === 'WTA');
    if (els.btnWeek) els.btnWeek.classList.toggle('active', selectedPeriod === 'week');
    if (els.btn52w) els.btn52w.classList.toggle('active', selectedPeriod === '52w');
    if (els.btnAll) els.btnAll.classList.toggle('active', selectedPeriod === 'all');
  }

  function periodLabel(period) {
    if (period === 'week') return 'Semaine';
    if (period === '52w') return '52 semaines';
    return 'All time';
  }

  function periodCutoff(period) {
    if (period === 'all') return null;
    const d = new Date();
    if (period === 'week') d.setDate(d.getDate() - 7);
    if (period === '52w') d.setDate(d.getDate() - 364);
    return d;
  }

  function parseRowDate(row) {
    const raw = row?.created_at || row?.created_day || null;
    if (!raw) return null;
    if (typeof raw === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(raw)) {
      return new Date(`${raw}T00:00:00Z`);
    }
    const d = new Date(raw);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  function normalizeUsers(rawUsers) {
    return (Array.isArray(rawUsers) ? rawUsers : []).map(u => ({
      id: String(u.id || u.user_id || ''),
      user_id: String(u.user_id || u.id || ''),
      pseudo: String(u.pseudo || u.username || u.name || ''),
      league: String(u.league || '—'),
      league_id: u.league_id !== undefined && u.league_id !== null ? String(u.league_id) : '',
      tour: String(u.tour || '').toUpperCase(),
      country: String(u.country || '')
    })).filter(u => u.user_id || u.id || u.pseudo);
  }

  function normalizeScores(rawScores) {
    return (Array.isArray(rawScores) ? rawScores : []).map(s => ({
      id: String(s.id || ''),
      user_id: s.user_id === null || s.user_id === undefined ? '' : String(s.user_id),
      pseudo: String(s.pseudo || ''),
      game_id: String(s.game_id || ''),
      points: Number(s.points || 0),
      meta: s.meta ?? null,
      created_at: s.created_at ?? null,
      mode: String(s.mode || ''),
      created_day: s.created_day ?? null,
      anon_id: String(s.anon_id || '')
    }));
  }

  function extractArraysFromPayload(payload) {
    if (!payload) return { users: [], scores: [], leaderboard: [] };

    const root = payload.data && typeof payload.data === 'object' ? payload.data : payload;
    const usersCandidates = [root.users, root.user_rows, root.userList, root.players, root.profiles];
    const scoresCandidates = [root.scores, root.score_rows, root.rows, root.entries];
    const leaderboardCandidates = [root.leaderboard, root.board, root.results];

    let usersRaw = [];
    for (const candidate of usersCandidates) {
      if (Array.isArray(candidate)) { usersRaw = candidate; break; }
    }

    let scoresRaw = [];
    for (const candidate of scoresCandidates) {
      if (Array.isArray(candidate)) { scoresRaw = candidate; break; }
    }

    let leaderboardRaw = [];
    for (const candidate of leaderboardCandidates) {
      if (Array.isArray(candidate)) { leaderboardRaw = candidate; break; }
    }

    return { users: usersRaw, scores: scoresRaw, leaderboard: leaderboardRaw };
  }

  function uniqueGamesFromScores(rows) {
    const set = new Set();
    for (const r of rows || []) {
      const g = String(r.game_id || '').trim();
      if (!g) continue;
      set.add(g);
    }
    const preferred = ['guess_player', 'guess_player_h2h', 'gill_the_grid', 'daily_winners_map', 'daily_city_guess', 'daily_country_guess'];
    const rest = Array.from(set).filter(g => !preferred.includes(g)).sort((a, b) => a.localeCompare(b));
    return preferred.filter(g => set.has(g)).concat(rest);
  }

  function buildGameSelectOptions() {
    if (!els.gameSelect) return;
    const current = selectedGame;
    const options = ['<option value="all">Tous les jeux</option>']
      .concat(gameList.map(g => `<option value="${escapeHtml(g)}">${escapeHtml(g)}</option>`));
    els.gameSelect.innerHTML = options.join('');
    els.gameSelect.value = current && (current === 'all' || gameList.includes(current)) ? current : 'all';
  }

  function rebuildUsersIndex() {
    usersById.clear();
    usersByPseudo.clear();

    for (const u of users) {
      const key = String(u.user_id || u.id || '');
      if (key) usersById.set(key, u);

      const pseudoKey = String(u.pseudo || '').trim().toLowerCase();
      if (pseudoKey && !usersByPseudo.has(pseudoKey)) usersByPseudo.set(pseudoKey, u);
    }
  }

  function inferTourFromRow(row) {
    const fromUser = String(row?.tour || '').toUpperCase();
    if (fromUser === 'ATP' || fromUser === 'WTA') return fromUser;

    const mode = String(row?.mode || '').toUpperCase();
    if (mode.startsWith('ATP')) return 'ATP';
    if (mode.startsWith('WTA')) return 'WTA';
    return '';
  }

  function resolveUserForScore(row) {
    if (!row) return null;

    const byId = row.user_id ? usersById.get(String(row.user_id)) : null;
    if (byId) return byId;

    const pseudoKey = String(row.pseudo || '').trim().toLowerCase();
    if (pseudoKey && usersByPseudo.has(pseudoKey)) return usersByPseudo.get(pseudoKey);

    return null;
  }

  function filterScores() {
    const cutoff = periodCutoff(selectedPeriod);

    return scores.filter(row => {
      const user = resolveUserForScore(row);
      const inferredTour = user?.tour || inferTourFromRow(row);

      if (selectedTour && inferredTour && inferredTour !== selectedTour) return false;
      if (selectedTour && !inferredTour && user && String(user.tour || '').toUpperCase() !== selectedTour) return false;

      if (selectedGame !== 'all' && String(row.game_id || '') !== selectedGame) return false;

      if (cutoff) {
        const d = parseRowDate(row);
        if (!d || d < cutoff) return false;
      }

      return true;
    });
  }

  function aggregateLeaderboard(filteredScores) {
    const map = new Map();

    for (const row of filteredScores) {
      const user = resolveUserForScore(row);
      const key = user?.user_id || user?.id || row.user_id || `pseudo:${String(row.pseudo || '').trim().toLowerCase()}`;
      if (!key) continue;

      if (!map.has(key)) {
        map.set(key, {
          user_id: String(user?.user_id || user?.id || row.user_id || ''),
          pseudo: user?.pseudo || row.pseudo || key,
          league: user?.league || '—',
          league_id: user?.league_id || '',
          tour: user?.tour || inferTourFromRow(row) || '',
          total: 0,
          scores: 0,
          lastDate: null
        });
      }

      const entry = map.get(key);
      entry.total += Number(row.points || 0);
      entry.scores += 1;

      const d = parseRowDate(row);
      if (d && (!entry.lastDate || d > entry.lastDate)) entry.lastDate = d;
    }

    return Array.from(map.values())
      .sort((a, b) => (b.total - a.total) || String(a.pseudo).localeCompare(String(b.pseudo)))
      .map((entry, idx) => ({ rank: idx + 1, ...entry }));
  }

  function renderRows(rows) {
    if (!els.tbody) return;

    if (!rows.length) {
      els.tbody.innerHTML = `<tr><td colspan="6" class="empty">Aucune donnée pour ce filtre.</td></tr>`;
      return;
    }

    els.tbody.innerHTML = rows.map(r => {
      const trClass = String(r.tour || '').toUpperCase() === 'WTA' ? 'wta' : 'atp';
      return `
        <tr class="${trClass}">
          <td class="rank">${fmt(r.rank)}</td>
          <td><span class="pseudo">${escapeHtml(r.pseudo || '—')}</span></td>
          <td class="league">${escapeHtml(r.league || '—')}</td>
          <td class="tour">${escapeHtml(r.tour || '—')}</td>
          <td class="total">${fmt(r.total)}</td>
          <td class="small">${fmt(r.scores)}</td>
        </tr>
      `;
    }).join('');
  }

  function updateMeta(rows, filteredScoresCount) {
    if (els.chipTour) els.chipTour.textContent = selectedTour;
    if (els.chipGame) els.chipGame.textContent = selectedGame === 'all' ? 'Tous les jeux' : selectedGame;
    if (els.chipPeriod) els.chipPeriod.textContent = periodLabel(selectedPeriod);
    if (els.chipPlayers) els.chipPlayers.textContent = fmt(rows.length);
    if (els.chipScores) els.chipScores.textContent = fmt(filteredScoresCount);

    if (els.titleView) els.titleView.textContent = `${selectedTour} — ${selectedGame === 'all' ? 'Tous les jeux' : selectedGame}`;
    if (els.subtitleView) {
      els.subtitleView.textContent = `Période : ${periodLabel(selectedPeriod)} · League lue depuis la table users`;
    }
    if (els.statusView) els.statusView.textContent = `${rows.length ? 'OK' : 'Vide'} · ${filteredScoresCount} score(s) pris en compte`;
  }

  function render() {
    updateSessionChip();
    setActiveButtons();

    const filteredScores = filterScores();
    const rows = aggregateLeaderboard(filteredScores);

    updateMeta(rows, filteredScores.length);
    renderRows(rows);

    if (els.gameSelect) {
      els.gameSelect.value = selectedGame;
    }
  }

  function normalizeLeaderboardRows(rawLeaderboard) {
    return (Array.isArray(rawLeaderboard) ? rawLeaderboard : []).map((r, i) => ({
      rank: i + 1,
      user_id: String(r.user_id || r.id || ''),
      pseudo: String(r.pseudo || r.user_name || r.name || '—'),
      league: String(r.league || '—'),
      league_id: r.league_id !== undefined && r.league_id !== null ? String(r.league_id) : '',
      tour: String(r.tour || inferTourFromRow(r) || selectedTour || '').toUpperCase(),
      total: Number(r.total || r.points || 0),
      scores: Number(r.scores || r.count || r.nb_scores || 0),
      game_id: r.game_id !== undefined && r.game_id !== null ? String(r.game_id) : ''
    }));
  }

  async function fetchFromEndpoint(endpoint) {
    const url = new URL(endpoint, window.location.origin);

    url.searchParams.set('tour', selectedTour);
    url.searchParams.set('period', selectedPeriod);
    url.searchParams.set('scope', selectedScope);
    if (selectedGame !== 'all') url.searchParams.set('game_id', selectedGame);

    const headers = {
      Accept: 'application/json',
      ...buildAuthHeaders()
    };

    const resp = await fetch(url.toString(), {
      method: 'GET',
      headers,
      cache: 'no-cache',
      credentials: 'same-origin'
    });

    const text = await resp.text().catch(() => '');
    let data = null;

    try {
      data = text ? JSON.parse(text) : null;
    } catch (e) {
      data = { ok: false, error: 'invalid_json', raw: text };
    }

    if (!resp.ok) {
      throw new Error(`GET ${endpoint} failed: ${resp.status} ${text}`);
    }

    return data;
  }

  async function loadData() {
    if (loading) return;
    loading = true;

    try {
      if (els.statusView) els.statusView.textContent = 'Chargement…';
      if (els.titleView) els.titleView.textContent = 'Chargement des données';
      if (els.subtitleView) els.subtitleView.textContent = 'Lecture via endpoint GET';
      if (els.tbody) els.tbody.innerHTML = `<tr><td colspan="6" class="loading">Chargement des données…</td></tr>`;

      let lastError = null;
      let payload = null;

      for (const endpoint of ENDPOINTS) {
        try {
          payload = await fetchFromEndpoint(endpoint);
          if (payload) break;
        } catch (err) {
          lastError = err;
        }
      }

      if (!payload) {
        throw lastError || new Error('No leaderboard endpoint available');
      }

      const extracted = extractArraysFromPayload(payload);
      users = normalizeUsers(extracted.users);
      scores = normalizeScores(extracted.scores);
      serverLeaderboard = normalizeLeaderboardRows(extracted.leaderboard);

      rebuildUsersIndex();
      gameList = uniqueGamesFromScores(scores);
      buildGameSelectOptions();

      if (scores.length) {
        render();
        showToast(`Données chargées : ${users.length} utilisateur(s), ${scores.length} score(s)`, 1800);
        return;
      }

      if (serverLeaderboard.length) {
        const filtered = serverLeaderboard.filter(r => {
          if (selectedTour && String(r.tour || '').toUpperCase() !== selectedTour) return false;
          if (selectedGame !== 'all' && String(r.game_id || '') !== selectedGame) return false;
          return true;
        });

        renderRows(filtered);
        updateMeta(filtered, filtered.reduce((acc, r) => acc + Number(r.scores || 0), 0));

        if (els.subtitleView) {
          els.subtitleView.textContent = 'Résultat déjà agrégé côté serveur · pour les filtres semaine/52w, renvoyer users + scores';
        }
        if (els.statusView) els.statusView.textContent = `${filtered.length ? 'OK' : 'Vide'} · données agrégées`;
        showToast(`Données chargées : ${filtered.length} ligne(s) agrégée(s)`, 1800);
        return;
      }

      renderRows([]);
      updateMeta([], 0);
      showToast('Aucune donnée reçue', 1800);
    } catch (err) {
      console.error(err);
      if (els.titleView) els.titleView.textContent = 'Erreur de chargement';
      if (els.subtitleView) els.subtitleView.textContent = String(err.message || err);
      if (els.statusView) els.statusView.textContent = 'Erreur';
      if (els.tbody) els.tbody.innerHTML = `<tr><td colspan="6" class="error">${escapeHtml(err.message || String(err))}</td></tr>`;
      showToast('Impossible de lire les données', 2200);
    } finally {
      loading = false;
    }
  }

  function wireEvents() {
    if (els.btnATP) {
      els.btnATP.addEventListener('click', () => {
        selectedTour = 'ATP';
        render();
      });
    }

    if (els.btnWTA) {
      els.btnWTA.addEventListener('click', () => {
        selectedTour = 'WTA';
        render();
      });
    }

    if (els.btnWeek) {
      els.btnWeek.addEventListener('click', () => {
        selectedPeriod = 'week';
        render();
      });
    }

    if (els.btn52w) {
      els.btn52w.addEventListener('click', () => {
        selectedPeriod = '52w';
        render();
      });
    }

    if (els.btnAll) {
      els.btnAll.addEventListener('click', () => {
        selectedPeriod = 'all';
        render();
      });
    }

    if (els.gameSelect) {
      els.gameSelect.addEventListener('change', () => {
        selectedGame = els.gameSelect.value || 'all';
        render();
      });
    }

    window.addEventListener('ta:auth-changed', () => {
      updateSessionChip();
      loadData();
    });

    window.addEventListener('lb:auth-changed', () => {
      updateSessionChip();
      loadData();
    });
  }

  function init() {
    wireEvents();
    updateSessionChip();
    setActiveButtons();
    buildGameSelectOptions();
    loadData();
  }

  window.LEADERBOARD_PAGE = {
    reload: loadData,
    setTour(tour) {
      selectedTour = String(tour || 'ATP').toUpperCase() === 'WTA' ? 'WTA' : 'ATP';
      render();
    },
    setGame(gameId) {
      selectedGame = gameId || 'all';
      buildGameSelectOptions();
      render();
    },
    setPeriod(period) {
      selectedPeriod = ['week', '52w', 'all'].includes(period) ? period : 'week';
      render();
    },
    setScope(scope) {
      selectedScope = String(scope || DEFAULT_SCOPE);
      render();
    }
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();