/* /js/leaderboard-client.js
   Leaderboard client for Supabase GET data only.
   It reads the users and scores tables, aggregates by user, and renders ATP/WTA leaderboards.
*/
(() => {
  'use strict';

  const CONFIG = window.LEADERBOARD_CONFIG || {};
  const SUPABASE_URL = String(CONFIG.supabaseUrl || window.SUPABASE_URL || '').replace(/\/$/, '');
  const SUPABASE_ANON_KEY = String(CONFIG.supabaseAnonKey || window.SUPABASE_ANON_KEY || '');
  const USERS_TABLE = String(CONFIG.usersTable || 'users');
  const SCORES_TABLE = String(CONFIG.scoresTable || 'scores');
  const PAGE_SIZE = 1000;

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
  let selectedTour = 'ATP';
  let selectedGame = 'all';
  let selectedPeriod = 'week';
  let gameList = [];
  let loading = false;

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

  function getLocalUserSafe() {
    try {
      if (window.LEADERBOARD && typeof window.LEADERBOARD.getLocalUser === 'function') {
        const u = window.LEADERBOARD.getLocalUser();
        if (u && (u.id || u.user_id || u.pseudo || u.username)) return u;
      }
    } catch (e) {}
    try {
      const raw = localStorage.getItem('lb_user_v1');
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (e) {
      return null;
    }
  }

  function updateSessionChip() {
    const me = getLocalUserSafe();
    if (!els.sessionLabel) return;
    if (me && (me.pseudo || me.username || me.id || me.user_id)) {
      const name = me.pseudo || me.username || me.name || me.id || me.user_id;
      els.sessionLabel.textContent = `Connecté : ${name}`;
      els.sessionChip.title = `Utilisateur local : ${name}`;
    } else {
      els.sessionLabel.textContent = 'Global view';
      els.sessionChip.title = 'Aucun utilisateur local détecté';
    }
  }

  function setActiveButtons() {
    els.btnATP.classList.toggle('active', selectedTour === 'ATP');
    els.btnWTA.classList.toggle('active', selectedTour === 'WTA');
    els.btnWeek.classList.toggle('active', selectedPeriod === 'week');
    els.btn52w.classList.toggle('active', selectedPeriod === '52w');
    els.btnAll.classList.toggle('active', selectedPeriod === 'all');
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
    else if (period === '52w') d.setDate(d.getDate() - 364);
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

  function isSupabaseConfigured() {
    return Boolean(SUPABASE_URL && SUPABASE_ANON_KEY);
  }

  async function supabaseFetchTableAll(table, select = '*', extraParams = {}) {
    if (!isSupabaseConfigured()) {
      throw new Error('SUPABASE_URL and SUPABASE_ANON_KEY are required');
    }

    const all = [];
    let offset = 0;

    while (true) {
      const url = new URL(`${SUPABASE_URL}/rest/v1/${table}`);
      url.searchParams.set('select', select);
      url.searchParams.set('limit', String(PAGE_SIZE));
      url.searchParams.set('offset', String(offset));

      for (const [k, v] of Object.entries(extraParams || {})) {
        if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, String(v));
      }

      const res = await fetch(url.toString(), {
        method: 'GET',
        headers: {
          apikey: SUPABASE_ANON_KEY,
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
          Accept: 'application/json'
        }
      });

      const text = await res.text().catch(() => '');
      if (!res.ok) {
        throw new Error(`Supabase GET ${table} failed: ${res.status} ${text}`);
      }

      let rows;
      try {
        rows = text ? JSON.parse(text) : [];
      } catch (e) {
        throw new Error(`Invalid JSON from Supabase ${table}`);
      }

      if (!Array.isArray(rows)) {
        throw new Error(`Unexpected payload from Supabase ${table}`);
      }

      all.push(...rows);

      if (rows.length < PAGE_SIZE) break;
      offset += PAGE_SIZE;
      if (offset > 200000) break;
    }

    return all;
  }

  function normalizeUsers(rawUsers) {
    return (rawUsers || []).map(u => ({
      id: String(u.id || u.user_id || ''),
      user_id: String(u.user_id || u.id || ''),
      pseudo: String(u.pseudo || u.username || u.name || ''),
      league: String(u.league || ''),
      league_id: u.league_id !== undefined && u.league_id !== null ? String(u.league_id) : '',
      tour: String(u.tour || '').toUpperCase(),
      country: String(u.country || '')
    })).filter(u => u.user_id || u.id || u.pseudo);
  }

  function normalizeScores(rawScores) {
    return (rawScores || []).map(s => ({
      id: String(s.id || ''),
      user_id: String(s.user_id || ''),
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

  function uniqueGamesFromScores(rows) {
    const set = new Set();
    for (const r of rows) {
      const g = String(r.game_id || '').trim();
      if (!g) continue;
      set.add(g);
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }

  function buildGameSelectOptions() {
    const current = selectedGame;
    const options = ['<option value="all">Tous les jeux</option>']
      .concat(gameList.map(g => `<option value="${escapeHtml(g)}">${escapeHtml(g)}</option>`));
    els.gameSelect.innerHTML = options.join('');
    els.gameSelect.value = current && (current === 'all' || gameList.includes(current)) ? current : 'all';
  }

  function filterScores() {
    const cutoff = periodCutoff(selectedPeriod);
    return scores.filter(row => {
      if (!row.user_id) return false;

      const user = usersById.get(String(row.user_id));
      if (!user) return false;

      if (selectedTour && String(user.tour || '').toUpperCase() !== selectedTour) return false;
      if (selectedGame !== 'all' && String(row.game_id || '') !== selectedGame) return false;

      if (cutoff) {
        const d = parseRowDate(row);
        if (!d || d < cutoff) return false;
      }

      return true;
    });
  }

  const usersById = new Map();

  function aggregateLeaderboard() {
    const filtered = filterScores();
    const map = new Map();

    for (const row of filtered) {
      const user = usersById.get(String(row.user_id));
      if (!user) continue;

      const key = String(user.user_id || user.id || row.user_id);
      if (!map.has(key)) {
        map.set(key, {
          user_id: key,
          pseudo: user.pseudo || row.pseudo || key,
          league: user.league || '—',
          league_id: user.league_id || '',
          tour: user.tour || '',
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

    return Array.from(map.values()).sort((a, b) => {
      if (b.total !== a.total) return b.total - a.total;
      return String(a.pseudo).localeCompare(String(b.pseudo));
    }).map((entry, idx) => ({
      rank: idx + 1,
      ...entry
    }));
  }

  function renderTable(rows) {
    if (!els.tbody) return;

    if (!rows.length) {
      els.tbody.innerHTML = `<tr><td colspan="6" class="empty">Aucune donnée pour ce filtre.</td></tr>`;
      return;
    }

    els.tbody.innerHTML = rows.map(r => {
      const trClass = String(r.tour || '').toUpperCase() === 'WTA' ? 'wta' : 'atp';
      return `
        <tr class="${trClass}">
          <td class="rank">${r.rank}</td>
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
    els.chipTour.textContent = selectedTour;
    els.chipGame.textContent = selectedGame === 'all' ? 'Tous les jeux' : selectedGame;
    els.chipPeriod.textContent = periodLabel(selectedPeriod);
    els.chipPlayers.textContent = fmt(rows.length);
    els.chipScores.textContent = fmt(filteredScoresCount);

    els.titleView.textContent = `${selectedTour} — ${selectedGame === 'all' ? 'Tous les jeux' : selectedGame}`;
    els.subtitleView.textContent = `Période : ${periodLabel(selectedPeriod)} · League affichée depuis la table users`;
    els.statusView.textContent = `${rows.length ? 'OK' : 'Vide'} · ${filteredScoresCount} score(s) pris en compte`;
  }

  function render() {
    updateSessionChip();
    setActiveButtons();

    const filteredScores = filterScores();
    const rows = aggregateLeaderboard();

    updateMeta(rows, filteredScores.length);
    renderTable(rows);

    if (els.gameSelect) {
      const label = els.gameSelect.options[els.gameSelect.selectedIndex]?.textContent || 'Tous les jeux';
      els.chipGame.textContent = label;
    }
  }

  async function loadData() {
    if (loading) return;
    loading = true;

    try {
      els.statusView.textContent = 'Chargement…';
      els.titleView.textContent = 'Chargement des tables Supabase';
      els.subtitleView.textContent = 'Lecture des données en GET';
      els.tbody.innerHTML = `<tr><td colspan="6" class="loading">Chargement des données…</td></tr>`;

      if (!isSupabaseConfigured()) {
        throw new Error('Supabase non configuré : SUPABASE_URL / SUPABASE_ANON_KEY manquants');
      }

      const [rawUsers, rawScores] = await Promise.all([
        supabaseFetchTableAll(USERS_TABLE, 'id,pseudo,league,country,tour,league_id'),
        supabaseFetchTableAll(SCORES_TABLE, 'id,user_id,pseudo,game_id,points,meta,created_at,mode,created_day,anon_id')
      ]);

      users = normalizeUsers(rawUsers);
      scores = normalizeScores(rawScores);

      usersById.clear();
      for (const u of users) {
        const key = String(u.user_id || u.id || '');
        if (key) usersById.set(key, u);
      }

      gameList = uniqueGamesFromScores(scores);
      buildGameSelectOptions();
      render();

      showToast(`Données chargées : ${users.length} utilisateur(s), ${scores.length} score(s)`, 1800);
    } catch (err) {
      console.error(err);
      els.titleView.textContent = 'Erreur de chargement';
      els.subtitleView.textContent = String(err.message || err);
      els.statusView.textContent = 'Erreur';
      els.tbody.innerHTML = `<tr><td colspan="6" class="error">${escapeHtml(err.message || String(err))}</td></tr>`;
      showToast('Impossible de lire Supabase', 2200);
    } finally {
      loading = false;
    }
  }

  function wireEvents() {
    els.btnATP.addEventListener('click', () => {
      selectedTour = 'ATP';
      render();
    });

    els.btnWTA.addEventListener('click', () => {
      selectedTour = 'WTA';
      render();
    });

    els.btnWeek.addEventListener('click', () => {
      selectedPeriod = 'week';
      render();
    });

    els.btn52w.addEventListener('click', () => {
      selectedPeriod = '52w';
      render();
    });

    els.btnAll.addEventListener('click', () => {
      selectedPeriod = 'all';
      render();
    });

    els.gameSelect.addEventListener('change', () => {
      selectedGame = els.gameSelect.value || 'all';
      render();
    });

    window.addEventListener('lb:auth-changed', () => {
      updateSessionChip();
    });
  }

  function init() {
    wireEvents();
    updateSessionChip();
    setActiveButtons();
    loadData();
  }

  // Expose a tiny API for debugging / future reuse
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
    }
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init, { once: true });
  } else {
    init();
  }
})();
