// docs/js/leaderboard-client.js
// Minimal leaderboard client (no <script> wrapper) - pure JS file.
// Expose window.LEADERBOARD with submitScore, fetchLeaderboard, createLeaderboardPanel, getLocalUser, getOrCreateAnonId

(function(){
  // --- utils ---
  function uuidv4(){
    return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g,c=>
      (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c/4).toString(16)
    );
  }
  async function sha256Hex(msg){
    const enc = new TextEncoder();
    const h = await crypto.subtle.digest('SHA-256', enc.encode(msg));
    return Array.from(new Uint8Array(h)).map(b=>b.toString(16).padStart(2,'0')).join('');
  }

  // --- storage keys ---
  const LB_USER_KEY = 'lb_user_v1';
  const LB_ANON_KEY = 'lb_anon_v1';
  const LB_LAST_SUB_PREFIX = 'lb_last_submit_';

  // --- anon id ---
  function getOrCreateAnonId(){
    let id = localStorage.getItem(LB_ANON_KEY);
    if (!id){
      id = uuidv4();
      localStorage.setItem(LB_ANON_KEY, id);
    }
    return id;
  }

  // --- local user simple store ---
  async function signupOrLoginLocal(pseudo, password){
    const hash = await sha256Hex(password);
    const obj = { pseudo: String(pseudo).trim(), password_hash: hash };
    localStorage.setItem(LB_USER_KEY, JSON.stringify(obj));
    return obj;
  }
  function getLocalUser(){
    const s = localStorage.getItem(LB_USER_KEY);
    if (!s) return null;
    try { return JSON.parse(s); } catch(e) { return null; }
  }
  function logoutLocal(){ localStorage.removeItem(LB_USER_KEY); }

  // --- local duplicate-guard ---
  function hasSubmittedTodayLocally(gameId){
    const key = LB_LAST_SUB_PREFIX + gameId;
    const last = localStorage.getItem(key);
    const today = (new Date()).toISOString().slice(0,10);
    return last === today;
  }
  function markSubmittedTodayLocally(gameId){
    const key = LB_LAST_SUB_PREFIX + gameId;
    const today = (new Date()).toISOString().slice(0,10);
    localStorage.setItem(key, today);
  }

  // --- submitScore -> calls Netlify function POST /.netlify/functions/submit-score
  /* ---- submit function ----
   - gameId: string
   - points: number
   - options.meta: optional string or object
   - options.displayName: optional display name (string)
   - options.mode: optional string (e.g. "ATP_top20")
   - options.anon_id: optional string (will be used if provided)
   - returns parsed JSON from server
  */
  async function submitScore(gameId, points, options = {}){
    const user = getLocalUser();
    // prefer anon_id passed in options only if provided; otherwise use/create local anon id
    const anonIdFromOpts = options.anon_id || null;
    const anonId = anonIdFromOpts || getOrCreateAnonId();
    const metaOpt = options.meta || null;
    const modeOpt = options.mode || null;
    const displayNameOpt = options.displayName || null;

    // local guard: already submitted today.
    if (hasSubmittedTodayLocally(gameId)) {
      return { ok:false, error:'already_submitted_local' };
    }

    // build payload
    const payload = { game_id: gameId, points: Number(points) };
    if (user) {
      // if local "logged" user, include pseudo + password_hash if present
      payload.pseudo = user.pseudo;
      if (user.password_hash) payload.password_hash = user.password_hash;
      // if your backend expects user_id instead, change here to include user.id
    } else {
      // anonymous path: include anon_id and optional pseudo for display
      payload.anon_id = anonId;
      if (displayNameOpt) payload.pseudo = String(displayNameOpt).slice(0,50);
    }

    if (modeOpt) payload.mode = String(modeOpt).slice(0,50);

    // meta: if object -> stringify
    if (metaOpt) {
      if (typeof metaOpt === 'object') payload.meta = JSON.stringify(metaOpt);
      else payload.meta = String(metaOpt);
    }

    try {
      const r = await fetch('/.netlify/functions/submit-score', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const text = await r.text();
      let data;
      try { data = text ? JSON.parse(text) : {}; } catch(e){ data = { ok:false, error:'invalid_json_from_server', raw:text }; }

      if (r.ok && data && data.ok) {
        // mark locally to prevent repeated submits from same browser
        markSubmittedTodayLocally(gameId);
      }

      return data;
    } catch (err) {
      console.error('submitScore error', err);
      return { ok:false, error:'network', detail: String(err) };
    }
  }


  // --- fetchLeaderboard -> GET /.netlify/functions/leaderboard?date=YYYY-MM-DD&game_id=...
  async function fetchLeaderboard(dateISO, gameId, limit=50){
    const q = new URLSearchParams();
    if (dateISO) q.set('date', dateISO);
    if (gameId) q.set('game_id', gameId);
    q.set('limit', String(limit));
    try {
      const r = await fetch('/.netlify/functions/leaderboard?' + q.toString());
      if (!r.ok) return null;
      const j = await r.json();
      return j;
    } catch(e) {
      console.error('fetchLeaderboard error', e);
      return null;
    }
  }

  // --- small auth modal (DOM) helper ---
  function openAuthModal(defaultAction = 'login'){
    // returns a Promise that resolves with { action: 'login'|'register', pseudo, password } or null if cancelled
    return new Promise((resolve) => {
      // avoid multiple modals
      if (document.getElementById('lb-auth-modal')) {
        resolve(null);
        return;
      }

      const overlay = document.createElement('div');
      overlay.id = 'lb-auth-modal';
      overlay.style.position = 'fixed';
      overlay.style.left = '0';
      overlay.style.top = '0';
      overlay.style.width = '100%';
      overlay.style.height = '100%';
      overlay.style.display = 'flex';
      overlay.style.alignItems = 'center';
      overlay.style.justifyContent = 'center';
      overlay.style.background = 'rgba(0,0,0,0.4)';
      overlay.style.zIndex = '99999';

      const panel = document.createElement('div');
      panel.style.width = '320px';
      panel.style.padding = '16px';
      panel.style.borderRadius = '8px';
      panel.style.background = '#fff';
      panel.style.boxShadow = '0 6px 20px rgba(0,0,0,0.12)';
      panel.style.fontFamily = 'system-ui, sans-serif';
      panel.style.color = '#111';

      panel.innerHTML = `
        <div style="font-weight:600;margin-bottom:8px">Connexion / Inscription</div>
        <div style="margin-bottom:8px">
          <input id="lb-modal-pseudo" placeholder="Pseudo" style="width:100%;padding:8px;border-radius:6px;border:1px solid #ddd" />
        </div>
        <div style="margin-bottom:12px">
          <input id="lb-modal-pass" type="password" placeholder="Mot de passe" style="width:100%;padding:8px;border-radius:6px;border:1px solid #ddd" />
        </div>
        <div style="display:flex;gap:8px;justify-content:flex-end">
          <button id="lb-modal-cancel" style="padding:8px 10px;border-radius:6px;background:transparent;border:1px solid #ccc">Annuler</button>
          <button id="lb-modal-register" style="padding:8px 10px;border-radius:6px;background:#eee;border:1px solid #ddd">S'inscrire</button>
          <button id="lb-modal-login" style="padding:8px 10px;border-radius:6px;background:#0b84ff;color:#fff;border:0">Se connecter</button>
        </div>
        <div style="margin-top:8px;font-size:12px;color:#666">Les comptes sont stockés localement (pour l'instant).</div>
      `;

      overlay.appendChild(panel);
      document.body.appendChild(overlay);

      const inpPseudo = document.getElementById('lb-modal-pseudo');
      const inpPass = document.getElementById('lb-modal-pass');
      const btnCancel = document.getElementById('lb-modal-cancel');
      const btnLogin = document.getElementById('lb-modal-login');
      const btnRegister = document.getElementById('lb-modal-register');

      // prefills
      const existing = getLocalUser();
      if (existing) inpPseudo.value = existing.pseudo || '';

      function cleanupAndResolve(result){
        try { overlay.remove(); } catch(e) { /* ignore */ }
        resolve(result);
      }

      btnCancel.addEventListener('click', () => cleanupAndResolve(null));
      overlay.addEventListener('click', (ev) => {
        if (ev.target === overlay) cleanupAndResolve(null);
      });

      btnLogin.addEventListener('click', () => {
        const pseudo = inpPseudo.value.trim();
        const password = inpPass.value;
        if (!pseudo || !password) {
          alert('Entrer pseudo et mot de passe.');
          return;
        }
        cleanupAndResolve({ action: 'login', pseudo, password });
      });

      btnRegister.addEventListener('click', () => {
        const pseudo = inpPseudo.value.trim();
        const password = inpPass.value;
        if (!pseudo || !password) {
          alert('Entrer pseudo et mot de passe.');
          return;
        }
        cleanupAndResolve({ action: 'register', pseudo, password });
      });

      // focus
      setTimeout(()=>inpPass.focus(), 50);
    });
  }

  // --- UI panel helper (simple) ---
  function createLeaderboardPanel(containerEl){
    if (!containerEl) return;
    containerEl.innerHTML = `
      <div style="display:flex;gap:8px;align-items:center">
        <div id="lb-auth" style="display:flex;gap:8px;align-items:center">
          <input id="lb-pseudo" placeholder="Pseudo (optionnel)" style="padding:6px;border-radius:6px" />
          <input id="lb-pass" type="password" placeholder="Mot de passe (inscription/login)" style="padding:6px;border-radius:6px" />
          <button id="lb-login" style="padding:6px 8px;border-radius:6px">Login / Signup</button>
          <button id="lb-logout" style="padding:6px 8px;border-radius:6px;display:none">Logout</button>
        </div>
        <div style="margin-left:auto">
          <button id="lb-refresh" style="padding:6px 8px;border-radius:6px">Refresh LB</button>
        </div>
      </div>
      <div id="lb-list" style="margin-top:10px">Chargement...</div>
    `;
    const btnLogin = containerEl.querySelector('#lb-login');
    const btnLogout = containerEl.querySelector('#lb-logout');
    const inputPseudo = containerEl.querySelector('#lb-pseudo');
    const inputPass = containerEl.querySelector('#lb-pass');

    btnLogin.onclick = async () => {
      const p = inputPseudo.value.trim();
      const pw = inputPass.value;
      if (!p || !pw) return alert('Entrer pseudo et mot de passe (au moins) pour enregistrer');
      await signupOrLoginLocal(p, pw);
      updateAuthUi(containerEl);
      // notify
      window.dispatchEvent(new Event('lb:auth-changed'));
      alert('Connecté localement (pseudo enregistré).');
    };
    btnLogout.onclick = () => { logoutLocal(); updateAuthUi(containerEl); window.dispatchEvent(new Event('lb:auth-changed')); };
    containerEl.querySelector('#lb-refresh').onclick = () => refreshLeaderboard(containerEl.dataset.gameId, containerEl);

    updateAuthUi(containerEl);
    refreshLeaderboard(containerEl.dataset.gameId, containerEl);
  }

  function updateAuthUi(containerEl){
    const user = getLocalUser();
    const inputPseudo = containerEl.querySelector('#lb-pseudo');
    const inputPass = containerEl.querySelector('#lb-pass');
    const btnLogin = containerEl.querySelector('#lb-login');
    const btnLogout = containerEl.querySelector('#lb-logout');
    if (user) {
      inputPseudo.value = user.pseudo;
      inputPass.value = '';
      btnLogin.style.display = 'none';
      btnLogout.style.display = '';
    } else {
      inputPseudo.value = '';
      inputPass.value = '';
      btnLogin.style.display = '';
      btnLogout.style.display = 'none';
    }
  }

  async function refreshLeaderboard(gameId, containerEl){
    const display = containerEl.querySelector('#lb-list');
    display.innerHTML = 'Chargement...';
    const dateISO = (new Date()).toISOString().slice(0,10);
    const data = await fetchLeaderboard(dateISO, gameId, 200);
    if (!data || !data.leaderboard) {
      display.innerHTML = 'Erreur de chargement';
      return;
    }
    // aggregate by user (client-side)
    const map = {};
    data.leaderboard.forEach(r => {
      const key = r.user_id || r.pseudo || r.anon_id || ('anon_' + (r.id || Math.random()));
      const name = r.pseudo || (r.user_id ? r.user_id : (r.anon_id || 'anonymous'));
      if (!map[key]) map[key] = { name, total: 0, rows: [] };
      map[key].total += (Number(r.points) || 0);
      map[key].rows.push(r);
    });
    const arr = Object.values(map).sort((a,b)=>b.total-a.total);
    const rowsHtml = arr.map((u,i) => `<div style="padding:6px;border-bottom:1px solid rgba(0,0,0,0.06)"><strong>#${i+1} ${u.name}</strong> — ${u.total} pts</div>`).join('');
    display.innerHTML = rowsHtml || '<div>Aucun score aujourd\'hui</div>';
  }

  // --- Auth integration functions exposed to page ---
  async function _openLoginPrompt(){
    // shows modal, returns user obj or null
    const res = await openAuthModal('login');
    if (!res) return null;
    // using local signup/login helper for now (stores locally)
    try {
      const obj = await signupOrLoginLocal(res.pseudo, res.password);
      // notify listeners
      window.dispatchEvent(new Event('lb:auth-changed'));
      return obj;
    } catch (e) {
      console.error('login error', e);
      return null;
    }
  }

  async function _openRegisterPrompt(){
    const res = await openAuthModal('register');
    if (!res) return null;
    try {
      const obj = await signupOrLoginLocal(res.pseudo, res.password);
      window.dispatchEvent(new Event('lb:auth-changed'));
      return obj;
    } catch (e) {
      console.error('register error', e);
      return null;
    }
  }

  function _logoutAndNotify(){
    try {
      logoutLocal();
      window.dispatchEvent(new Event('lb:auth-changed'));
      return true;
    } catch (e) {
      console.warn('logout failed', e);
      return false;
    }
  }

  // export API
  window.LEADERBOARD = {
    submitScore,
    fetchLeaderboard,
    createLeaderboardPanel,
    getLocalUser,
    getOrCreateAnonId,

    // auth shims expected by pages
    openLogin: _openLoginPrompt,
    login: _openLoginPrompt,
    showAuth: _openLoginPrompt,

    openRegister: _openRegisterPrompt,
    register: _openRegisterPrompt,

    // logout names
    logout: _logoutAndNotify,
    signOut: _logoutAndNotify,
    signoff: _logoutAndNotify
  };
})();