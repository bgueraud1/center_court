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
      alert('Connecté localement (pseudo enregistré).');
    };
    btnLogout.onclick = () => { logoutLocal(); updateAuthUi(containerEl); };
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

  // export API
  window.LEADERBOARD = {
    submitScore,
    fetchLeaderboard,
    createLeaderboardPanel,
    getLocalUser,
    getOrCreateAnonId
  };
})();
