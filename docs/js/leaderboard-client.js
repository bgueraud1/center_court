/* tiny utils */
function uuidv4(){
  return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g,c=>
    (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c/4).toString(16)
  );
}
async function sha256Hex(msg){
  const enc = new TextEncoder();
  const data = enc.encode(msg);
  const h = await crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(h)).map(b=>b.toString(16).padStart(2,'0')).join('');
}

/* storage keys */
const LB_USER_KEY = 'lb_user_v1';
const LB_ANON_KEY = 'lb_anon_v1';
const LB_LAST_SUB_PREFIX = 'lb_last_submit_'; // + gameId -> yyyy-mm-dd

/* anon id management */
function getOrCreateAnonId(){
  let id = localStorage.getItem(LB_ANON_KEY);
  if (!id){
    id = uuidv4();
    localStorage.setItem(LB_ANON_KEY, id);
  }
  return id;
}

/* user management (local only) */
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
function logoutLocal(){
  localStorage.removeItem(LB_USER_KEY);
}

/* duplicate-prevent (local only) */
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

/* submit function */
async function submitScore(gameId, points, options = {}){
  const user = getLocalUser();
  const anonId = getOrCreateAnonId();
  const meta = options.meta || '';

  if (hasSubmittedTodayLocally(gameId)) {
    return { ok:false, error:'already_submitted_local' };
  }

  const payload = { game_id: gameId, points };
  if (user) {
    payload.pseudo = user.pseudo;
    payload.password_hash = user.password_hash;
  } else {
    payload.anon_id = anonId;
    if (options.displayName) payload.pseudo = options.displayName.slice(0,50);
  }
  if (meta) payload.meta = meta;

  try {
    const r = await fetch('/.netlify/functions/submit-score', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await r.json();
    if (r.ok && data && data.ok) {
      markSubmittedTodayLocally(gameId);
    }
    return data;
  } catch (err) {
    console.error('submitScore error', err);
    return { ok:false, error:'network' };
  }
}

/* leaderboard fetcher */
async function fetchLeaderboard(dateISO, gameId, limit=30){
  const q = new URLSearchParams();
  if (dateISO) q.set('date', dateISO);
  if (gameId) q.set('game_id', gameId);
  q.set('limit', String(limit));
  const r = await fetch('/.netlify/functions/leaderboard?' + q.toString());
  return r.ok ? r.json() : null;
}

/* UI helper */
function createLeaderboardPanel(containerEl){
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
    <div id="lb-list" style="margin-top:10px"></div>
  `;
  containerEl.querySelector('#lb-login').onclick = async () => {
    const p = containerEl.querySelector('#lb-pseudo').value.trim();
    const pw = containerEl.querySelector('#lb-pass').value;
    if (!p || !pw) return alert('Entrer pseudo et mot de passe (au moins) pour enregistrer');
    await signupOrLoginLocal(p, pw);
    updateAuthUi(containerEl);
    alert('Connecté localement (pseudo enregistré).');
  };
  containerEl.querySelector('#lb-logout').onclick = () => { logoutLocal(); updateAuthUi(containerEl); };
  containerEl.querySelector('#lb-refresh').onclick = () => {
    const gid = containerEl.dataset.gameId;
    refreshLeaderboard(gid, containerEl);
  };
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
  const data = await fetchLeaderboard((new Date()).toISOString().slice(0,10), gameId, 30);
  if (!data || !data.leaderboard) {
    display.innerHTML = 'Erreur de chargement';
    return;
  }
  const rows = data.leaderboard.map((u,i) => `<div style="padding:6px;border-bottom:1px solid rgba(255,255,255,0.04)"><strong>#${i+1} ${u.pseudo}</strong> — ${u.total} pts</div>`).join('');
  display.innerHTML = rows || '<div>Aucun score aujourd\\'hui</div>';
}

/* export API */
window.LEADERBOARD = {
  submitScore,
  fetchLeaderboard,
  createLeaderboardPanel,
  getLocalUser,
  getOrCreateAnonId
};
