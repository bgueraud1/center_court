// docs/js/leaderboard-client.js
// Minimal leaderboard client - exposes window.LEADERBOARD with:
// submitScore, fetchLeaderboard, createLeaderboardPanel, getLocalUser, getOrCreateAnonId
// Auth uses server Netlify functions:
//  - POST /.netlify/functions/create-user  (body: { pseudo, password_hash })
//  - POST /.netlify/functions/check-user   (body: { pseudo, password_hash })

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
  const LB_USER_KEY = 'lb_user_v1';        // stores only minimal user session: { id, pseudo }
  const LB_ANON_KEY = 'lb_anon_v1';        // anonymous id for non-logged users
  const LB_LAST_SUB_PREFIX = 'lb_last_submit_';

  // --- in-memory list of panels created by createLeaderboardPanel so we can update them on auth changes ---
  const _registeredPanels = [];

  // --- anon id ---
  function getOrCreateAnonId(){
    let id = localStorage.getItem(LB_ANON_KEY);
    if (!id){
      id = uuidv4();
      localStorage.setItem(LB_ANON_KEY, id);
    }
    return id;
  }

  // --- minimal session store (only id + pseudo) ---
  function saveLocalSession(userObj){
    if (!userObj || !userObj.id) return;
    const s = { id: String(userObj.id), pseudo: String(userObj.pseudo || '') };
    localStorage.setItem(LB_USER_KEY, JSON.stringify(s));
  }
  function getLocalUser(){
    const s = localStorage.getItem(LB_USER_KEY);
    if (!s) return null;
    try { return JSON.parse(s); } catch(e) { return null; }
  }
  function clearLocalSession(){ localStorage.removeItem(LB_USER_KEY); }

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

  // --- HTTP helper for Netlify functions ---
  async function callNetlifyFunction(path, bodyObj){
    try {
      const resp = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(bodyObj)
      });
      const txt = await resp.text();
      let json;
      try { json = txt ? JSON.parse(txt) : {}; } catch(e) { json = { ok:false, error: 'invalid_json', raw: txt }; }
      return { ok: resp.ok, status: resp.status, body: json };
    } catch (err) {
      return { ok:false, status: 0, body: { ok:false, error:'network', detail: String(err) } };
    }
  }

  // --- submitScore -> calls Netlify function POST /.netlify/functions/submit-score
  async function submitScore(gameId, points, options = {}){
    const user = getLocalUser();
    const anonIdFromOpts = options.anon_id || null;
    const anonId = anonIdFromOpts || getOrCreateAnonId();
    const metaOpt = options.meta || null;
    const modeOpt = options.mode || null;
    const displayNameOpt = options.displayName || null;

    if (hasSubmittedTodayLocally(gameId)) {
      return { ok:false, error:'already_submitted_local' };
    }

    const payload = { game_id: gameId, points: Number(points) };
    if (user) {
      payload.user_id = user.id;
      payload.pseudo = user.pseudo;
    } else {
      payload.anon_id = anonId;
      if (displayNameOpt) payload.pseudo = String(displayNameOpt).slice(0,50);
    }

    if (modeOpt) payload.mode = String(modeOpt).slice(0,50);

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

  // --- Inject clean modal CSS (site-friendly) ---
  (function injectModalStyles(){
    if (document.getElementById('lb-modal-styles')) return;
    const css = `
      /* leaderboard client modal styles */
      .lb-modal-overlay {
        position: fixed;
        inset: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(12, 18, 30, 0.55);
        z-index: 99999;
        padding: 20px;
      }
      .lb-modal {
        width: 420px;
        max-width: calc(100% - 40px);
        background: linear-gradient(180deg, #ffffff 0%, #fbfbff 100%);
        border-radius: 12px;
        box-shadow: 0 16px 40px rgba(2,6,23,0.18);
        padding: 20px;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial;
        color: #0f172a;
      }
      .lb-modal h3 { margin: 0 0 12px 0; font-size: 18px; font-weight: 700; }
      .lb-modal .lb-field { margin-bottom: 12px; }
      .lb-modal input[type="text"], .lb-modal input[type="password"] {
        width: 100%;
        padding: 10px 12px;
        border-radius: 8px;
        border: 1px solid #e6e9ef;
        outline: none;
        font-size: 14px;
      }
      .lb-modal .lb-actions { display:flex; gap:10px; justify-content:flex-end; margin-top:6px; }
      .lb-btn {
        padding: 9px 12px;
        border-radius: 8px;
        font-weight: 600;
        border: 0;
        cursor: pointer;
      }
      .lb-btn:active { transform: translateY(1px); }
      .lb-btn-ghost { background: transparent; border: 1px solid #e3e6ee; color: #1f2937; }
      .lb-btn-primary { background: linear-gradient(90deg,#2563eb,#4f46e5); color: #fff; box-shadow: 0 6px 18px rgba(37,99,235,0.18); }
      .lb-modal .lb-note { font-size: 12px; color: #6b7280; margin-top:10px; }
      @media (max-width:480px){ .lb-modal { width: 100%; padding: 14px; border-radius: 10px; } }
    `;
    const st = document.createElement('style');
    st.id = 'lb-modal-styles';
    st.appendChild(document.createTextNode(css));
    document.head.appendChild(st);
  })();

  // --- small auth modal (DOM) helper, in English, styled nicely ---
  function openAuthModal(defaultAction = 'login'){
    // returns a Promise that resolves with { action: 'login'|'register', pseudo, password } or null if cancelled
    return new Promise((resolve) => {
      if (document.getElementById('lb-auth-modal')) { resolve(null); return; }

      const overlay = document.createElement('div');
      overlay.id = 'lb-auth-modal';
      overlay.className = 'lb-modal-overlay';

      const panel = document.createElement('div');
      panel.className = 'lb-modal';
      panel.innerHTML = `
        <h3>Sign In / Sign Up</h3>
        <div class="lb-field">
          <label style="display:block;font-size:12px;color:#374151;margin-bottom:6px">Username</label>
          <input id="lb-modal-pseudo" type="text" placeholder="Choose a username" />
        </div>
        <div class="lb-field">
          <label style="display:block;font-size:12px;color:#374151;margin-bottom:6px">Password</label>
          <input id="lb-modal-pass" type="password" placeholder="Enter a password" />
        </div>
        <div class="lb-actions">
          <button id="lb-modal-cancel" class="lb-btn lb-btn-ghost">Cancel</button>
          <button id="lb-modal-register" class="lb-btn lb-btn-ghost">Sign Up</button>
          <button id="lb-modal-login" class="lb-btn lb-btn-primary">Sign In</button>
        </div>
        <div class="lb-note">Accounts are created and validated on the server (Supabase).</div>
      `;

      overlay.appendChild(panel);
      document.body.appendChild(overlay);

      const inpPseudo = document.getElementById('lb-modal-pseudo');
      const inpPass = document.getElementById('lb-modal-pass');
      const btnCancel = document.getElementById('lb-modal-cancel');
      const btnLogin = document.getElementById('lb-modal-login');
      const btnRegister = document.getElementById('lb-modal-register');

      // prefill with existing minimal session if present
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
          alert('Please enter username and password.');
          return;
        }
        cleanupAndResolve({ action: 'login', pseudo, password });
      });

      btnRegister.addEventListener('click', () => {
        const pseudo = inpPseudo.value.trim();
        const password = inpPass.value;
        if (!pseudo || !password) {
          alert('Please enter username and password.');
          return;
        }
        cleanupAndResolve({ action: 'register', pseudo, password });
      });

      setTimeout(()=>inpPass.focus(), 50);
    });
  }

  // --- UI panel helper (simple) ---
  function createLeaderboardPanel(containerEl){
    if (!containerEl) return;
    // register this panel (for automatic updates after auth change)
    if (!_registeredPanels.includes(containerEl)) _registeredPanels.push(containerEl);

    containerEl.setAttribute('data-leaderboard-panel','1');
    containerEl.innerHTML = `
      <div style="display:flex;gap:8px;align-items:center">
        <div id="lb-auth" style="display:flex;gap:8px;align-items:center">
          <input id="lb-pseudo" placeholder="Username (optional)" style="padding:6px;border-radius:6px" />
          <input id="lb-pass" type="password" placeholder="Password (for sign in/up)" style="padding:6px;border-radius:6px" />
          <button id="lb-login" style="padding:6px 8px;border-radius:6px">Sign In / Sign Up</button>
          <button id="lb-logout" style="padding:6px 8px;border-radius:6px;display:none">Sign Out</button>
        </div>
        <div style="margin-left:auto">
          <button id="lb-refresh" style="padding:6px 8px;border-radius:6px">Refresh</button>
        </div>
      </div>
      <div id="lb-list" style="margin-top:10px">Loading...</div>
    `;
    const btnLogin = containerEl.querySelector('#lb-login');
    const btnLogout = containerEl.querySelector('#lb-logout');
    const inputPseudo = containerEl.querySelector('#lb-pseudo');
    const inputPass = containerEl.querySelector('#lb-pass');

    btnLogin.onclick = async () => {
      const p = inputPseudo.value.trim();
      const pw = inputPass.value;
      // If the user filled both fields, try server login first; otherwise open modal
      if (p && pw) {
        const success = await performServerLogin(p, pw);
        if (success) {
          updateAuthUi(containerEl);
          _onAuthChanged();
          showTemporaryMessage(containerEl, 'Signed in.');
        }
      } else {
        // open modal
        const res = await openAuthModal('login');
        if (!res) return;
        if (res.action === 'login') {
          const ok = await performServerLogin(res.pseudo, res.password);
          if (ok) {
            updateAuthUi(containerEl);
            _onAuthChanged();
            showTemporaryMessage(containerEl, 'Signed in.');
          }
        } else if (res.action === 'register') {
          const ok = await performServerSignup(res.pseudo, res.password);
          if (ok) {
            updateAuthUi(containerEl);
            _onAuthChanged();
            showTemporaryMessage(containerEl, 'Account created and signed in.');
          }
        }
      }
    };

    btnLogout.onclick = () => {
      clearLocalSession();
      updateAuthUi(containerEl);
      _onAuthChanged();
      showTemporaryMessage(containerEl, 'Signed out.');
    };

    containerEl.querySelector('#lb-refresh').onclick = () => refreshLeaderboard(containerEl.dataset.gameId, containerEl);

    updateAuthUi(containerEl);
    refreshLeaderboard(containerEl.dataset.gameId, containerEl);
  }

  function showTemporaryMessage(containerEl, text){
    // small ephemeral message near panel
    try {
      let note = containerEl.querySelector('.lb-temp-note');
      if (!note) {
        note = document.createElement('div');
        note.className = 'lb-temp-note';
        note.style.fontSize = '13px';
        note.style.color = '#065f46';
        note.style.marginTop = '8px';
        containerEl.appendChild(note);
      }
      note.textContent = text;
      setTimeout(()=>{ if (note && note.parentNode) note.remove(); }, 3000);
    } catch(e){ /* ignore */ }
  }

  function updateAuthUi(containerEl){
    const user = getLocalUser();
    const inputPseudo = containerEl.querySelector('#lb-pseudo');
    const inputPass = containerEl.querySelector('#lb-pass');
    const btnLogin = containerEl.querySelector('#lb-login');
    const btnLogout = containerEl.querySelector('#lb-logout');
    if (user) {
      if (inputPseudo) inputPseudo.value = user.pseudo;
      if (inputPass) inputPass.value = '';
      if (btnLogin) btnLogin.style.display = 'none';
      if (btnLogout) btnLogout.style.display = '';
    } else {
      if (inputPseudo) inputPseudo.value = '';
      if (inputPass) inputPass.value = '';
      if (btnLogin) btnLogin.style.display = '';
      if (btnLogout) btnLogout.style.display = 'none';
    }
  }

  async function refreshLeaderboard(gameId, containerEl){
    const display = containerEl.querySelector('#lb-list');
    display.innerHTML = 'Loading...';
    const dateISO = (new Date()).toISOString().slice(0,10);
    const data = await fetchLeaderboard(dateISO, gameId, 200);
    if (!data || !data.leaderboard) {
      display.innerHTML = 'Failed to load leaderboard';
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
    const rowsHtml = arr.map((u,i) => `<div style="padding:6px;border-bottom:1px solid rgba(0,0,0,0.06)"><strong>#${i+1} ${escapeHtml(u.name)}</strong> — ${u.total} pts</div>`).join('');
    display.innerHTML = rowsHtml || '<div>No scores today</div>';
  }

  function escapeHtml(s){
    return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }

  // --- server signup / login integration ---
  async function performServerSignup(pseudo, password){
    try {
      const hash = await sha256Hex(password);
      const res = await callNetlifyFunction('/.netlify/functions/create-user', { pseudo, password_hash: hash });
      if (!res.ok) {
        const body = res.body || {};
        if (body.error) {
          alert(`Sign up failed: ${body.error}${body.detail ? ' - ' + JSON.stringify(body.detail) : ''}`);
        } else {
          alert('Sign up failed (server error).');
        }
        return false;
      }
      const b = res.body || {};
      if (b.ok && Array.isArray(b.inserted) && b.inserted.length > 0) {
        const u = b.inserted[0];
        // prefer id fields returned by PostgREST (id)
        const idVal = u.id ?? u.ID ?? u.Id ?? null;
        saveLocalSession({ id: idVal ?? '', pseudo: u.pseudo || pseudo });
        _onAuthChanged();
        return true;
      } else {
        if (b.error) {
          alert('Sign up error: ' + (b.error || 'unknown'));
        } else {
          alert('Sign up failed: unexpected server response.');
        }
        return false;
      }
    } catch (e) {
      console.error('performServerSignup error', e);
      alert('Sign up failed (network or client error).');
      return false;
    }
  }

  async function performServerLogin(pseudo, password){
    try {
      const hash = await sha256Hex(password);
      const res = await callNetlifyFunction('/.netlify/functions/check-user', { pseudo, password_hash: hash });
      if (!res.ok) {
        const body = res.body || {};
        if (body.error) {
          alert(`Sign in failed: ${body.error}${body.detail ? ' - ' + JSON.stringify(body.detail) : ''}`);
        } else {
          alert('Sign in failed (server error).');
        }
        return false;
      }
      const b = res.body || {};
      if (b.ok && b.user && b.user.id) {
        saveLocalSession({ id: b.user.id, pseudo: b.user.pseudo || pseudo });
        _onAuthChanged();
        return true;
      } else {
        if (b.error) {
          alert('Sign in error: ' + b.error);
        } else {
          alert('Sign in failed: unexpected server response.');
        }
        return false;
      }
    } catch (e) {
      console.error('performServerLogin error', e);
      alert('Sign in failed (network or client error).');
      return false;
    }
  }

  // --- Centralized reaction to auth change: update panels & try to update page UI ---
  function _onAuthChanged(){
    // dispatch event so other parts of app can listen
    try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){ /* ignore */ }

    // update all registered leaderboard panels
    try {
      for (const p of _registeredPanels) {
        try { updateAuthUi(p); } catch(e){ /* ignore */ }
      }
    } catch(e){}

    // Try to call page-level helpers if present (some pages expose these)
    try {
      if (typeof window.renderUserPanel === 'function') {
        try { window.renderUserPanel(); } catch(e) { /* ignore */ }
      }
      if (typeof window.updateUserPanel === 'function') {
        try { window.updateUserPanel(); } catch(e) { /* ignore */ }
      }
      if (typeof window.refreshPageAuthUI === 'function') {
        try { window.refreshPageAuthUI(); } catch(e) { /* ignore */ }
      }
    } catch(e){}
  }

  // --- small helpers to expose programmatic auth UI ---
  async function _openLoginPrompt(){
    const res = await openAuthModal('login');
    if (!res) return null;
    if (res.action === 'login') {
      const ok = await performServerLogin(res.pseudo, res.password);
      return ok ? getLocalUser() : null;
    } else if (res.action === 'register') {
      const ok = await performServerSignup(res.pseudo, res.password);
      return ok ? getLocalUser() : null;
    }
    return null;
  }

  async function _openRegisterPrompt(){
    const res = await openAuthModal('register');
    if (!res) return null;
    if (res.action === 'register') {
      const ok = await performServerSignup(res.pseudo, res.password);
      return ok ? getLocalUser() : null;
    }
    return null;
  }

  function _logoutAndNotify(){
    clearLocalSession();
    _onAuthChanged();
    return true;
  }

  // --- export API ---
  window.LEADERBOARD = {
    submitScore,
    fetchLeaderboard,
    createLeaderboardPanel,
    getLocalUser,
    getOrCreateAnonId,

    // auth functions expected by pages
    openLogin: _openLoginPrompt,
    login: _openLoginPrompt,
    showAuth: _openLoginPrompt,

    openRegister: _openRegisterPrompt,
    register: _openRegisterPrompt,

    logout: _logoutAndNotify,
    signOut: _logoutAndNotify
  };

  // --- listening for global auth-change to refresh registered panels (in case some external code dispatches it) ---
  window.addEventListener('lb:auth-changed', () => {
    for (const p of _registeredPanels) {
      try { updateAuthUi(p); } catch(e){}
    }
  });

})(); 