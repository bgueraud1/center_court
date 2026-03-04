// docs/js/leaderboard-client.js
// Minimal leaderboard client - exposes window.LEADERBOARD with:
// submitScore, fetchLeaderboard, createLeaderboardPanel, getLocalUser, getOrCreateAnonId
// Also provides auth functions that call your Netlify functions:
//  - POST /.netlify/functions/create-user  (body: { pseudo, password_hash, tour, country })
//  - POST /.netlify/functions/check_user   (body: { pseudo, password_hash })

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
  const LB_USER_KEY = 'lb_user_v1';        // stores only minimal user session: { id, pseudo, tour, country }
  const LB_ANON_KEY = 'lb_anon_v1';        // anonymous id for non-logged users
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

  // --- minimal session store (id + pseudo + tour + country) ---
  function saveLocalSession(userObj){
    // userObj should be { id: <string|number>, pseudo: <string>, tour?: "ATP"|"WTA", country?: string }
    if (!userObj || !userObj.id) return;
    const s = {
      id: String(userObj.id),
      pseudo: String(userObj.pseudo || ''),
      tour: (userObj.tour ? String(userObj.tour).toUpperCase() : null),
      country: userObj.country ? String(userObj.country) : null
    };
    localStorage.setItem(LB_USER_KEY, JSON.stringify(s));
    try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){}
    try { if (typeof renderLbBox === 'function') renderLbBox(); } catch(e){}
    try { if (typeof ensureModeCheckedAndStart === 'function') ensureModeCheckedAndStart(); } catch(e){}
  }
  function getLocalUser(){
    const s = localStorage.getItem(LB_USER_KEY);
    if (!s) return null;
    try { return JSON.parse(s); } catch(e) { return null; }
  }
  function clearLocalSession(){ localStorage.removeItem(LB_USER_KEY); try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){} }

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
      const txt = await resp.text().catch(()=>null);
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
      // include user id and pseudo (server-side submit-score prefers user_id if provided)
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

  // ------------------------------
  // Countries list for autocompletion (English names)
  // ------------------------------
  const COUNTRIES = [
"Afghanistan","Albania","Algeria","Andorra","Angola","Antigua and Barbuda","Argentina","Armenia","Australia","Austria",
"Azerbaijan","Bahamas","Bahrain","Bangladesh","Barbados","Belarus","Belgium","Belize","Benin","Bhutan",
"Bolivia","Bosnia and Herzegovina","Botswana","Brazil","Brunei","Bulgaria","Burkina Faso","Burundi","Cabo Verde","Cambodia",
"Cameroon","Canada","Central African Republic","Chad","Chile","China","Colombia","Comoros","Congo, Republic of the","Congo, Democratic Republic of the",
"Costa Rica","Côte d'Ivoire","Croatia","Cuba","Cyprus","Czech Republic","Denmark","Djibouti","Dominica","Dominican Republic",
"Ecuador","Egypt","El Salvador","Equatorial Guinea","Eritrea","Estonia","Eswatini","Ethiopia","Federated States of Micronesia","Fiji",
"Finland","France","Gabon","Gambia","Georgia","Germany","Ghana","Greece","Grenada","Guatemala",
"Guinea","Guinea-Bissau","Guyana","Haiti","Honduras","Hungary","Iceland","India","Indonesia","Iran",
"Iraq","Ireland","Israel","Italy","Jamaica","Japan","Jordan","Kazakhstan","Kenya","Kiribati",
"Korea, North","Korea, South","Kosovo","Kuwait","Kyrgyzstan","Laos","Latvia","Lebanon","Lesotho","Liberia",
"Libya","Liechtenstein","Lithuania","Luxembourg","Madagascar","Malawi","Malaysia","Maldives","Mali","Malta",
"Marshall Islands","Mauritania","Mauritius","Mexico","Moldova","Monaco","Mongolia","Montenegro","Morocco","Mozambique",
"Myanmar","Namibia","Nauru","Nepal","Netherlands","New Zealand","Nicaragua","Niger","Nigeria","North Macedonia",
"Norway","Oman","Pakistan","Palau","Panama","Papua New Guinea","Paraguay","Peru","Philippines","Poland",
"Portugal","Qatar","Romania","Russia","Rwanda","Saint Kitts and Nevis","Saint Lucia","Saint Vincent and the Grenadines","Samoa","San Marino",
"Sao Tome and Principe","Saudi Arabia","Senegal","Serbia","Seychelles","Sierra Leone","Singapore","Slovakia","Slovenia","Solomon Islands",
"Somalia","South Africa","South Sudan","Spain","Sri Lanka","Sudan","Suriname","Sweden","Switzerland","Syria",
"Taiwan","Tajikistan","Tanzania","Thailand","Timor-Leste","Togo","Tonga","Trinidad and Tobago","Tunisia","Turkey",
"Turkmenistan","Tuvalu","Uganda","Ukraine","United Arab Emirates","United Kingdom","United States","Uruguay","Uzbekistan","Vanuatu",
"Vatican City","Venezuela","Vietnam","Yemen","Zambia","Zimbabwe"
  ];

  // --- AUTH MODAL (dark, site-friendly) ---
  (function ensureModalStyles(){
    if (document.getElementById('lb-dark-modal-style')) return;
    const css = `
      .lb-dark-overlay { position:fixed; inset:0; display:flex; align-items:center; justify-content:center; background: rgba(2,6,23,0.6); z-index: 99999; padding: 20px; }
      .lb-dark-card { width:460px; max-width:calc(100% - 40px); background: linear-gradient(180deg,#071226,#09142a); border-radius:12px; padding:18px; box-shadow: 0 18px 50px rgba(2,6,23,0.7); color: #e6eef8; font-family: Inter, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial; border:1px solid rgba(255,255,255,0.03);}
      .lb-dark-card h3 { margin:0 0 10px 0; font-size:18px; font-weight:700; color:#fff; }
      .lb-field { margin-bottom:10px; }
      .lb-field label{ display:block; font-size:13px; color:#9aa6bd; margin-bottom:6px; }
      .lb-dark-input { width:100%; padding:10px 12px; border-radius:8px; background: rgba(255,255,255,0.02); border:1px solid rgba(255,255,255,0.03); color:#e6eef8; outline:none; font-size:14px; }
      .lb-dark-card select.lb-dark-input { appearance:none; -webkit-appearance:none; -moz-appearance:none; padding-right:30px; }
      .lb-actions { display:flex; gap:8px; justify-content:flex-end; margin-top:8px; }
      .lb-btn { padding:9px 12px; border-radius:8px; font-weight:600; border:0; cursor:pointer; }
      .lb-btn-ghost { background: transparent; border: 1px solid rgba(255,255,255,0.06); color: #cbd5e1; }
      .lb-btn-primary { background: linear-gradient(90deg,#2563eb,#6d28d9); color:#fff; box-shadow: 0 8px 24px rgba(37,99,235,0.14); }
      .lb-note { font-size:12px; color:#94a3b8; margin-top:10px; }
      .lb-country-suggestions { max-height:160px; overflow:auto; border-radius:6px; background: rgba(0,0,0,0.35); margin-top:6px; border:1px solid rgba(255,255,255,0.03); padding:6px; display:none; }
      .lb-country-suggestions div { padding:6px 8px; cursor:pointer; color:#e6eef8; }
      .lb-country-suggestions div:hover { background: rgba(255,255,255,0.02); }
      @media (max-width:480px){ .lb-dark-card{ width:100%; padding:14px; border-radius:10px; } }
    `;
    const st = document.createElement('style');
    st.id = 'lb-dark-modal-style';
    st.appendChild(document.createTextNode(css));
    document.head.appendChild(st);
  })();

  function openAuthModal(defaultAction = 'login'){
    return new Promise((resolve) => {
      if (document.getElementById('lb-auth-modal')) { resolve(null); return; }

      const overlay = document.createElement('div');
      overlay.id = 'lb-auth-modal';
      overlay.className = 'lb-dark-overlay';

      const card = document.createElement('div');
      card.className = 'lb-dark-card';

      // build country datalist + suggestions wrapper (we also implement a tiny suggestion dropdown)
      const countryDatalistId = 'lb-country-datalist';
      const countrySugId = 'lb-country-sug';

      card.innerHTML = `
        <h3>Sign in / Sign up</h3>

        <div class="lb-field">
          <label for="lb-modal-pseudo">Username</label>
          <input id="lb-modal-pseudo" class="lb-dark-input" type="text" placeholder="Choose a username" />
        </div>

        <div class="lb-field">
          <label for="lb-modal-pass">Password</label>
          <input id="lb-modal-pass" class="lb-dark-input" type="password" placeholder="Enter a password" />
        </div>

        <div style="display:flex;gap:10px;">
          <div style="flex:1">
            <div class="lb-field">
              <label for="lb-modal-tour">Tour</label>
              <select id="lb-modal-tour" class="lb-dark-input">
                <option value="ATP">ATP</option>
                <option value="WTA">WTA</option>
              </select>
            </div>
          </div>
          <div style="flex:1">
            <div class="lb-field">
              <label for="lb-modal-country">Country</label>
              <input id="lb-modal-country" class="lb-dark-input" type="text" placeholder="Country (English)" autocomplete="off" list="${countryDatalistId}" />
              <div id="${countrySugId}" class="lb-country-suggestions" aria-hidden="true"></div>
              <datalist id="${countryDatalistId}">${COUNTRIES.map(c => `<option value="${c}">`).join('')}</datalist>
            </div>
          </div>
        </div>

        <div class="lb-actions">
          <button id="lb-modal-cancel" class="lb-btn lb-btn-ghost">Cancel</button>
          <button id="lb-modal-register" class="lb-btn lb-btn-ghost">Sign Up</button>
          <button id="lb-modal-login" class="lb-btn lb-btn-primary">Sign In</button>
        </div>
        <div class="lb-note">Accounts are created and validated with the server (Supabase). Country selection uses autocomplete.</div>
      `;

      overlay.appendChild(card);
      document.body.appendChild(overlay);

      const inp = document.getElementById('lb-modal-pseudo');
      const pwd = document.getElementById('lb-modal-pass');
      const selTour = document.getElementById('lb-modal-tour');
      const countryInp = document.getElementById('lb-modal-country');
      const countrySug = document.getElementById(countrySugId);
      const btnCancel = document.getElementById('lb-modal-cancel');
      const btnLogin = document.getElementById('lb-modal-login');
      const btnRegister = document.getElementById('lb-modal-register');

      // prefill pseudo/tour/country if we have a local session
      const existing = getLocalUser();
      if (existing) {
        inp.value = existing.pseudo || '';
        if (existing.tour) selTour.value = existing.tour;
        if (existing.country) countryInp.value = existing.country;
      }

      function cleanupAndResolve(result){
        try { overlay.remove(); } catch(e){}
        resolve(result);
      }

      btnCancel.addEventListener('click', ()=> cleanupAndResolve(null));
      overlay.addEventListener('click', (ev)=> { if (ev.target === overlay) cleanupAndResolve(null); });

      btnLogin.addEventListener('click', ()=> {
        const pseudo = inp.value.trim();
        const password = pwd.value;
        if (!pseudo || !password) { alert('Please enter username and password.'); return; }
        cleanupAndResolve({ action: 'login', pseudo, password });
      });

      btnRegister.addEventListener('click', ()=> {
        const pseudo = inp.value.trim();
        const password = pwd.value;
        const tour = (selTour.value || 'ATP').toUpperCase();
        const country = (countryInp.value || '').trim();
        if (!pseudo || !password || !country) { alert('Please enter username, password and country.'); return; }
        cleanupAndResolve({ action: 'register', pseudo, password, tour, country });
      });

      // country suggestions logic (tiny client-side filter)
      countryInp.addEventListener('input', (e) => {
        const q = (countryInp.value || '').trim().toLowerCase();
        if (!q) { countrySug.style.display = 'none'; countrySug.setAttribute('aria-hidden','true'); countrySug.innerHTML = ''; return; }
        const matches = COUNTRIES.filter(c => c.toLowerCase().includes(q)).slice(0,8);
        if (!matches.length) { countrySug.style.display = 'none'; countrySug.setAttribute('aria-hidden','true'); countrySug.innerHTML = ''; return; }
        countrySug.innerHTML = matches.map(m => `<div data-val="${m}">${m}</div>`).join('');
        countrySug.style.display = 'block';
        countrySug.setAttribute('aria-hidden','false');
        // click handler delegation
        Array.from(countrySug.children).forEach(ch => {
          ch.onclick = () => { countryInp.value = ch.dataset.val || ch.textContent; countrySug.style.display = 'none'; countryInp.focus(); };
        });
      });

      // hide suggestions on outside click
      document.addEventListener('click', function onDocClick(ev){
        if (!card.contains(ev.target)) {
          countrySug.style.display = 'none';
          document.removeEventListener('click', onDocClick);
        }
      });

      setTimeout(()=> pwd.focus(), 50);
    });
  }

  // --- UI panel helper (simple) ---
  function createLeaderboardPanel(containerEl){
    if (!containerEl) return;
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
      if (p && pw) {
        const success = await performServerLogin(p, pw);
        if (success) {
          updateAuthUi(containerEl);
          // notify and try to update page immediately
          try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){}
          try { if (typeof renderLbBox === 'function') renderLbBox(); } catch(e){}
          try { if (typeof updateLBStatusUI === 'function') updateLBStatusUI(); } catch(e){}
          try { if (typeof ensureModeCheckedAndStart === 'function') ensureModeCheckedAndStart(); } catch(e){}
          alert('Signed in.');
        }
      } else {
        const res = await openAuthModal('login');
        if (!res) return;
        if (res.action === 'login') {
          const ok = await performServerLogin(res.pseudo, res.password);
          if (ok) {
            updateAuthUi(containerEl);
            try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){}
            try { if (typeof renderLbBox === 'function') renderLbBox(); } catch(e){}
            try { if (typeof updateLBStatusUI === 'function') updateLBStatusUI(); } catch(e){}
            try { if (typeof ensureModeCheckedAndStart === 'function') ensureModeCheckedAndStart(); } catch(e){}
            alert('Signed in.');
          }
        } else if (res.action === 'register') {
          // PASS tour & country to signup
          const ok = await performServerSignup(res.pseudo, res.password, res.tour, res.country);
          if (ok) {
            updateAuthUi(containerEl);
            try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){}
            try { if (typeof renderLbBox === 'function') renderLbBox(); } catch(e){}
            try { if (typeof updateLBStatusUI === 'function') updateLBStatusUI(); } catch(e){}
            try { if (typeof ensureModeCheckedAndStart === 'function') ensureModeCheckedAndStart(); } catch(e){}
            alert('Account created and signed in.');
          }
        }
      }
    };

    btnLogout.onclick = () => {
      clearLocalSession();
      updateAuthUi(containerEl);
      try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){}
    };

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
    const rowsHtml = arr.map((u,i) => `<div style="padding:6px;border-bottom:1px solid rgba(0,0,0,0.06)"><strong>#${i+1} ${u.name}</strong> — ${u.total} pts</div>`).join('');
    display.innerHTML = rowsHtml || '<div>No scores today</div>';
  }

  // --- server signup / login integration ---
  async function performServerSignup(pseudo, password, tour=null, country=null){
    try {
      const hash = await sha256Hex(password);
      const payload = {
        pseudo,
        password_hash: hash,
        tour: (tour?String(tour).toUpperCase():'ATP'),
        country: (country?String(country):'')
      };
      // call your Netlify function create-user
      const res = await callNetlifyFunction('/.netlify/functions/create-user', payload);
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
        const idVal = u.id ?? u.ID ?? u.Id ?? '';
        saveLocalSession({
          id: idVal,
          pseudo: u.pseudo || pseudo,
          tour: (u.tour || payload.tour),
          country: (u.country || payload.country)
        });
        return true;
      } else if (b.ok && b.user && b.user.id) {
        saveLocalSession({
          id: b.user.id,
          pseudo: b.user.pseudo || pseudo,
          tour: b.user.tour || payload.tour,
          country: b.user.country || payload.country
        });
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
      // IMPORTANT: use dash 'check_user' (matches your Netlify function file check-user.js)
      const res = await callNetlifyFunction('/.netlify/functions/check_user', { pseudo, password_hash: hash });
      if (!res.ok) {
        const body = res.body || {};
        if (body && body.error) {
          alert(`Sign in failed: ${body.error}${body.detail ? ' - ' + JSON.stringify(body.detail) : ''}`);
        } else if (res.status === 401) {
          alert('Sign in failed: invalid credentials.');
        } else {
          alert('Sign in failed (server error).');
        }
        return false;
      }
      const b = res.body || {};
      if (b.ok && b.user && (b.user.id || b.user.pseudo)) {
        // user object expected from check_user (we included tour & country in check_user earlier)
        saveLocalSession({ id: b.user.id || b.user.ID || '', pseudo: b.user.pseudo || pseudo, tour: b.user.tour || null, country: b.user.country || null });
        return true;
      } else if (b.ok && b.user_id) {
        // fallback
        saveLocalSession({ id: b.user_id, pseudo });
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

  // --- Auth integration functions exposed to page ---
  async function _openLoginPrompt(){
    const res = await openAuthModal('login');
    if (!res) return null;
    if (res.action === 'login') {
      const ok = await performServerLogin(res.pseudo, res.password);
      return ok ? getLocalUser() : null;
    } else if (res.action === 'register') {
      const ok = await performServerSignup(res.pseudo, res.password, res.tour, res.country);
      return ok ? getLocalUser() : null;
    }
    return null;
  }

  async function _openRegisterPrompt(){
    const res = await openAuthModal('register');
    if (!res) return null;
    if (res.action === 'register') {
      const ok = await performServerSignup(res.pseudo, res.password, res.tour, res.country);
      return ok ? getLocalUser() : null;
    }
    return null;
  }

  function _logoutAndNotify(){
    clearLocalSession();
    try { window.dispatchEvent(new Event('lb:auth-changed')); } catch(e){}
    try { if (typeof renderLbBox === 'function') renderLbBox(); } catch(e){}
    try { if (typeof updateLBStatusUI === 'function') updateLBStatusUI(); } catch(e){}
    try { if (typeof ensureModeCheckedAndStart === 'function') ensureModeCheckedAndStart(); } catch(e){}
    return true;
  }

  // export API
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

  // Also listen for auth-changed in case other modules dispatch it
  window.addEventListener('lb:auth-changed', ()=> {
    try { if (typeof renderLbBox === 'function') renderLbBox(); } catch(e){}
    try { if (typeof updateLBStatusUI === 'function') updateLBStatusUI(); } catch(e){}
    try { if (typeof ensureModeCheckedAndStart === 'function') ensureModeCheckedAndStart(); } catch(e){}
  });

})();