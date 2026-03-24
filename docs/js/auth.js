(function () {
  const SESSION_KEY = 'ta_session_v1';
  const LEGACY_KEYS = ['lb_user_v1', 'lb_local_user_v1'];
  const CHANNEL_NAME = 'ta-auth';
  const EVENT_NAME = 'ta:auth-changed';

  const bc = 'BroadcastChannel' in window ? new BroadcastChannel(CHANNEL_NAME) : null;

  function safeParse(raw) {
    try { return raw ? JSON.parse(raw) : null; } catch { return null; }
  }

  function normalizeSession(u) {
    if (!u || typeof u !== 'object') return null;
    const pseudo = String(u.pseudo || u.username || '').trim();
    if (!pseudo) return null;

    return {
      id: u.id ? String(u.id) : '',
      pseudo,
      country: u.country ? String(u.country) : '',
      tour: u.tour ? String(u.tour).toUpperCase() : '',
      league: u.league ? String(u.league) : ''
    };
  }

  function readLegacySession() {
    for (const key of LEGACY_KEYS) {
      const raw = localStorage.getItem(key);
      const session = normalizeSession(safeParse(raw));
      if (session) return session;
    }
    return null;
  }

  function getSession() {
    const current = normalizeSession(safeParse(localStorage.getItem(SESSION_KEY)));
    if (current) return current;

    // migration silencieuse des anciennes clés
    const legacy = readLegacySession();
    if (legacy) {
      localStorage.setItem(SESSION_KEY, JSON.stringify(legacy));
      return legacy;
    }
    return null;
  }

  function setSession(session) {
    const normalized = normalizeSession(session);
    if (!normalized) throw new Error('Invalid session');

    localStorage.setItem(SESSION_KEY, JSON.stringify(normalized));

    // nettoyage des anciennes clés
    LEGACY_KEYS.forEach(k => localStorage.removeItem(k));

    dispatchAuthChange();
    return normalized;
  }

  function clearSession() {
    localStorage.removeItem(SESSION_KEY);
    LEGACY_KEYS.forEach(k => localStorage.removeItem(k));
    dispatchAuthChange();
  }

  function dispatchAuthChange() {
    window.dispatchEvent(new Event(EVENT_NAME));
    if (bc) bc.postMessage({ type: 'auth-changed' });
  }

  function onAuthChange(callback) {
    window.addEventListener(EVENT_NAME, callback);
    window.addEventListener('storage', (e) => {
      if (e.key === SESSION_KEY || LEGACY_KEYS.includes(e.key)) callback();
    });
    if (bc) {
      bc.onmessage = (msg) => {
        if (msg?.data?.type === 'auth-changed') callback();
      };
    }
  }

  async function login(pseudo, password) {
    const resp = await fetch('/.netlify/functions/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pseudo, password })
    });

    const data = await resp.json().catch(() => null);
    if (!resp.ok) {
      return { ok: false, status: resp.status, error: data };
    }

    const session = setSession(data.user);
    return { ok: true, user: session };
  }

  async function register(payload) {
    const resp = await fetch('/.netlify/functions/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const data = await resp.json().catch(() => null);
    if (!resp.ok) {
      return { ok: false, status: resp.status, error: data };
    }

    const session = setSession(data.user);
    return { ok: true, user: session };
  }

  async function logout() {
    clearSession();
    return { ok: true };
  }

  function isAuthenticated() {
    return !!getSession();
  }

  function initials(pseudo) {
    return String(pseudo || '')
      .split(/\s+/)
      .filter(Boolean)
      .map(x => x[0])
      .slice(0, 2)
      .join('')
      .toUpperCase() || '??';
  }

  function mount(container, options = {}) {
    const root = typeof container === 'string' ? document.querySelector(container) : container;
    if (!root) return;

    const loginLabel = options.loginLabel || 'Log in';
    const signupLabel = options.signupLabel || 'Sign up';
    const logoutLabel = options.logoutLabel || 'Log out';

    function render() {
      const s = getSession();
      root.innerHTML = '';

      if (s) {
        const box = document.createElement('div');
        box.className = 'user-chip';

        const avatar = document.createElement('div');
        avatar.className = 'lb-avatar';
        avatar.textContent = initials(s.pseudo);

        const name = document.createElement('div');
        name.className = 'lb-name';
        name.textContent = s.pseudo;

        const btn = document.createElement('button');
        btn.type = 'button';
        btn.textContent = logoutLabel;
        btn.addEventListener('click', async () => {
          await logout();
        });

        box.appendChild(avatar);
        box.appendChild(name);
        box.appendChild(btn);
        root.appendChild(box);
      } else {
        const wrap = document.createElement('div');
        wrap.className = 'user-chip';

        const loginBtn = document.createElement('button');
        loginBtn.type = 'button';
        loginBtn.textContent = loginLabel;
        loginBtn.addEventListener('click', () => {
          window.dispatchEvent(new CustomEvent('ta:open-auth', { detail: { tab: 'login' } }));
        });

        const signupBtn = document.createElement('button');
        signupBtn.type = 'button';
        signupBtn.textContent = signupLabel;
        signupBtn.addEventListener('click', () => {
          window.dispatchEvent(new CustomEvent('ta:open-auth', { detail: { tab: 'signup' } }));
        });

        wrap.appendChild(loginBtn);
        wrap.appendChild(signupBtn);
        root.appendChild(wrap);
      }
    }

    render();
    onAuthChange(render);
  }

  window.TA_AUTH = {
    getSession,
    setSession,
    clearSession,
    isAuthenticated,
    onAuthChange,
    login,
    register,
    logout,
    mount
  };
})();