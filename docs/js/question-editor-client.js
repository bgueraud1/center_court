(() => {
  "use strict";

  const CONFIG = window.QUESTION_GAME_CONFIG || {};
  const ENDPOINT = CONFIG.endpoint || "/.netlify/functions/question_game_submit";
  const LOGIN_URL = CONFIG.usersLoginUrl || "/games/tennis_arena/index.html#login";
  const REGISTER_URL = CONFIG.usersRegisterUrl || "/games/tennis_arena/index.html#signup";

  const QUESTIONS_TEMPLATES = {
    open: {
      question_type: "open",
      question_corps: "",
      answer: "",
      open_player: false,
      difficulty: 1,
      tags: ""
    },
    qcm: {
      question_type: "qcm",
      question_corps: "",
      qcm_a: "",
      qcm_b: "",
      qcm_c: "",
      qcm_d: "",
      qcm_answer: "a",
      difficulty: 1,
      tags: ""
    },
    tf: {
      question_type: "tf",
      question_corps: "",
      true_false: true,
      true_false_additional: "",
      difficulty: 1,
      tags: ""
    }
  };

  const state = {
    cards: [],
    focusedTagsInput: null,
    submitting: false,
    session: null
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

  function toast(msg, t = 2400) {
    const el = $("toast");
    el.textContent = msg;
    el.style.display = "block";
    clearTimeout(el._t);
    el._t = setTimeout(() => {
      el.style.display = "none";
    }, t);
  }

  function normalizeText(value) {
    return String(value ?? "").trim();
  }

  function normalizeTags(value) {
    return String(value ?? "")
      .split(/[,;\n]+/g)
      .map((t) => t.trim())
      .filter(Boolean)
      .filter((t, idx, arr) => arr.findIndex((x) => x.toLowerCase() === t.toLowerCase()) === idx);
  }

  function tagsContainTour(tags) {
    const upper = tags.map((t) => t.toUpperCase());
    return upper.includes("ATP") || upper.includes("WTA");
  }

  function getAuthApi() {
    return window.TA_AUTH || null;
  }

  function getSession() {
    const api = getAuthApi();
    if (api && typeof api.getSession === "function") {
      try {
        return api.getSession();
      } catch (e) {}
    }

    try {
      const raw = localStorage.getItem("ta_session_v1");
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      return null;
    }
  }

  function displayName(session) {
    if (!session) return "";
    return String(session.pseudo || session.username || session.name || "").trim();
  }

  function getAuthHeaders() {
    const s = getSession();
    const headers = {};
    if (s) {
      if (s.id) headers["x-user-id"] = s.id;
      if (s.pseudo) headers["x-user-name"] = s.pseudo;
      if (s.tour) headers["x-user-tour"] = s.tour;
      if (s.country) headers["x-user-country"] = s.country;
      if (s.league) headers["x-user-league"] = s.league;
      if (s.user_world_rank) headers["x-user-rank"] = s.user_world_rank;
      if (s.access_token) headers["Authorization"] = `Bearer ${s.access_token}`;
    }
    return headers;
  }

  function openLogin() {
    window.location.href = LOGIN_URL;
  }

  function openRegister() {
    window.location.href = REGISTER_URL;
  }

  function renderAuthBox() {
    const placeholder = $("authPlaceholder");
    const status = $("authStatus");
    const connectedUser = $("connectedUser");

    placeholder.innerHTML = "";
    const session = getSession();
    state.session = session;

    const name = displayName(session);

    if (name) {
      const box = document.createElement("div");
      box.className = "user-chip";
      box.style.position = "relative";

      const av = document.createElement("div");
      av.className = "avatar";
      av.textContent = name.split(/\s+/).map((x) => x[0]).slice(0, 2).join("").toUpperCase() || "??";

      const label = document.createElement("div");
      label.textContent = name;

      box.appendChild(av);
      box.appendChild(label);

      box.addEventListener("click", (e) => {
        e.stopPropagation();
        toggleDropdown(box);
      });

      placeholder.appendChild(box);
      status.textContent = `Connected as ${name}`;
      status.className = "status-line success-line";
      connectedUser.textContent = name;
    } else {
      const link = document.createElement("a");
      link.className = "user-chip connect";
      link.href = LOGIN_URL;
      link.textContent = "Connect";
      placeholder.appendChild(link);
      status.textContent = "Not connected";
      status.className = "status-line";
      connectedUser.textContent = "—";
    }

    $("submitBtn").disabled = !name || state.submitting;
  }

  function toggleDropdown(anchor) {
    const existing = document.querySelector(".dropdown");
    if (existing) {
      existing.remove();
      return;
    }

    const dd = document.createElement("div");
    dd.className = "dropdown";

    const b1 = document.createElement("button");
    b1.textContent = "Connected";
    b1.disabled = true;

    const b2 = document.createElement("button");
    b2.textContent = "Log out";
    b2.addEventListener("click", async () => {
      dd.remove();
      try {
        const api = getAuthApi();
        if (api && typeof api.logout === "function") {
          await api.logout();
        } else {
          localStorage.removeItem("ta_session_v1");
          window.dispatchEvent(new Event("ta:auth-changed"));
        }
      } catch (e) {
        console.warn(e);
      }
    });

    dd.appendChild(b1);
    dd.appendChild(b2);
    anchor.appendChild(dd);

    const close = (ev) => {
      if (!anchor.contains(ev.target)) {
        dd.remove();
        document.removeEventListener("click", close);
      }
    };
    setTimeout(() => document.addEventListener("click", close), 20);
  }

  function makeId() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return `qg_${window.crypto.randomUUID().replace(/-/g, "").slice(0, 12)}`;
    }
    return `qg_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
  }

  function setDraftCount() {
    $("draftCount").textContent = String(state.cards.length);
  }

  function questionSummary(card) {
    const type = card.querySelector("[data-field='question_type']")?.value || "open";
    const body = normalizeText(card.querySelector("[data-field='question_corps']")?.value);
    const label =
      type === "qcm" ? "QCM" :
      type === "tf" ? "True / False" :
      "Open";

    return body ? `${label}: ${body.slice(0, 64)}${body.length > 64 ? "…" : ""}` : `${label} question`;
  }

  function updateCardTitle(cardEl) {
    const title = cardEl.querySelector("[data-card-title]");
    if (title) title.textContent = questionSummary(cardEl);
  }

  function updateVisibility(cardEl) {
    const type = cardEl.querySelector("[data-field='question_type']").value;
    const openSection = cardEl.querySelector("[data-section='open']");
    const qcmSection = cardEl.querySelector("[data-section='qcm']");
    const tfSection = cardEl.querySelector("[data-section='tf']");

    openSection.classList.toggle("hidden", type !== "open");
    qcmSection.classList.toggle("hidden", type !== "qcm");
    tfSection.classList.toggle("hidden", type !== "tf");
    updateCardTitle(cardEl);
  }

  function bindTagsInput(input) {
    input.addEventListener("focus", () => {
      state.focusedTagsInput = input;
    });
    input.addEventListener("input", () => {
      renderTagsPreview(input.closest(".question-card"));
    });
  }

  function addTagToFocusedInput(tag) {
    const input = state.focusedTagsInput;
    if (!input) {
      toast("Click a tags field first.");
      return;
    }

    const current = normalizeTags(input.value);
    if (!current.some((t) => t.toLowerCase() === tag.toLowerCase())) {
      current.push(tag);
    }
    input.value = current.join(", ");
    renderTagsPreview(input.closest(".question-card"));
  }

  function renderTagsPreview(cardEl) {
    const input = cardEl.querySelector("[data-field='tags']");
    const preview = cardEl.querySelector("[data-tags-preview]");
    const tags = normalizeTags(input.value);
    preview.innerHTML = tags.length
      ? tags.map((t) => `<span class="tag-pill">${escapeHtml(t)}</span>`).join("")
      : `<span class="help">No tags yet.</span>`;
  }

  function createQuestionCard(data = QUESTIONS_TEMPLATES.open) {
    const cardId = makeId();
    const card = document.createElement("div");
    card.className = "question-card";
    card.dataset.cardId = cardId;

    card.innerHTML = `
      <div class="question-card-head">
        <div>
          <strong data-card-title>Open question</strong>
          <div class="small" style="color:var(--muted);font-size:.86rem;margin-top:4px">
            ID: <span data-card-id>${escapeHtml(cardId)}</span>
          </div>
        </div>
        <button class="danger-btn" type="button" data-remove-card>Remove</button>
      </div>

      <div class="question-card-body">
        <div class="grid-2">
          <div class="field">
            <label>Question ID</label>
            <input type="text" data-field="id" value="${escapeHtml(cardId)}" />
          </div>

          <div class="field">
            <label>Question type</label>
            <select data-field="question_type">
              <option value="open">Open</option>
              <option value="qcm">QCM</option>
              <option value="tf">True / False</option>
            </select>
          </div>
        </div>

        <div class="field">
          <label>Question text</label>
          <textarea data-field="question_corps" placeholder="Write the question exactly as it should appear to players."></textarea>
        </div>

        <div class="grid-2">
          <div class="field">
            <label>Difficulty</label>
            <select data-field="difficulty">
              <option value="1">1 — Easy</option>
              <option value="2">2 — Normal</option>
              <option value="3">3 — Hard</option>
              <option value="4">4 — Very hard</option>
            </select>
          </div>

          <div class="field">
            <label>Tags</label>
            <input type="text" data-field="tags" placeholder="ATP, WTA, Grand Slam, player name, country..." />
            <div class="help">
              Each question must include <strong>ATP</strong>, <strong>WTA</strong>, or both. Other tags can be added freely.
            </div>
            <div class="tag-preview" data-tags-preview></div>
          </div>
        </div>

        <div data-section="open">
          <div class="grid-2">
            <div class="field">
              <label>Open answer</label>
              <input type="text" data-field="answer" placeholder="Correct answer text" />
            </div>
            <div class="field">
              <label>Player answer?</label>
              <label class="mini-chip" style="justify-content:flex-start; width:max-content">
                <input type="checkbox" data-field="open_player" />
                This question expects a player name
              </label>
            </div>
          </div>
        </div>

        <div data-section="qcm" class="hidden">
          <div class="grid-4">
            <div class="field">
              <label>Option A</label>
              <input type="text" data-field="qcm_a" placeholder="Option A" />
            </div>
            <div class="field">
              <label>Option B</label>
              <input type="text" data-field="qcm_b" placeholder="Option B" />
            </div>
            <div class="field">
              <label>Option C</label>
              <input type="text" data-field="qcm_c" placeholder="Option C" />
            </div>
            <div class="field">
              <label>Option D</label>
              <input type="text" data-field="qcm_d" placeholder="Option D" />
            </div>
          </div>

          <div class="grid-2" style="margin-top:12px">
            <div class="field">
              <label>Correct answer</label>
              <select data-field="qcm_answer">
                <option value="a">A</option>
                <option value="b">B</option>
                <option value="c">C</option>
                <option value="d">D</option>
              </select>
            </div>
            <div class="field">
              <label>Tip</label>
              <div class="help">Only the correct letter is stored in <strong>qcm_answer</strong>.</div>
            </div>
          </div>
        </div>

        <div data-section="tf" class="hidden">
          <div class="grid-2">
            <div class="field">
              <label>Correct answer</label>
              <select data-field="true_false">
                <option value="true">True</option>
                <option value="false">False</option>
              </select>
            </div>
            <div class="field">
              <label>Additional explanation</label>
              <input type="text" data-field="true_false_additional" placeholder="Optional explanation shown to reviewers or later use" />
            </div>
          </div>
        </div>
      </div>
    `;

    const typeField = card.querySelector("[data-field='question_type']");
    const removeBtn = card.querySelector("[data-remove-card]");
    const fields = card.querySelectorAll("[data-field]");
    const tagsInput = card.querySelector("[data-field='tags']");

    typeField.value = data.question_type || "open";

    for (const field of fields) {
      const key = field.getAttribute("data-field");
      if (key in data) {
        if (field.type === "checkbox") {
          field.checked = Boolean(data[key]);
        } else {
          field.value = data[key];
        }
      }
    }

    removeBtn.addEventListener("click", () => {
      if (state.cards.length === 1) {
        toast("At least one question card must remain.");
        return;
      }
      card.remove();
      state.cards = state.cards.filter((x) => x !== card);
      setDraftCount();
    });

    typeField.addEventListener("change", () => updateVisibility(card));
    card.querySelector("[data-field='question_corps']").addEventListener("input", () => updateCardTitle(card));

    bindTagsInput(tagsInput);

    card.querySelectorAll("input, textarea, select").forEach((el) => {
      el.addEventListener("input", () => {
        if (el.getAttribute("data-field") === "tags") renderTagsPreview(card);
      });
      el.addEventListener("change", () => {
        if (el.getAttribute("data-field") === "tags") renderTagsPreview(card);
      });
    });

    updateVisibility(card);
    renderTagsPreview(card);

    return card;
  }

  function addQuestionCard(type = "open") {
    const card = createQuestionCard({ ...QUESTIONS_TEMPLATES[type], question_type: type });
    $("questionList").appendChild(card);
    state.cards.push(card);
    setDraftCount();
    card.scrollIntoView({ behavior: "smooth", block: "center" });
    return card;
  }

  function collectCard(cardEl) {
    const id = normalizeText(cardEl.querySelector("[data-field='id']").value) || makeId();
    const type = cardEl.querySelector("[data-field='question_type']").value;
    const question_corps = normalizeText(cardEl.querySelector("[data-field='question_corps']").value);
    const difficulty = Number(cardEl.querySelector("[data-field='difficulty']").value || 1);
    const tags = normalizeTags(cardEl.querySelector("[data-field='tags']").value);

    const row = {
      id,
      question_type: type,
      question_corps,
      difficulty: Number.isFinite(difficulty) ? difficulty : 1,
      tags: tags.join(", ")
    };

    if (type === "open") {
      row.answer = normalizeText(cardEl.querySelector("[data-field='answer']").value);
      row.open_player = Boolean(cardEl.querySelector("[data-field='open_player']").checked);
      row.qcm_a = null;
      row.qcm_b = null;
      row.qcm_c = null;
      row.qcm_d = null;
      row.qcm_answer = null;
      row.true_false = null;
      row.true_false_additional = null;
    } else if (type === "qcm") {
      row.qcm_a = normalizeText(cardEl.querySelector("[data-field='qcm_a']").value);
      row.qcm_b = normalizeText(cardEl.querySelector("[data-field='qcm_b']").value);
      row.qcm_c = normalizeText(cardEl.querySelector("[data-field='qcm_c']").value);
      row.qcm_d = normalizeText(cardEl.querySelector("[data-field='qcm_d']").value);
      row.qcm_answer = normalizeText(cardEl.querySelector("[data-field='qcm_answer']").value).toLowerCase();
      row.answer = null;
      row.open_player = null;
      row.true_false = null;
      row.true_false_additional = null;
    } else if (type === "tf") {
      row.true_false = cardEl.querySelector("[data-field='true_false']").value === "true";
      row.true_false_additional = normalizeText(cardEl.querySelector("[data-field='true_false_additional']").value) || null;
      row.answer = null;
      row.open_player = null;
      row.qcm_a = null;
      row.qcm_b = null;
      row.qcm_c = null;
      row.qcm_d = null;
      row.qcm_answer = null;
    }

    return row;
  }

  function validateCard(row, index) {
    if (!row.id) return `Question #${index + 1}: ID is required.`;
    if (!row.question_corps) return `Question #${index + 1}: the question text is required.`;
    if (![1, 2, 3, 4].includes(Number(row.difficulty))) return `Question #${index + 1}: difficulty must be between 1 and 4.`;
    if (!row.tags || !normalizeTags(row.tags).length) return `Question #${index + 1}: at least one tag is required.`;
    if (!tagsContainTour(normalizeTags(row.tags))) return `Question #${index + 1}: add ATP, WTA, or both to the tags.`;

    if (row.question_type === "open") {
      if (!row.answer) return `Question #${index + 1}: the open answer is required.`;
    }

    if (row.question_type === "qcm") {
      if (!row.qcm_a || !row.qcm_b || !row.qcm_c || !row.qcm_d) {
        return `Question #${index + 1}: all four QCM options are required.`;
      }
      if (!["a", "b", "c", "d"].includes(String(row.qcm_answer || "").toLowerCase())) {
        return `Question #${index + 1}: QCM correct answer must be A, B, C, or D.`;
      }
    }

    if (row.question_type === "tf") {
      if (typeof row.true_false !== "boolean") {
        return `Question #${index + 1}: the True / False answer is required.`;
      }
    }

    return null;
  }

  async function fetchJson(url, options = {}) {
    const res = await fetch(url, { cache: "no-store", ...options });
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = text; }

    if (!res.ok) {
      const msg = data && (data.error || data.message) ? (data.error || data.message) : `HTTP ${res.status}`;
      const err = new Error(msg);
      err.status = res.status;
      err.data = data;
      throw err;
    }

    return data;
  }

  async function submitAll() {
    if (state.submitting) return;

    const name = displayName(getSession());
    if (!name) {
      toast("You must be connected to submit questions.");
      openLogin();
      return;
    }

    const payload = state.cards.map(collectCard);

    for (let i = 0; i < payload.length; i++) {
      const error = validateCard(payload[i], i);
      if (error) {
        $("submissionStatus").textContent = error;
        $("submissionStatus").className = "status-line error-line";
        toast(error, 3200);
        return;
      }
    }

    state.submitting = true;
    renderAuthBox();
    $("submissionStatus").textContent = "Submitting…";
    $("submissionStatus").className = "status-line";

    try {
      const data = await fetchJson(ENDPOINT, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...getAuthHeaders()
        },
        body: JSON.stringify({ questions: payload })
      });

      const inserted = Array.isArray(data?.inserted) ? data.inserted.length : payload.length;
      $("submissionStatus").textContent = `${inserted} question${inserted > 1 ? "s" : ""} sent for review.`;
      $("submissionStatus").className = "status-line success-line";
      toast(`${inserted} question${inserted > 1 ? "s" : ""} submitted successfully.`);

      $("questionList").innerHTML = "";
      state.cards = [];
      addQuestionCard("open");
      setDraftCount();
    } catch (err) {
      console.error(err);
      const message = err.message || "Submission failed.";
      $("submissionStatus").textContent = message;
      $("submissionStatus").className = "status-line error-line";
      toast(message, 3500);
    } finally {
      state.submitting = false;
      renderAuthBox();
    }
  }

  function bindUi() {
    $("addQuestionBtn").addEventListener("click", () => addQuestionCard("open"));
    $("submitBtn").addEventListener("click", submitAll);

    document.querySelectorAll("[data-insert-tag]").forEach((btn) => {
      btn.addEventListener("click", () => addTagToFocusedInput(btn.getAttribute("data-insert-tag")));
    });

    const loginBtn = document.querySelector("[data-login-btn]");
    const registerBtn = document.querySelector("[data-register-btn]");
    if (loginBtn) loginBtn.addEventListener("click", openLogin);
    if (registerBtn) registerBtn.addEventListener("click", openRegister);

    document.addEventListener("click", (ev) => {
      if (!ev.target.closest(".auth-box")) {
        const existing = document.querySelector(".dropdown");
        if (existing) existing.remove();
      }
    });
  }

  function init() {
    bindUi();
    addQuestionCard("open");
    renderAuthBox();
    $("endpointLabel").textContent = ENDPOINT;
    setDraftCount();

    const api = getAuthApi();
    if (api && typeof api.onAuthChange === "function") {
      api.onAuthChange(() => renderAuthBox());
    } else {
      window.addEventListener("storage", () => renderAuthBox());
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();