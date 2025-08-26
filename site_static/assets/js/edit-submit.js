// assets/js/edit-submit.js
// Usage: inclure sur la page /edit (en bas du body or with defer)

(function () {
  function q(name) { return document.querySelector('[name="'+name+'"]'); }
  function qAllEditable() { return Array.from(document.querySelectorAll('[data-editable]')); }

  // récupère player depuis query string si absent dans form
  function getPlayerFromUrl() {
    try {
      const u = new URL(window.location.href);
      return u.searchParams.get('player');
    } catch(e) { return null; }
  }

  async function submitEdit(payload) {
    const resp = await fetch('/.netlify/functions/submit_edit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      credentials: 'same-origin'
    });
    return resp.json();
  }

  function showMessage(msg, isError) {
    let el = document.getElementById('submit-edit-message');
    if (!el) {
      el = document.createElement('div');
      el.id = 'submit-edit-message';
      el.style.marginTop = '0.5rem';
      document.body.appendChild(el);
    }
    el.textContent = msg;
    el.style.color = isError ? 'crimson' : 'green';
  }

  // main handler to be called on form submit
  window.centerCourtHandleEditSubmit = async function (evt) {
    if (evt && evt.preventDefault) evt.preventDefault();
    const form = evt ? evt.target : document.querySelector('form#edit-form');
    if (!form) { showMessage('Formulaire introuvable', true); return; }

    // build payload
    const player = form.querySelector('[name="player"]')?.value || getPlayerFromUrl();
    const name = form.querySelector('[name="name"]')?.value || form.querySelector('[name="player_name"]')?.value || '';
    if (!player) { showMessage('Erreur: player non trouvé', true); return; }

    // edits: tous les champs data-editable
    const edits = {};
    qAllEditable().forEach(el => {
      if (!el.name) return;
      // ignore empty strings (optional)
      edits[el.name] = el.value;
    });

    // build payload (replace existing payload builder)
    const payload = {
      player: form.querySelector('[name="player"]').value || '',
      name: form.querySelector('[name="name"]').value || '',
      edits: {}
    };
    
    // collect editable fields
        // collect editable fields (handle checkboxes correctly)
    document.querySelectorAll('[data-editable]').forEach(el => {
      if (!el.name) return;
      // checkboxes: use checked state -> send "true" or "false"
      if (el.type === 'checkbox') {
        payload.edits[el.name] = el.checked ? (el.value || 'true') : 'false';
      } else if (el.tagName && el.tagName.toLowerCase() === 'select') {
        payload.edits[el.name] = el.value || '';
      } else {
        payload.edits[el.name] = el.value || '';
      }
    });

    
    // IMPORTANT: include admin_code trimmed if present
    const adminEl = form.querySelector('[name="admin_code"]') || document.getElementById('admin_code');
    const adminVal = adminEl ? (adminEl.value || '').toString().trim() : '';
    if (adminVal) payload.admin_code = adminVal;
    
    // optionally include source/notes
    const notesEl = form.querySelector('[name="notes"]');
    if (notesEl && notesEl.value) payload.edits.notes = notesEl.value;
    

    // disable submit button
    const submitBtn = form.querySelector('[type="submit"]');
    if (submitBtn) { submitBtn.disabled = true; submitBtn.dataset.origText = submitBtn.textContent; submitBtn.textContent = 'Envoi…'; }

    try {
      const result = await submitEdit(payload);
      if (result && result.ok) {
        if (result.suggestion) {
          showMessage('Suggestion envoyée. Merci !', false);
        } else if (result.committed) {
          showMessage('Modifications appliquées (commit créé).', false);
        } else {
          showMessage('Réponse inconnue du serveur', false);
        }
      } else {
        showMessage('Erreur serveur: ' + (result && result.error ? result.error : 'unknown'), true);
      }
    } catch (err) {
      showMessage('Erreur réseau / serveur: ' + err.message, true);
    } finally {
      if (submitBtn) { submitBtn.disabled = false; submitBtn.textContent = submitBtn.dataset.origText || 'Envoyer'; }
    }
  };

  // If page has a form with id edit-form, wire it automatically
  document.addEventListener('DOMContentLoaded', function () {
    const form = document.querySelector('form#edit-form');
    if (form) {
      form.addEventListener('submit', window.centerCourtHandleEditSubmit);
    }
  });
})();
