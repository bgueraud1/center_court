// site_static/js/birthday_box.js
// Robust birthday box loader + renderer
(async function(){
  const PLACEHOLDER_ID = 'birthday-box-placeholder';
  const WRAPPER_ID = 'birthday-box-wrapper'; // earlier code used this too

  // Candidate JSON paths (ordered: prefer root /tools/ when site_static is publish dir)
  const jsonCandidates = [
    '/tools/birthday_today.json',
    'tools/birthday_today.json',
    '/site_static/tools/birthday_today.json',
    'site_static/tools/birthday_today.json',
    '/docs/tools/birthday_today.json',
    'docs/tools/birthday_today.json'
  ];

  function debugLog(...args){ try{ console.debug('[birthday_box]', ...args); }catch(e){} }

  function findPlaceholder(){
    // prefer an explicit placeholder if present
    const ph = document.getElementById(PLACEHOLDER_ID);
    if(ph) return ph;
    const wrap = document.getElementById(WRAPPER_ID);
    if(wrap) return wrap;
    // fallback: first child of <main>.container or main
    const main = document.querySelector('main .container') || document.querySelector('main') || document.body;
    // create and prepend a node to main if nothing found
    const wrapper = document.createElement('div');
    wrapper.id = WRAPPER_ID;
    main.prepend(wrapper);
    return wrapper;
  }

  function renderFallback(container, message){
    container.innerHTML = '';
    const card = document.createElement('div');
    card.className = 'card shadow-sm';
    card.style.padding = '.6rem';
    card.style.borderLeft = '6px solid #ffc107';
    card.innerHTML = `<div class="d-flex justify-content-between align-items-center">
      <div><strong>Birthday</strong> <small class="text-muted">— Today</small></div>
      <div class="small text-muted">${message}</div>
    </div>`;
    container.appendChild(card);
  }

  function makeCard(entries){
    const card = document.createElement('div');
    card.className = 'card shadow-sm';
    card.style.borderLeft = '6px solid #ffc107';
    card.style.padding = '0.6rem';
    card.style.background = 'linear-gradient(180deg, #fff, #fffaf0)';

    const title = document.createElement('div');
    title.innerHTML = '<strong>Birthday</strong> <small class="text-muted">— Today</small>';
    card.appendChild(title);

    const list = document.createElement('div');
    list.style.marginTop = '0.45rem';
    entries.forEach(e => {
      const row = document.createElement('div');
      row.style.display = 'flex';
      row.style.alignItems = 'center';
      row.style.justifyContent = 'space-between';
      row.style.padding = '0.45rem';
      row.style.borderRadius = '0.4rem';
      row.style.marginBottom = '0.35rem';
      if((e.circuit||'').toUpperCase()==='WTA'){
        row.style.background = 'linear-gradient(90deg, rgba(156,89,182,0.08), rgba(255,255,255,0))';
        row.style.borderLeft = '4px solid #9b59b6';
      } else {
        row.style.background = 'linear-gradient(90deg, rgba(13,110,253,0.06), rgba(255,255,255,0))';
        row.style.borderLeft = '4px solid #0d6efd';
      }
      const left = document.createElement('div'); left.style.display='flex'; left.style.alignItems='center'; left.style.gap='0.6rem';
      const flag = document.createElement('span'); flag.textContent = e.flag_emoji || '';
      flag.style.fontSize = '1.05rem';
      const name = document.createElement('div');
      // add link to player page if player_id present
      if(e.player_id){
        const a = document.createElement('a');
        // build a slug-ish filename (simple; mirrors generator logic)
        const slug = (e.full_name||'').toLowerCase().replace(/\s+/g,'-').replace(/[^a-z0-9\-]/g,'');
        a.href = (e.circuit || '').toUpperCase() === 'ATP' ? `/players_atp/${e.player_id}-${slug}` : `/players/${e.player_id}-${slug}`;
        a.textContent = e.full_name || '(unknown)';
        a.style.textDecoration = 'none';
        name.appendChild(a);
      } else {
        name.textContent = e.full_name || '(unknown)';
      }
      left.appendChild(flag); left.appendChild(name);
      const right = document.createElement('div');
      right.style.minWidth='6rem'; right.style.textAlign='right'; right.style.fontSize='0.9rem'; right.style.color='#6c757d';
      right.textContent = (e.current_rank!=null ? `#${e.current_rank}` : '—');
      row.appendChild(left); row.appendChild(right);
      list.appendChild(row);
    });
    card.appendChild(list);
    return card;
  }

  const container = findPlaceholder();
  // immediately show small loading state
  if(container) {
    container.innerHTML = '';
    const loadingCard = document.createElement('div');
    loadingCard.className = 'card shadow-sm';
    loadingCard.style.padding = '.6rem';
    loadingCard.style.borderLeft = '6px solid #ffc107';
    loadingCard.innerHTML = '<div class="d-flex justify-content-between align-items-center"><div><strong>Birthday</strong> <small class="text-muted">— Today</small></div><div class="small text-muted">Loading…</div></div>';
    container.appendChild(loadingCard);
  }

  // try fetch candidates in order
  let fetched = null;
  let fetchedFrom = null;
  for(const c of jsonCandidates){
    try{
      debugLog('trying fetch', c);
      const resp = await fetch(c, {cache:'no-store'});
      if(!resp.ok){
        debugLog('not ok', c, resp.status);
        continue;
      }
      const j = await resp.json();
      if(Array.isArray(j) && j.length>0){
        fetched = j;
        fetchedFrom = c;
        break;
      } else if (j && Array.isArray(j.rows) && j.rows.length>0){
        fetched = j.rows;
        fetchedFrom = c;
        break;
      } else {
        debugLog('json empty or not array at', c);
        // still accept empty array but continue to try others
        fetched = j;
        fetchedFrom = c;
        // break? no, continue to try others for non-empty first
      }
    }catch(err){
      debugLog('fetch error', c, err);
      continue;
    }
  }

  if(!fetched || !Array.isArray(fetched) || fetched.length===0){
    // no data found
    const msg = 'No birthday data (JSON missing or empty)';
    debugLog(msg);
    renderFallback(container, 'Not available');
    return;
  }

  // render up to 5
  const toRender = fetched.slice(0,5);
  container.innerHTML = '';
  container.appendChild(makeCard(toRender));
  debugLog('rendered birthday box from', fetchedFrom);

})();
