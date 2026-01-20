// RankingsUI - simple vanilla rendering, filtering, highlighting, pagination
const RankingsUI = (function(){
  let cfg = {};
  let data = [];
  let filtered = [];
  let countries = new Set();
  let currentPage = 1;

  function el(id){ return document.getElementById(id); }
  function fmtNum(n){ if(n==null) return '—'; return n.toLocaleString ? n.toLocaleString() : String(n); }

  function load(){
    el('status').textContent = 'Loading ranking…';
    fetch(cfg.jsonUrl, {cache: 'no-store'}).then(r => r.json()).then(js => {
      data = js.slice().sort((a,b)=> (a.ranking||999999) - (b.ranking||999999));
      // populate country list
      countries = new Set(data.map(d => d.country_code || '').filter(Boolean));
      populateCountrySelects();
      applyFilters();
      el('status').textContent = `Loaded ${data.length} players.`;
    }).catch(e=>{
      el('status').textContent = 'Failed to load ranking: '+e;
      console.error(e);
    });
  }

  function populateCountrySelects(){
    const sel = el('filter_country');
    const hsel = el('highlight_country');
    // clear
    sel.innerHTML = '<option value="">— all —</option>';
    hsel.innerHTML = '<option value="">— none —</option>';
    const arr = Array.from(countries).sort((a,b)=> a.localeCompare(b));
    arr.forEach(code=>{
      const o = document.createElement('option');
      o.value = code;
      o.textContent = code;
      sel.appendChild(o);
      const o2 = o.cloneNode(true);
      hsel.appendChild(o2);
    });
  }

  function applyFilters(){
    const age_le = parseInt(el('age_le').value) || null;
    const age_ge = parseInt(el('age_ge').value) || null;
    const country = el('filter_country').value || null;
    filtered = data.filter(p=>{
      if(age_le !== null && (p.age === null || p.age > age_le)) return false;
      if(age_ge !== null && (p.age === null || p.age < age_ge)) return false;
      if(country && ( (p.country_code||'').toUpperCase() !== country.toUpperCase())) return false;
      return true;
    });
    currentPage = 1;
    renderPage();
  }

  function renderPage(){
    const per = cfg.rowsPerPage || 50;
    const start = (currentPage-1)*per;
    const page = filtered.slice(start, start+per);
    const tbody = document.querySelector('#ranking_table tbody');
    tbody.innerHTML = '';
    const highlight = el('highlight_country').value || null;

    page.forEach((p, idx)=>{
      const tr = document.createElement('tr');
      // row background / class
      tr.classList.add(cfg.rowClass || '');
      // highlight if country matches
      const isHl = highlight && ((p.country_code||'').toUpperCase() === highlight.toUpperCase());
      if(isHl){
        tr.classList.add('highlight-row');
        // style of highlight controlled by CSS classes .highlight-wta .highlight-atp
      }

      const tdRank = document.createElement('td');
      tdRank.textContent = (p.ranking != null) ? p.ranking : '—';
      tdRank.className = 'small text-muted';
      tr.appendChild(tdRank);

      const tdName = document.createElement('td');
      const a = document.createElement('a');
      a.href = p.player_url || '#';
      a.target = '_self';
      a.rel = 'noopener';
      // flag + name
      const spanFlag = document.createElement('span');
      spanFlag.className = 'flag-emoji';
      spanFlag.textContent = p.flag_emoji || (p.country_code ? countryCodeToEmoji(p.country_code) : '');
      a.appendChild(spanFlag);
      const nameText = document.createTextNode(' ' + p.full_name);
      a.appendChild(nameText);
      if(isHl){
        a.style.fontWeight = '700';
        if(cfg.circuit === 'WTA') a.style.color = '#00449e'; // blue-ish for highlight? (user asked blue for WTA, but earlier said blue for WTA and red for ATP — adapt)
        else a.style.color = '#c82333'; // red for ATP highlight
      }
      tdName.appendChild(a);
      tr.appendChild(tdName);

      const tdAge = document.createElement('td');
      tdAge.textContent = (p.age != null) ? p.age : '—';
      tdAge.className = 'small-muted';
      tr.appendChild(tdAge);

      const tdPoints = document.createElement('td');
      tdPoints.textContent = fmtNum(p.points);
      tr.appendChild(tdPoints);

      tbody.appendChild(tr);
    });

    renderPagination(Math.ceil(filtered.length / per));
  }

  function renderPagination(totalPages){
    const ul = el('pagination');
    ul.innerHTML = '';
    const max = totalPages || 1;
    const cur = currentPage;
    function mk(n, text){
      const li = document.createElement('li');
      li.className = 'page-item' + (n===cur?' active':'');
      const a = document.createElement('a');
      a.className = 'page-link';
      a.href = '#';
      a.textContent = text || String(n);
      a.onclick = (ev)=>{ ev.preventDefault(); currentPage = n; renderPage(); };
      li.appendChild(a);
      return li;
    }
    if(max <= 1) return;
    // Prev
    const prev = document.createElement('li');
    prev.className = 'page-item' + (cur===1?' disabled':'');
    const pa = document.createElement('a'); pa.className='page-link'; pa.href='#'; pa.textContent='«';
    pa.onclick = (ev)=>{ ev.preventDefault(); if(cur>1){ currentPage--; renderPage(); } };
    prev.appendChild(pa); ul.appendChild(prev);
    // pages (simple window)
    const windowSize = 7;
    let start = Math.max(1, cur - Math.floor(windowSize/2));
    let end = Math.min(max, start + windowSize - 1);
    if(end - start + 1 < windowSize){
      start = Math.max(1, end - windowSize + 1);
    }
    for(let i=start;i<=end;i++) ul.appendChild(mk(i));
    const next = document.createElement('li');
    next.className = 'page-item' + (cur>=max?' disabled':'');
    const na = document.createElement('a'); na.className='page-link'; na.href='#'; na.textContent='»';
    na.onclick = (ev)=>{ ev.preventDefault(); if(cur<max){ currentPage++; renderPage(); } };
    next.appendChild(na); ul.appendChild(next);
  }

  function countryCodeToEmoji(code){
    if(!code) return '';
    code = code.trim().toUpperCase();
    try{
      return String.fromCodePoint(127397 + code.charCodeAt(0), 127397 + code.charCodeAt(1));
    }catch(e){
      return '';
    }
  }

  function attachHandlers(){
    el('age_le').addEventListener('input', applyFilters);
    el('age_ge').addEventListener('input', applyFilters);
    el('filter_country').addEventListener('change', applyFilters);
    el('highlight_country').addEventListener('change', renderPage);
    el('reset_filters').addEventListener('click', function(){
      el('age_le').value = ''; el('age_ge').value = '';
      el('filter_country').value = ''; el('highlight_country').value = '';
      applyFilters();
    });
  }

  return {
    init: function(options){
      cfg = Object.assign({}, options);
      attachHandlers();
      load();
    }
  };
})();
