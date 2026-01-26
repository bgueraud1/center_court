/* site_static/js/rankings.js
   Renders whole ranking table (all rows), with flag images + emoji fallback,
   filters (age le/ge, country) and highlight.
*/

(function(window){
  // helpers
  function escapeHtml(s){ if(s==null) return ''; return String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'); }
  function emojiFromAlpha2(alpha2){
    if(!alpha2) return '';
    const s = alpha2.trim().toUpperCase();
    if(!/^[A-Z]{2}$/.test(s)) return '';
    try { return String.fromCodePoint(127397 + s.charCodeAt(0), 127397 + s.charCodeAt(1)); }
    catch(e){ return ''; }
  }
  function createFlagImageElement(alpha2, size = 20, fallbackEmoji = ''){
    if(!alpha2 || typeof alpha2 !== 'string') {
      if(fallbackEmoji){
        const sp = document.createElement('span'); sp.className='flag-emoji'; sp.textContent = fallbackEmoji + ' ';
        return sp;
      }
      return document.createTextNode('');
    }
    const code = alpha2.trim().toLowerCase();
    // Use small PNG (works well cross-browser). If you prefer SVG: https://flagcdn.com/{code}.svg
    const src = `https://flagcdn.com/${size}x${Math.round(size*0.75)}/${code}.png`;
    const img = document.createElement('img');
    img.src = src;
    img.alt = code;
    img.width = size;
    img.height = Math.round(size*0.75);
    img.className = 'flag-img';
    img.style.marginRight = '.4rem';
    img.style.verticalAlign = 'middle';
    img.loading = 'lazy';
    img.onerror = function(){
      try {
        const span = document.createElement('span');
        span.className = 'flag-emoji';
        span.textContent = (fallbackEmoji || emojiFromAlpha2(alpha2) || alpha2.toUpperCase()) + ' ';
        if(this.parentNode) this.parentNode.replaceChild(span, this);
      } catch(e){}
    };
    return img;
  }

  async function tryFetchMany(candidates){
    for(const u of candidates){
      try{
        const res = await fetch(u, {cache: 'no-store'});
        if(res.ok){
          const json = await res.json();
          return {json, url: u};
        }
      }catch(e){
        // ignore, try next
      }
    }
    throw new Error('No candidate JSON fetched');
  }

  // main renderer
  async function render(options){
    options = options || {};
    const jsonUrlCandidates = options.jsonUrlCandidates || [
      "/docs/tools/latest_wta_ranking.json",
      "/docs/tools/latest_atp_ranking.json",
      "/tools/latest_wta_ranking.json",
      "/tools/latest_atp_ranking.json",
      "/docs/tools/" + (options.jsonFile || ""),
      options.jsonUrl || "./latest_" + (options.circuit||'') + "_ranking.json",
      "../docs/tools/" + (options.jsonFile || ""),
      "../../docs/tools/" + (options.jsonFile || ""),
      options.jsonFile || ""
    ];
    const circuit = (options.circuit || 'WTA').toUpperCase();

    const statusEl = document.getElementById('status');
    const tbody = document.querySelector('#ranking_table tbody');
    const ageLeEl = document.getElementById('age_le');
    const ageGeEl = document.getElementById('age_ge');
    const filterCountryEl = document.getElementById('filter_country');
    const highlightEl = document.getElementById('highlight_country');
    const resetBtn = document.getElementById('reset_filters');

    function setStatus(t){ if(statusEl) statusEl.textContent = t; }

    setStatus('Loading…');

    let data, sourceUrl;
    try {
      const res = await tryFetchMany(jsonUrlCandidates);
      data = res.json;
      sourceUrl = res.url;
    } catch(e){
      console.error('Rankings JSON load failed', e);
      setStatus('Failed to load ranking JSON — see console');
      return;
    }

    // normalize structure minimally
    data = (Array.isArray(data) ? data : []).map((r) => ({
      ranking: r.ranking != null ? Number(r.ranking) : (r.rank != null ? Number(r.rank) : null),
      full_name: r.full_name || r.fullName || r.fullName || r.name || '',
      player_id: r.player_id || r.id || '',
      player_slug: r.player_slug || (r.full_name ? r.full_name.replace(/\s+/g,'-').toLowerCase() : ''),
      player_url: r.player_url || r.playerUrl || (r.player_id ? (circuit==='ATP' ? `/players_atp/${r.player_id}-${r.player_slug || ''}.html` : `/players/${r.player_id}-${r.player_slug || ''}.html`) : (circuit==='ATP' ? `/players_atp/${r.player_slug || ''}.html` : `/players/${r.player_slug || ''}.html`)),
      birth_date: r.birth_date || r.birthDate || r.birth || '',
      age: (r.age === null || r.age === undefined) ? null : (isNaN(Number(r.age)) ? null : Number(r.age)),
      points: (r.points === null || r.points === undefined) ? null : (isNaN(Number(r.points)) ? r.points : Number(r.points)),
      country_code: (r.country_code || r.country || r.country_name || '').toString().trim().toUpperCase(),
      country_name: r.country_name || r.country || '',
      flag_emoji: r.flag_emoji || r.flag || ''
    }));

    // if some ages missing, compute client-side from birth_date + ranking date
    const rankingDate = (data.length && data[0].date) ? new Date(data[0].date) : new Date();
    data.forEach(d=>{
      if((d.age === null || d.age === undefined) && d.birth_date){
        const bd = new Date(d.birth_date);
        if(!isNaN(bd.getTime())){
          const diffDays = (rankingDate - bd) / (1000*60*60*24);
          const years = Math.floor(diffDays / 365.25);
          d.age = Number.isFinite(years) ? years : null;
        }else{
          // fallback: try parsing common formats with Date parsing hacks (Month DD YYYY)
          const tryIso = Date.parse(d.birth_date.replace(/\s+/, ' '));
          if(!isNaN(tryIso)){
            const bd2 = new Date(tryIso);
            const years = Math.floor((rankingDate - bd2) / (1000*60*60*24*365.25));
            d.age = Number.isFinite(years) ? years : null;
          } else {
            d.age = null;
          }
        }
      }
    });

    // populate country lists
    const countrySet = new Map();
    data.forEach(d=>{
      const cc = (d.country_code||'').toString().trim().toUpperCase();
      if(cc) countrySet.set(cc, d.country_name || cc);
    });
    // populate selects
    if(filterCountryEl){
      filterCountryEl.innerHTML = '<option value="">— all —</option>';
      Array.from(countrySet.keys()).sort().forEach(cc=>{
        const opt = document.createElement('option'); opt.value = cc; opt.textContent = `${countrySet.get(cc)} (${cc})`; filterCountryEl.appendChild(opt);
      });
    }
    if(highlightEl){
      highlightEl.innerHTML = '<option value="">— none —</option>';
      Array.from(countrySet.keys()).sort().forEach(cc=>{
        const opt = document.createElement('option'); opt.value = cc; opt.textContent = `${countrySet.get(cc)} (${cc})`; highlightEl.appendChild(opt);
      });
    }

    function buildRow(d, highlightCountry){
      const tr = document.createElement('tr');
      tr.className = (circuit === 'WTA') ? 'row-wta' : 'row-atp';

      // ranking
      const tdR = document.createElement('td'); tdR.className='small text-muted'; tdR.textContent = (d.ranking!=null ? String(d.ranking) : '—'); tr.appendChild(tdR);

      // player (flag + link)
      const tdP = document.createElement('td');
      tdP.className = 'name-cell';
      const strong = document.createElement('strong');

      // flag image prefered
      const cc = (d.country_code||'').toString().trim().toUpperCase();
      const fallbackEmoji = (d.flag_emoji && d.flag_emoji.length) ? d.flag_emoji : (cc ? emojiFromAlpha2(cc) : '');
      const flagNode = createFlagImageElement(cc, 20, fallbackEmoji);
      if(flagNode) strong.appendChild(flagNode);

      const a = document.createElement('a');
      a.href = d.player_url || '#';
      a.textContent = d.full_name || '(unknown)';
      if(highlightCountry && cc && cc === highlightCountry.toUpperCase()){
        a.classList.add(circuit === 'WTA' ? 'highlight-wta' : 'highlight-atp');
      }
      strong.appendChild(a);
      tdP.appendChild(strong);
      tr.appendChild(tdP);

      // age
      const tdA = document.createElement('td'); tdA.className='small-muted'; tdA.textContent = (d.age == null ? '—' : String(d.age)); tr.appendChild(tdA);

      // points
      const tdPts = document.createElement('td'); tdPts.className='small-muted'; tdPts.textContent = (d.points == null ? '—' : String(d.points)); tr.appendChild(tdPts);

      return tr;
    }

    // render function (renders ALL rows)
    function renderAll(){
      tbody.innerHTML = '';
      const ageLe = (ageLeEl && ageLeEl.value.trim() !== '') ? Number(ageLeEl.value) : null;
      const ageGe = (ageGeEl && ageGeEl.value.trim() !== '') ? Number(ageGeEl.value) : null;
      const filterCountry = (filterCountryEl && filterCountryEl.value) ? filterCountryEl.value.toUpperCase() : null;
      const highlightCountry = (highlightEl && highlightEl.value) ? highlightEl.value.toUpperCase() : null;

      const filtered = data.filter(d=>{
        const age = (d.age == null ? null : Number(d.age));
        if(ageLe != null && (age == null || age > ageLe)) return false;
        if(ageGe != null && (age == null || age < ageGe)) return false;
        if(filterCountry && ((d.country_code||'').toString().trim().toUpperCase() !== filterCountry)) return false;
        return true;
      }).sort((a,b)=>((a.ranking||1e9) - (b.ranking||1e9)));

      // append all rows
      for(const r of filtered){
        tbody.appendChild(buildRow(r, highlightCountry));
      }
      setStatus(`${filtered.length} players — source: ${sourceUrl}`);
    }

    // events
    [ageLeEl, ageGeEl, filterCountryEl, highlightEl].forEach(el=>{
      if(!el) return;
      el.addEventListener('input', renderAll);
      el.addEventListener('change', renderAll);
    });
    if(resetBtn){
      resetBtn.addEventListener('click', (ev)=>{
        ev.preventDefault();
        if(ageLeEl) ageLeEl.value='';
        if(ageGeEl) ageGeEl.value='';
        if(filterCountryEl) filterCountryEl.value='';
        if(highlightEl) highlightEl.value='';
        renderAll();
      });
    }

    // first render
    renderAll();
  } // render

  // expose a simple initializer
  window.RankingsUI = {
    init: function(opts){ render(opts).catch(err => { console.error(err); if(document.getElementById('status')) document.getElementById('status').textContent = 'Error rendering rankings (see console)'; }); }
  };
})(window);
