// assets/js/advanced-stats.js (REPLACEMENT)
// Advanced statistics tab + rendering (Plotly optional fallback to HTML).
(function(){
  'use strict';

  const ADV_STATS_FOLDER = '/general_advanced_stats/';

  // -------------------- Utilities --------------------
  function safeSlug(s){
    if(!s) return '';
    return String(s).trim().toLowerCase()
      .replace(/[^\w\s-]/g,'')    // remove special chars
      .replace(/\s+/g,'-')        // spaces -> dash
      .replace(/-+/g,'-');
  }
  async function findAdvancedJson(playerId, playerName, slug){
    const candidates = [];
    if(playerId && slug) candidates.push(`${playerId}_${slug}.json`);
    if(playerId && playerName) candidates.push(`${playerId}_${safeSlug(playerName)}.json`);
    if(playerId) candidates.push(`${playerId}.json`);
    for(const c of candidates.slice()) { candidates.push(c.toLowerCase()); candidates.push(c.toUpperCase()); }
    for(const fname of candidates){
      const url = ADV_STATS_FOLDER + fname;
      try{
        const res = await fetch(url, { method: 'GET' });
        if(res.ok){
          const json = await res.json();
          return { json, url };
        }
      }catch(e){}
    }
    return null;
  }
  function getPlayerMeta(){
    if(window.PLAYER_META && typeof window.PLAYER_META === 'object'){
      return {
        playerId: String(window.PLAYER_META.playerId || window.PLAYER_META.player_id || window.PLAYER_META.id || ''),
        playerName: window.PLAYER_META.playerName || window.PLAYER_META.player_name || '',
        slug: window.PLAYER_META.slug || safeSlug(window.PLAYER_META.playerName || window.PLAYER_META.player_name || '')
      };
    }
    const root = document.getElementById('player-root');
    if(root){
      return {
        playerId: String(root.dataset.playerId || root.dataset.player_id || ''),
        playerName: root.dataset.playerName || root.dataset.player_name || '',
        slug: root.dataset.playerSlug || root.dataset.player_slug || safeSlug(root.dataset.playerName || root.dataset.player_name || '')
      };
    }
    if(window.player && typeof window.player === 'object'){
      return {
        playerId: String(window.player.id || window.player.playerId || ''),
        playerName: window.player.name || '',
        slug: window.player.slug || safeSlug(window.player.name || '')
      };
    }
    return { playerId: '', playerName: '', slug: '' };
  }

  // Case-insensitive getter
  function ciGet(obj, key){
    if(!obj || !key) return undefined;
    if(Object.prototype.hasOwnProperty.call(obj, key)) return obj[key];
    const low = key.toLowerCase();
    for(const k of Object.keys(obj)){
      if(k.toLowerCase() === low) return obj[k];
    }
    return undefined;
  }
  function readFrom(obj, ...keys){
    if(!obj) return null;
    for(const k of keys){
      if(!k) continue;
      const v = ciGet(obj, k);
      if(v !== undefined && v !== null) return v;
    }
    return null;
  }

  // Normalize numeric / fraction values: accepts percents (37.9), fractions (0.379), or counts.
  function toNumRaw(v){
    if(v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return isNaN(n) ? null : n;
  }
  function toFrac(v){
    const n = toNumRaw(v);
    if(n === null) return null;
    if(Math.abs(n) > 1 && Math.abs(n) <= 100) return n/100;
    return n;
  }
  function fmtPct(f, digits=1){
    if(f === null || f === undefined) return '—';
    const perc = (Math.abs(f) > 1 && Math.abs(f) <= 100) ? f : (f*100);
    return (Math.round(perc * Math.pow(10,digits)) / Math.pow(10,digits)) + '%';
  }
  function fmtNum(v, digits=1){
    if(v === null || v === undefined) return '—';
    if(typeof v === 'number') return (Math.round(v * Math.pow(10,digits)) / Math.pow(10,digits)).toLocaleString();
    return String(v);
  }
  function sumVariants(obj, prefix, suffixes){
    // returns {count: <sum counts if any or null>, frac: <sum fracs if any or null>}
    let countSum = null;
    let fracSum = 0;
    let foundFrac = false;
    for(const s of suffixes){
      const v = readFrom(obj, prefix + s);
      if(v === null || v === undefined) continue;
      if(s.endsWith('_count') || s.endsWith('_num')) {
        const n = toNumRaw(v);
        if(n !== null) countSum = (countSum === null ? 0 : countSum) + n;
      } else if(s.endsWith('_frac') || s.endsWith('_pct') || s.indexOf('won_pct')!==-1 || s.indexOf('pct')!==-1) {
        const f = toFrac(v);
        if(f !== null){ fracSum += f; foundFrac = true; }
      } else {
        const n = toNumRaw(v);
        if(n !== null){ fracSum += toFrac(n) || 0; foundFrac = true; }
      }
    }
    return { count: countSum, frac: foundFrac ? fracSum : null };
  }

  // Plotly helpers: create shared legend element
  const GLOBAL_LEGEND_ID = '__adv_shared_legend';
  function ensureLegend(container){
    if(document.getElementById(GLOBAL_LEGEND_ID)) return;
    const legend = document.createElement('div');
    legend.id = GLOBAL_LEGEND_ID;
    legend.className = 'adv-shared-legend';
    legend.innerHTML = `
      <div class="legend-title">Legend</div>
      <div class="legend-items">
        <div class="legend-item"><span class="swatch swatch-cg"></span><span>Winner</span></div>
        <div class="legend-item"><span class="swatch swatch-fd"></span><span>Unforced Error</span></div>
        <div class="legend-item"><span class="swatch swatch-fp"></span><span>Forced Error</span></div>
      </div>
    `;
    container.insertBefore(legend, container.firstChild);
  }

  // -------------------- RENDERERS --------------------

  // RALLY STATS (summary, pies, is_leading table, shot series chart)
  function renderRallySection(small_stats){
    // small_stats can be either numeric obj or {columns:...}
    const numeric = small_stats && small_stats.columns ? small_stats.columns : (small_stats && small_stats.numeric ? small_stats.numeric : small_stats);
    if(!numeric) return '<div class="adv-section"><h3>Rally (point length) stats</h3><p class="muted">No rally stats available.</p></div>';

    // short / medium / long reading keys tolerant
    const get = (k) => {
      const v = ciGet(numeric, k);
      if(v !== undefined) return v;
      const low = k.toLowerCase();
      for(const kk of Object.keys(numeric)) if(kk.toLowerCase()===low) return numeric[kk];
      return null;
    };

    const buckets = [
      { key:'short', label:'Short', pctKey:'match_pct_short', winKey:'pct_won_short', loseKey:'pct_lost_short' },
      { key:'medium', label:'Medium', pctKey:'match_pct_medium', winKey:'pct_won_medium', loseKey:'pct_lost_medium' },
      { key:'long', label:'Long', pctKey:'match_pct_long', winKey:'pct_won_long', loseKey:'pct_lost_long' }
    ];

    // Build HTML skeleton
    let html = `<section class="adv-section" id="adv-rally"><h3>Rally (point length)</h3><div class="rally-summary">`;
    // pretty 3-column summary
    html += `<div class="rally-summary-grid">`;
    buckets.forEach(b => {
      const pct = toFrac(get(b.pctKey));
      const win = toFrac(get(b.winKey));
      html += `<div class="rally-cell"><div class="rally-label">${b.label}</div><div class="rally-pct">${pct===null?'—':fmtPct(pct,1)}</div><div class="rally-win">Win: ${win===null?'—':fmtPct(win,1)}</div></div>`;
    });
    html += `</div>`; // reveal grid
    html += `</div>`; // summary

    // legend container (one for the whole rally area)
    html += `<div id="adv-rally-legend" class="adv-legend-placeholder"></div>`;

    // Pie placeholders (two-by-two layout but leave space)
    html += `<div class="rally-pies">`;
    buckets.forEach((b, idx) => {
      html += `<div class="rally-pair"><div class="rally-pair-title">${b.label} — Win</div><div id="rally-pie-win-${idx}" class="rally-pie"></div><div id="rally-pie-win-info-${idx}" class="rally-pie-info"></div></div>`;
      html += `<div class="rally-pair"><div class="rally-pair-title">${b.label} — Lose</div><div id="rally-pie-lose-${idx}" class="rally-pie"></div><div id="rally-pie-lose-info-${idx}" class="rally-pie-info"></div></div>`;
    });
    html += `</div>`;

    // is_leading small table
    html += `<div class="rally-leading"><h4>Proportion of matches where player led (by rally range)</h4><table class="adv-table-small"><thead><tr><th>Range</th><th>Winner</th><th>Unforced Error</th><th>Forced Error</th></tr></thead><tbody>`;
    const leadingKeys = [
      {key:'short', label:'Short', suffix:'is_leading_short'},
      {key:'medium', label:'Medium', suffix:'is_leading_medium'},
      {key:'long', label:'Long', suffix:'is_leading_long'}
    ];
    leadingKeys.forEach(r=>{
      const cg = toFrac(get(r.suffix + '_CG')) ?? toFrac(get(r.suffix + '_cg')) ?? null;
      const fd = toFrac(get(r.suffix + '_FD')) ?? toFrac(get(r.suffix + '_fd')) ?? null;
      const fp = toFrac(get(r.suffix + '_FP')) ?? toFrac(get(r.suffix + '_fp')) ?? null;
      html += `<tr><td>${r.label}</td><td>${cg===null?'—':fmtPct(cg,1)}</td><td>${fd===null?'—':fmtPct(fd,1)}</td><td>${fp===null?'—':fmtPct(fp,1)}</td></tr>`;
    });
    html += `</tbody></table></div>`;

    // shot series placeholder (bigger)
    html += `<div id="adv-shot-series" class="adv-shot-series"></div>`;

    html += `</section>`;
    return html;
  }

  function drawRallyPies(numeric){
    const get = (k) => {
      const v = ciGet(numeric, k);
      if(v !== undefined) return v;
      const low = k.toLowerCase();
      for(const kk of Object.keys(numeric)) if(kk.toLowerCase()===low) return numeric[kk];
      return null;
    };
    const buckets = [
      { key:'short', label:'Short', winKey:'pct_won_short', loseKey:'pct_lost_short' },
      { key:'medium', label:'Medium', winKey:'pct_won_medium', loseKey:'pct_lost_medium' },
      { key:'long', label:'Long', winKey:'pct_won_long', loseKey:'pct_lost_long' }
    ];
    // ensure shared legend
    const container = document.getElementById('adv-rally') || document.querySelector('.tab-panel-advanced') || document.body;
    ensureLegend(container);

    buckets.forEach((b, idx) => {
      // win vals
      const winCG = toFrac(get(b.winKey + '_CG')) ?? toFrac(get(b.winKey + '_cg')) ?? 0;
      const winFD = toFrac(get(b.winKey + '_FD')) ?? toFrac(get(b.winKey + '_fd')) ?? 0;
      const winFP = toFrac(get(b.winKey + '_FP')) ?? toFrac(get(b.winKey + '_fp')) ?? 0;
      const loseCG = toFrac(get(b.loseKey + '_CG')) ?? toFrac(get(b.loseKey + '_cg')) ?? 0;
      const loseFD = toFrac(get(b.loseKey + '_FD')) ?? toFrac(get(b.loseKey + '_fd')) ?? 0;
      const loseFP = toFrac(get(b.loseKey + '_FP')) ?? toFrac(get(b.loseKey + '_fp')) ?? 0;

      // Plotly pies: hovertemplate shows only rounded percent, hide label & extra
      const hoverTpl = '%{percent:.0%}<extra></extra>';

      [['win', [winCG, winFD, winFP]], ['lose', [loseCG, loseFD, loseFP]]].forEach((pair,i2)=>{
        const mode = pair[0];
        const vals = pair[1];
        const domId = `rally-pie-${mode}-${idx}`.replace(/^rally-/, '').replace('-','-'); // not used, keep stable
        const el = document.getElementById(`rally-pie-${mode === 'win' ? 'win' : 'lose'}-${idx}`);
        const infoEl = document.getElementById(`rally-pie-${mode === 'win' ? 'win' : 'lose'}-info-${idx}`);
        if(!el) return;
        if(window.Plotly && typeof window.Plotly.newPlot === 'function'){
          const trace = [{ labels:['Winner','Unforced Error','Forced Error'], values: vals, type:'pie', marker:{colors:['#2563eb','#ef4444','#f59e0b']}, hovertemplate: hoverTpl, textinfo:'label+percent' }];
          Plotly.newPlot(el, trace, {height:160,width:220,margin:{t:8,l:4,r:4,b:8}, showlegend:false}, {displayModeBar:false});
          el.on('plotly_click', function(evt){
            const p = evt.points && evt.points[0];
            if(!p) return;
            // For rally pies we usually don't have counts; show percent only (rounded) — that's the spec
            if(infoEl) infoEl.innerHTML = `${p.label} — ${fmtPct(p.percent/100,0)}`;
          });
          // remove label from hover (we used hovertemplate to show only percent)
        } else {
          el.innerHTML = `<div style="font-size:12px;color:#666">Pie (no Plotly)</div><div>${['Winner','Unforced Error','Forced Error'].map((L,i)=>L+': '+fmtPct(vals[i])).join('<br>')}</div>`;
          if(infoEl) infoEl.innerHTML = '';
        }
      });
    });
  }

  // SHOT SERIES (3 curves) – larger area
  function drawShotSeries(numeric){
    // shotKeys order (service, return, 3rd... up to 10th)
    const shotKeys = [
      ['Service','shot_Service_won_pct_CG','shot_Service_won_pct_FD','shot_Service_won_pct_FP'],
      ['Return','shot_Retour_won_pct_CG','shot_Retour_won_pct_FD','shot_Retour_won_pct_FP'],
      ['3rd','shot_3me_coup_won_pct_CG','shot_3me_coup_won_pct_FD','shot_3me_coup_won_pct_FP'],
      ['4th','shot_4me_coup_won_pct_CG','shot_4me_coup_won_pct_FD','shot_4me_coup_won_pct_FP'],
      ['5th','shot_5me_coup_won_pct_CG','shot_5me_coup_won_pct_FD','shot_5me_coup_won_pct_FP'],
      ['6th','shot_6me_coup_won_pct_CG','shot_6me_coup_won_pct_FD','shot_6me_coup_won_pct_FP'],
      ['7th','shot_7me_coup_won_pct_CG','shot_7me_coup_won_pct_FD','shot_7me_coup_won_pct_FP'],
      ['8th','shot_8me_coup_won_pct_CG','shot_8me_coup_won_pct_FD','shot_8me_coup_won_pct_FP'],
      ['9th','shot_9_coups_impairs_won_pct_CG','shot_9_coups_impairs_won_pct_FD','shot_9_coups_impairs_won_pct_FP'],
      ['10th','shot_10_coups_pairs_won_pct_CG','shot_10_coups_pairs_won_pct_FD','shot_10_coups_pairs_won_pct_FP']
    ];
    const xs = [], yCG = [], yFD = [], yFP = [];
    shotKeys.forEach(s => {
      xs.push(s[0]);
      const a = toFrac(readFrom(numeric, s[1], s[1].toLowerCase())) ?? 0;
      const b = toFrac(readFrom(numeric, s[2], s[2].toLowerCase())) ?? 0;
      const c = toFrac(readFrom(numeric, s[3], s[3].toLowerCase())) ?? 0;
      yCG.push(a); yFD.push(b); yFP.push(c);
    });
    const el = document.getElementById('adv-shot-series');
    if(!el) return;
    // size larger
    if(window.Plotly && typeof window.Plotly.newPlot === 'function'){
      const traces = [
        { x: xs, y: yCG, name:'Winner', mode:'lines+markers', line:{color:'#2563eb'} },
        { x: xs, y: yFD, name:'Unforced Error', mode:'lines+markers', line:{color:'#ef4444'} },
        { x: xs, y: yFP, name:'Forced Error', mode:'lines+markers', line:{color:'#f59e0b'} }
      ];
      Plotly.newPlot(el, traces, {height:360, margin:{t:20,l:50,r:20,b:80}, yaxis:{tickformat:'.0%'}}, {displayModeBar:false});
    } else {
      let t = '<table class="adv-table" style="min-width:100%;"><thead><tr><th>Shot</th><th>Winner</th><th>Unforced Err</th><th>Forced Err</th></tr></thead><tbody>';
      for(let i=0;i<xs.length;i++){
        t += `<tr><td style="font-weight:700">${xs[i]}</td><td>${yCG[i]===0?'—':fmtPct(yCG[i],1)}</td><td>${yFD[i]===0?'—':fmtPct(yFD[i],1)}</td><td>${yFP[i]===0?'—':fmtPct(yFP[i],1)}</td></tr>`;
      }
      t += '</tbody></table>';
      el.innerHTML = t;
    }
  }

  // ---------- STROKE (big_stats) section ----------
  function renderStrokeSection(big_stats){
    if(!big_stats) return `<section class="adv-section"><h3>Stroke & match-level stats</h3><p class="muted">No stroke statistics available.</p></section>`;
    const numeric = big_stats.numeric ? big_stats.numeric : big_stats;

    // General info block (pretty labels)
    const infoMap = [
      ['matches_count','Matches (count)'],
      ['aces_per_match','Aces / match'],
      ['double_faults_per_match','Double faults / match'],
      ['vitesse_max_avg_per_match','Max serve speed (avg)'],
      ['vitesse_1st_mean_avg_per_match','1st serve mean speed (avg)'],
      ['winners_per_match','Winners / match'],
      ['direct_errors_per_match','Direct errors / match'],
      ['forced_errors_per_match','Forced errors / match'],
      ['points_at_net_per_match','Points at net / match'],
      ['white_games_per_match','White games / match']
    ];

    let html = `<section class="adv-section" id="adv-strokes"><h3>Stroke statistics</h3>`;
    html += `<div class="adv-grid">`;
    infoMap.forEach(p => {
      const v = readFrom(numeric, p[0]);
      if(v !== null && v !== undefined){
        html += `<div class="adv-kv"><dt>${p[1]}</dt><dd>${(Math.abs(v) <= 1 ? fmtPct(toFrac(v),1) : fmtNum(v,1))}</dd></div>`;
      }
    });
    html += `</div>`;

    // shot-type pies area + exclude exchange checkbox
    html += `<div style="margin-top:10px;display:flex;justify-content:space-between;align-items:center"><div style="font-weight:600">Shot type outcomes (fractions)</div><div><label style="font-size:13px"><input id="adv-exclude-exchange" type="checkbox" style="margin-right:6px">Exclude in-exchange</label></div></div>`;
    html += `<div class="shot-type-grid">`;
    const shotTypes = [
      { key:'shot_coups_fond_de_court', label:'Baseline' },
      { key:'shot_smashs', label:'Smash' },
      { key:'shot_passing', label:'Passing' },
      { key:'shot_volées', label:'Volley' },
      { key:'shot_montées_au_filet', label:'Net points' },
      { key:'shot_amorties', label:'Dropshot' },
      { key:'shot_lobs', label:'Lobs' }
    ];
    shotTypes.forEach((st, i)=>{
      html += `<div class="shot-type-card"><div class="shot-type-title">${st.label}</div><div id="shottype-pie-${i}" class="shottype-pie"></div><div id="shottype-pie-info-${i}" class="shottype-pie-info"></div></div>`;
    });
    html += `</div>`; // shot-type-grid

    // Break points & Return boxes
    html += `<div style="display:flex;gap:12px;flex-wrap:wrap;margin-top:12px">`;
    html += `<div class="bp-card"><div class="bp-title">Break points (summary)</div><div id="bp-block"></div></div>`;
    html += `<div class="return-card"><div class="bp-title">Return</div><div id="return-block"></div></div>`;
    html += `</div>`;

    html += `</section>`;
    return html;
  }

  // Draw shot type pies using big numeric. Sum typo variants.
  function drawShotTypePies(bigNumeric){
    if(!bigNumeric) return;
    const shotTypes = [
      { key:'shot_coups_fond_de_court', label:'Baseline' },
      { key:'shot_smashs', label:'Smash' },
      { key:'shot_passing', label:'Passing' },
      { key:'shot_volées', label:'Volley' },
      { key:'shot_montées_au_filet', label:'Net points' },
      { key:'shot_amorties', label:'Dropshot' },
      { key:'shot_lobs', label:'Lobs' }
    ];
    // candidate suffix variants to catch typos
    const winSuffixes = ['_outcome_coupsgagnants_frac','_outcome_coupgagnant_frac','_outcome_coupsgagnant_frac','_outcome_coupsgagnants_frac','_outcome_coupgagnants_frac','_outcome_coupgagnant_frac'];
    const provSuffixes = ['_outcome_fautesprovoquées_frac','_outcome_fautesprovoquée_frac','_outcome_fautesprovoquees_frac'];
    const dirSuffixes = ['_outcome_fautedirecte_frac','_outcome_fautesdirectes_frac','_outcome_fautedirectes_frac'];
    const exSuffixes = ['_outcome_coupsdansl_echange_frac','_outcome_coupsdanslexchange_frac','_outcome_coupsdansl\'echange_frac','_outcome_coupsdansl\u00E9change_frac','_outcome_coupsdansl\'échange_frac'];

    // prepare redraw registry for exclude-exchange toggle
    window.__adv_shot_drawers = window.__adv_shot_drawers || [];
    window.__adv_shot_drawers.length = 0;

    shotTypes.forEach((st,i)=>{
      // sum fracs and counts for the variants
      const win = sumVariants(bigNumeric, st.key, winSuffixes.concat(['_outcome_coupsgagnants_frac','_outcome_coupgagnant_frac']));
      const prov = sumVariants(bigNumeric, st.key, provSuffixes.concat(['_outcome_fautesprovoquée_frac','_outcome_fautesprovoquées_frac']));
      const dir = sumVariants(bigNumeric, st.key, dirSuffixes.concat(['_outcome_fautedirecte_frac','_outcome_fautesdirectes_frac']));
      // exchange
      const ex = sumVariants(bigNumeric, st.key, exSuffixes.concat(['_outcome_coupsdansl_echange_frac','_outcome_coupsdanslexchange_frac']));
      const totalCount = readFrom(bigNumeric, st.key + '_count', st.key + '_num');

      const valuesBase = [ (win.frac || 0), (prov.frac || 0), (dir.frac || 0) ];
      const labels = ['Winner','Unforced Error','Forced Error'];
      const colors = ['#2563eb','#ef4444','#f59e0b'];
      const el = document.getElementById(`shottype-pie-${i}`);
      const infoEl = document.getElementById(`shottype-pie-info-${i}`);

      const drawFunc = (excludeExchange=false) => {
        let vals = valuesBase.slice();
        if(excludeExchange && ex.frac){
          const denom = (valuesBase.reduce((a,b)=>a+b,0) + ex.frac);
          if(denom>0){
            const base = denom - ex.frac;
            if(base>0) vals = vals.map(v => (v / base) || 0);
          }
        }
        if(!el) return;
        if(window.Plotly && typeof window.Plotly.newPlot === 'function'){
          const hoverTpl = '%{percent:.0%}<extra></extra>'; // only rounded percent
          const trace = [{ labels, values: vals, type:'pie', marker:{colors}, hovertemplate: hoverTpl, textinfo:'label+percent' }];
          Plotly.newPlot(el, trace, {height:200,width:260,margin:{t:10,l:4,r:4,b:10}, showlegend:false}, {displayModeBar:false});
          el.on('plotly_click', function(evt){
            const p = evt.points && evt.points[0];
            if(!p) return;
            // Try to compute a count: prefer *_count fields for the outcome, else estimate if totalCount exists
            const outcomeKeySuffix = p.label === 'Winner' ? ['_outcome_coupsgagnants_count','_outcome_coupsgagnant_count'] :
                                     (p.label === 'Unforced Error' ? ['_outcome_fautesprovoquées_count','_outcome_fautesprovoquée_count'] :
                                     ['_outcome_fautedirecte_count','_outcome_fautesdirectes_count']);
            // try sum of candidate count keys
            let outcomeCount = null;
            for(const sfx of outcomeKeySuffix){
              const v = readFrom(bigNumeric, st.key + sfx);
              if(v !== null && v !== undefined) outcomeCount = (outcomeCount === null ? 0 : outcomeCount) + toNumRaw(v);
            }
            if(outcomeCount !== null){
              if(infoEl) infoEl.innerHTML = `${st.label} — ${p.label}: ${Math.round(outcomeCount)}`;
              return;
            }
            // else try estimate with totalCount * frac
            if(totalCount !== null && toNumRaw(totalCount) !== null){
              const cnt = Math.round((p.value) * toNumRaw(totalCount));
              if(infoEl) infoEl.innerHTML = `${st.label} — ${p.label}: ${cnt} (estimated)`;
              return;
            }
            // fallback show percent
            if(infoEl) infoEl.innerHTML = `${st.label} — ${p.label}: ${fmtPct(p.percent/100,0)}`;
          });
        } else {
          // fallback HTML display
          el.innerHTML = `<div style="font-size:12px;color:#666">Pie (no Plotly)</div><div>${labels.map((L,idx)=>L+': '+fmtPct(vals[idx])).join('<br>')}</div>`;
          if(infoEl) infoEl.innerHTML = '';
        }
      };

      // initial draw (exclude = false)
      drawFunc(false);
      window.__adv_shot_drawers.push(drawFunc);
    });
  }

  function drawBreakpointsAndReturn(bigNumeric){
    const containerBP = document.getElementById('bp-block');
    const containerReturn = document.getElementById('return-block');
    if(!containerBP || !containerReturn) return;
    const bdb_played_num = readFrom(bigNumeric, 'bdb_played_num','bdb_played_count');
    const bdb_played_den = readFrom(bigNumeric, 'bdb_played_den','bdb_played_total');
    const bdb_played_pct = toFrac(readFrom(bigNumeric, 'bdb_played_pct'));
    const bdb_converted_num = readFrom(bigNumeric, 'bdb_converted_num','bdb_converted_count');
    const bdb_converted_den = readFrom(bigNumeric, 'bdb_converted_den','bdb_converted_total');
    const bdb_converted_pct = toFrac(readFrom(bigNumeric, 'bdb_converted_pct'));
    const bdb_games_with_num = readFrom(bigNumeric, 'bdb_games_with_num','bdb_games_with_count');
    const bdb_games_with_den = readFrom(bigNumeric, 'bdb_games_with_den','bdb_games_with_total');
    const bdb_games_with_pct = toFrac(readFrom(bigNumeric, 'bdb_games_with_pct'));

    // mean break points per match = bdb_played_num / matches_count
    const matches_count = readFrom(bigNumeric, 'matches_count','matchesCount','matches_count');
    let mean_bp = null;
    if(bdb_played_num !== null && matches_count !== null && toNumRaw(matches_count) !== null){
      mean_bp = toNumRaw(bdb_played_num) / toNumRaw(matches_count);
    }

    containerBP.innerHTML = `
      <div class="bp-row"><div class="bp-main">${bdb_converted_pct===null?'—':fmtPct(bdb_converted_pct,1)}</div>
        <div class="bp-sub">Converted <span class="muted">(${bdb_converted_num===null?'—':bdb_converted_num}/${bdb_converted_den===null?'—':bdb_converted_den})</span></div></div>
      <div class="bp-row"><div class="bp-main">${bdb_games_with_pct===null?'—':fmtPct(bdb_games_with_pct,1)}</div>
        <div class="bp-sub">Games with BP <span class="muted">(${bdb_games_with_num===null?'—':bdb_games_with_num}/${bdb_games_with_den===null?'—':bdb_games_with_den})</span></div></div>
      <div style="margin-top:8px;color:#666;font-size:13px">Mean break points / match: <strong>${mean_bp===null?'—':fmtNum(mean_bp,2)}</strong></div>
    `;

    const return_points_pct = toFrac(readFrom(bigNumeric, 'return_points_pct'));
    const return_points_num = readFrom(bigNumeric, 'return_points_num');
    const return_points_den = readFrom(bigNumeric, 'return_points_den');
    const return_1st_pct = toFrac(readFrom(bigNumeric, 'return_1st_pct'));
    const return_1st_num = readFrom(bigNumeric, 'return_1st_num');
    const return_1st_den = readFrom(bigNumeric, 'return_1st_den');
    const return_2nd_pct = toFrac(readFrom(bigNumeric, 'return_2nd_pct'));
    const return_2nd_num = readFrom(bigNumeric, 'return_2nd_num');
    const return_2nd_den = readFrom(bigNumeric, 'return_2nd_den');

    containerReturn.innerHTML = `
      <div class="bp-row"><div class="bp-main">${return_points_pct===null?'—':fmtPct(return_points_pct,1)}</div>
        <div class="bp-sub">Points won on return <span class="muted">(${return_points_num===null?'—':return_points_num}/${return_points_den===null?'—':return_points_den})</span></div></div>
      <div class="bp-row"><div class="bp-main">${return_1st_pct===null?'—':fmtPct(return_1st_pct,1)}</div>
        <div class="bp-sub">Return of 1st serve <span class="muted">(${return_1st_num===null?'—':return_1st_num}/${return_1st_den===null?'—':return_1st_den})</span></div></div>
      <div class="bp-row"><div class="bp-main">${return_2nd_pct===null?'—':fmtPct(return_2nd_pct,1)}</div>
        <div class="bp-sub">Return of 2nd serve <span class="muted">(${return_2nd_num===null?'—':return_2nd_num}/${return_2nd_den===null?'—':return_2nd_den})</span></div></div>
    `;
  }

  // -------------------- Main injection --------------------
  function injectAdvancedTab(json, jsonUrl){
    // find nav/content as before, fallback small nav near #player-root
    const tabNav = document.querySelector('#tabs-nav, .tabs-nav, #player-tabs .tabs-nav, .player-tabs .tabs-nav');
    const tabContentRoot = document.querySelector('#tabs-content, .tabs-content, #player-tabs .tabs-content, .player-tabs .tabs-content');
    const tablist = document.querySelector('[role="tablist"]');
    const tabpanelRoot = document.querySelector('[role="tabpanel"]');

    let nav, contentRoot;
    if(tabNav && tabContentRoot){ nav = tabNav; contentRoot = tabContentRoot; }
    else if(tablist && tabpanelRoot){ nav = tablist; contentRoot = tabpanelRoot.parentElement || tabpanelRoot; }
    else {
      const playerRoot = document.getElementById('player-root') || document.body;
      const container = document.createElement('div');
      container.className = 'player-advanced-tabs-fallback';
      container.innerHTML = `<div class="tabs-nav" id="__adv_tabs_nav"></div><div class="tabs-content" id="__adv_tabs_content"></div>`;
      playerRoot.insertBefore(container, playerRoot.firstChild);
      nav = container.querySelector('#__adv_tabs_nav');
      contentRoot = container.querySelector('#__adv_tabs_content');
    }

    const tabBtn = document.createElement('button');
    tabBtn.type = 'button';
    tabBtn.className = 'tab-item tab-item-advanced';
    tabBtn.setAttribute('role','tab');
    tabBtn.setAttribute('aria-controls','tab-advanced-stats');
    tabBtn.textContent = 'Advanced statistics';
    nav.appendChild(tabBtn);

    const panel = document.createElement('section');
    panel.id = 'tab-advanced-stats';
    panel.className = 'tab-panel tab-panel-advanced';
    panel.setAttribute('role','tabpanel');
    panel.style.display = 'none';
    panel.innerHTML = `<div class="adv-meta"><p class="muted">Data source: ${jsonUrl}</p></div><div class="adv-body">Loading…</div>`;
    contentRoot.appendChild(panel);

    tabBtn.addEventListener('click', ()=>{
      const siblings = (contentRoot.querySelectorAll('.tab-panel'));
      siblings.forEach(s => { s.style.display = 'none'; s.setAttribute('aria-hidden','true'); });
      const otherBtns = (nav.querySelectorAll('.tab-item'));
      otherBtns.forEach(b => b.classList.remove('active'));
      panel.style.display = '';
      panel.setAttribute('aria-hidden','false');
      tabBtn.classList.add('active');

      const body = panel.querySelector('.adv-body');
      if(body && body.dataset.rendered === '1') return;
      // Compose sections: rally first, then strokes, then draw interactive pieces
      const htmlParts = [];
      htmlParts.push(renderRallySection(json.small_stats));
      htmlParts.push(renderStrokeSection(json.big_stats));
      body.innerHTML = htmlParts.join('\n');
      body.dataset.rendered = '1';

      // Post-render: draw pies/charts
      try{
        if(json.small_stats){
          const numeric = json.small_stats.columns ? json.small_stats.columns : (json.small_stats.numeric ? json.small_stats.numeric : json.small_stats);
          drawRallyPies(numeric);
          drawShotSeries(numeric);
        }
        if(json.big_stats){
          const bigNumeric = json.big_stats.numeric ? json.big_stats.numeric : json.big_stats;
          drawShotTypePies(bigNumeric);
          drawBreakpointsAndReturn(bigNumeric);
          // wire exclude-exchange checkbox to redraw shotType pies
          const chk = document.getElementById('adv-exclude-exchange');
          if(chk){
            chk.addEventListener('change', ()=>{
              const exclude = !!chk.checked;
              if(window.__adv_shot_drawers && window.__adv_shot_drawers.length){
                window.__adv_shot_drawers.forEach(fn => {
                  try{ fn(exclude); }catch(e){ console.warn('redraw error', e); }
                });
              }
            });
          }
        }
        // ensure a shared legend near rally
        const rallyContainer = document.getElementById('adv-rally') || body;
        ensureLegend(rallyContainer);
      }catch(e){
        console.error('post-render error', e);
      }
    });

    // optionally auto-open? not by default
  }

  // -------------------- Init --------------------
  async function init(){
    const meta = getPlayerMeta();
    if(!meta.playerId) return;
    const found = await findAdvancedJson(meta.playerId, meta.playerName, meta.slug);
    if(found && found.json) injectAdvancedTab(found.json, found.url);
  }

  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();

})();