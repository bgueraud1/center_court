// assets/js/advanced-stats.js
// Adds an "Advanced statistics" tab to a player's profile if a JSON exists under /general_advanced_stats/
// Assumptions: the profile page exposes either a global PLAYER_META = { playerId: "310926", playerName: "Laura Siegemund", slug: "310926-laura-siegemund" }
// or data attributes on a container: <div id="player-root" data-player-id="310926" data-player-name="Laura Siegemund" data-player-slug="310926-laura-siegemund"></div>

(function(){
  'use strict';

  // CONFIG: folder where advanced stats are stored
  const ADV_STATS_FOLDER = '/general_advanced_stats/';

  // Utility: safe slugify (for constructing filenames)
  function safeSlug(s){
    if(!s) return '';
    return String(s).trim().toLowerCase()
      .replace(/[^\w\s-]/g,'')    // remove special chars
      .replace(/\s+/g,'-')        // spaces -> dash
      .replace(/-+/g,'-');
  }

  // Format helpers for display
  function fmtNumber(v, decimals=1){
    if(v === null || v === undefined) return '—';
    if(typeof v === 'number'){
      if(Math.abs(v) >= 100 || decimals === 0) return String(Math.round(v));
      return v.toFixed(decimals);
    }
    return String(v);
  }
  function fmtPct(v, decimals=1){
    if(v === null || v === undefined) return '—';
    return fmtNumber(v, decimals) + '%';
  }
  function fmtMeanStd(obj, isPct=false){
    if(!obj) return '—';
    const mean = obj.mean;
    const std = obj.std;
    const n = obj.non_null_samples ?? obj.n ?? '';
    const val = isPct ? fmtPct(mean) : fmtNumber(mean);
    const stdStr = (std !== undefined && std !== null) ? (' ± ' + fmtNumber(std)) : '';
    return `${val}${stdStr} ${n ? ` (n=${n})` : ''}`;
  }

  // Build few filename candidates from playerId and playerName/slug
  async function findAdvancedJson(playerId, playerName, slug){
    const candidates = [];
    if(playerId && slug) candidates.push(`${playerId}_${slug}.json`);
    if(playerId && playerName) candidates.push(`${playerId}_${safeSlug(playerName)}.json`);
    if(playerId) candidates.push(`${playerId}.json`);
    // try lower/upper mixes
    for(const c of candidates.slice()) {
      candidates.push(c.toLowerCase());
      candidates.push(c.toUpperCase());
    }

    for(const fname of candidates){
      const url = ADV_STATS_FOLDER + fname;
      try{
        const res = await fetch(url, { method: 'GET' });
        if(res.ok){
          const json = await res.json();
          return { json, url };
        }
      }catch(e){
        // ignore network errors for candidates
      }
    }
    return null;
  }

  // Find player meta on page
  function getPlayerMeta(){
    if(window.PLAYER_META && typeof window.PLAYER_META === 'object'){
      return {
        playerId: String(window.PLAYER_META.playerId || window.PLAYER_META.player_id || window.PLAYER_META.id || ''),
        playerName: window.PLAYER_META.playerName || window.PLAYER_META.player_name || window.PLAYER_META.playerName || '',
        slug: window.PLAYER_META.slug || safeSlug(window.PLAYER_META.playerName || window.PLAYER_META.player_name || '')
      };
    }
    // fallback: try data attributes on #player-root
    const root = document.getElementById('player-root');
    if(root){
      return {
        playerId: String(root.dataset.playerId || root.dataset.player_id || ''),
        playerName: root.dataset.playerName || root.dataset.player_name || '',
        slug: root.dataset.playerSlug || root.dataset.player_slug || safeSlug(root.dataset.playerName || root.dataset.player_name || '')
      };
    }
    // last resort: try global variables inserted by backend
    if(window.player && typeof window.player === 'object'){
      return {
        playerId: String(window.player.id || window.player.playerId || ''),
        playerName: window.player.name || '',
        slug: window.player.slug || safeSlug(window.player.name || '')
      };
    }
    return { playerId: '', playerName: '', slug: '' };
  }

  // Render short/rally stats table
  function renderRallyStats(small_stats){
    if(!small_stats || !small_stats.numeric) return '<p>No rally (short) stats available.</p>';

    const numeric = small_stats.numeric;
    // short / medium / long sets
    const rows = [
      { key: 'short', label: 'Short rallies (1–4 shots)', pctKey: 'pct_short', cgKey: 'pct_short_cg', fdKey: 'pct_short_fd', fpKey: 'pct_short_fp', combinedKey: 'pct_short_cg_fd_fp_combined' },
      { key: 'medium', label: 'Medium rallies (5–8 shots)', pctKey: 'pct_medium', cgKey: 'pct_medium_cg', fdKey: 'pct_medium_fd', fpKey: 'pct_medium_fp', combinedKey: 'pct_medium_cg_fd_fp_combined' },
      { key: 'long', label: 'Long rallies (≥9 shots)', pctKey: 'pct_long', cgKey: 'pct_long_cg', fdKey: 'pct_long_fd', fpKey: 'pct_long_fp', combinedKey: 'pct_long_cg_fd_fp_combined' },
    ];

    let html = '<section class="adv-section"><h3>Rally (point length) stats — aggregated</h3>';
    html += '<p class="muted">Percentages are averages across matches (mean ± std where available).</p>';
    html += '<div class="adv-table-wrap"><table class="adv-table" aria-label="Rally statistics"><thead><tr><th>Rally length</th><th>% of points</th><th>% winners (avg)</th><th>% unforced errors (avg)</th><th>% forced errors (avg)</th><th>% combined (CG+FD+FP)</th></tr></thead><tbody>';
    for(const r of rows){
      const pct = fmtMeanStd(numeric[r.pctKey] ? numeric[r.pctKey] : numeric[`pct_${r.key}`], true);
      const cg = fmtMeanStd(numeric[r.cgKey], true);
      const fd = fmtMeanStd(numeric[r.fdKey], true);
      const fp = fmtMeanStd(numeric[r.fpKey], true);
      const comb = fmtMeanStd(numeric[r.combinedKey], true);
      html += `<tr>
        <td>${r.label}</td>
        <td>${pct}</td>
        <td>${cg}</td>
        <td>${fd}</td>
        <td>${fp}</td>
        <td>${comb}</td>
      </tr>`;
    }
    html += '</tbody></table></div></section>';
    return html;
  }

  // Render stroke / big stats
  function renderStrokeStats(big_stats, small_stats){
    // big_stats may be null. We'll render any relevant numeric fields available (from small or big)
    const numeric = (big_stats && big_stats.numeric) ? big_stats.numeric : (small_stats ? small_stats.numeric : null);
    if(!numeric) return '<p>No stroke stats available.</p>';

    // pick a set of readable keys (if present)
    const candidates = [
      { key: 'total_points_won', label: 'Total points won (avg)' },
      { key: 'CG_count', label: 'Winners — count (avg)' },
      { key: 'FD_count', label: 'Unforced errors — count (avg)' },
      { key: 'FP_count', label: 'Forced errors — count (avg)' },
      { key: 'DF_count', label: 'Double faults — count (avg)' },
      { key: 'CG_mean_shot', label: 'Winner — mean shot (avg)' },
      { key: 'FD_mean_shot', label: 'Unforced error — mean shot (avg)' },
      { key: 'FP_mean_shot', label: 'Forced error — mean shot (avg)' }
    ];
    let html = '<section class="adv-section"><h3>Stroke & match-level stats — aggregated</h3>';
    html += '<p class="muted">Shown as mean ± std where available.</p>';
    html += '<div class="adv-grid"><dl>';
    for(const c of candidates){
      if(numeric[c.key] !== undefined && numeric[c.key] !== null){
        html += `<div class="adv-kv"><dt>${c.label}</dt><dd>${fmtMeanStd(numeric[c.key], /pct|pct_/.test(c.key))}</dd></div>`;
      }
    }
    html += '</dl></div></section>';
    return html;
  }

  // Main renderer: given JSON, inject tab + content
  function injectAdvancedTab(json, jsonUrl){
    // Find tab nav container and tab content container on page.
    // Common patterns: #tabs-nav, .tabs-nav, .tabs, #player-tabs, .player-tabs
    const tabNav = document.querySelector('#tabs-nav, .tabs-nav, #player-tabs .tabs-nav, .player-tabs .tabs-nav');
    const tabContentRoot = document.querySelector('#tabs-content, .tabs-content, #player-tabs .tabs-content, .player-tabs .tabs-content');

    // Fallback: try generic element with role="tablist"
    const tablist = document.querySelector('[role="tablist"]');
    const tabpanelRoot = document.querySelector('[role="tabpanel"]');

    // If none detected, create a small tab area near top of #player-root (non-destructive)
    let nav, contentRoot;
    if(tabNav && tabContentRoot){
      nav = tabNav;
      contentRoot = tabContentRoot;
    } else if(tablist && tabpanelRoot){
      nav = tablist;
      contentRoot = tabpanelRoot.parentElement || tabpanelRoot;
    } else {
      // fallback: create a simple nav at top of page within #player-root
      const playerRoot = document.getElementById('player-root') || document.body;
      const container = document.createElement('div');
      container.className = 'player-advanced-tabs-fallback';
      container.innerHTML = `
        <div class="tabs-nav" id="__adv_tabs_nav"></div>
        <div class="tabs-content" id="__adv_tabs_content"></div>
      `;
      playerRoot.insertBefore(container, playerRoot.firstChild);
      nav = container.querySelector('#__adv_tabs_nav');
      contentRoot = container.querySelector('#__adv_tabs_content');
    }

    // Create a tab button
    const tabBtn = document.createElement('button');
    tabBtn.type = 'button';
    tabBtn.className = 'tab-item tab-item-advanced';
    tabBtn.setAttribute('role','tab');
    tabBtn.setAttribute('aria-controls','tab-advanced-stats');
    tabBtn.textContent = 'Advanced statistics';
    nav.appendChild(tabBtn);

    // Create content panel (hidden initially)
    const panel = document.createElement('section');
    panel.id = 'tab-advanced-stats';
    panel.className = 'tab-panel tab-panel-advanced';
    panel.setAttribute('role','tabpanel');
    panel.style.display = 'none';
    panel.innerHTML = `<div class="adv-meta"><p class="muted">Data source: aggregated Infosys (Roland-Garros) exports — file: ${jsonUrl}</p></div><div class="adv-body">Loading…</div>`;
    contentRoot.appendChild(panel);

    // Click behavior: toggle tab
    tabBtn.addEventListener('click', ()=>{
      // hide other tab panels in same container
      const siblings = (contentRoot.querySelectorAll('.tab-panel'));
      siblings.forEach(s => { s.style.display = 'none'; s.setAttribute('aria-hidden','true'); });
      // remove active class from sibling tab buttons
      const otherBtns = (nav.querySelectorAll('.tab-item'));
      otherBtns.forEach(b => b.classList.remove('active'));
      // show this panel
      panel.style.display = '';
      panel.setAttribute('aria-hidden','false');
      tabBtn.classList.add('active');

      // Render content (render once)
      const body = panel.querySelector('.adv-body');
      if(body && body.dataset.rendered !== '1'){
        const htmlParts = [];
        if(json.small_stats) htmlParts.push(renderRallyStats(json.small_stats));
        if(json.big_stats) htmlParts.push(renderStrokeStats(json.big_stats, json.small_stats));
        // if big_stats is null, we still try to render stroke-ish numbers from small_stats
        if(!json.big_stats && json.small_stats){
          htmlParts.push(renderStrokeStats(null, json.small_stats));
        }
        body.innerHTML = htmlParts.join('\n') || '<p>No advanced statistics found in the file.</p>';
        body.dataset.rendered = '1';
      }
    });

    // Optionally auto-open the tab (comment/uncomment as desired)
    // tabBtn.click();
  }

  // Entry: try to find JSON. If found, inject tab.
  async function init(){
    const meta = getPlayerMeta();
    if(!meta.playerId){
      // nothing to do
      return;
    }
    const found = await findAdvancedJson(meta.playerId, meta.playerName, meta.slug);
    if(found && found.json){
      injectAdvancedTab(found.json, found.url);
    } else {
      // no file – do nothing
    }
  }

  // Wait DOM ready
  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();