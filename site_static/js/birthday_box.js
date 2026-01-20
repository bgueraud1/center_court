
// site_static/js/birthday_box.js
// Fetches /tools/birthday_today.json and renders a compact, colored box

(async function(){
  const urlCandidates = [
    '/tools/birthday_today.json',
    '/site_static/tools/birthday_today.json',
    '/docs/tools/birthday_today.json',
  ];
  async function fetchFirst(list){
    for(const u of list){
      try{
        const r = await fetch(u, {cache:'no-store'});
        if(!r.ok) continue;
        const j = await r.json();
        return {data:j, url:u};
      }catch(e){/*try next*/}
    }
    return null;
  }

  const res = await fetchFirst(urlCandidates);
  const container = (function(){
    // try to find a logical insertion point
    const main = document.querySelector('main .container') || document.querySelector('main') || document.body;
    // create wrapper
    const wrapper = document.createElement('div');
    wrapper.id = 'birthday-box-wrapper';
    wrapper.style.marginBottom = '1rem';
    main.prepend(wrapper);
    return wrapper;
  })();

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
      const left = document.createElement('div');
      left.style.display='flex'; left.style.alignItems='center'; left.style.gap='0.6rem';
      const flag = document.createElement('span'); flag.textContent = e.flag_emoji || '';
      flag.style.fontSize = '1.05rem';
      const name = document.createElement('div'); name.innerHTML = `<strong>${e.full_name}</strong>`;
      left.appendChild(flag); left.appendChild(name);
      const right = document.createElement('div');
      right.style.minWidth='6rem'; right.style.textAlign='right';
      right.style.fontSize='0.9rem'; right.style.color='#6c757d';
      right.textContent = (e.current_rank!=null ? `#${e.current_rank}` : '—');
      row.appendChild(left); row.appendChild(right);
      list.appendChild(row);
    });
    card.appendChild(list);
    return card;
  }

  if(!res || !Array.isArray(res.data) || res.data.length===0){
    container.innerHTML = '';
    const info = document.createElement('div'); info.className='small text-muted'; info.textContent = 'Birthday box not available today.';
    container.appendChild(info);
    return;
  }

  const card = makeCard(res.data.slice(0,5));
  container.appendChild(card);
})();
