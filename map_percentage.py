import re
import json
from collections import Counter
import pandas as pd
import folium
from folium import Element
from branca.element import Template, MacroElement
from branca.colormap import linear
import pycountry



def load_and_normalize_percentage(ioc_to_iso3, csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    # IOC to ISO3 mapping, preserve case
    ioc_map = {k.upper(): v for k, v in ioc_to_iso3.items()}
    df['represented_country'] = (
        df['represented_country'].astype(str)
          .str.strip().str.upper()
          .map(lambda c: ioc_map.get(c, c))
    )

    # Keep rows that have a valid represented_country ISO3.
    # Do NOT drop rows just because birthplace is missing — we want to
    # count missing birthplaces in the presence map.
    valid = {c.alpha_3 for c in pycountry.countries}
    # Keep rows where represented_country is valid and (optionally) birth_date exists.
    # If you don't need birth_date for this map, remove that condition too.
    df = df[df['represented_country'].isin(valid)]

    return df


def prepare_players(df: pd.DataFrame) -> list:
    players = []
    def safe_str(x):
        if x is None: return ''
        if isinstance(x, float) and pd.isna(x): return ''
        return str(x)

    for _, r in df.iterrows():
        # use safe_str to avoid NaN float getting through
        rc_raw = safe_str(r.get('represented_country')).strip()
        rc = rc_raw.upper() if rc_raw else 'UNK'   # keep unknowns under 'UNK'

        full_name = safe_str(r.get('full_name')).strip()
        birthplace_raw = safe_str(r.get('birthplace')).strip()
        if birthplace_raw.lower() in ('nan', 'none', 'null'):
            birthplace_raw = ''

        # Robust has_birthplace test:
        has_birthplace = False
        if birthplace_raw:
            parts = [p.strip() for p in re.split(r',', birthplace_raw) if p and p.strip()!='']
            if len(parts) >= 2:
                last = parts[-1]
                # require the final token contains at least one letter (unicode)
                if any(ch.isalpha() for ch in last):
                    has_birthplace = True

        # birth_date
        birth_date = safe_str(r.get('birth_date')).strip()

        # player id
        pid = None
        try:
            if pd.notna(r.get('player_id')):
                pid = int(r.get('player_id'))
        except Exception:
            pid = None

        # best_rank
        br = r.get('best_rank')
        try:
            best_rank = float(br) if pd.notna(br) else float('inf')
        except Exception:
            best_rank = float('inf')

        plays_raw = r.get('plays')
        plays = '' if (plays_raw is None or (isinstance(plays_raw, float) and pd.isna(plays_raw))) else str(plays_raw).lower()

        hm = r.get('height_m')
        try:
            height_m = float(hm) if pd.notna(hm) else None
        except Exception:
            height_m = None

        players.append({
            "represented_country": rc,
            "full_name": full_name,
            "player_id": pid,
            "birthplace": birthplace_raw,
            "has_birthplace": has_birthplace,
            "birth_date": birth_date,
            "best_rank": best_rank,
            "plays": plays,
            "height_m": height_m
        })
    return players



def build_and_save_presence_map(players: list, out_html: str, geojson):
    """
    Build the presence map using the players list (each player now has has_birthplace boolean).
    """
    # initial unfiltered aggregates (for initial style before JS redraw)
    total_by_country = Counter()
    have_by_country = Counter()
    for p in players:
        iso = p.get('represented_country') or ''
        total_by_country[iso] += 1
        if p.get('has_birthplace'):
            have_by_country[iso] += 1

    initial_pct = {}
    for iso, tot in total_by_country.items():
        h = have_by_country.get(iso, 0)
        initial_pct[iso] = 0 if tot == 0 else round(100.0 * h / tot)

    # --- Use blue colormap; keep 0..100 scale ---
    colormap = linear.Blues_09.scale(0, 100)
    colormap.caption = "Percent with birthplace recorded (%)"
    pct2color = {i: colormap(i) for i in range(0, 101)}

    m = folium.Map(location=[20,0], zoom_start=2, tiles="CartoDB Positron")
    colormap.add_to(m)

    # inject data
    m.get_root().html.add_child(Element(
        "<script>\n"
        f"var presencePlayers = {json.dumps(players)};\n"
        f"var initialPctByCountry = {json.dumps(initial_pct)};\n"
        f"var pct2color = {json.dumps(pct2color)};\n"
        "</script>"
    ))

    # template (modified to render grey for countries with no players)
    template = r"""
{% macro html(this, kwargs) %}
<style>
  #presence_filters { position: absolute; top: 10px; left: 10px; z-index: 9999;
    background: white; padding: 8px; box-shadow: 0 0 6px rgba(0,0,0,0.3);
    font-family: Arial, sans-serif; font-size: 12px; border-radius: 4px; }
  #presence_filters label { display:block; margin:4px 0; }
  .presence-tooltip { font-size: 12px; padding:6px 8px; background: rgba(255,255,255,0.95); border-radius:4px; border:1px solid rgba(0,0,0,0.12); }
  .presence-popup { max-height: 350px; overflow:auto; font-size:13px; }
  .presence-popup ul { padding-left:1em; margin:0; }
</style>

<div id="presence_filters">
  <label>Search Name: <input type="text" id="p_name" placeholder="e.g. Oktiabreva"/></label>
  <hr/>
  <label>Born From: <input type="date" id="p_start"/></label>
  <label>Born To:   <input type="date" id="p_end"/></label>
  <label>Max Rank:  <input type="number" id="p_rank" min="1"/></label>
  <hr/>
  <label>Min Height (m): <input type="number" step="0.01" id="p_min_h"/></label>
  <label>Max Height (m): <input type="number" step="0.01" id="p_max_h"/></label>
  <label><input type="checkbox" id="p_keep_hu" checked/> Keep Unknown Heights</label>
  <hr/>
  <label><input type="checkbox" id="p_RH" checked/> Right-Handed</label>
  <label><input type="checkbox" id="p_LH" checked/> Left-Handed</label>
  <label><input type="checkbox" id="p_UL" checked/> Unlabelled</label>
</div>

<script>
(function(){
  setTimeout(function(){
    const mapKey = Object.keys(window).find(k=>k.startsWith("map_"));
    if (!mapKey) { console.error("Map not found"); return; }
    const map = window[mapKey];

    const players = window.presencePlayers || [];
    const initialPct = window.initialPctByCountry || {};
    const pct2color = window.pct2color || {};

    const geojsonUrl = "%GEOJSON%";
    const geoLayer = L.geoJson(null, {
      style: function(feature) {
        const iso = (feature.id || (feature.properties && feature.properties.iso_a3) || '').toUpperCase();
        // if initialPct does not have this iso, there are no players -> grey
        if (initialPct[iso] === undefined) {
          return { fillColor: '#dddddd', color: "#999", weight: 1, fillOpacity: 0.75 };
        }
        const pct = initialPct[iso];
        return { fillColor: pct2color[pct] || pct2color[0], color: "#999", weight: 1, fillOpacity: 0.75 };
      }
    }).addTo(map);

    fetch(geojsonUrl).then(r => r.json()).then(js => { geoLayer.addData(js); redraw(); }).catch(e => console.error(e));

    function filterPlayers() {
      const nameF = document.getElementById('p_name').value.trim().toLowerCase();
      const s = document.getElementById('p_start').value;
      const e = document.getElementById('p_end').value;
      const maxR = parseInt(document.getElementById('p_rank').value) || Infinity;
      const minH = parseFloat(document.getElementById('p_min_h').value);
      const maxH = parseFloat(document.getElementById('p_max_h').value);
      const keepHU = document.getElementById('p_keep_hu').checked;
      const showRH = document.getElementById('p_RH').checked;
      const showLH = document.getElementById('p_LH').checked;
      const showUL = document.getElementById('p_UL').checked;

      return players.filter(p => {
        if (nameF && !p.full_name.toLowerCase().includes(nameF)) return false;
        if ((s && p.birth_date && p.birth_date < s) || (e && p.birth_date && p.birth_date > e)) return false;
        if (p.best_rank > maxR) return false;
        if (p.height_m === null) {
          if (!keepHU) return false;
        } else {
          if (!isNaN(minH) && p.height_m < minH) return false;
          if (!isNaN(maxH) && p.height_m > maxH) return false;
        }
        const play = (p.plays || "").toLowerCase();
        if (play.includes("right") && !showRH) return false;
        if (play.includes("left") && !showLH) return false;
        if (!play && !showUL) return false;
        return true;
      });
    }

    // safe escape for popup names
    function escapeHtml(s){ if(!s) return ''; return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }

    function redraw() {
      const filtered = filterPlayers();
      const total = {};
      const have = {};
      const missingLists = {};

      filtered.forEach(p => {
        const iso = (p.represented_country || '').toUpperCase();
        total[iso] = (total[iso]||0) + 1;
        if (p.has_birthplace) {
          have[iso] = (have[iso]||0) + 1;
        } else {
          missingLists[iso] = missingLists[iso] || [];
          missingLists[iso].push({name: p.full_name, id: p.player_id});
        }
      });

      // update geo layer style and tooltip
      geoLayer.eachLayer(layer => {
        const iso = (layer.feature.id || (layer.feature.properties && layer.feature.properties.iso_a3) || '').toUpperCase();
        const t = total[iso] || 0;
        const h = have[iso] || 0;
        const pct = t === 0 ? 0 : Math.round(100 * h / t);

        // if no players at all for this iso, grey it
        let color;
        if (t === 0) {
          color = '#dddddd';
        } else {
          color = pct2color[pct] || pct2color[0];
        }

        try { layer.setStyle({ fillColor: color, weight: 1, color: '#999' }); } catch(e){}
        const tipText = `${pct}% — ${h} on ${t}`;
        layer.unbindTooltip();
        layer.bindTooltip("<div class='presence-tooltip'>"+tipText+"</div>", {sticky:true, direction:'auto'});

        // click popup: list missing players for this country
        layer.off('click');
        layer.on('click', function(e){
          L.DomEvent.stopPropagation(e);
          const miss = missingLists[iso] || [];
          let html = "<div class='presence-popup'><strong>" + tipText + "</strong><hr/>";
          if (miss.length === 0) {
            html += "<div>All players have birthplace data (for current filters).</div>";
          } else {
            html += "<div>Players missing birthplace:</div><ul>";
            miss.forEach(p => {
              const wiki = "https://en.wikipedia.org/wiki/" + encodeURIComponent(p.name.replace(/ /g,'_'));
              let slug = (p.name || "").toLowerCase().replace(/[^a-z0-9\u00C0-\u024F]+/g, '-').replace(/(^-|-$)/g,'');
              slug = encodeURIComponent(slug);
              const wta = p.id ? ("https://www.wtatennis.com/players/" + p.id + "/" + slug) : '#';
              html += "<li><a href='"+wiki+"' target='_blank' rel='noopener'>"+escapeHtml(p.name)+"</a>";
              if (p.id) html += " — <a href='"+wta+"' target='_blank' rel='noopener'>WTA</a>";
              html += "</li>";
            });
            html += "</ul>";
          }
          html += "</div>";
          L.popup({maxWidth:420, className:'presence-popup'}).setLatLng(e.latlng).setContent(html).openOn(map);
        });
      });
    }

    // wire inputs
    ['p_name','p_start','p_end','p_rank','p_min_h','p_max_h','p_keep_hu','p_RH','p_LH','p_UL']
      .forEach(id => { const el = document.getElementById(id); if(el) el.addEventListener('input', redraw); });

    // close popup on map click
    map.on('click', function(){ map.closePopup(); });

  }, 300);
})();
</script>
{% endmacro %}
    """

    html = template.replace("%GEOJSON%", geojson)
    macro = MacroElement()
    macro._template = Template(html)
    m.get_root().add_child(macro)

    m.save(out_html)
    print(f"✅ Presence map saved to {out_html}")
