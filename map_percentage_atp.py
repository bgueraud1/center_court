# map_percentage_atp.py
"""
Carte de 'presence/percentage' adaptée au CSV masculin (ATP).
Attendu: player_data_atp.csv avec au moins ces colonnes :
  full_name, player_id, represented_country, birthplace, birth_date,
  height_inches, height_cm, plays, highest_ranking

Fournit :
 - load_and_normalize_percentage_atp(ioc_to_iso3, csv_path) -> DataFrame
 - prepare_players_atp(df) -> list of player dicts (fields utilisés par JS)
 - build_and_save_presence_map_atp(players, out_html, geojson_url)

Usage:
  df = load_and_normalize_percentage_atp(ioc_map, "player_data_atp.csv")
  players = prepare_players_atp(df)
  build_and_save_presence_map_atp(players, "out/presence_atp.html", "https://.../countries.geo.json")
"""
import re
import json
from collections import Counter
import pandas as pd
import folium
from folium import Element
from branca.element import Template, MacroElement
from branca.colormap import linear
import pycountry
import math


def load_and_normalize_percentage_atp(ioc_to_iso3, csv_path: str) -> pd.DataFrame:
    """
    Lit le CSV ATP et normalise 'represented_country' en ISO3 à l'aide de ioc_to_iso3 map.
    Ne supprime PAS les lignes sans birthplace (on veut compter les manquants).
    """
    df = pd.read_csv(csv_path, dtype=str)  # read as strings for robustness
    # trim string fields
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # IOC to ISO3 mapping
    ioc_map = {k.upper(): v for k, v in (ioc_to_iso3 or {}).items()}

    def map_country(c):
        if c is None or (isinstance(c, float) and pd.isna(c)):
            return None
        s = str(c).strip().upper()
        if s == '':
            return None
        if s in ioc_map:
            return ioc_map[s]
        # already ISO3?
        try:
            if pycountry.countries.get(alpha_3=s):
                return s
        except Exception:
            pass
        # alpha2 -> alpha3
        try:
            cobj = pycountry.countries.get(alpha_2=s)
            if cobj:
                return cobj.alpha_3
        except Exception:
            pass
        return s

    df['represented_country'] = df.get('represented_country', pd.Series()).map(map_country)

    # keep rows with valid represented_country ISO3
    valid_iso3 = {c.alpha_3 for c in pycountry.countries}
    df = df[df['represented_country'].isin(valid_iso3)].copy()

    # normalize highest_ranking -> best_rank (float)
    def parse_rank(v):
        try:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return float('inf')
            s = str(v).strip()
            if s == '' or s == '-' or s.lower() in ('nan', 'none'):
                return float('inf')
            m = re.search(r'(\d+)', s)
            return float(m.group(1)) if m else float('inf')
        except Exception:
            return float('inf')

    df['best_rank'] = df.get('highest_ranking', df.get('best_rank', pd.Series())).map(parse_rank)

    # prepare height_m column from height_cm or height_inches
    def parse_height_to_meters(h_cm, h_in):
        # try height_cm (e.g. "1.91m" or "191")
        if isinstance(h_cm, str):
            s = h_cm.strip().lower().replace(',', '.')
            if s.endswith('m'):
                s2 = s[:-1].strip()
                try:
                    val = float(s2)
                    if val > 3:  # centimeters probably
                        return val / 100.0
                    return val
                except Exception:
                    pass
            else:
                try:
                    val = float(s)
                    if val > 3:  # cm
                        return val / 100.0
                    return val
                except Exception:
                    pass
        # fallback to inches string like 6' 3""
        if isinstance(h_in, str) and h_in.strip():
            s = h_in.strip()
            m = re.match(r"""^\s*(\d+)\s*['’]\s*(\d+)\s*(?:"{0,2})\s*$""", s)
            if m:
                try:
                    feet = int(m.group(1)); inches = int(m.group(2))
                    total_inches = feet * 12 + inches
                    return round(total_inches * 0.0254, 3)
                except Exception:
                    pass
            # single feet like "6'"
            m2 = re.match(r'^\s*(\d+)\s*(?:ft|feet|\'|’)\s*$', s)
            if m2:
                try:
                    feet = int(m2.group(1))
                    return round(feet * 12 * 0.0254, 3)
                except Exception:
                    pass
        return None

    df['height_m'] = df.apply(lambda r: parse_height_to_meters(r.get('height_cm'), r.get('height_inches')), axis=1)

    # normalize plays lower-case
    df['plays_norm'] = df.get('plays', pd.Series()).fillna('').astype(str).str.lower()

    # ensure full_name exists
    if 'full_name' not in df.columns and 'name' in df.columns:
        df['full_name'] = df['name']
    df['full_name'] = df.get('full_name', pd.Series()).fillna('')

    # keep the important columns
    return df


def prepare_players_atp(df: pd.DataFrame) -> list:
    """
    Transforme le dataframe en liste de dicts utilisée par le JS :
      represented_country, full_name, player_id (string), birthplace, has_birthplace,
      birth_date, best_rank (float), plays, height_m
    """
    players = []

    def safe_get_str(v):
        if v is None: return ''
        if isinstance(v, float) and pd.isna(v): return ''
        return str(v)

    for _, r in df.iterrows():
        rc = safe_get_str(r.get('represented_country')).upper() or 'UNK'
        full_name = safe_get_str(r.get('full_name')).strip()
        birthplace_raw = safe_get_str(r.get('birthplace')).strip()
        if birthplace_raw.lower() in ('nan', 'none', 'null'):
            birthplace_raw = ''

        # robust has_birthplace detection: require at least one comma and final token has letters
        has_birthplace = False
        if birthplace_raw:
            parts = [p.strip() for p in re.split(r',', birthplace_raw) if p and p.strip() != '']
            if len(parts) >= 2 and any(ch.isalpha() for ch in parts[-1]):
                has_birthplace = True

        birth_date = safe_get_str(r.get('birth_date')).strip()

        # player_id keep as string (ATP uses alpha ids)
        pid = safe_get_str(r.get('player_id'))

        # best_rank already computed by load_and_normalize_percentage_atp
        br = r.get('best_rank')
        try:
            best_rank = float(br) if br not in (None, '', float('nan')) else float('inf')
        except Exception:
            best_rank = float('inf')

        plays = safe_get_str(r.get('plays_norm')).lower()

        hm = r.get('height_m')
        try:
            height_m = float(hm) if (hm not in (None, '') and not (isinstance(hm, float) and pd.isna(hm))) else None
        except Exception:
            height_m = None

                # NEW: reviewed_player boolean
        rev_raw = r.get('reviewed_player') if 'reviewed_player' in r.index else None
        def _parse_rev(v):
            if v is None: return False
            if isinstance(v, bool): return v
            s = str(v).strip().lower()
            return s in ("true","t","1","yes","y")
        reviewed_flag = _parse_rev(rev_raw)

        players.append({
            "represented_country": rc,
            "full_name": full_name,
            "player_id": pid,
            "birthplace": birthplace_raw,
            "has_birthplace": has_birthplace,
            "birth_date": birth_date,
            "best_rank": best_rank,
            "plays": plays,
            "height_m": height_m,
            "reviewed_player": reviewed_flag
        })

    return players


def build_and_save_presence_map_atp(players: list, out_html: str, geojson: str):
    """
    Génère la carte HTML (identique en UI à ta version WTA, adaptée aux IDs ATP).
    'geojson' doit être une URL string to world geojson.
    """
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
        initial_pct[iso] = 0 if tot == 0 else int(round(100.0 * h / tot))

    colormap = linear.Blues_09.scale(0, 100)
    colormap.caption = "Percent with birthplace recorded (%)"
    colormap.add_to(folium.Map(location=[0,0]))  # attach temporarily to get colors; we'll create final map below
    pct2color = {i: colormap(i) for i in range(0, 101)}

    m = folium.Map(location=[20,0], zoom_start=2, tiles="CartoDB Positron")
    # add colormap again properly
    colormap.add_to(m)

    # --- safety: convert non-finite best_rank to None so json.dumps ne plante pas ---
    safe_players = []
    for p in players:
        sp = dict(p)  # shallow copy
        br = sp.get('best_rank', None)
        try:
            if br is None:
                sp['best_rank'] = None
            else:
                brf = float(br)
                if not math.isfinite(brf):
                    sp['best_rank'] = None
                else:
                    sp['best_rank'] = brf
        except Exception:
            sp['best_rank'] = None
        # ensure boolean for reviewed_player (defensive)
        sp['reviewed_player'] = bool(sp.get('reviewed_player'))
        safe_players.append(sp)

    # inject data (use safe_players)
    m.get_root().html.add_child(Element(
        "<script>\n"
        f"var presencePlayers = {json.dumps(safe_players)};\n"
        f"var initialPctByCountry = {json.dumps(initial_pct)};\n"
        f"var pct2color = {json.dumps(pct2color)};\n"
        "</script>"
    ))

    # template (almost identical to your original, but include reviewed_player in missingLists and simplify test)
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
  /* optional: class to style reviewed names */
  .reviewed-name { font-weight: 700; }
</style>

<div id="presence_filters">
  <label>Search Name: <input type="text" id="p_name" placeholder="e.g. Poullain"/></label>
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
          // <<< include reviewed_player so popup can style names accordingly
          missingLists[iso].push({
            name: p.full_name,
            id: p.player_id,
            reviewed_player: p.reviewed_player
          });
        }
      });

      geoLayer.eachLayer(layer => {
        const iso = (layer.feature.id || (layer.feature.properties && layer.feature.properties.iso_a3) || '').toUpperCase();
        const t = total[iso] || 0;
        const h = have[iso] || 0;
        const pct = t === 0 ? 0 : Math.round(100 * h / t);
        const SITE_BASE = 'https://www.center-court.net';

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
              const name = p.name || '';
              const id = p.id || '';

              let slug = name.toLowerCase().replace(/[^a-z0-9\u00C0-\u024F]+/g, '-').replace(/(^-|-$)/g,'');
              slug = encodeURIComponent(slug);

              const localPath = SITE_BASE + '/players_atp/' + (id ? (encodeURIComponent(id) + '-' + slug + '.html') : (slug + '.html'));
              const atp = id ? ("https://www.atptour.com/en/players/" + slug + "/" + encodeURIComponent(id.toString().toLowerCase()) + "/overview") : '#';

              // reviewed flag (coerce to boolean)
              const reviewed = !!p.reviewed_player;

              let nameLink = "<a href='" + localPath + "'>" + escapeHtml(name) + "</a>";
              if (reviewed) {
                // mark visually (bold) and also add class if you want custom CSS
                nameLink = "<span class='reviewed-name'>" + nameLink + "</span>";
              }

              html += "<li>" + nameLink;
              if (id) html += " — <a href='" + atp + "' target='_blank' rel='noopener'>ATP</a>";
              html += "</li>";
            });

            html += "</ul>";
          }
          html += "</div>";
          L.popup({maxWidth:420, className:'presence-popup'}).setLatLng(e.latlng).setContent(html).openOn(map);
        });
      });
    }

    ['p_name','p_start','p_end','p_rank','p_min_h','p_max_h','p_keep_hu','p_RH','p_LH','p_UL']
      .forEach(id => { const el = document.getElementById(id); if(el) el.addEventListener('input', redraw); });

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

    from pathlib import Path
    out_path = Path(out_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_path))
    print(f"✅ Map saved to {out_path} (exists={out_path.exists()})")
