# map_birthplace_atp.py
"""
Carte 'birthplace' adaptée au CSV masculin (ATP).

Fonctions exportées :
 - load_and_clean_atp(csv_path) -> DataFrame (filtre sur birthplace)
 - geocode_with_cache_atp(df, cache_file, ...) -> DataFrame enrichi lat/lon
 - normalize_dates_and_heights_atp(df) -> list of points (all_pts)
 - build_and_save_map_atp(all_pts, out_html)

Utilise scripts.geocode_utils_atp (geocode_place, bulk_geocode, load_cache, save_cache).
"""
import pandas as pd
import re
import json
from datetime import datetime
from pathlib import Path
import folium
from folium import Element
from branca.element import Template, MacroElement

# utilise la version ATP du module geocode utils
from scripts.geocode_utils_atp import load_cache, save_cache, geocode_place, bulk_geocode, is_skip_geocode, get_geolocator

import math


def load_and_clean_atp(csv_path: str) -> pd.DataFrame:
    """
    Lit le CSV ATP et garde les lignes qui ont un birthplace contenant au moins une virgule.
    (les lieux valides attendus comme 'City, Region, Country')
    """
    df = pd.read_csv(csv_path, dtype=str)
    # normalise les strings
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # garde les lignes avec birthplace non-null contenant une virgule
    df = df[df['birthplace'].notna() & df['birthplace'].str.contains(r',')].copy()
    return df


def load_cache_safe(cache_file: str):
    """
    Wrapper pour charger cache en renvoyant {} si absent.
    """
    try:
        return load_cache(cache_file)
    except Exception:
        return {"geocode": {}, "reverse": {}}


def save_cache_safe(cache: dict, cache_file: str):
    try:
        save_cache(cache, cache_file)
    except Exception:
        pass


def geocode_with_cache_atp(df: pd.DataFrame,
                           cache_file: str,
                           user_agent: str = "birthplace-mapper-atp",
                           delay: float = 1.0,
                           timeout: int = 10) -> pd.DataFrame:
    """
    Garantit que chaque 'birthplace' du df est présente dans le cache (bulk_geocode).
    Puis lit le cache et attache lat/lon dans df. Renvoie df filtré aux rows qui ont lat/lon.
    Respecte SKIP_GEOCODE si défini.
    """
    cache = load_cache_safe(cache_file)

    # place keys distinctes (non vides)
    places = [p for p in df['birthplace'].unique() if isinstance(p, str) and p.strip()]
    # bulk geocode: va vérifier le cache avant d'appeler le geocode réseau
    bulk_geocode(places, cache_file, user_agent=user_agent, delay=delay, timeout=timeout)

    # reload cache
    cache = load_cache_safe(cache_file)

    # helper mapping using normalized keys (geocode_utils_atp normalizes keys)
    def get_coords_for_place(place):
        if not place:
            return None
        # geocode_utils_atp.normalize_place was used to store keys; we'll try both raw and normalized
        from scripts.geocode_utils_atp import normalize_place
        k = normalize_place(place)
        v = cache.get("geocode", {}).get(k)
        if v:
            return tuple(v)
        # fallback: maybe the raw key exists
        v2 = cache.get("geocode", {}).get(place)
        if v2:
            return tuple(v2)
        return None

    # map to lat/lon
    df['lat'] = df['birthplace'].map(lambda p: (get_coords_for_place(p) or (None, None))[0])
    df['lon'] = df['birthplace'].map(lambda p: (get_coords_for_place(p) or (None, None))[1])

    df = df.dropna(subset=['lat', 'lon']).copy()
    # coerce to floats
    df['lat'] = df['lat'].astype(float)
    df['lon'] = df['lon'].astype(float)
    return df


def normalize_dates_and_heights_atp(df: pd.DataFrame) -> list:
    """
    Construit all_pts list pour la carte à partir du df enrichi lat/lon.
    Chaque élément: {lat, lon, birth_date (ISO), full_name, player_id (string), birthplace, best_rank, plays, height_m}
    """
    all_pts = []

    def parse_birth_date(raw):
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return None
        s = str(raw).strip()
        if s == '':
            return None
        cleaned = re.sub(r'[^A-Za-z0-9 ]', ' ', s).strip()
        # try a few common formats
        patterns = ["%Y-%m-%d", "%b %d %Y", "%B %d %Y", "%d %b %Y", "%d %B %Y"]
        for patt in patterns:
            try:
                dt = datetime.strptime(cleaned, patt)
                return dt.date().isoformat()
            except Exception:
                continue
        # if only year present, fallback to year-01-01
        m = re.search(r'(\d{4})', s)
        if m:
            return f"{m.group(1)}-01-01"
        return None

    def parse_height(h_cm, h_in):
        # reuse logic similar to percentage parser
        if isinstance(h_cm, str):
            s = h_cm.strip().lower().replace(',', '.')
            if s.endswith('m'):
                s2 = s[:-1].strip()
                try:
                    val = float(s2)
                    if val > 3:
                        return val / 100.0
                    return val
                except Exception:
                    pass
            else:
                try:
                    val = float(s)
                    if val > 3:
                        return val / 100.0
                    return val
                except Exception:
                    pass
        if isinstance(h_in, str) and h_in.strip():
            m = re.match(r"""^\s*(\d+)\s*['’]\s*(\d+)\s*(?:"{0,2})\s*$""", h_in.strip())
            if m:
                try:
                    feet = int(m.group(1)); inches = int(m.group(2))
                    total_inches = feet * 12 + inches
                    return round(total_inches * 0.0254, 3)
                except Exception:
                    pass
            m2 = re.match(r'^\s*(\d+)\s*(?:ft|feet|\'|’)\s*$', h_in.strip())
            if m2:
                try:
                    feet = int(m2.group(1))
                    return round(feet * 12 * 0.0254, 3)
                except Exception:
                    pass
        return None

    for _, row in df.iterrows():
        birth_iso = parse_birth_date(row.get('birth_date'))
        if not birth_iso:
            # skip if no parseable birth date (optional — keep or drop depending on use-case)
            continue

        height_m = parse_height(row.get('height_cm'), row.get('height_inches'))

        all_pts.append({
            "lat": float(row['lat']),
            "lon": float(row['lon']),
            "birth_date": birth_iso,
            "full_name": row.get('full_name') or '',
            "player_id": row.get('player_id') or '',
            "birthplace": row.get('birthplace') or '',
            "best_rank": (float(row.get('highest_ranking')) if row.get('highest_ranking') not in (None, '', float('nan')) else None)
                         if 'highest_ranking' in row else (float(row.get('best_rank')) if row.get('best_rank') not in (None, '', float('nan')) else None),
            "plays": row.get('plays') or row.get('plays_norm') or '',
            "height_m": height_m
        })
    return all_pts


def build_and_save_map_atp(all_pts: list, out_html: str):
    """
    Build a simple point map with the interactive filter UI similar to your WTA code.
    """
    m = folium.Map(location=[20,0], zoom_start=2)
    map_var = m.get_name()

    # inject JS data
    m.get_root().html.add_child(Element(
        '<script>var allPoints = ' + json.dumps(all_pts) + ';</script>'
    ))

    # In-page template: same controls as your WTA version, but ATP links used in popup
    template = r"""
    {% macro html(this, kwargs) %}
  <style>
    #filters {
      position: absolute; top: 10px; left: 10px;
      z-index: 9999; background: white; padding: 8px;
      box-shadow: 0 0 6px rgba(0,0,0,0.3);
      font-family: Arial, sans-serif; font-size: 12px; border-radius: 4px;
    }
    #filters label { display: block; margin-bottom: 4px; }
    #filters input { margin-left: 4px; width: 130px; }
    #filters hr { margin: 6px 0; }
  </style>

  <div id="filters">
    <label>Search Name <input type="text" id="name_search" placeholder="e.g. Korda"/></label>
    <hr/>
    <label>From     <input type="date"   id="start"/></label>
    <label>To       <input type="date"   id="end"  /></label>
    <label>Max Rank <input type="number" id="rank" min="1"/></label>
    <hr/>
    <label>Min Height (m) <input type="number" step="0.01" id="min_h" /></label>
    <label>Max Height (m) <input type="number" step="0.01" id="max_h" /></label>
    <label><input type="checkbox" id="chk_HU" checked/> Keep Unknown Heights</label>
    <hr/>
    <label><input type="checkbox" id="chk_RH" checked/> Right-Handed</label>
    <label><input type="checkbox" id="chk_LH" checked/> Left-Handed</label>
    <label><input type="checkbox" id="chk_UL" checked/> Unlabelled</label>
  </div>

  <script>
  // Définitions de sécurité : SITE_BASE fallback et escapeHtml
    // safer SITE_BASE reference: check window/globalThis to avoid TDZ when const/let exist in same scope
const SITE_BASE = (typeof globalThis !== 'undefined' && globalThis.SITE_BASE !== undefined)
                   ? globalThis.SITE_BASE
                   : 'https://www.center-court.net';

    function escapeHtml(s){
      if (s === null || s === undefined) return '';
      return String(s)
        .replace(/&/g,'&amp;')
        .replace(/</g,'&lt;')
        .replace(/>/g,'&gt;')
        .replace(/"/g,'&quot;')
        .replace(/'/g,'&#39;');
    }

    document.addEventListener("DOMContentLoaded", function() {
      const mapObj      = window["%MAP_VAR%"];
      const circleLayer = L.layerGroup().addTo(mapObj);

      function redraw() {
        circleLayer.clearLayers();
        const nameF = document.getElementById('name_search').value.trim().toLowerCase();
        const s     = document.getElementById('start').value;
        const e     = document.getElementById('end'  ).value;
        const r     = parseInt(document.getElementById('rank').value) || Infinity;
        const minH  = parseFloat(document.getElementById('min_h').value);
        const maxH  = parseFloat(document.getElementById('max_h').value);
        const keepHU= document.getElementById('chk_HU').checked;
        const showRH= document.getElementById('chk_RH').checked;
        const showLH= document.getElementById('chk_LH').checked;
        const showUL= document.getElementById('chk_UL').checked;

        const pts = allPoints.filter(p => {
          if(nameF && !p.full_name.toLowerCase().includes(nameF)) return false;
          if((s && p.birth_date < s) || (e && p.birth_date > e) || (p.best_rank && p.best_rank > r))
            return false;
          if(p.height_m === null || p.height_m === undefined) {
            if(!keepHU) return false;
          } else {
            if(!isNaN(minH) && p.height_m < minH) return false;
            if(!isNaN(maxH) && p.height_m > maxH) return false;
          }
          const play = (p.plays || '').toLowerCase().replace(/[^a-z]/g, '');
          if (play.includes('right') && !showRH) return false;
          if (play.includes('left')  && !showLH) return false;
          if (!play && !showUL) return false;
          return true;
        });

        const agg = {};
        pts.forEach(p => {
          const key = p.lat.toFixed(5)+','+p.lon.toFixed(5);
          if(!agg[key]) {
            agg[key] = {lat:p.lat,lon:p.lon,names:[],births:[],ids:[],birthplace:p.birthplace};
          }
          agg[key].names.push(p.full_name);
          agg[key].births.push(p.birth_date);
          agg[key].ids.push(p.player_id);
        });
        Object.values(agg).forEach(g => {
          let html = `<div><strong>${g.birthplace} — ${g.names.length} player${g.names.length>1?'s':''}</strong><ul style="padding-left:1em;margin:0;">`;
          for (let i = 0; i < g.names.length; i++) {
              const name = g.names[i] || '';
              const dob = g.births[i] || '';
              const id = g.ids[i] || '';
            
              let slug = name.toLowerCase().replace(/[^a-z0-9\u00C0-\u024F]+/g,'-').replace(/(^-|-$)/g,'');
              slug = encodeURIComponent(slug);
            
              // local ATP player page absolute (production)
              const localPath = SITE_BASE + '/players_atp/' + (id ? (encodeURIComponent(id) + '-' + slug + '.html') : (slug + '.html'));

              const atp = id ? ("https://www.atptour.com/en/players/" + slug + "/" + encodeURIComponent(id.toString().toLowerCase()) + "/overview") : '#';
            
              html += `<li><a href="${localPath}">${escapeHtml(name)}</a>, ${dob}`;
              if (id) html += ` — <a href="${atp}" target="_blank" rel="noopener">ATP</a>`;
              html += `</li>`;
            }



          html+=`</ul></div>`;
          L.circleMarker([g.lat,g.lon],{radius:3+g.names.length,color:"crimson",fill:true,fillOpacity:0.6})
            .bindPopup(html).addTo(circleLayer);
        });
      }

      ['name_search','start','end','rank','min_h','max_h','chk_HU','chk_RH','chk_LH','chk_UL']
        .forEach(id => { const el = document.getElementById(id); if(el) el.addEventListener('input', redraw); });

      redraw();
    });
  </script>
{% endmacro %}
    """
    html = template.replace("%MAP_VAR%", map_var)
    macro = MacroElement()
    macro._template = Template(html)
    m.get_root().add_child(macro)

    out_path = Path(out_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_path))
    print(f"✅ Map saved to {out_path} (exists={out_path.exists()})")
