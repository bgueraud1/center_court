import pandas as pd
import re
import json
from datetime import datetime
from scripts.geocode_utils import load_cache, save_cache, geocode_place, bulk_geocode, is_skip_geocode

import folium
from folium import Element
from branca.element import Template, MacroElement
from pathlib import Path



def load_and_clean(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = df[df['birthplace'].notna() & df['birthplace'].str.contains(r',')].copy()
    return df

def load_cache(cache_file):
    """
    Load coords cache safely using UTF-8 and falling back gracefully.
    Returns {} if file not found or malformed.
    """
    p = Path(cache_file)
    if not p.exists():
        return {}

    # Try robust utf-8 read, fallback to cp1252 then replace errors if needed
    try:
        text = p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            # try reading as windows-1252 and re-encode to utf-8
            text = p.read_text(encoding="cp1252")
            # optionally re-save in utf-8 for future runs
            p.write_text(text,encoding='utf-8')
            
        except Exception:
            # last resort: read bytes and decode with replacement
            b = p.read_bytes()
            text = b.decode("utf-8", errors="replace")

    try:
        return json.loads(text)
    except Exception as e:
        print(f"WARNING: could not parse JSON cache {cache_file}: {e}")
        # don't raise — return empty cache so the build can continue
        return {}

def save_cache(cache: dict, path: str):
    with open(path, "w") as f:
        json.dump(cache, f, indent=2)

def geocode_with_cache(df: pd.DataFrame,
                       cache_file: str,
                       user_agent: str = "birthplace-mapper",
                       delay: float = 1.0,
                       timeout: int = 10) -> pd.DataFrame:
    """
    Fill df with lat/lon using scripts.geocode_utils. Honors SKIP_GEOCODE env var.
    Returns dataframe filtered to rows that have lat/lon.
    """
    # load cache once
    cache = load_cache(cache_file)

    # get distinct places to ensure they are cached (bulk will skip cached items)
    places = [p for p in df['birthplace'].unique() if isinstance(p, str) and p.strip()]
    # bulk_geocode will call geocode_place for entries not in cache
    # but will also check SKIP_GEOCODE and then not perform network lookups
    bulk_geocode(places, cache_file, user_agent=user_agent, delay=delay, timeout=timeout)

    # reload cache (bulk_geocode already saved)
    cache = load_cache(cache_file)

    # map coords into df
    def _get_lat(p):
        val = cache.get("geocode", {}).get(p)
        return None if val is None else val[0]
    def _get_lon(p):
        val = cache.get("geocode", {}).get(p)
        return None if val is None else val[1]

    df['lat'] = df['birthplace'].map(_get_lat)
    df['lon'] = df['birthplace'].map(_get_lon)
    df = df.dropna(subset=['lat', 'lon'])
    return df


def normalize_dates_and_heights(df: pd.DataFrame) -> list:
    all_pts = []
    for _, row in df.iterrows():
        raw = row['birth_date']
        if pd.isna(raw) or not isinstance(raw, str) or raw.strip()=="":
            continue

        cleaned = re.sub(r'[^A-Za-z0-9 ]','', raw.strip())
        try:
            dt = datetime.strptime(cleaned, "%b %d %Y")
        except ValueError:
            dt = datetime.strptime(cleaned, "%B %d %Y")
        iso = dt.date().isoformat()

        # parse height (e.g. "1.75m")
        raw_h = row.get('height_cm', '')
        try:
            height_m = float(raw_h.strip().rstrip('m')) if isinstance(raw_h, str) and raw_h.strip().endswith('m') else None
        except:
            height_m = None

        all_pts.append({
            "lat":        row['lat'],
            "lon":        row['lon'],
            "birth_date": iso,
            "full_name":  row['full_name'],
            "player_id":  int(row['player_id']),
            "birthplace": row['birthplace'],
            "best_rank":  row['best_rank'],
            "plays":      row.get('plays',''),
            "height_m":   height_m
        })
    return all_pts

def build_and_save_map(all_pts: list, out_html: str):
    m = folium.Map(location=[20,0], zoom_start=2)
    map_var = m.get_name()

    # inject JS data
    m.get_root().html.add_child(Element(
        '<script>var allPoints = ' + json.dumps(all_pts) + ';</script>'
    ))

    # filter‐UI template
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
    <label>Search Name <input type="text" id="name_search" placeholder="e.g. Kournikova"/></label>
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
  // Debug + safe helpers (single definition)
  console.log("[center-court] map_birthplace script loaded");
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

      const nameF = (document.getElementById('name_search') && document.getElementById('name_search').value || '').trim().toLowerCase();
      const s     = (document.getElementById('start') && document.getElementById('start').value) || '';
      const e     = (document.getElementById('end') && document.getElementById('end').value) || '';
      const r     = parseInt(document.getElementById('rank').value) || Infinity;
      const minH  = parseFloat(document.getElementById('min_h').value);
      const maxH  = parseFloat(document.getElementById('max_h').value);
      const keepHU= document.getElementById('chk_HU').checked;
      const showRH= document.getElementById('chk_RH').checked;
      const showLH= document.getElementById('chk_LH').checked;
      const showUL= document.getElementById('chk_UL').checked;

      const pts_all = (typeof allPoints !== 'undefined' ? allPoints : []);
      if (!Array.isArray(pts_all) || pts_all.length === 0) {
        console.warn("[center-court] no allPoints available (0 points). Check Python injection of allPoints.");
        circleLayer.clearLayers();
        return;
      }

      const pts = pts_all.filter(p => {
        if(nameF && !(p.full_name || '').toLowerCase().includes(nameF)) return false;
        if((s && (p.birth_date || '') < s) || (e && (p.birth_date || '') > e) || ((p.best_rank===undefined?Infinity:p.best_rank) > r)) return false;

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
        if (typeof p.lat === 'undefined' || typeof p.lon === 'undefined') return;
        const key = Number(p.lat).toFixed(5)+','+Number(p.lon).toFixed(5);
        if(!agg[key]) {
          agg[key] = {lat: Number(p.lat), lon: Number(p.lon), names:[], births:[], ids:[], birthplace: p.birthplace || ''};
        }
        agg[key].names.push(p.full_name || '');
        agg[key].births.push(p.birth_date || '');
        agg[key].ids.push(p.player_id || '');
      });

      Object.values(agg).forEach(g => {
        let html = `<div><strong>${escapeHtml(g.birthplace)} — ${g.names.length} player${g.names.length>1?'s':''}</strong><ul style="padding-left:1em;margin:0;">`;
        for (let i = 0; i < g.names.length; i++) {
          const name = g.names[i] || '';
          const dob = g.births[i] || '';
          const id = g.ids[i] || '';
          let slug = name.toLowerCase().replace(/[^a-z0-9\u00C0-\u024F]+/g,'-').replace(/(^-|-$)/g,'');
          slug = encodeURIComponent(slug);
          const localPath = SITE_BASE + '/players/' + (id && /^\d+$/.test(id) ? (encodeURIComponent(id) + '-' + slug + '.html') : (slug + '.html'));
          const wta = id ? ("https://www.wtatennis.com/players/" + id + "/" + slug) : '#';
          html += `<li><a href="${localPath}">${escapeHtml(name)}</a>, ${escapeHtml(dob)}`;
          if (id) html += ` — <a href="${wta}" target="_blank" rel="noopener">WTA</a>`;
          html += `</li>`;
        }
        html += `</ul></div>`;
        L.circleMarker([g.lat,g.lon],{radius:3+g.names.length,color:"crimson",fill:true,fillOpacity:0.6})
          .bindPopup(html).addTo(circleLayer);
      });
    }

    ['name_search','start','end','rank','min_h','max_h','chk_HU','chk_RH','chk_LH','chk_UL']
      .forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('input', redraw);
      });

    redraw();
  });
  </script>
{% endmacro %}
    """
    html = template.replace("%MAP_VAR%", map_var)
    macro = MacroElement()
    macro._template = Template(html)
    m.get_root().add_child(macro)

    from pathlib import Path
    out_path = Path(out_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(out_path))
    print(f"✅ Map saved to {out_path} (exists={out_path.exists()})")

    

