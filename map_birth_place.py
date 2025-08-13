import pandas as pd
import re
import json
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import folium
from folium import Element
from branca.element import Template, MacroElement



def load_and_clean(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = df[df['birthplace'].notna() & df['birthplace'].str.contains(r',')].copy()
    return df

def load_cache(path: str) -> dict:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_cache(cache: dict, path: str):
    with open(path, "w") as f:
        json.dump(cache, f, indent=2)

def geocode_with_cache(df: pd.DataFrame,
                       cache_file: str,
                       user_agent: str = "birthplace-mapper",
                       delay: float = 1.0) -> pd.DataFrame:
    geolocator = Nominatim(user_agent=user_agent)
    geocode    = RateLimiter(geolocator.geocode, min_delay_seconds=delay)
    cache = load_cache(cache_file)
    places = df['birthplace'].unique()

    for place in places:
        if place in cache:
            continue
        loc = geocode(place)
        cache[place] = (loc.latitude, loc.longitude) if loc else (None, None)

    save_cache(cache, cache_file)

    # map coords back into df
    df['lat'] = df['birthplace'].map(lambda p: cache[p][0])
    df['lon'] = df['birthplace'].map(lambda p: cache[p][1])
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

    # filter‐UI template (as in your original code)
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
    <label><input type="checkbox" id="chk_RH" checked/> Right‑Handed</label>
    <label><input type="checkbox" id="chk_LH" checked/> Left‑Handed</label>
    <label><input type="checkbox" id="chk_UL" checked/> Unlabelled</label>
  </div>

  <script>
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
          // 1) Name search: if non-empty, require substring match
          if(nameF && !p.full_name.toLowerCase().includes(nameF)) return false;

          // 2) Date & rank
          if((s && p.birth_date < s) || (e && p.birth_date > e) || (p.best_rank > r))
            return false;

          // 3) Height
          if(p.height_m === null) {
            if(!keepHU) return false;
          } else {
            if(!isNaN(minH) && p.height_m < minH) return false;
            if(!isNaN(maxH) && p.height_m > maxH) return false;
          }

          // 4) Plays—normalized
          const play = (p.plays || '').toLowerCase().replace(/[^a-z]/g, '');
          if (play.includes('right') && !showRH) return false;
          if (play.includes('left')  && !showLH) return false;
          if (!play && !showUL) return false;

          return true;
        });

        // aggregate & draw as before…
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
          for(let i=0;i<g.names.length;i++){
            const name=g.names[i], dob=g.births[i], id=g.ids[i],
                  wiki=`https://en.wikipedia.org/wiki/${name.replace(/ /g,'_')}`,
                  slug=name.toLowerCase().replace(/ /g,'-'),
                  wta=`https://www.wtatennis.com/players/${id}/${slug}`;
            html+=`<li><a href="${wiki}" target="_blank">${name}</a>, ${dob}, <a href="${wta}" target="_blank">WTA</a></li>`;
          }
          html+=`</ul></div>`;
          L.circleMarker([g.lat,g.lon],{radius:3+g.names.length,color:"crimson",fill:true,fillOpacity:0.6})
            .bindPopup(html).addTo(circleLayer);
        });
      }

      // attach redraw to every input, including the new text box
      ['name_search','start','end','rank','min_h','max_h','chk_HU','chk_RH','chk_LH','chk_UL']
        .forEach(id => document.getElementById(id).addEventListener('input', redraw));

      // initial draw
      redraw();
    });
  </script>
{% endmacro %}
    """
    html = template.replace("%MAP_VAR%", map_var)
    macro = MacroElement()
    macro._template = Template(html)
    m.get_root().add_child(macro)

    m.save(out_html)
    print(f"✅ Map saved to {out_html}")

