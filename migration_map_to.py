import os
import time
import json
import re
from datetime import datetime
import pandas as pd
import pycountry
import folium
from folium import Element, JavascriptLink
from branca.element import Template, MacroElement
import math


# ── CACHE HANDLING ──────────────────────────────────────────
def load_cache_to(path: str) -> dict:
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {'geocode': {}, 'reverse': {}}


def save_cache(cache: dict, path: str):
    with open(path, 'w') as f:
        json.dump(cache, f, indent=2)


def geocode(cache_file, geolocator, place: str, cache: dict, delay: float = 1.0):
    """
    Return (lat, lon) or None. Uses cache['geocode'] where values are either
    [lat, lon] or None (if previous lookup failed).
    """
    cache_geo = cache.setdefault('geocode', {})

    # If present in cache: return either None or the tuple
    if place in cache_geo:
        v = cache_geo[place]
        return None if v is None else tuple(v)

    # Not cached — perform lookup
    time.sleep(delay)
    loc = geolocator.geocode(place)
    coords = (loc.latitude, loc.longitude) if loc else None

    # store either [lat, lon] or None
    cache_geo[place] = list(coords) if coords else None
    save_cache(cache, cache_file)
    return coords


def reverse_iso3(cache_file, geolocator, lat: float, lon: float, cache: dict, delay: float = 1.0):
    """
    Return ISO3 string or None. Uses cache['reverse'] where values are either
    iso3 string or None.
    """
    cache_rev = cache.setdefault('reverse', {})
    key = f"{lat:.5f},{lon:.5f}"

    # If present, return cached value (may be None)
    if key in cache_rev:
        return cache_rev[key]

    time.sleep(delay)
    loc = geolocator.reverse((lat, lon), language="en", timeout=10)
    iso3 = None
    if loc and loc.raw.get('address', {}).get('country_code'):
        try:
            iso2 = loc.raw['address']['country_code'].upper()
            iso3 = pycountry.countries.get(alpha_2=iso2).alpha_3
        except Exception:
            iso3 = None

    cache_rev[key] = iso3
    save_cache(cache, cache_file)
    return iso3


# ── LOAD & NORMALIZE DATA ──────────────────────────────────
def load_and_normalize_to(ioc_to_iso3, csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # IOC to ISO3
    ioc_map = {k.upper(): v for k, v in ioc_to_iso3.items()}
    df['represented_country'] = (
        df['represented_country'].astype(str)
          .str.strip().str.upper()
          .map(lambda c: ioc_map.get(c, c))
    )
    # Drop missing and keep valid ISO3
    valid = {c.alpha_3 for c in pycountry.countries}
    df = df[df['birth_date'].notna() & df['birthplace'].notna()]
    df = df[df['represented_country'].isin(valid)]
    return df

# ── BUILD POINTS & MIGRATIONS ───────────────────────────────
def build_points_and_migrations_to(cache_file, geolocator, df: pd.DataFrame, cache: dict):
    all_pts = []
    migrations = []
    for _, row in df.iterrows():
        # Date parse
        raw = row['birth_date'].strip()
        cleaned = re.sub(r'[^A-Za-z0-9 ]', '', raw)
        try:
            dt = datetime.strptime(cleaned, "%b %d %Y")
        except ValueError:
            dt = datetime.strptime(cleaned, "%B %d %Y")
        iso_date = dt.date().isoformat()
        # Height parse
        h = row.get('height_cm', '')
        try:
            height_m = float(h.strip().rstrip('m')) if isinstance(h, str) and h.strip().endswith('m') else None
        except:
            height_m = None
        to_iso = row['represented_country']
        # add point
        all_pts.append({
            'country': to_iso,
            'birth_date': iso_date,
            'full_name': row['full_name'],
            'player_id': int(row['player_id']),
            'best_rank': float(row['best_rank']),
            'plays': str(row.get('plays','') or '').lower(),
            'height_m': height_m
        })
        # migration logic
        birth_coords = geocode(cache_file, geolocator, row['birthplace'], cache)
        if not birth_coords:
            continue
        from_iso = reverse_iso3(cache_file,geolocator,birth_coords[0],birth_coords[1],cache)
        if from_iso and from_iso != to_iso:
            dest_name = pycountry.countries.get(alpha_3=to_iso).name
            dest_coords = geocode(cache_file, geolocator, dest_name, cache)
            if dest_coords:
                # when building migrations (in Python)
                migrations.append({
                    'from_iso': from_iso,
                    'to_iso': to_iso,
                    'name': row['full_name'],
                    'player_id': int(row['player_id']),
                    'coords': [birth_coords, dest_coords],
                    'birthplace_text': row['birthplace'],            # <-- add this
                    'dest_name': pycountry.countries.get(alpha_3=to_iso).name,  # <-- and this
                    'birth_date': iso_date,
                    'height_m': height_m,
                    'plays': str(row.get('plays','') or '').lower(),
                    'best_rank': float(row['best_rank'])
                })





    good_migrations = []
    bad_entries = []

    def coords_valid(coord):
        try:
            if not coord or len(coord) < 2: 
                return False
            lat = float(coord[0])
            lon = float(coord[1])
            return math.isfinite(lat) and math.isfinite(lon)
        except Exception:
            return False

    for idx, m in enumerate(migrations):
        A, B = m.get('coords', [None, None])
        if coords_valid(A) and coords_valid(B):
            # normalize to [[lat, lon], [lat, lon]] with floats
            m['coords'] = [[float(A[0]), float(A[1])], [float(B[0]), float(B[1])]]
            good_migrations.append(m)
        else:
            bad_entries.append((idx, m.get('name'), m.get('coords')))

    if bad_entries:
        print(f"⚠️ Dropped {len(bad_entries)} migration records with invalid coords. Examples:")
        for i, name, coords in bad_entries[:10]:
            print(f"  - idx={i}, name={name!r}, coords={coords!r}")

    migrations = good_migrations

    return all_pts, migrations

# ── BUILD MAP & SAVE ───────────────────────────────────────

from collections import Counter
from branca.colormap import linear

def build_and_save_map_migration_to(all_pts, migrations, out_html: str):
    # create the folium map (keep a clear name)
    map_obj = folium.Map(location=[20,0], zoom_start=2, tiles="CartoDB Positron")

    # show first 20 migrations (raw) — use different var names so we don't shadow map_obj
    print("First 20 migrations (raw):")
    for i, rec in enumerate(migrations[:20]):
        print(i, rec.get('name'), rec.get('coords'))

    # find any migrations with non-numeric coords or wrong shape
    bad = []
    for i, rec in enumerate(migrations):
        coords = rec.get('coords')
        if not isinstance(coords, (list, tuple)) or len(coords) < 2:
            bad.append((i, rec.get('name'), 'bad-shape', coords))
            continue
        A, B = coords
        if not (isinstance(A, (list, tuple)) and isinstance(B, (list, tuple)) and len(A) >= 2 and len(B) >= 2):
            bad.append((i, rec.get('name'), 'bad-subshape', coords))
            continue
        # numeric check
        try:
            a0, a1, b0, b1 = float(A[0]), float(A[1]), float(B[0]), float(B[1])
            if not all(math.isfinite(x) for x in (a0, a1, b0, b1)):
                bad.append((i, rec.get('name'), 'non-finite', coords))
        except Exception as e:
            bad.append((i, rec.get('name'), f'not-numeric: {e}', coords))

    print(f"Found {len(bad)} suspicious migration(s). Examples (up to 20):")
    for entry in bad[:20]:
        print(entry)

    # add turf dependency to the real map object
    map_obj.get_root().header.add_child(
        JavascriptLink("https://unpkg.com/@turf/turf@6.5.0/turf.min.js")
    )

    # --- choropleth setup (arrivals = TO) ---
    inbound_counts = Counter(rec['to_iso'] for rec in migrations)
    max_count = max(inbound_counts.values(), default=1)
    colormap = linear.YlOrRd_09.scale(0, max_count)
    colormap.caption = "Number of immigrant players"
    colormap.add_to(map_obj)

    count2color = {i: colormap(i) for i in range(0, max_count+1)}


    # inject JS variables (still using the map_obj variable name)
    map_obj.get_root().html.add_child(Element(
        f"<script>\n"
        f"var allPoints = {json.dumps(all_pts)};\n"
        f"var migrations = {json.dumps(migrations)};\n"
        f"var count2color = {json.dumps(count2color)};\n"
        f"</script>"
    ))

    # add GeoJSON polygons
    geojson_url = "https://raw.githubusercontent.com/johan/world.geo.json/master/countries.geo.json"
    folium.GeoJson(
        geojson_url,
        name="countries",
        featureidkey="id",
        style_function=lambda feature: {
            'fillColor':   colormap(inbound_counts.get(feature['id'], 0)),
            'color':       '#999',
            'weight':      1,
            'fillOpacity': 0.7
        },
        highlight_function=lambda feature: {
            'weight':      2,
            'color':       '#333',
            'fillOpacity': 0.3
        }
    ).add_to(map_obj)

    # inject the macro (unchanged)
    macro = MacroElement()
    # --- 3) Inject the interactive macro (filters + hover/click lines) ---
    macro_html = """
{% macro html(this, kwargs) %}
  <style>
    #migfilters {
      position: absolute; top: 10px; left: 10px; right: auto;
      z-index: 9999; background: white; padding: 8px;
      box-shadow: 0 0 6px rgba(0,0,0,0.3);
      font-size: 12px; border-radius: 4px;
      font-family: Arial, sans-serif;
    }
    #migfilters label { display: block; margin: 4px 0; }
    #migfilters input { width: 110px; }
    #migfilters hr { margin: 6px 0; border: none; border-top: 1px solid #ccc; }

    .player-tooltip {
      background: rgba(255,255,255,0.95);
      border: 1px solid rgba(0,0,0,0.12);
      padding: 6px 8px;
      border-radius: 4px;
      font-size: 12px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.15);
      pointer-events: auto;
    }
    .player-tooltip a { color: #1a73e8; text-decoration: none; margin-right:6px; pointer-events: auto; cursor: pointer; }
    .player-tooltip .more-toggle {
      background: #f1f1f1;
      border: 1px solid #ddd;
      border-radius: 2px;
      padding: 0 6px;
      cursor: pointer;
      line-height: 18px;
      font-size: 12px;
    }
    .player-tooltip .more-info { display:none; margin-top:6px; font-size:12px; color:#333; }
  </style>

  <div id="migfilters">
    <label>Search Name: <input type="text" id="flt_name" placeholder="e.g. Vesnina"/></label>
    <hr/>
    <label>Born From: <input type="date" id="flt_start"/></label>
    <label>Born To:   <input type="date" id="flt_end"/></label>
    <label>Max Rank:  <input type="number" id="flt_rank" min="1"/></label>
    <hr/>
    <label>Min Height (m): <input type="number" step="0.01" id="flt_min_h"/></label>
    <label>Max Height (m): <input type="number" step="0.01" id="flt_max_h"/></label>
    <label><input type="checkbox" id="flt_keep_hu" checked/> Keep Unknown Heights</label>
    <hr/>
    <label><input type="checkbox" id="flt_RH" checked/> Right-Handed</label>
    <label><input type="checkbox" id="flt_LH" checked/> Left-Handed</label>
    <label><input type="checkbox" id="flt_UL" checked/> Unlabelled</label>
  </div>

  <script>
  (function(){
    setTimeout(function(){
      const mapKey = Object.keys(window).find(k=>k.startsWith("map_"));
      if (!mapKey) return console.error("Map not found");
      const map = window[mapKey];

      // panes
      map.createPane("migPane"); map.getPane("migPane").style.zIndex = 700;
      map.createPane("tooltipPane"); map.getPane("tooltipPane").style.zIndex = 800;
      map.getPane("tooltipPane").style.pointerEvents = 'auto';

      const countryLayers = [];
      map.eachLayer(l => { if (l.feature && l.feature.id) countryLayers.push(l); });
    
      let groups = {};

      
      // precompute totals
      const originTotals = {};
      const dupCounts = {};
      migrations.forEach(m => {
        originTotals[m.from_iso] = (originTotals[m.from_iso]||0)+1;
        const key = m.from_iso + '|' + m.to_iso;
        dupCounts[key] = (dupCounts[key]||0)+1;
      });

      const originIsos = Object.keys(originTotals).sort();
      const originBaseHue = {};
      originIsos.forEach((iso,i) => {
        originBaseHue[iso] = 160 + Math.round((i / Math.max(1, originIsos.length-1))*140);
      });

      let migrationLines = {};
      let allBuilt = false;

      function hideAll() {
        Object.values(migrationLines).flat().forEach(line => {
          try {
            line.setStyle({ opacity: 0 });
            if (line._tooltipBound) { try { line.closeTooltip(); } catch(e){} line.unbindTooltip(); line._tooltipBound = false; }
            if (line._popupBound) { try { line.closePopup(); } catch(e){} }
            if (line.options) line.options.interactive = false;
          } catch(e){}
        });
      }

      // clicking the map outside country layers should deselect & close popups
      map.on('click', function(){
        if (selected) {
          selected = null;
          hideAll();
          buildAndWireLines();
        } else {
          hideAll();
        }
        Object.values(migrationLines).flat().forEach(l => { try { l.closePopup(); } catch(e){} });
      });

      // prevent clicks inside popups from propagating to the map
      map.on('popupopen', function(e){
        try {
          const el = e.popup.getElement();
          if (el) {
            el.addEventListener('click', function(ev){ ev.stopPropagation(); }, { passive: false });
          }
        } catch(err){}
      });

      function buildAllLinesOnce() {
        if (allBuilt) return;
        const dupIndex = {}, originIndex = {};
        let skipped = 0;

        migrations.forEach((m, idx) => {
          try {
            const coordsRaw = m && m.coords;
            if (!Array.isArray(coordsRaw) || coordsRaw.length < 2) { skipped++; return; }
            const A = coordsRaw[0], B = coordsRaw[1];
            if (!Array.isArray(A) || !Array.isArray(B) || A.length<2 || B.length<2) { skipped++; return; }
            const aLat = Number(A[0]), aLon = Number(A[1]), bLat = Number(B[0]), bLon = Number(B[1]);
            if (![aLat,aLon,bLat,bLon].every(v=>Number.isFinite(v))) { skipped++; return; }

            // indices
            const origin = m.from_iso;
            const totalOrigin = originTotals[origin] || 1;
            const oIdx = originIndex[origin] || 0; originIndex[origin] = oIdx + 1;

            const key = origin + '|' + m.to_iso;
            const totalDup = dupCounts[key] || 1;
            const dIdx = dupIndex[key] || 0; dupIndex[key] = dIdx + 1;

            // arc (turf expects [lon,lat])
            const arc = turf.greatCircle(turf.point([aLon,aLat]), turf.point([bLon,bLat]), { npoints: 50 });
            if (!arc || !arc.geometry || !Array.isArray(arc.geometry.coordinates)) { skipped++; return; }
            let coords = arc.geometry.coordinates.map(c => [c[1], c[0]]); // [lat,lon]

            // offset duplicates but KEEP endpoints exact
            const dx = bLon - aLon, dy = bLat - aLat;
            let perp_lon = -dy, perp_lat = dx;
            let plen = Math.sqrt(perp_lon*perp_lon + perp_lat*perp_lat);
            if (plen === 0) { perp_lon = 0; perp_lat = 0; plen = 1; }
            const ux = perp_lon / plen, uy = perp_lat / plen;
            const degDist = Math.sqrt((bLat-aLat)*(bLat-aLat) + (bLon-aLon)*(bLon-aLon));
            const base_offset = 0.9, scale = Math.min(1, 8 / Math.max(0.0001, degDist));
            const dup_offset = (dIdx - (totalDup-1)/2) * base_offset * (1 + dIdx*0.35) * scale;

            if (dup_offset !== 0) {
              const n = coords.length;
              coords = coords.map((c,i) => {
                const t = i / (n-1);
                const taper = Math.sin(Math.PI * t);
                return [ c[0] + (uy * dup_offset * taper), c[1] + (ux * dup_offset * taper) ];
              });
              coords[0] = [ aLat, aLon ];
              coords[coords.length-1] = [ bLat, bLon ];
            }

            // color & weight
            const baseHue = originBaseHue[origin] || 200;
            const hueDelta = 6;
            const hue = baseHue + ((oIdx - (totalOrigin-1)/2) * hueDelta);
            const lightMin = 36, lightMax = 58;
            const light = totalOrigin>1 ? (lightMin + (oIdx/(totalOrigin-1))*(lightMax-lightMin)) : ((lightMin+lightMax)/2);
            const color = 'hsl(' + Math.round(hue) + ',72%,' + Math.round(light) + '%)';
            const weight = Math.max(1, Math.min(5, 1.0 + Math.log1p(totalDup)));

            const anyBad = coords.some(c => !Array.isArray(c) || c.length < 2 || !Number.isFinite(Number(c[0])) || !Number.isFinite(Number(c[1])));
            if (anyBad) { skipped++; return; }

            const line = L.polyline(coords, { pane: 'migPane', weight: weight, color: color, opacity: 0, interactive: false }).addTo(map);
            line._tooltipBound = false;

            // store metadata including the birthplace_text and dest_name present in the migration object
            line._meta = {
              from_iso: m.from_iso,
              to_iso: m.to_iso,
              birth_date: m.birth_date,
              best_rank: m.best_rank,
              height_m: m.height_m,
              plays: m.plays || '',
              name: m.name,
              player_id: m.player_id || null,
              birthplace_text: m.birthplace_text || '',
              dest_name: m.dest_name || ''
            };

            migrationLines[m.to_iso] = migrationLines[m.to_iso] || [];
            migrationLines[m.to_iso].push(line);

          } catch (err) { skipped++; return; }
        });

        if (skipped > 0) console.warn("buildAllLinesOnce: skipped", skipped, "bad migrations");
        allBuilt = true;
      }

      function buildAndWireLines() {
        buildAllLinesOnce();

        const nameF = document.getElementById('flt_name').value.trim().toLowerCase();
        const s = document.getElementById('flt_start').value;
        const e = document.getElementById('flt_end').value;
        const maxR = parseInt(document.getElementById('flt_rank').value) || Infinity;
        const minH = parseFloat(document.getElementById('flt_min_h').value);
        const maxH = parseFloat(document.getElementById('flt_max_h').value);
        const keepHU = document.getElementById('flt_keep_hu').checked;
        const showRH = document.getElementById('flt_RH').checked;
        const showLH = document.getElementById('flt_LH').checked;
        const showUL = document.getElementById('flt_UL').checked;

        Object.values(migrationLines).flat().forEach(line => {
          const m = line._meta;
          let ok = true;
          if (nameF && !m.name.toLowerCase().includes(nameF)) ok = false;
          if ((s && m.birth_date < s) || (e && m.birth_date > e)) ok = false;
          if (m.best_rank > maxR) ok = false;
          if (m.height_m === null) { if (!keepHU) ok = false; }
          else {
            if (!isNaN(minH) && m.height_m < minH) ok = false;
            if (!isNaN(maxH) && m.height_m > maxH) ok = false;
          }
          const play = (m.plays || '');
          if ((play.includes("right") && !showRH) || (play.includes("left") && !showLH) || (!play && !showUL)) ok = false;

          line._visibleByFilter = ok;
          line.setStyle({ opacity: 0 });
          if (line.options) line.options.interactive = false;
        });

        groups = {};
        Object.values(migrationLines).flat().forEach(line => {
          if (line._visibleByFilter) {
            const iso = line._meta.to_iso;   // <- use destination ISO
            groups[iso] = (groups[iso] || 0) + 1;
          }
        });


        countryLayers.forEach(layer => {
          const c = (groups[layer.feature.id] || 0);
          try { layer.setStyle({ fillColor: count2color[c] || count2color[0] }); } catch(e) {}
        });

        // show selected country's visible lines
        if (selected) {
          const lines = migrationLines[selected] || [];
          lines.forEach(l => {
            if (l._visibleByFilter) {
              if (l.options) l.options.interactive = true;

              // lazy-bind popup (once) using DOM creation (safer than string-building)
              if (!l._popupBound) {
                const safeName = l._meta.name || '';
                const wikiName = encodeURIComponent(safeName.replace(/\s+/g, '_'));
                const wikiUrl = 'https://fr.wikipedia.org/wiki/' + wikiName;
                const pid = l._meta.player_id || '';
                let slug = safeName.toLowerCase().replace(/[^a-z0-9\u00C0-\u024F]+/g, '-').replace(/(^-|-$)/g,'');
                slug = encodeURIComponent(slug);
                const wtaUrl = pid ? ('https://www.wtatennis.com/players/' + pid + '/' + slug) : '#';

                const originText = l._meta.birthplace_text || '';
                const destText = l._meta.dest_name || '';

                // build DOM popup content
                const contentEl = document.createElement('div');
                contentEl.className = 'player-tooltip';
                contentEl.addEventListener('click', function(ev){ ev.stopPropagation(); });

                const row = document.createElement('div');
                row.style.display = 'flex';
                row.style.alignItems = 'center';
                row.style.gap = '8px';

                const aWiki = document.createElement('a');
                aWiki.href = wikiUrl;
                aWiki.target = '_blank';
                aWiki.rel = 'noopener noreferrer';
                aWiki.textContent = safeName;
                aWiki.addEventListener('click', function(ev){ ev.stopPropagation(); });

                const aWta = document.createElement('a');
                aWta.href = wtaUrl;
                aWta.target = '_blank';
                aWta.rel = 'noopener noreferrer';
                aWta.textContent = 'WTA';
                aWta.addEventListener('click', function(ev){ ev.stopPropagation(); });

                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'more-toggle';
                btn.textContent = '+';
                btn.addEventListener('click', function(ev){
                  ev.stopPropagation();
                  const info = contentEl.querySelector('.more-info');
                  if (!info) return;
                  info.style.display = (info.style.display === 'block') ? 'none' : 'block';
                });

                row.appendChild(aWiki);
                row.appendChild(aWta);
                row.appendChild(btn);

                const info = document.createElement('div');
                info.className = 'more-info';
                info.style.display = 'none';
                info.textContent = (originText ? (originText + ' → ') : '') + (destText || '');

                contentEl.appendChild(row);
                contentEl.appendChild(info);

                // bind popup using the DOM node (Leaflet accepts HTMLElement)
                l.bindPopup(contentEl, {
                  className: 'player-tooltip',
                  pane: 'tooltipPane',
                  closeOnClick: false,
                  autoClose: false,
                  interactive: true,
                  maxWidth: 350
                });

                l._popupBound = true;
              }

              // open the popup for this line
              try { l.openPopup(); } catch(e){}

              l.setStyle({ opacity: 1 });
              l.bringToFront();
              try { l.openTooltip(); } catch(e) {}
            }
          });
        } else {
          // when deselected ensure all popups closed
          Object.values(migrationLines).flat().forEach(l => { try { l.closePopup(); } catch(e){} });
        }
      }

      // attach handlers
      // attach handlers
let selected = null;
countryLayers.forEach(layer => {
  const iso = layer.feature.id;

  layer.on('click', function(e) {
    if (e && e.originalEvent) { L.DomEvent.stopPropagation(e); }
    selected = (selected === iso ? null : iso);
    buildAndWireLines();
  });

  layer.on('mouseover', function() {
    // compute the filtered color for this country; if absent use default 0
    const c = (groups[iso] || 0);
    const fill = count2color[c] || count2color[0];
    try { layer.setStyle({ fillColor: fill, weight: 2, color: '#333' }); } catch(e) {}
    if (!selected) {
      (migrationLines[iso]||[]).forEach(l => {
        if (l._visibleByFilter) { l.setStyle({opacity:1}); l.bringToFront(); }
      });
    }
  });

  layer.on('mouseout', function() {
    // reapply the filtered color (so hover doesn't revert to original)
    const c = (groups[iso] || 0);
    const fill = count2color[c] || count2color[0];
    try { layer.setStyle({ fillColor: fill, weight: 1, color: '#999' }); } catch(e) {}
    if (!selected) {
      (migrationLines[iso]||[]).forEach(l => { l.setStyle({opacity:0}); try{ l.closeTooltip(); }catch(e){} });
    }
  });
});


      // inputs redraw
      [
         'flt_name','flt_start','flt_end','flt_rank',
         'flt_min_h','flt_max_h','flt_keep_hu',
         'flt_RH','flt_LH','flt_UL'
      ].forEach(id => {
         document.getElementById(id).addEventListener('input', () => {
           selected = null;
           buildAndWireLines();
         });
      });

      // initial draw
      buildAndWireLines();

    }, 500);
  })();
  </script>
{% endmacro %}
"""






    macro = MacroElement()
    macro._template = Template(macro_html)
    map_obj.get_root().add_child(macro)

    # finally save (use map_obj)
    map_obj.save(out_html)
    print(f"🔗 {out_html} written.")
