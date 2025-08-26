# migration_map_to_atp.py
"""
Version 'to' (arrivées) adaptée au CSV masculin (ATP).
Principales adaptations :
 - player_id gardé comme string (ex: 'S0AG') — NE PAS caster en int
 - highest_ranking -> best_rank parsing robuste
 - parsing hauteur à partir de height_cm (1.91m / 191) ou height_inches ("6' 3\"")
 - utilise scripts.geocode_utils_atp (geocode_place, reverse_to_iso3, load_cache, save_cache, get_geolocator)
 - popups / liens ATP (atptour) au lieu de WTA
"""
import os
import time
import json
import re
from datetime import datetime
import math
from collections import Counter

import pandas as pd
import pycountry
import folium
from folium import Element, JavascriptLink
from branca.element import Template, MacroElement
from branca.colormap import linear

# utilitaires geocode pour ATP (fichier séparé attendu)
from scripts.geocode_utils_atp import (
    load_cache, save_cache, geocode_place, reverse_to_iso3, is_skip_geocode, get_geolocator
)

# robust cache I/O helpers (atomic write fallback similar to other modules)
import tempfile

def load_cache_to_safe(path: str) -> dict:
    try:
        return load_cache(path)
    except Exception:
        # fallback minimal structure
        return {"geocode": {}, "reverse": {}}

def save_cache_to_safe(cache: dict, path: str):
    try:
        save_cache(cache, path)
    except Exception:
        pass


# ── parsing helpers ───────────────────────────────────────────

_DATE_PATTERNS = [
    "%Y-%m-%d",   # 2001-08-16
    "%Y/%m/%d",
    "%b %d %Y",   # Aug 16 2001
    "%B %d %Y",   # August 16 2001
    "%d %b %Y",
    "%d %B %Y",
]

def parse_birth_date(raw):
    if not raw or (isinstance(raw, float) and math.isnan(raw)):
        return None
    s = str(raw).strip()
    if s == '':
        return None
    s_clean = re.sub(r'[,]', ' ', s).strip()
    for patt in _DATE_PATTERNS:
        try:
            dt = datetime.strptime(s_clean, patt)
            return dt.date().isoformat()
        except Exception:
            continue
    cleaned = re.sub(r'[^A-Za-z0-9 ]+', ' ', s_clean).strip()
    for patt in ["%b %d %Y", "%B %d %Y", "%d %b %Y", "%d %B %Y"]:
        try:
            dt = datetime.strptime(cleaned, patt)
            return dt.date().isoformat()
        except Exception:
            continue
    m = re.search(r'(\d{4})', s_clean)
    if m:
        return f"{m.group(1)}-01-01"
    return None

def parse_height_to_meters(height_cm_field, height_inches_field=None):
    # try height_cm first
    h = height_cm_field
    if isinstance(h, str):
        s = h.strip().lower().replace(',', '.')
        if s.endswith('m'):
            s2 = s[:-1].strip()
            try:
                val = float(s2)
                if val > 3:  # in case it's centimeters
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
    # fallback to inches (e.g. 6' 3"")
    h2 = height_inches_field
    if isinstance(h2, str) and h2.strip():
        s = h2.strip()
        m = re.match(r"""^\s*(\d+)\s*['’]\s*(\d+)\s*(?:"{0,2})\s*$""", s)
        if m:
            try:
                feet = int(m.group(1))
                inches = int(m.group(2))
                total_inches = feet * 12 + inches
                meters = total_inches * 0.0254
                return round(meters, 3)
            except Exception:
                pass
        m2 = re.match(r'^\s*(\d+)\s*(?:ft|feet|\'|’)\s*$', s)
        if m2:
            try:
                feet = int(m2.group(1))
                meters = feet * 12 * 0.0254
                return round(meters, 3)
            except Exception:
                pass
    return None

def parse_best_rank(v):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return float('inf')
        s = str(v).strip()
        if s == '' or s == '-' or s.lower() in ('nan', 'none'):
            return float('inf')
        m = re.search(r'(\d+)', s)
        return float(m.group(1)) if m else float('inf')
    except Exception:
        return float('inf')


# ── LOAD & NORMALIZE DATA ──────────────────────────────────────────

def load_and_normalize_to(ioc_to_iso3, csv_path: str) -> pd.DataFrame:
    """
    Charge le CSV ATP 'to' (arrivées) et normalise:
     - represented_country -> ISO3 (via ioc_to_iso3 map or pycountry)
     - garde uniquement lignes ayant birth_date et birthplace non null (comme version WTA)
    """
    df = pd.read_csv(csv_path, dtype=str)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    ioc_map = {k.upper(): v for k, v in (ioc_to_iso3 or {}).items()}

    def map_country(c):
        if c is None or (isinstance(c, float) and pd.isna(c)):
            return None
        s = str(c).strip().upper()
        if s == '':
            return None
        if s in ioc_map:
            return ioc_map[s]
        try:
            if pycountry.countries.get(alpha_3=s):
                return s
        except Exception:
            pass
        try:
            c2 = pycountry.countries.get(alpha_2=s)
            if c2:
                return c2.alpha_3
        except Exception:
            pass
        return s

    df['represented_country'] = df.get('represented_country', pd.Series()).map(map_country)

    # keep only rows that have birth_date and birthplace
    df['birth_date_iso'] = df.get('birth_date', pd.Series()).map(parse_birth_date)
    df = df[df['birth_date_iso'].notna() & df.get('birthplace', pd.Series()).notna()]

    # filter valid ISO3
    valid_iso3 = {c.alpha_3 for c in pycountry.countries}
    df = df[df['represented_country'].isin(valid_iso3)].copy()

    # compute best_rank
    if 'best_rank' not in df.columns and 'highest_ranking' in df.columns:
        df['best_rank'] = df['highest_ranking'].map(parse_best_rank)
    else:
        df['best_rank'] = df.get('best_rank', pd.Series()).map(parse_best_rank)

    # compute height_m
    df['height_m'] = df.apply(lambda r: parse_height_to_meters(r.get('height_cm'), r.get('height_inches')), axis=1)

    # plays normalized
    df['plays_norm'] = df.get('plays', pd.Series()).fillna('').astype(str).str.lower()

    # ensure full_name field
    if 'full_name' not in df.columns and 'name' in df.columns:
        df['full_name'] = df['name']
    df['full_name'] = df.get('full_name', pd.Series()).fillna('')

    return df


# ── BUILD POINTS & MIGRATIONS ───────────────────────────────────────
def build_points_and_migrations_to(cache_file: str, geolocator, df: pd.DataFrame, cache: dict = None):
    """
    Retourne (all_pts, migrations)
     - all_pts: liste de points (destinations) pour chaque joueur
     - migrations: listes d'objets décrivant les migrations (from_iso -> to_iso)

    Signature compatible avec main_maps_atp (on reçoit le geolocator en 2e argument).
    """
    # assure cache loaded
    if cache is None:
        cache = load_cache_safe(cache_file)

    all_pts = []
    migrations = []

    # use provided geolocator or fallback
    if geolocator is None:
        geolocator = get_geolocator(user_agent="migration-atp", timeout=10)

    for _, row in df.iterrows():
        birth_date_iso = row.get('birth_date_iso') or parse_birth_date(row.get('birth_date', ''))
        if not birth_date_iso:
            continue

        height_m = row.get('height_m')
        try:
            height_m = float(height_m) if (height_m is not None and str(height_m) != 'nan') else None
        except Exception:
            height_m = None

        to_iso = row.get('represented_country')
        if not to_iso:
            continue

        # add point (destination)
        all_pts.append({
            'country': to_iso,
            'birth_date': birth_date_iso,
            'full_name': row.get('full_name') or '',
            'player_id': row.get('player_id') or '',
            'best_rank': float(row.get('best_rank', float('inf')) if row.get('best_rank') not in (None, '') else float('inf')),
            'plays': str(row.get('plays_norm') or '').lower(),
            'height_m': height_m
        })

        # geocode birthplace -> coords
        birthplace_text = row.get('birthplace') or ''
        if not birthplace_text:
            continue

        birth_coords = geocode_place(
            birthplace_text, cache, cache_file,
            user_agent="migration-atp", delay=1.0, timeout=10
        )
        if not birth_coords:
            # either not found or SKIP_GEOCODE
            continue

        # reverse to ISO3
        from_iso = reverse_to_iso3(
            float(birth_coords[0]), float(birth_coords[1]), cache, cache_file,
            user_agent="migration-atp", delay=1.0, timeout=10
        )
        if not from_iso:
            continue

        # only create migration if different from represented_country
        if from_iso and from_iso != to_iso:
            # resolve dest coordinates by country name
            try:
                dest_name = pycountry.countries.get(alpha_3=to_iso).name
            except Exception:
                dest_name = None

            dest_coords = None
            if dest_name:
                # use geocode_place for country name (respect cache)
                dest_coords = geocode_place(dest_name, cache, cache_file,
                                            user_agent="migration-atp", delay=1.0, timeout=10)
            if not dest_coords:
                # fallback: skip if can't geocode destination country
                continue

            migrations.append({
                'from_iso': from_iso,
                'to_iso': to_iso,
                'name': row.get('full_name') or '',
                'player_id': row.get('player_id') or '',
                'coords': [birth_coords, dest_coords],
                'birthplace_text': birthplace_text,
                'dest_name': dest_name or '',
                'birth_date': birth_date_iso,
                'height_m': height_m,
                'plays': str(row.get('plays_norm') or '').lower(),
                'best_rank': float(row.get('best_rank', float('inf')) if row.get('best_rank') not in (None, '') else float('inf'))
            })

    # validate coords shapes and convert to floats
    good_migrations = []
    bad_entries = []

    def coords_valid(coord):
        try:
            if not coord or len(coord) < 2:
                return False
            lat = float(coord[0]); lon = float(coord[1])
            return math.isfinite(lat) and math.isfinite(lon)
        except Exception:
            return False

    for idx, m in enumerate(migrations):
        A, B = m.get('coords', [None, None])
        if coords_valid(A) and coords_valid(B):
            m['coords'] = [[float(A[0]), float(A[1])], [float(B[0]), float(B[1])]]
            good_migrations.append(m)
        else:
            bad_entries.append((idx, m.get('name'), m.get('coords')))

    if bad_entries:
        print(f"⚠️ Dropped {len(bad_entries)} migration records with invalid coords. Examples:")
        for i, name, coords in bad_entries[:10]:
            print(f"  - idx={i}, name={name!r}, coords={coords!r}")

    return all_pts, good_migrations



# ── BUILD MAP & SAVE ─────────────────────────────────────────────

def build_and_save_map_migration_to(all_pts, migrations, out_html: str):
    """
    Build the folium map for arrivals (TO). Interactivity & UI is the same as WTA version,
    but popups link to ATP where appropriate.
    """
    map_obj = folium.Map(location=[20, 0], zoom_start=2, tiles="CartoDB Positron")

    # debug
    print("First 20 migrations (raw):")
    for i, rec in enumerate(migrations[:20]):
        print(i, rec.get('name'), rec.get('coords'))

    # sanity check migrations
    bad = []
    for i, rec in enumerate(migrations):
        coords = rec.get('coords')
        if not isinstance(coords, (list, tuple)) or len(coords) < 2:
            bad.append((i, rec.get('name'), 'bad-shape', coords)); continue
        A, B = coords
        if not (isinstance(A, (list, tuple)) and isinstance(B, (list, tuple)) and len(A) >= 2 and len(B) >= 2):
            bad.append((i, rec.get('name'), 'bad-subshape', coords)); continue
        try:
            a0, a1, b0, b1 = float(A[0]), float(A[1]), float(B[0]), float(B[1])
            if not all(math.isfinite(x) for x in (a0, a1, b0, b1)):
                bad.append((i, rec.get('name'), 'non-finite', coords))
        except Exception as e:
            bad.append((i, rec.get('name'), f'not-numeric: {e}', coords))

    print(f"Found {len(bad)} suspicious migration(s). Examples (up to 20):")
    for entry in bad[:20]:
        print(entry)

    # add turf dependency
    map_obj.get_root().header.add_child(
        JavascriptLink("https://unpkg.com/@turf/turf@6.5.0/turf.min.js")
    )

    inbound_counts = Counter(rec['to_iso'] for rec in migrations)
    max_count = max(inbound_counts.values(), default=1)
    colormap = linear.YlOrRd_09.scale(0, max_count)
    colormap.caption = "Number of immigrant players"
    colormap.add_to(map_obj)
    count2color = {i: colormap(i) for i in range(0, max_count+1)}

    # inject data variables
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

    # macro: interactive JS (adapted to use ATP links in popups)
    macro = MacroElement()
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
    <label>Search Name: <input type="text" id="flt_name" placeholder="e.g. Djokovic"/></label>
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

            const origin = m.from_iso;
            const totalOrigin = originTotals[origin] || 1;
            const oIdx = originIndex[origin] || 0; originIndex[origin] = oIdx + 1;

            const key = origin + '|' + m.to_iso;
            const totalDup = dupCounts[key] || 1;
            const dIdx = dupIndex[key] || 0; dupIndex[key] = dIdx + 1;

            const arc = turf.greatCircle(turf.point([aLon,aLat]), turf.point([bLon,bLat]), { npoints: 50 });
            if (!arc || !arc.geometry || !Array.isArray(arc.geometry.coordinates)) { skipped++; return; }
            let coords = arc.geometry.coordinates.map(c => [c[1], c[0]]); // [lat,lon]

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
            const iso = line._meta.to_iso;
            groups[iso] = (groups[iso] || 0) + 1;
          }
        });

        countryLayers.forEach(layer => {
          const c = (groups[layer.feature.id] || 0);
          try { layer.setStyle({ fillColor: count2color[c] || count2color[0] }); } catch(e) {}
        });

        if (selected) {
          const lines = migrationLines[selected] || [];
          lines.forEach(l => {
            if (l._visibleByFilter) {
              if (l.options) l.options.interactive = true;





              if (!l._popupBound) {
                  const safeName = l._meta.name || '';
                  const pid = l._meta.player_id || '';
                  let slug = safeName.toLowerCase().replace(/[^a-z0-9\u00C0-\u024F]+/g,'-').replace(/(^-|-$)/g,'');
                  slug = encodeURIComponent(slug);

                                // use SITE_BASE defined once at script top (do not redeclare here)
                  const localUrl = (typeof SITE_BASE !== 'undefined' ? SITE_BASE.replace(/\/$/, '') : 'https://www.center-court.net') 
                                   + '/players_atp/' 
                                   + (pid ? (encodeURIComponent(pid) + '-' + slug + '.html') : (slug + '.html'));

                  const atpUrl = pid ? ('https://www.atptour.com/en/players/' + slug + '/' + encodeURIComponent(String(pid).toLowerCase()) + '/overview') : '#';


                  const originText = l._meta.birthplace_text || '';
                  const destText = l._meta.dest_name || '';

                  const contentEl = document.createElement('div');
                  contentEl.className = 'player-tooltip';
                  contentEl.addEventListener('click', function(ev){ ev.stopPropagation(); });

                  const row = document.createElement('div');
                  row.style.display = 'flex';
                  row.style.alignItems = 'center';
                  row.style.gap = '8px';

                  const aLocal = document.createElement('a');
                  aLocal.href = localUrl;
                  aLocal.textContent = safeName;
                  aLocal.addEventListener('click', function(ev){ ev.stopPropagation(); });

                  const aAtp = document.createElement('a');
                  aAtp.href = atpUrl;
                  aAtp.target = '_blank';
                  aAtp.rel = 'noopener noreferrer';
                  aAtp.textContent = 'ATP';
                  aAtp.addEventListener('click', function(ev){ ev.stopPropagation(); });

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

                  row.appendChild(aLocal);
                  row.appendChild(aAtp);
                  row.appendChild(btn);

                  const info = document.createElement('div');
                  info.className = 'more-info';
                  info.style.display = 'none';
                  info.textContent = (originText ? (originText + ' → ') : '') + (destText || '');

                  contentEl.appendChild(row);
                  contentEl.appendChild(info);

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



              try { l.openPopup(); } catch(e){}
              l.setStyle({ opacity: 1 });
              l.bringToFront();
              try { l.openTooltip(); } catch(e) {}
            }
          });
        } else {
          Object.values(migrationLines).flat().forEach(l => { try { l.closePopup(); } catch(e){} });
        }
      }

      let selected = null;
      countryLayers.forEach(layer => {
        const iso = layer.feature.id;

        layer.on('click', function(e) {
          if (e && e.originalEvent) { L.DomEvent.stopPropagation(e); }
          selected = (selected === iso ? null : iso);
          buildAndWireLines();
        });

        layer.on('mouseover', function() {
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
          const c = (groups[iso] || 0);
          const fill = count2color[c] || count2color[0];
          try { layer.setStyle({ fillColor: fill, weight: 1, color: '#999' }); } catch(e) {}
          if (!selected) {
            (migrationLines[iso]||[]).forEach(l => { l.setStyle({opacity:0}); try{ l.closeTooltip(); }catch(e){} });
          }
        });
      });

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

      buildAndWireLines();

    }, 500);
  })();
  </script>
{% endmacro %}
"""
    macro._template = Template(macro_html)
    map_obj.get_root().add_child(macro)

    # save
    from pathlib import Path
    out_path = Path(out_html)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    map_obj.save(str(out_path))
    print(f"✅ Map saved to {out_path} (exists={out_path.exists()})")
