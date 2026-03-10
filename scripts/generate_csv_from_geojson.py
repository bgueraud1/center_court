#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
geojson_to_mymaps.py
Convertit un GeoJSON (ou JSON GeoGuessr avec key "customCoordinates") en CSV lisible par Google My Maps.
Usage:
    python geojson_to_mymaps.py input.geojson output.csv
"""

import json
import csv
import argparse
from typing import Any, Dict, List, Tuple, Optional

# Colonnes de sortie (Google My Maps reconnaîtra Latitude/Longitude)
CSV_FIELDS = [
    "Name",
    "Description",
    "Latitude",
    "Longitude",
    "heading",
    "pitch",
    "zoom",
    "panoId",
    "tags",
    "panoDate"
]

def safe_get(d: Dict[str, Any], *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return default
    return d

def coords_from_feature(feature: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """
    Retourne (lat, lng) à partir d'une Feature GeoJSON si possible.
    Note: GeoJSON stocke coords en [lng, lat].
    """
    geom = feature.get("geometry")
    if not geom:
        return None
    gtype = geom.get("type", "").lower()
    coords = geom.get("coordinates")
    if not coords:
        return None
    try:
        if gtype == "point":
            lng, lat = coords[0], coords[1]
            return (lat, lng)
        if gtype == "multipoint" and len(coords) > 0:
            lng, lat = coords[0][0], coords[0][1]
            return (lat, lng)
        if gtype in ("lineString".lower(), "linestring", "polygon") and len(coords) > 0:
            # fallback: prendre le premier point
            c = coords[0]
            if isinstance(c[0], (list, tuple)):
                lng, lat = c[0], c[1]
            else:
                lng, lat = c[0], c[1]
            return (lat, lng)
    except Exception:
        return None
    return None

def coords_from_geoguessr_coord(obj: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """
    Pour JSON de type GeoGuessr customCoordinates: objet avec lat, lng clés.
    """
    if "lat" in obj and "lng" in obj:
        try:
            return (float(obj["lat"]), float(obj["lng"]))
        except Exception:
            return None
    return None

def flatten_properties(props: Dict[str, Any]) -> Dict[str, str]:
    """
    Extrait les champs utiles et renvoie un dict de chaînes.
    """
    out = {}
    out["heading"] = str(safe_get(props, "heading", default="")) if props else ""
    out["pitch"] = str(safe_get(props, "pitch", default=""))
    out["zoom"] = str(safe_get(props, "zoom", default=""))
    out["panoId"] = str(safe_get(props, "panoId", default=""))
    # certains exports ont "extra": {"tags": [...], "panoDate": "YYYY-MM"}
    tags = safe_get(props, "tags")
    if tags is None:
        tags = safe_get(props, "extra", "tags")
    if isinstance(tags, (list, tuple)):
        out["tags"] = ";".join(map(str, tags))
    elif isinstance(tags, str):
        out["tags"] = tags
    else:
        out["tags"] = ""
    panoDate = safe_get(props, "panoDate") or safe_get(props, "extra", "panoDate") or ""
    out["panoDate"] = str(panoDate)
    return out

def convert(input_path: str, output_path: str):
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows: List[Dict[str, str]] = []

    # Cas 1: GeoGuessr-like export avec clé "customCoordinates" au root
    if isinstance(data, dict) and "customCoordinates" in data and isinstance(data["customCoordinates"], list):
        map_name = data.get("name", "")
        for i, c in enumerate(data["customCoordinates"]):
            latlng = coords_from_geoguessr_coord(c)
            if not latlng:
                # parfois lat/lng inversés
                latlng = coords_from_feature({"geometry": {"type": "Point", "coordinates": [c.get("lng"), c.get("lat")]}})
            if not latlng:
                # skip si on ne trouve pas de coordonnées valides
                continue
            lat, lng = latlng
            props = c.get("extra", {}) if isinstance(c.get("extra"), dict) else c.get("properties", {})
            flat = flatten_properties({**c, **(props or {})})
            name = c.get("name") or f"Point {i+1}"
            description = safe_get(c, "description") or ""
            rows.append({
                "Name": name,
                "Description": description,
                "Latitude": str(lat),
                "Longitude": str(lng),
                "heading": flat.get("heading", ""),
                "pitch": flat.get("pitch", ""),
                "zoom": flat.get("zoom", ""),
                "panoId": flat.get("panoId", ""),
                "tags": flat.get("tags", ""),
                "panoDate": flat.get("panoDate", "")
            })

    # Cas 2: GeoJSON FeatureCollection
    elif isinstance(data, dict) and data.get("type", "").lower() == "featurecollection" and isinstance(data.get("features"), list):
        for i, feat in enumerate(data["features"]):
            props = feat.get("properties") or {}
            latlng = coords_from_feature(feat)
            # fallback : certaines données mettent lat/lng dans properties directement
            if latlng is None:
                lat_prop = safe_get(props, "lat") or safe_get(props, "latitude")
                lng_prop = safe_get(props, "lng") or safe_get(props, "lon") or safe_get(props, "longitude")
                if lat_prop is not None and lng_prop is not None:
                    try:
                        latlng = (float(lat_prop), float(lng_prop))
                    except Exception:
                        latlng = None
            if latlng is None:
                # skip si aucune coordonnée trouvée
                continue
            lat, lng = latlng
            flat = flatten_properties(props)
            name = props.get("name") or props.get("title") or f"Feature {i+1}"
            # description : concatène quelques propriétés non listées si utiles
            description = props.get("description") or props.get("desc") or ""
            rows.append({
                "Name": name,
                "Description": description,
                "Latitude": str(lat),
                "Longitude": str(lng),
                "heading": flat.get("heading", ""),
                "pitch": flat.get("pitch", ""),
                "zoom": flat.get("zoom", ""),
                "panoId": flat.get("panoId", ""),
                "tags": flat.get("tags", ""),
                "panoDate": flat.get("panoDate", "")
            })

    else:
        # Tentative générique : chercher récursivement une liste d'objets contenant lat/lng
        def find_latlngs(obj):
            if isinstance(obj, dict):
                if "lat" in obj and "lng" in obj:
                    yield obj
                for v in obj.values():
                    yield from find_latlngs(v)
            elif isinstance(obj, list):
                for item in obj:
                    yield from find_latlngs(item)
        found = list(find_latlngs(data))
        if found:
            for i, c in enumerate(found):
                latlng = coords_from_geoguessr_coord(c)
                if not latlng:
                    continue
                lat, lng = latlng
                flat = flatten_properties(c.get("extra", {}) if isinstance(c.get("extra"), dict) else c)
                name = c.get("name") or f"Point {i+1}"
                description = c.get("description") or ""
                rows.append({
                    "Name": name,
                    "Description": description,
                    "Latitude": str(lat),
                    "Longitude": str(lng),
                    "heading": flat.get("heading", ""),
                    "pitch": flat.get("pitch", ""),
                    "zoom": flat.get("zoom", ""),
                    "panoId": flat.get("panoId", ""),
                    "tags": flat.get("tags", ""),
                    "panoDate": flat.get("panoDate", "")
                })

    # Écriture CSV
    if not rows:
        print("Aucun point trouvé dans le fichier d'entrée. Vérifie le format.")
        return

    with open(output_path, "w", encoding="utf-8", newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"Conversion terminée : {len(rows)} points écrits dans '{output_path}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert GeoJSON / GeoGuessr JSON -> CSV pour Google My Maps")
    parser.add_argument("input", help="Fichier GeoJSON ou JSON d'entrée")
    parser.add_argument("output", nargs='?', default="output_mymaps.csv", help="Fichier CSV de sortie (par défaut output_mymaps.csv)")
    args = parser.parse_args()
    convert(args.input, args.output)