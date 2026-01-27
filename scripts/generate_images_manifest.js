// Node script: scripts/generate_images_manifest.js
// npm deps: cloudinary, csv-parse (sync)
const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const cloudinary = require('cloudinary').v2;

const IMAGES_CSV = path.join(__dirname, '..', 'docs','games', 'blur_game', 'images.csv');
const OUT_JSON = path.join(__dirname, '..', 'docs','games', 'blur_game', 'images_manifest.json');

if (!process.env.CLOUDINARY_CLOUD_NAME || !process.env.CLOUDINARY_API_KEY || !process.env.CLOUDINARY_API_SECRET) {
  console.error('Missing Cloudinary credentials in env. Set CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET.');
  process.exit(2);
}

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

function normalizeTags(s) {
  if (!s) return [];
  // CSV has a quoted tags column with comma-separated tags; split by comma then trim.
  return s.split(',').map(x => x.trim()).filter(Boolean);
}

(async function main(){
  const csvRaw = fs.readFileSync(IMAGES_CSV, 'utf8');
  const rows = parse(csvRaw, { columns: true, skip_empty_lines: true });
  const out = [];

  for (const r of rows) {
    const id = (r.id || '').trim();
    const tags = normalizeTags(r.tags || r.tag || '');
    if (!id) continue;

    try {
      // Check resource existence (this will throw if not found)
      const res = await cloudinary.api.resource(id, { colors: false }).catch(err => null);
      if (!res) {
        console.warn(`Not found in Cloudinary: ${id} — skipping`);
        continue;
      }
      // Build a delivery URL with safe transformations you want (600x600 crop fill example)
      const url = cloudinary.url(id, {
        secure: true,
        // use a sensible transform to keep client bandwidth low and predictable
        transformation: [
          { width: 800, height: 800, crop: 'fill', gravity: 'auto' }, // big for canvas scaling
          { fetch_format: 'auto', quality: 'auto' }
        ]
      });
      out.push({ id, url, tags });
    } catch (err) {
      console.warn(`Error checking ${id}:`, err.message || err);
      // continue; don't block the manifest generation for one error
    }
  }

  fs.writeFileSync(OUT_JSON, JSON.stringify(out, null, 2), 'utf8');
  console.log(`Wrote ${OUT_JSON} with ${out.length} images.`);
})();
