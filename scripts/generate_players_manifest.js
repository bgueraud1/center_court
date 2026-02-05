// scripts/generate_players_manifest.js
// Lists all Cloudinary resources under prefix 'players' and matches them to players index.
// Usage (PowerShell):
//   $env:CLOUDINARY_CLOUD_NAME="cloud"; $env:CLOUDINARY_API_KEY="key"; $env:CLOUDINARY_API_SECRET="secret"; node scripts/generate_players_manifest.js

const fs = require('fs');
const path = require('path');
const cloudinary = require('cloudinary').v2;

const INDEX_JSON = path.join(__dirname, '..', 'docs', 'index', 'players_wta_index.json'); // adapte si besoin
const OUT_JSON = path.join(__dirname, '..', 'docs', 'index', 'players_images_manifest.json');

if (!process.env.CLOUDINARY_CLOUD_NAME || !process.env.CLOUDINARY_API_KEY || !process.env.CLOUDINARY_API_SECRET) {
  console.error('Missing Cloudinary credentials in env. Set CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET.');
  process.exit(2);
}

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true
});

async function listAllPlayersResources(prefix='players') {
  let next_cursor;
  const resources = [];
  while (true) {
    const opts = { type: 'upload', prefix, max_results: 500 };
    if (next_cursor) opts.next_cursor = next_cursor;
    const res = await cloudinary.api.resources(opts).catch(err => { throw err; });
    (res.resources || []).forEach(r => resources.push(r));
    next_cursor = res.next_cursor;
    if (!next_cursor) break;
  }
  return resources;
}

function slugify(s) {
  if (!s) return '';
  return String(s).trim().toLowerCase().replace(/[^\w\s-]/g,'').replace(/[\s_]+/g,'-').replace(/^-+|-+$/g,'');
}

(async () => {
  try {
    if (!fs.existsSync(INDEX_JSON)) {
      console.error('Index file not found:', INDEX_JSON);
      process.exit(1);
    }
    const idx = JSON.parse(fs.readFileSync(INDEX_JSON, 'utf8'));
    const players = (idx.players || []).map(p => ({
      player_id: String(p.player_id || '').trim(),
      slug: slugify(p.slug || p.name || ''),
      name: p.name || ''
    }));

    console.log('Listing Cloudinary resources with prefix "players" (this may take a few seconds)...');
    const resources = await listAllPlayersResources('players');
    console.log('Cloudinary resources found:', resources.length);

    // build map public_id_lower -> resource info (url + public_id)
    const rcmap = {};
    resources.forEach(r => {
      const pid = String(r.public_id || '').toLowerCase();
      // create delivery url with safe transform
      const url = cloudinary.url(r.public_id, { secure: true, transformation: [{ width: 800, height: 800, crop: 'fill', gravity: 'auto' }, { fetch_format: 'auto', quality: 'auto' }] });
      rcmap[pid] = { public_id: r.public_id, url, version: r.version || null };
      // also store without prefix maybe
      if (pid.startsWith('players/')) {
        const short = pid.replace(/^players\//,'');
        if (!rcmap[short]) rcmap[short] = { public_id: r.public_id, url, version: r.version || null };
      }
    });

    const out = [];
    players.forEach(p => {
      const pid = p.player_id;
      const slug = p.slug;
      const candidates = [];
      if (pid && slug) candidates.push(`players/${pid}-${slug}`);
      if (slug) candidates.push(`players/${slug}`);
      if (pid) candidates.push(`players/${pid}`);
      // also short forms
      if (slug) candidates.push(slug);
      for (const c of candidates) {
        const cl = String(c).toLowerCase();
        if (rcmap[cl]) {
          out.push(Object.assign({ player_id: pid, slug: slug, name: p.name }, rcmap[cl]));
          break;
        }
      }
    });

    // write manifest array of objects: { player_id, slug, public_id, url }
    fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
    fs.writeFileSync(OUT_JSON, JSON.stringify(out, null, 2), 'utf8');
    console.log('Wrote', OUT_JSON, 'entries:', out.length);
  } catch (err) {
    console.error('ERROR', err.message || err);
    process.exit(1);
  }
})();
