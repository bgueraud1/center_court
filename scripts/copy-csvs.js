const fs = require('fs');
const path = require('path');
const files = ['player_data_wta.csv','player_data_atp.csv'];
const destDir = path.join(__dirname, '..', 'public'); // -> ajuster si publish différent
files.forEach(f => {
  const src = path.join(__dirname, '..', f);
  const dst = path.join(destDir, f);
  try { fs.copyFileSync(src, dst); console.log('copied', f); } catch(e){ console.warn('copy failed', f, e); }
});
