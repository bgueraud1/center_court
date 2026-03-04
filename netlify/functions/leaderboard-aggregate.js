// netlify/functions/leaderboard-aggregate.js
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Access-Control-Allow-Methods": "GET, OPTIONS"
};

function jsonResponse(status, body) {
  return {
    statusCode: status,
    headers: Object.assign({ "Content-Type": "application/json" }, CORS_HEADERS),
    body: JSON.stringify(body)
  };
}

module.exports.handler = async function(event) {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  if (event.httpMethod !== "GET") return jsonResponse(405, { error: "Method Not Allowed" });
  if (!SUPABASE_URL || !SUPABASE_KEY) return jsonResponse(500, { error: "Server misconfigured", detail: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" });

  const qs = event.queryStringParameters || {};
  const scope = (qs.scope || 'league').toLowerCase(); // league | global
  const time = (qs.time || 'week').toLowerCase();     // week | all
  const userIdParam = qs.user_id || null;
  const pseudoParam = qs.pseudo || null;

  // monday and sunday in UTC (ISO yyyy-mm-dd)
  function weekBoundsIsoUTC(){
    const dt = new Date();
    const day = dt.getUTCDay(); // 0..6 Sun..Sat
    const diff = (day + 6) % 7;
    dt.setUTCDate(dt.getUTCDate() - diff); // now Monday UTC
    const monday = new Date(Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate()));
    const sunday = new Date(monday);
    sunday.setUTCDate(monday.getUTCDate() + 6);
    function iso(d){ return d.toISOString().slice(0,10); }
    return { monday: iso(monday), sunday: iso(sunday) };
  }
  const { monday: weekMonday, sunday: weekSunday } = weekBoundsIsoUTC();

  try {
    // 1) if scope=league and we have user_id or pseudo -> fetch that user's league & league_id
    let targetLeague = null;
    let targetLeagueId = null;
    if (scope === 'league' && (userIdParam || pseudoParam)) {
      const qUser = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
      qUser.searchParams.set('select', 'id,pseudo,league,league_id');
      if(userIdParam) qUser.searchParams.set('id', `eq.${encodeURIComponent(userIdParam)}`);
      else if(pseudoParam) qUser.searchParams.set('pseudo', `eq.${encodeURIComponent(pseudoParam)}`);
      qUser.searchParams.set('limit', '1');

      const ru = await fetch(qUser.toString(), {
        method: 'GET',
        headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }
      });
      if (ru.ok) {
        const arr = await ru.json();
        if (Array.isArray(arr) && arr.length > 0) {
          targetLeague = arr[0].league || null;
          targetLeagueId = (arr[0].league_id !== undefined && arr[0].league_id !== null) ? String(arr[0].league_id) : null;
        }
      } else {
        console.warn('user lookup for league failed', ru.status);
      }
    }

    // 2) fetch users: either all users (global) or users in the target league (+ league_id)
    let users = [];
    if (scope === 'league' && targetLeague) {
      const q = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
      q.searchParams.set('select','id,pseudo,league,league_id,tour,country');
      q.searchParams.set('league', `eq.${encodeURIComponent(targetLeague)}`);
      if (targetLeagueId !== null) q.searchParams.set('league_id', `eq.${encodeURIComponent(targetLeagueId)}`);
      q.searchParams.set('limit','5000');
      const r = await fetch(q.toString(), { method:'GET', headers:{ 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }});
      if (r.ok) users = await r.json();
      else {
        console.warn('users-by-league fetch failed', r.status);
        const qa = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
        qa.searchParams.set('select','id,pseudo,league,league_id,tour,country');
        qa.searchParams.set('limit','5000');
        const ra = await fetch(qa.toString(), { method:'GET', headers:{ 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }});
        users = ra.ok ? await ra.json() : [];
      }
    } else {
      const q = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
      q.searchParams.set('select','id,pseudo,league,league_id,tour,country');
      q.searchParams.set('limit','5000');
      const r = await fetch(q.toString(), { method:'GET', headers:{ 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }});
      users = r.ok ? await r.json() : [];
    }

    // 3) build ids list and fetch scores for these users (chunked)
    const ids = (Array.isArray(users) ? users.map(u => u.id).filter(Boolean) : []);
    let scores = [];
    if (ids.length > 0) {
      const chunkSize = 700;
      for (let i=0;i<ids.length;i+=chunkSize){
        const chunk = ids.slice(i, i+chunkSize);
        const qS = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/scores`);
        qS.searchParams.set('select','id,user_id,pseudo,game_id,points,created_day,mode');
        qS.searchParams.set('user_id', `in.(${chunk.join(',')})`);
        if (time === 'week') {
          // use range monday..sunday inclusive
          qS.searchParams.set('created_day', `gte.${encodeURIComponent(weekMonday)}`);
          qS.searchParams.append('created_day', `lte.${encodeURIComponent(weekSunday)}`);
        }
        qS.searchParams.set('limit', '20000');
        const rs = await fetch(qS.toString(), { method:'GET', headers:{ 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }});
        if (rs.ok) {
          const arr = await rs.json();
          scores = scores.concat(arr || []);
        } else {
          console.warn('scores fetch chunk failed', rs.status);
        }
      }
    } else {
      scores = [];
    }

    // 4) aggregate server-side
    const map = new Map();
    function ensureKey(k, fallbackPseudo=''){
      if (!k) return null;
      if (!map.has(k)) {
        map.set(k, { id:null, user_id:null, pseudo:fallbackPseudo||'', league:'', league_id:null, tour:'', country:'', totals_week:0, totals_all:0, by_game:{ guess_player_week:0, guess_player_all:0, guess_player_h2h_week:0, guess_player_h2h_all:0, gill_the_grid_week:0, gill_the_grid_all:0 } });
      }
      return map.get(k);
    }

    for (const u of users){
      const k = u.id ? 'id:'+String(u.id) : 'pseudo:'+String((u.pseudo||'').toLowerCase());
      const ent = ensureKey(k, u.pseudo||'');
      ent.id = u.id || null;
      ent.user_id = u.id || null;
      ent.pseudo = u.pseudo || '';
      ent.league = u.league || '';
      ent.league_id = (u.league_id !== undefined && u.league_id !== null) ? String(u.league_id) : null;
      ent.tour = (u.tour||'').toUpperCase();
      ent.country = u.country || '';
    }

    for (const s of scores){
      const k = s.user_id ? 'id:'+String(s.user_id) : (s.pseudo ? 'pseudo:'+String(s.pseudo).toLowerCase() : null);
      if (!k) continue;
      const ent = ensureKey(k, s.pseudo || '');
      ent.id = ent.id || (s.user_id || null);
      ent.user_id = ent.user_id || (s.user_id || null);
      ent.pseudo = ent.pseudo || (s.pseudo || '');
      const pts = Number(s.points || 0);
      if (time === 'week') {
        ent.totals_week += pts;
        ent.totals_all += pts;
      } else {
        ent.totals_all += pts;
      }
      const gid = (s.game_id || '').toLowerCase();
      if (gid === 'guess_player') {
        if (time === 'week') { ent.by_game.guess_player_week += pts; ent.by_game.guess_player_all += pts; }
        else { ent.by_game.guess_player_all += pts; }
      } else if (gid === 'guess_player_h2h') {
        if (time === 'week') { ent.by_game.guess_player_h2h_week += pts; ent.by_game.guess_player_h2h_all += pts; }
        else { ent.by_game.guess_player_h2h_all += pts; }
      } else if (gid === 'gill_the_grid' || gid === 'fill_grid') {
        if (time === 'week') { ent.by_game.gill_the_grid_week += pts; ent.by_game.gill_the_grid_all += pts; }
        else { ent.by_game.gill_the_grid_all += pts; }
      }
    }

    const usersArr = Array.from(map.values()).map(u => ({
      id: u.id,
      user_id: u.user_id,
      pseudo: u.pseudo,
      league: u.league,
      league_id: u.league_id,
      tour: u.tour,
      country: u.country,
      scores: { week: Number(u.totals_week || 0), alltime: Number(u.totals_all || 0) },
      by_game: {
        guess_player: { week: Number(u.by_game.guess_player_week || 0), alltime: Number(u.by_game.guess_player_all || 0) },
        guess_player_h2h: { week: Number(u.by_game.guess_player_h2h_week || 0), alltime: Number(u.by_game.guess_player_h2h_all || 0) },
        gill_the_grid: { week: Number(u.by_game.gill_the_grid_week || 0), alltime: Number(u.by_game.gill_the_grid_all || 0) }
      }
    }));

    return jsonResponse(200, { ok:true, week: weekMonday, users: usersArr, meta: { scope, time, requested_user_id: userIdParam, requested_pseudo: pseudoParam } });
  } catch (err) {
    console.error('leaderboard-aggregate error', String(err));
    return jsonResponse(500, { error: 'Server error', detail: String(err) });
  }
};