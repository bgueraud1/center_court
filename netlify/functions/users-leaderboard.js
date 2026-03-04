// netlify/functions/users-leaderboard.js
// GET-only: returns aggregated leaderboard data (week / alltime) for either "league" or "global".
// Query params:
//   user_id (optional)  -> used to determine the user's league/league_id if scope=league
//   scope=league|global (default: league)
//   time=week|all (default: week)
// NOTE: uses PostgREST endpoints (/rest/v1/users and /rest/v1/scores) with service role key.

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
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: CORS_HEADERS, body: "" };
  }
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return jsonResponse(500, { error: "Server misconfigured", detail: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" });
  }

  const qs = event.queryStringParameters || {};
  const user_id = qs.user_id || null;
  const scope = (qs.scope || 'league').toLowerCase(); // league or global
  const time = (qs.time || 'week').toLowerCase(); // week or all

  // compute week monday (UTC)
  function mondayOfTodayISO(){
    const dt = new Date();
    const day = dt.getUTCDay();
    const diff = (day + 6) % 7;
    dt.setUTCDate(dt.getUTCDate() - diff);
    return dt.toISOString().slice(0,10);
  }
  const weekMonday = mondayOfTodayISO();

  try {
    // 1) If scope=league and user_id provided -> fetch that user row to get league & league_id
    let targetLeague = null;
    let targetLeagueId = null;
    if(scope === 'league' && user_id){
      const qUser = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
      qUser.searchParams.set('select','id,pseudo,league,league_id,tour,country');
      qUser.searchParams.set('id', `eq.${encodeURIComponent(user_id)}`);
      qUser.searchParams.set('limit','1');

      const ru = await fetch(qUser.toString(), {
        method: 'GET',
        headers: {
          'apikey': SUPABASE_KEY,
          'Authorization': `Bearer ${SUPABASE_KEY}`,
          'Accept': 'application/json'
        }
      });

      if (ru.ok) {
        const arr = await ru.json();
        if(Array.isArray(arr) && arr.length>0){
          targetLeague = arr[0].league || null;
          targetLeagueId = (arr[0].league_id !== undefined && arr[0].league_id !== null) ? String(arr[0].league_id) : null;
        }
      }
    }

    // 2) fetch users: either those in league (if determined) or all users (global)
    let users = [];
    if(scope === 'league' && targetLeague){
      const qu = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
      qu.searchParams.set('select','id,pseudo,league,league_id,tour,country');
      qu.searchParams.set('league', `eq.${encodeURIComponent(targetLeague)}`);
      if(targetLeagueId !== null) qu.searchParams.set('league_id', `eq.${encodeURIComponent(targetLeagueId)}`);
      qu.searchParams.set('limit','2000');
      const rUsers = await fetch(qu.toString(), {
        method: 'GET',
        headers: {
          'apikey': SUPABASE_KEY,
          'Authorization': `Bearer ${SUPABASE_KEY}`,
          'Accept': 'application/json'
        }
      });
      if(!rUsers.ok){
        // fallback: fetch all users
        console.warn('users-by-league failed, fallback to all users', rUsers.status);
        const qa = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
        qa.searchParams.set('select','id,pseudo,league,league_id,tour,country');
        qa.searchParams.set('limit','5000');
        const rr = await fetch(qa.toString(), { method: 'GET', headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }});
        users = rr.ok ? await rr.json() : [];
      } else {
        users = await rUsers.json();
      }
    } else {
      // global: fetch all users
      const qa = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/users`);
      qa.searchParams.set('select','id,pseudo,league,league_id,tour,country');
      qa.searchParams.set('limit','5000');
      const rr = await fetch(qa.toString(), { method: 'GET', headers: { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }});
      users = rr.ok ? await rr.json() : [];
    }

    // 3) fetch scores: either for those users (by user_id in (...)) and filtered by created_day if time=week, else all
    let scores = [];
    if(Array.isArray(users) && users.length > 0){
      const ids = users.map(u => u.id).filter(Boolean);
      // chunk if necessary (PostgREST in list)
      const chunkSize = 700;
      for(let i=0;i<ids.length;i+=chunkSize){
        const chunk = ids.slice(i, i+chunkSize);
        const qS = new URL(`${SUPABASE_URL.replace(/\/$/,'')}/rest/v1/scores`);
        qS.searchParams.set('select','id,user_id,pseudo,game_id,points,created_day,mode');
        qS.searchParams.set('user_id', `in.(${encodeURIComponent(chunk.join(','))})`);
        if(time === 'week') qS.searchParams.set('created_day', `eq.${encodeURIComponent(weekMonday)}`);
        qS.searchParams.set('limit','20000');
        const rs = await fetch(qS.toString(), { method:'GET', headers: { 'apikey': SUPABASE_KEY, 'Authorization':`Bearer ${SUPABASE_KEY}`, 'Accept':'application/json' }});
        if(rs.ok){
          const arr = await rs.json();
          scores = scores.concat(arr || []);
        } else {
          // keep going, but log
          console.warn('scores fetch chunk failed', rs.status);
        }
      }
    } else {
      // no users found: optionally fetch recent scores (but return empty set)
      scores = [];
    }

    // 4) aggregate client-side (server aggregated)
    const map = new Map();
    function ensureKey(k, fallbackPseudo=''){
      if(!k) return null;
      if(!map.has(k)){
        map.set(k, { id:null, user_id:null, pseudo:fallbackPseudo||'', league:'', league_id:null, tour:'', country:'', totals:0, by_game:{guess_player:0,guess_player_h2h:0,gill_the_grid:0} });
      }
      return map.get(k);
    }

    // init from users
    for(const u of users){
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

    for(const s of scores){
      const k = s.user_id ? 'id:'+String(s.user_id) : (s.pseudo ? 'pseudo:'+String(s.pseudo).toLowerCase() : null);
      if(!k) continue;
      const ent = ensureKey(k, s.pseudo || '');
      ent.id = ent.id || (s.user_id || null);
      ent.user_id = ent.user_id || (s.user_id || null);
      ent.pseudo = ent.pseudo || (s.pseudo || '');
      const pts = Number(s.points || 0);
      ent.totals += pts;
      const gid = (s.game_id || '').toLowerCase();
      if(gid === 'guess_player') ent.by_game.guess_player += pts;
      else if(gid === 'guess_player_h2h') ent.by_game.guess_player_h2h += pts;
      else if(gid === 'gill_the_grid' || gid === 'fill_grid') ent.by_game.gill_the_grid += pts;
    }

    const usersArr = Array.from(map.values()).map(u => ({
      id: u.id,
      user_id: u.user_id,
      pseudo: u.pseudo,
      league: u.league,
      league_id: u.league_id,
      tour: u.tour,
      country: u.country,
      totals: { week: u.totals, all: u.totals },
      by_game: {
        guess_player: { week: u.by_game.guess_player, alltime: u.by_game.guess_player },
        guess_player_h2h: { week: u.by_game.guess_player_h2h, alltime: u.by_game.guess_player_h2h },
        gill_the_grid: { week: u.by_game.gill_the_grid, alltime: u.by_game.gill_the_grid }
      }
    }));

    return jsonResponse(200, { ok:true, week: weekMonday, users: usersArr, meta: { scope, time } });
  } catch (err) {
    console.error('users-leaderboard error', String(err));
    return jsonResponse(500, { error: 'Server error', detail: String(err) });
  }
};