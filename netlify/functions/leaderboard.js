const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

exports.handler = async function (event) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, body: "Missing Supabase env variables" };
  }

  const fetchImpl = global.fetch || (await import("node-fetch")).default;

  try {
    const { date, game_id, limit = 50 } =
      event.queryStringParameters || {};

    const targetDate =
      date || new Date().toISOString().slice(0, 10);

    const isoStart = `${targetDate}T00:00:00`;

    let url =
      `${SUPABASE_URL}/rest/v1/scores?` +
      `select=pseudo,points&` +
      `created_at=gte.${encodeURIComponent(isoStart)}`;

    if (game_id) {
      url += `&game_id=eq.${encodeURIComponent(game_id)}`;
    }

    const r = await fetchImpl(url, {
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
      },
    });

    const rows = await r.json();

    // agrégation
    const totals = {};

    rows.forEach((r) => {
      if (!totals[r.pseudo]) {
        totals[r.pseudo] = 0;
      }
      totals[r.pseudo] += Number(r.points);
    });

    const leaderboard = Object.entries(totals)
      .map(([pseudo, total]) => ({ pseudo, total }))
      .sort((a, b) => b.total - a.total)
      .slice(0, Number(limit));

    return {
      statusCode: 200,
      body: JSON.stringify({
        ok: true,
        date: targetDate,
        game_id: game_id || "all",
        leaderboard,
      }),
    };
  } catch (err) {
    console.error(err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Server error" }),
    };
  }
};
