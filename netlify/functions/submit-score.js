const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

exports.handler = async function (event) {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, body: "Missing Supabase env variables" };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }

  const fetchImpl = global.fetch || (await import("node-fetch")).default;

  try {
    const body = JSON.parse(event.body || "{}");
    const { game_id, points, pseudo, password_hash, anon_id, meta } = body;

    if (!game_id || typeof points === "undefined") {
      return { statusCode: 400, body: "Missing game_id or points" };
    }

    let user_id = null;

    // ===============================
    // 1️⃣ Gestion utilisateur enregistré
    // ===============================
    if (pseudo && password_hash) {
      const findUserUrl =
        `${SUPABASE_URL}/rest/v1/users?` +
        `select=id,password_hash&pseudo=eq.${encodeURIComponent(pseudo)}&limit=1`;

      const rFind = await fetchImpl(findUserUrl, {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
        },
      });

      const users = await rFind.json();

      if (users.length > 0) {
        // utilisateur existe → vérifier mot de passe
        if (users[0].password_hash !== password_hash) {
          return {
            statusCode: 403,
            body: JSON.stringify({ error: "Invalid password" }),
          };
        }
        user_id = users[0].id;
      } else {
        // créer utilisateur
        const createUserUrl = `${SUPABASE_URL}/rest/v1/users`;

        const rCreate = await fetchImpl(createUserUrl, {
          method: "POST",
          headers: {
            apikey: SUPABASE_KEY,
            Authorization: `Bearer ${SUPABASE_KEY}`,
            "Content-Type": "application/json",
            Prefer: "return=representation",
          },
          body: JSON.stringify([
            { pseudo, password_hash },
          ]),
        });

        const created = await rCreate.json();
        user_id = created[0].id;
      }
    }

    // ===============================
    // 2️⃣ Vérification doublon du jour
    // ===============================
    const today = new Date().toISOString().slice(0, 10);
    const checkUrl =
      `${SUPABASE_URL}/rest/v1/scores?` +
      `select=id,user_id,anon_id,pseudo&` +
      `game_id=eq.${encodeURIComponent(game_id)}&` +
      `created_at=gte.${today}`;

    const rCheck = await fetchImpl(checkUrl, {
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
      },
    });

    const existing = await rCheck.json();

    const already = existing.some((r) => {
      if (user_id && r.user_id === user_id) return true;
      if (anon_id && r.anon_id === anon_id) return true;
      if (!user_id && !anon_id && pseudo && r.pseudo === pseudo) return true;
      return false;
    });

    if (already) {
      return {
        statusCode: 403,
        body: JSON.stringify({ error: "Already submitted today" }),
      };
    }

    // ===============================
    // 3️⃣ Insertion score
    // ===============================
    const insertUrl = `${SUPABASE_URL}/rest/v1/scores`;

    const record = {
      user_id,
      pseudo: pseudo || `anon_${anon_id?.slice(0, 8)}`,
      anon_id: anon_id || null,
      game_id,
      points: Number(points),
      meta: meta || null,
    };

    const rInsert = await fetchImpl(insertUrl, {
      method: "POST",
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=representation",
      },
      body: JSON.stringify([record]),
    });

    const inserted = await rInsert.json();

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, record: inserted[0] }),
    };
  } catch (err) {
    console.error(err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Server error" }),
    };
  }
};
