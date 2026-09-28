# Illegal Pool matchmaking server

Pairs players (random queue, private/public rooms, lobby list) and relays their messages.
The game itself stays in the browser; this server never runs physics.

## Deploy to Railway
1. Put this folder in its own GitHub repo (or use `railway up` from the Railway CLI inside this folder).
2. Railway → New Project → Deploy from GitHub repo → pick it. Railway detects Node and runs `npm start`.
3. Settings → Networking → Generate Domain. You get something like `illegal-pool-server-production.up.railway.app`.
4. Open `https://<that-domain>/health` — it should say `ok`.
5. In the game's `index.html` set `const MATCH_SERVER='wss://<that-domain>';` and redeploy the game.

Optional env var: `ALLOWED_ORIGINS=illegal-pool.vercel.app,crazygames.com` to only accept those sites.
