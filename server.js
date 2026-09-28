// Illegal Pool matchmaking + relay server.
// The game's physics is deterministic and the host's browser is authoritative, so this server only
// pairs players and forwards their messages. It never simulates the table.
//
// Client -> server:  hello{name}  queue  unqueue  create{pub,name}  join{code}  list  unlist  leave  m{d}
// Server -> client:  welcome{online}  queued  created{code}  paired{role,code,opp}  rooms{list}  stats{online,waiting}
//                    m{d}  peer-left  err{e}
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ALLOWED = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean); // empty = allow all
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const MAX_MSG_PER_SEC = 40;          // aim streaming is ~12/s, leave headroom
const ROOM_IDLE_MS = 30 * 60 * 1000; // an unjoined room expires after 30 min

const clients = new Set();
const rooms = new Map();   // code -> {code, host, guest, pub, name, created}
let queue = [];            // sockets waiting for a random opponent
const listeners = new Set(); // sockets watching the public lobby

const send = (ws, o) => { if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (e) {} } };
const clean = s => String(s || '').replace(/[^\w .\-]/g, '').slice(0, 16) || 'Player';
function newCode() { let c; do { c = Array.from({ length: 6 }, () => CODE_CHARS[Math.random() * CODE_CHARS.length | 0]).join(''); } while (rooms.has(c)); return c; }
const publicRooms = () => [...rooms.values()].filter(r => r.pub && !r.guest).map(r => ({ code: r.code, name: r.name, age: Math.round((Date.now() - r.created) / 1000) })).slice(0, 50);
const stats = () => ({ online: clients.size, waiting: queue.length, open: publicRooms().length });
function pushLobby() { const msg = { t: 'rooms', list: publicRooms(), ...stats() }; for (const ws of listeners) send(ws, msg); }

function pair(room) {
  const h = room.host, g = room.guest;
  h.room = g.room = room.code;
  send(h, { t: 'paired', role: 'host', code: room.code, opp: g.name });
  send(g, { t: 'paired', role: 'guest', code: room.code, opp: h.name });
  listeners.delete(h); listeners.delete(g);
  pushLobby();
}
function leaveRoom(ws, notify = true) {
  const code = ws.room; if (!code) return; ws.room = null;
  const r = rooms.get(code); if (!r) return;
  const other = r.host === ws ? r.guest : r.host;
  rooms.delete(code);
  if (other) { other.room = null; if (notify) send(other, { t: 'peer-left' }); }
  pushLobby();
}
function unqueue(ws) { queue = queue.filter(q => q !== ws); }

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.url === '/health') { res.end('ok'); return; }
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify({ name: 'illegal-pool-server', ...stats() }));
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

wss.on('connection', (ws, req) => {
  const origin = req.headers.origin || '';
  if (ALLOWED.length && !ALLOWED.some(a => origin === a || origin.endsWith(a))) { ws.close(1008, 'origin'); return; }
  ws.alive = true; ws.name = 'Player'; ws.room = null; ws.win = { t: Date.now(), n: 0 };
  clients.add(ws);
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    const now = Date.now();
    if (now - ws.win.t > 1000) { ws.win = { t: now, n: 0 }; }
    if (++ws.win.n > MAX_MSG_PER_SEC) return; // drop floods
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    switch (m.t) {
      case 'hello': ws.name = clean(m.name); send(ws, { t: 'welcome', ...stats() }); break;
      case 'queue': {
        if (ws.room) leaveRoom(ws);
        unqueue(ws);
        queue = queue.filter(q => q.readyState === 1);
        const other = queue.shift();
        if (other) { const code = newCode(); const r = { code, host: other, guest: ws, pub: false, name: other.name, created: now }; rooms.set(code, r); pair(r); }
        else { queue.push(ws); send(ws, { t: 'queued', ...stats() }); }
        break;
      }
      case 'unqueue': unqueue(ws); break;
      case 'create': {
        if (ws.room) leaveRoom(ws); unqueue(ws);
        const code = newCode(); rooms.set(code, { code, host: ws, guest: null, pub: !!m.pub, name: ws.name, created: now });
        ws.room = code; send(ws, { t: 'created', code, pub: !!m.pub }); pushLobby();
        break;
      }
      case 'join': {
        const code = String(m.code || '').toUpperCase().trim(); const r = rooms.get(code);
        if (!r) { send(ws, { t: 'err', e: 'nope' }); break; }
        if (r.host === ws) break;
        if (r.guest) { send(ws, { t: 'err', e: 'full' }); break; }
        if (ws.room) leaveRoom(ws); unqueue(ws);
        r.guest = ws; pair(r);
        break;
      }
      case 'list': listeners.add(ws); send(ws, { t: 'rooms', list: publicRooms(), ...stats() }); break;
      case 'unlist': listeners.delete(ws); break;
      case 'leave': unqueue(ws); leaveRoom(ws); break;
      case 'm': {
        const r = ws.room && rooms.get(ws.room); if (!r) break;
        const other = r.host === ws ? r.guest : r.host; if (other) send(other, { t: 'm', d: m.d });
        break;
      }
    }
  });
  ws.on('close', () => { clients.delete(ws); listeners.delete(ws); unqueue(ws); leaveRoom(ws); });
});

// heartbeat, room expiry and a stats tick for lobby watchers
setInterval(() => {
  for (const ws of clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; try { ws.ping(); } catch (e) {} }
  const now = Date.now();
  for (const r of rooms.values()) if (!r.guest && now - r.created > ROOM_IDLE_MS) { rooms.delete(r.code); if (r.host) r.host.room = null; }
  const s = { t: 'stats', ...stats() }; for (const ws of listeners) send(ws, s); for (const ws of queue) send(ws, s);
}, 10000);

server.listen(PORT, () => console.log('Illegal Pool server on :' + PORT));
