// Pickleholics — zero-dependency server (Node >= 22.13): node:sqlite + SSE real-time.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { DatabaseSync } = require('node:sqlite');
const PORT = process.env.PORT || 3000, PIN = process.env.ADMIN_PIN || '1234';
const db = new DatabaseSync(path.join(__dirname, 'pickleholics.db'));
db.exec(`
CREATE TABLE IF NOT EXISTS players(id INTEGER PRIMARY KEY, name TEXT NOT NULL, skill INTEGER DEFAULT 2, elo INTEGER DEFAULT 1000,
  wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, present INTEGER DEFAULT 0, qr TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS queue(id INTEGER PRIMARY KEY, player_id INTEGER UNIQUE, joined INTEGER);
CREATE TABLE IF NOT EXISTS matches(id INTEGER PRIMARY KEY, court INTEGER, t1a INT, t1b INT, t2a INT, t2b INT,
  s1 INT, s2 INT, status TEXT DEFAULT 'live', started INTEGER, ended INTEGER);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS tournaments(id INTEGER PRIMARY KEY, name TEXT, status TEXT DEFAULT 'active', round INT DEFAULT 1);
CREATE TABLE IF NOT EXISTS tmatches(id INTEGER PRIMARY KEY, tid INT, round INT, t1 TEXT, t2 TEXT, winner TEXT, s1 INT, s2 INT);
INSERT OR IGNORE INTO settings VALUES('courts','4');INSERT OR IGNORE INTO settings VALUES('auto','0');INSERT OR IGNORE INTO settings VALUES('upn','3');
CREATE TABLE IF NOT EXISTS sessions(
    id INTEGER PRIMARY KEY,
    name TEXT,
    created INTEGER
);

CREATE TABLE IF NOT EXISTS session_results(
    id INTEGER PRIMARY KEY,
    session_id INTEGER,
    player_name TEXT,
    wins INTEGER,
    losses INTEGER,
    points_diff INTEGER,
    rank INTEGER
);
CREATE TABLE IF NOT EXISTS upnext(id INTEGER PRIMARY KEY, s0 INT, s1 INT, s2 INT, s3 INT, mode TEXT DEFAULT 'auto', note TEXT, called INT DEFAULT 0);`);

for (const c of ['last_res TEXT', 'last_partner INT']) try { db.exec('ALTER TABLE players ADD COLUMN ' + c); } catch {}
try {
    db.exec(
      'ALTER TABLE players ADD COLUMN points_diff INTEGER DEFAULT 0'
    );
} catch {}
const q = (sql, ...a) => db.prepare(sql).all(...a), one = (sql, ...a) => db.prepare(sql).get(...a), run = (sql, ...a) => db.prepare(sql).run(...a);
const courts = () => +one("SELECT v FROM settings WHERE k='courts'").v;
const names = ids => ids.map(i => one('SELECT name FROM players WHERE id=?', i)?.name).join(' & ');

function state() {
  syncUp();
  const up = getUp().map(u => ({ id: u.id, slots: slots(u), mode: u.mode, note: u.note, called: u.called }));
  const players = q('SELECT * FROM players ORDER BY points_diff DESC, wins DESC');
  const P = Object.fromEntries(players.map(p => [p.id, p]));
  const queue = q('SELECT * FROM queue ORDER BY joined,id').map(r => ({ ...r, player: P[r.player_id] }));
  const matches = q("SELECT * FROM matches WHERE status='live' ORDER BY court");
  const history = q("SELECT * FROM matches WHERE status='done' ORDER BY ended DESC LIMIT 15");
  const tournaments = q('SELECT * FROM tournaments ORDER BY id DESC').map(t => ({ ...t, matches: q('SELECT * FROM tmatches WHERE tid=? ORDER BY round,id', t.id) }));
  return { players, queue, matches, history, tournaments, courts: courts(), auto: one("SELECT v FROM settings WHERE k='auto'").v === '1', upnext: up, suggestions: up.filter(u => u.slots.every(Boolean)).map(u => ({ t1: u.slots.slice(0, 2), t2: u.slots.slice(2), note: u.note || 'Manual', called: u.called })), now: Date.now() };
}
// SSE clients
const clients = new Set();
const broadcast = () => clients.forEach(c => c.write('data: update\n\n'));

// Suggestion engine: most-idle first; winners meet winners, losers meet losers; no repeat partner; random teams.
const shuffle = a => { a = [...a]; for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const P1 = id => one('SELECT 1 x FROM players WHERE id=?', id);
const liveIds = () => q("SELECT * FROM matches WHERE status='live'").flatMap(m => [m.t1a, m.t1b, m.t2a, m.t2b]);
function pickFour(list) {
  const head = list[0], same = head.last_res ? list.filter(p => p.last_res === head.last_res) : list;
  const four = same.length >= 4 ? same.slice(0, 4) : [...same, ...list.filter(p => !same.includes(p))].slice(0, 4);
  const mixed = four.some(p => p.last_res !== head.last_res);
  const [a, b, c, d] = four, pairings = [[[a, b], [c, d]], [[a, c], [b, d]], [[a, d], [b, c]]];
  const ok = pairings.filter(pr => !pr.some(([m, n]) => m.last_partner === n.id || n.last_partner === m.id));
  const [t1, t2] = shuffle(ok.length ? ok : pairings)[0];
  const note = !head.last_res ? 'New' : mixed ? 'Mixed' : head.last_res === 'W' ? 'Winners' : 'Losers';
  return { t1: shuffle(t1).map(p => p.id), t2: shuffle(t2).map(p => p.id), note };
}
function startMatch(court, ids) {
  run('INSERT INTO matches(court,t1a,t1b,t2a,t2b,started) VALUES(?,?,?,?,?,?)', court, ids[0], ids[1], ids[2], ids[3], Date.now());
  ids.forEach(p => { run('DELETE FROM queue WHERE player_id=?', p); run('UPDATE players SET present=1 WHERE id=?', p); });
}
// ---- Up Next cards: persistent, editable next matches (slots 0-1 = team 1, 2-3 = team 2) ----
const getUp = () => q('SELECT * FROM upnext ORDER BY id');
const slots = u => [u.s0, u.s1, u.s2, u.s3];
const upn = () => +one("SELECT v FROM settings WHERE k='upn'").v;
const setUpn = n => run("UPDATE settings SET v=? WHERE k='upn'", String(Math.max(0, n)));
function pool(except) { // waiting players (most idle first) not already placed in another Up Next card
  const used = new Set(getUp().filter(u => u.id !== except).flatMap(slots).filter(Boolean));
  return q('SELECT p.id,p.last_res,p.last_partner FROM queue u JOIN players p ON p.id=u.player_id ORDER BY u.joined,u.id').filter(p => !used.has(p.id));
}
function fillAuto(id) {
  const list = pool(id); if (list.length < 4) return false;
  const m = pickFour(list); run("UPDATE upnext SET s0=?,s1=?,s2=?,s3=?,mode='auto',note=?,called=0 WHERE id=?", ...m.t1, ...m.t2, m.note, id); return true;
}
function syncUp() { // keep cards valid as the queue changes, and top up to the wanted number of cards
  const inQ = new Set(q('SELECT player_id FROM queue').map(r => r.player_id));
  for (const u of getUp()) {
    const old = slots(u), s = old.map(p => inQ.has(p) ? p : null);
    if (s.some((p, i) => p !== old[i])) run('UPDATE upnext SET s0=?,s1=?,s2=?,s3=? WHERE id=?', ...s, u.id);
    if (u.mode === 'auto' && s.some(p => !p) && !fillAuto(u.id)) run('DELETE FROM upnext WHERE id=?', u.id);
  }
  for (let n = getUp().length; n < upn(); n++) { const id = run("INSERT INTO upnext(mode) VALUES('auto')").lastInsertRowid; if (!fillAuto(id)) { run('DELETE FROM upnext WHERE id=?', id); break; } }
}
function sendUp(id, court) {
  syncUp(); const ups = getUp(), u = ups.find(x => x.id === id);
  if (!u) throw 'Match not found'; 
  const ids = slots(u); if (ids.some(p => !p)) throw 'Fill all 4 spots first';
  const busy = new Set(q("SELECT court FROM matches WHERE status='live'").map(m => m.court));
  let c = +court || 0; if (!c) for (let i = 1; i <= courts(); i++) if (!busy.has(i)) { c = i; break; }
  if (!c || c > courts() || busy.has(c)) throw 'No free court';
  run('DELETE FROM upnext WHERE id=?', id); startMatch(c, ids);
}
function autoAssign() { let made = 0; for (;;) { syncUp(); const u = getUp()[0]; if (!u) break; try { sendUp(u.id); made++; } catch { break; } } return made; }
const maybeAuto = () => { if (one("SELECT v FROM settings WHERE k='auto'").v === '1') autoAssign(); };
const four = b => { const ids = (b.players || []).map(Number); if (ids.length !== 4 || new Set(ids).size !== 4 || ids.some(i => !P1(i))) throw 'Pick 4 different players'; return ids; };

const avg = ids => ids.reduce((s, i) => s + one('SELECT elo FROM players WHERE id=?', i).elo, 0) / ids.length;
function finish(id, s1, s2) {
  const m = one("SELECT * FROM matches WHERE id=? AND status='live'", id); if (!m) throw 'No such live match';
  const A = [m.t1a, m.t1b], B = [m.t2a, m.t2b], win = s1 > s2 ? A : B, lose = s1 > s2 ? B : A;
  const diffA = s1 - s2;
  const diffB = s2 - s1;
  A.forEach(p =>
      run(
        'UPDATE players SET points_diff=COALESCE(points_diff,0)+? WHERE id=?',
        diffA,
        p
      )
  );

  B.forEach(p =>
      run(
        'UPDATE players SET points_diff=COALESCE(points_diff,0)+? WHERE id=?',
        diffB,
        p
      )
  );
  const exp = 1 / (1 + 10 ** ((avg(lose) - avg(win)) / 400)), d = Math.round(32 * (1 - exp));
  win.forEach(p => run('UPDATE players SET elo=elo+?, wins=wins+1 WHERE id=?', d, p));
  lose.forEach(p => run('UPDATE players SET elo=MAX(100,elo-?), losses=losses+1 WHERE id=?', d, p));
  [[win, 'W'], [lose, 'L']].forEach(([t, r]) => { run('UPDATE players SET last_res=?,last_partner=? WHERE id=?', r, t[1], t[0]); run('UPDATE players SET last_res=?,last_partner=? WHERE id=?', r, t[0], t[1]); });
  run("UPDATE matches SET s1=?,s2=?,status='done',ended=? WHERE id=?", s1, s2, Date.now(), id);
  [...A, ...B].forEach(p => run('INSERT OR IGNORE INTO queue(player_id,joined) VALUES(?,?)', p, Date.now())); // back of the line
  maybeAuto();
}
function nextRound(tid) {
  const t = one('SELECT * FROM tournaments WHERE id=?', tid), ms = q('SELECT * FROM tmatches WHERE tid=? AND round=?', tid, t.round);
  if (!ms.length || ms.some(m => !m.winner)) return;
  const w = ms.map(m => m.winner);
  if (w.length === 1) return run("UPDATE tournaments SET status='finished' WHERE id=?", tid);
  run('UPDATE tournaments SET round=round+1 WHERE id=?', tid);
  for (let i = 0; i < w.length; i += 2) run('INSERT INTO tmatches(tid,round,t1,t2,winner) VALUES(?,?,?,?,?)', tid, t.round + 1, w[i], w[i + 1] || 'BYE', w[i + 1] ? null : w[i]);
  nextRound(tid);
}
function createTournament(name, ids) {
  const ps = ids.map(i => one('SELECT * FROM players WHERE id=?', i)).filter(Boolean).sort((a, b) => b.elo - a.elo);
  if (ps.length < 4) throw 'Need at least 4 players';
  const teams = []; while (ps.length > 1) teams.push(`${ps.shift().name} & ${ps.pop().name}`); // strongest + weakest
  const tid = run('INSERT INTO tournaments(name) VALUES(?)', name).lastInsertRowid;
  for (let i = 0; i < teams.length; i += 2) run('INSERT INTO tmatches(tid,round,t1,t2,winner) VALUES(?,?,?,?,?)', tid, 1, teams[i], teams[i + 1] || 'BYE', teams[i + 1] ? null : teams[i]);
  nextRound(tid);
}

const routes = [
  ['POST', /^\/api\/players$/, b => { if (!b.name?.trim()) throw 'Name required'; const skill = b.skill || 2;
    run('INSERT INTO players(name,skill,elo,qr) VALUES(?,?,?,?)', b.name.trim(), skill, 800 + skill * 200, 'PH-' + crypto.randomBytes(4).toString('hex')); }],
  ['POST', /^\/api\/checkin$/, b => { const p = one('SELECT * FROM players WHERE qr=?', (b.qr || '').trim()); if (!p) throw 'Unknown QR code';
    run('UPDATE players SET present=1 WHERE id=?', p.id); run('INSERT OR IGNORE INTO queue(player_id,joined) VALUES(?,?)', p.id, Date.now()); maybeAuto(); return { name: p.name }; }],
  ['POST', /^\/api\/queue$/, b => { run('UPDATE players SET present=1 WHERE id=?', b.id); run('INSERT OR IGNORE INTO queue(player_id,joined) VALUES(?,?)', b.id, Date.now()); maybeAuto(); }],

  ['POST', /^\/api\/leave$/, b => { run('UPDATE players SET present=0 WHERE id=?', b.id); run('DELETE FROM queue WHERE player_id=?', b.id); }],
  ['POST', /^\/api\/assign$/, () => ({ made: autoAssign() })],
  ['POST', /^\/api\/match\/(\d+)\/finish$/, (b, m) => { if (!Number.isInteger(b.s1) || !Number.isInteger(b.s2) || b.s1 === b.s2) throw 'Enter a valid, non-tied score'; finish(+m[1], b.s1, b.s2); }],
  ['POST', /^\/api\/tmatch\/(\d+)$/, (b, m) => { const t = one('SELECT * FROM tmatches WHERE id=?', m[1]); if (!t || t.winner) throw 'Not available';
    if (b.s1 === b.s2) throw 'No ties'; run('UPDATE tmatches SET s1=?,s2=?,winner=? WHERE id=?', b.s1, b.s2, b.s1 > b.s2 ? t.t1 : t.t2, t.id); nextRound(t.tid); }],
  ['POST', /^\/api\/assign\/manual$/, b => { const ids = four(b), c = +b.court;
    if (!(c >= 1 && c <= courts())) throw 'Pick a court';
    if (one("SELECT 1 x FROM matches WHERE status='live' AND court=?", c)) throw 'Court is occupied';
    if (liveIds().some(i => ids.includes(i))) throw 'A selected player is already on a court';
    startMatch(c, ids); }],
  ['POST', /^\/api\/match\/(\d+)\/edit$/, (b, m) => { const old = one("SELECT * FROM matches WHERE id=? AND status='live'", m[1]); if (!old) throw 'No such live match';
    const ids = four(b), before = [old.t1a, old.t1b, old.t2a, old.t2b];
    if (liveIds().filter(i => !before.includes(i)).some(i => ids.includes(i))) throw 'A selected player is on another court';
    const front = (one('SELECT MIN(joined) j FROM queue').j ?? Date.now()) - 1; // removed players go to the front of the line
    before.filter(i => !ids.includes(i)).forEach(i => run('INSERT OR REPLACE INTO queue(player_id,joined) VALUES(?,?)', i, front));
    ids.forEach(i => run('DELETE FROM queue WHERE player_id=?', i));
    run('UPDATE matches SET t1a=?,t1b=?,t2a=?,t2b=? WHERE id=?', ids[0], ids[1], ids[2], ids[3], old.id); }],
  ['POST', /^\/api\/auto$/, b => run("UPDATE settings SET v=? WHERE k='auto'", b.on ? '1' : '0')],
  ['POST', /^\/api\/upnext$/, () => { run("INSERT INTO upnext(mode) VALUES('manual')"); setUpn(upn() + 1); }],
  ['POST', /^\/api\/upnext\/refresh$/, () => { run('DELETE FROM upnext'); syncUp(); }],
  ['POST', /^\/api\/upnext\/(\d+)\/rebuild$/, (b, m) => { syncUp(); if (!fillAuto(+m[1])) throw 'Not enough waiting players'; }],
  ['DELETE', /^\/api\/upnext\/(\d+)$/, (b, m) => { run('DELETE FROM upnext WHERE id=?', m[1]); setUpn(upn() - 1); }],
  ['POST', /^\/api\/upnext\/(\d+)\/slot$/, (b, m) => { syncUp(); const i = +b.i, id = +m[1], p = b.player ? +b.player : null;
    const u = getUp().find(x => x.id === id); if (!u || !(i >= 0 && i < 4)) throw 'Match not found';
    if (p) { if (!pool(id).some(x => x.id === p)) throw 'Player is not available'; if (slots(u).some((x, k) => x === p && k !== i)) throw 'Already in this match'; }
    run(`UPDATE upnext SET s${i}=?,mode='manual',note=NULL,called=0 WHERE id=?`, p, id); }],
  ['POST', /^\/api\/upnext\/(\d+)\/send$/, (b, m) => sendUp(+m[1], b.court)],
  ['POST', /^\/api\/upnext\/(\d+)\/call$/, (b, m) => run('UPDATE upnext SET called=1-called WHERE id=?', m[1])],
  ['POST', /^\/api\/courts\/(add|remove)$/, (b, m) => { const n = courts(), add = m[1] === 'add';
    if (add ? n >= 20 : n <= 1 || one("SELECT 1 x FROM matches WHERE status='live' AND court=?", n)) throw add ? 'Max 20 courts' : 'Last court is in use';
    run("UPDATE settings SET v=? WHERE k='courts'", String(n + (add ? 1 : -1))); }],
  // admin-only
  ['POST', /^\/api\/admin\/courts$/, b => run("UPDATE settings SET v=? WHERE k='courts'", String(Math.min(20, Math.max(1, +b.n || 1))))],
  ['PATCH', /^\/api\/admin\/players\/(\d+)$/, (b, m) => run('UPDATE players SET name=?,skill=?,elo=? WHERE id=?', b.name, b.skill, b.elo, m[1])],
  ['DELETE',
 /^\/api\/admin\/players\/all$/,
 () => {

    run('DELETE FROM queue');
    run('DELETE FROM matches');
    run('DELETE FROM upnext');
    run('DELETE FROM tmatches');
    run('DELETE FROM tournaments');
    run('DELETE FROM players');

    return {
        ok: true,
        message: 'All players deleted'
    };

}],
['POST', /^\/api\/admin\/save-session$/, b => {

    const name = b.name || `Session ${new Date().toLocaleDateString()}`;

    const sessionId = run(
        'INSERT INTO sessions(name,created) VALUES(?,?)',
        name,
        Date.now()
    ).lastInsertRowid;

    const players = q(`
        SELECT *
        FROM players
        ORDER BY points_diff DESC,wins DESC
    `);

    players.forEach((p,index)=>{

        run(`
            INSERT INTO session_results(
                session_id,
                player_name,
                wins,
                losses,
                points_diff,
                rank
            )
            VALUES(?,?,?,?,?,?)
        `,
        sessionId,
        p.name,
        p.wins,
        p.losses,
        p.points_diff||0,
        index+1);

    });

    return {
        ok:true,
        sessionId
    };

}],
['GET', /^\/api\/admin\/sessions$/, ()=>{

    return q(`
        SELECT *
        FROM sessions
        ORDER BY created DESC
    `);

}],

['GET', /^\/api\/admin\/sessions\/(\d+)$/, (b,m)=>{

    return q(`
        SELECT *
        FROM session_results
        WHERE session_id=?
        ORDER BY rank
    `, m[1]);

}],
  ['DELETE', /^\/api\/admin\/players\/(\d+)$/, (b, m) => { run('DELETE FROM queue WHERE player_id=?', m[1]); run('DELETE FROM players WHERE id=?', m[1]); }],
  ['POST', /^\/api\/admin\/cancel\/(\d+)$/, (b, m) => { const x = one('SELECT * FROM matches WHERE id=?', m[1]); [x.t1a, x.t1b, x.t2a, x.t2b].forEach(p => run('INSERT OR IGNORE INTO queue(player_id,joined) VALUES(?,?)', p, Date.now())); run('DELETE FROM matches WHERE id=?', m[1]); }],
  ['POST', /^\/api\/admin\/tournaments$/, b => createTournament(b.name || 'Tournament', b.players || [])],
  ['DELETE', /^\/api\/admin\/tournaments\/(\d+)$/, (b, m) => { run('DELETE FROM tmatches WHERE tid=?', m[1]); run('DELETE FROM tournaments WHERE id=?', m[1]); }],
  ['POST', /^\/api\/admin\/reset$/, () => {

    run('DELETE FROM queue');
    run('DELETE FROM matches');
    run('DELETE FROM upnext');
    run('DELETE FROM tmatches');
    run('DELETE FROM tournaments');

    run(`
      UPDATE players
      SET
        present = 0,
        wins = 0,
        losses = 0,
        last_res = NULL,
        last_partner = NULL,
        points_diff = 0,
        elo = 800 + skill * 200
    `);

    return {
        ok: true,
        message: 'Session reset complete'
    };

}],
];
const mime = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css' };
http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/events') { res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    res.write('retry: 2000\n\n'); clients.add(res); req.on('close', () => clients.delete(res)); return; }
  const json = (c, o) => { res.writeHead(c, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(o)); };
  if (url.pathname === '/api/state') return json(200, state());
  if (url.pathname === '/api/admin/verify') return json(req.headers['x-admin-pin'] === PIN ? 200 : 401, {});
  const route = routes.find(r => r[0] === req.method && r[1].test(url.pathname));
  if (route) {
    if (url.pathname.startsWith('/api/admin') && req.headers['x-admin-pin'] !== PIN) return json(401, { error: 'Admin PIN required' });
    if(req.method === 'GET'){

    try{

        const out = route[2](
            {},
            url.pathname.match(route[1])
        );

        return json(
            200,
            out || { ok:true }
        );

    }catch(e){

        return json(
            400,
            {
                error:String(e.message||e)
            }
        );

    }

}

let body='';

req.on('data',c=>body+=c);

req.on('end',()=>{

    try{

        const out=route[2](
            body ? JSON.parse(body) : {},
            url.pathname.match(route[1])
        );

        broadcast();

        json(
            200,
            out || {ok:true}
        );

    }catch(e){

        json(
            400,
            {
                error:String(e.message||e)
            }
        );

    }

}); return;
  }
  const f = path.join(__dirname, 'public', url.pathname === '/' ? 'index.html' : path.normalize(url.pathname).replace(/^(\.\.[\/\\])+/, ''));
  fs.readFile(f, (e, d) => e ? (res.writeHead(404), res.end('Not found')) : (res.writeHead(200, { 'Content-Type': mime[path.extname(f)] || 'application/octet-stream' }), res.end(d)));
}).listen(PORT, '0.0.0.0', () => console.log(`Pickleholics running → http://localhost:${PORT}  (admin PIN: ${PIN})`));
