/* 아발론 온라인 - 화면 & 네트워크 (MQTT 공개 중계 서버, WebSocket) */
(function () {
  'use strict';
  const A = window.Avalon;
  const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

  const $ = (s) => document.querySelector(s);
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

  const store = {
    get(k, area) { try { return JSON.parse((area || localStorage).getItem(k)); } catch (e) { return null; } },
    set(k, v, area) { try { (area || localStorage).setItem(k, JSON.stringify(v)); } catch (e) { /* ignore */ } },
    del(k, area) { try { (area || localStorage).removeItem(k); } catch (e) { /* ignore */ } },
  };

  function genCode() {
    let c = '';
    for (let i = 0; i < 5; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    return c;
  }
  function newId() {
    return (window.crypto && crypto.randomUUID) ? crypto.randomUUID().replace(/-/g, '') : Math.random().toString(36).slice(2) + Date.now().toString(36);
  }
  function getCid() {
    let c = store.get('avalon-cid', sessionStorage);
    if (!c) { c = newId(); store.set('avalon-cid', c, sessionStorage); }
    return c;
  }

  const app = {
    mode: 'home', // home | host | client
    session: 0,
    cid: getCid(),
    state: null,
    view: null,
    code: '',
    name: store.get('avalon-name') || '',
    status: '',
    roleHidden: true,
    selection: [],
    selKey: '',
    // host
    clients: new Map(), // pid -> {cid, seen}
    kicked: new Set(),
    ver: 0,
    epoch: '',
    // client
    joined: false,
    info: null,
    viewEpoch: '',
    viewVer: -1,
    hostSeen: 0,
    t0: 0,
    lastHello: 0,
  };

  /* ---------------- 네트워크: 공개 MQTT 중계 서버 ----------------
     여러 중계 서버에 동시에 접속하고 모든 서버로 메시지를 보냅니다.
     두 사람이 공통으로 닿는 서버가 하나만 있어도 게임이 됩니다. */
  const PROTO = 2;
  const NS = 'avalon-kr-online';
  const ROOMS = `${NS}/rooms/`;
  const roomBase = (code) => `${NS}/room/${code}`;
  const BROKERS = [
    { u: 'wss://broker.emqx.io:8084/mqtt' },
    { u: 'wss://mqtt.eclipseprojects.io:443/mqtt' },
    { u: 'wss://public.cloud.shiftr.io', o: { username: 'public', password: 'public' } },
    { u: 'wss://broker.hivemq.com:8884/mqtt' },
    { u: 'wss://test.mosquitto.org:8081/mqtt' },
  ];
  const PAGE_ID = Math.random().toString(36).slice(2, 10);
  const NET = { clis: [], pending: [], seen: new Set(), seq: 0, onConnect: null };

  const netUp = () => NET.clis.filter((c) => c.connected).length;
  function netEnd() {
    [...NET.clis, ...NET.pending].forEach((c) => { try { c.end(true); } catch (e) { /* ignore */ } });
    NET.clis = [];
    NET.pending = [];
    NET.onConnect = null;
  }
  function rawPub(topic, payload, retain) {
    for (const c of NET.clis) if (c.connected) { try { c.publish(topic, payload, { qos: 0, retain: !!retain }); } catch (e) { /* ignore */ } }
  }
  function pub(topic, m, retain) {
    rawPub(topic, JSON.stringify(Object.assign({}, m, { _i: `${PAGE_ID}.${++NET.seq}` })), retain);
  }
  function parseMsg(txt) {
    let m;
    try { m = JSON.parse(txt); } catch (e) { return null; }
    if (!m || typeof m !== 'object') return null;
    if (m._i) {
      if (NET.seen.has(m._i)) return null;
      NET.seen.add(m._i);
      if (NET.seen.size > 5000) NET.seen = new Set([...NET.seen].slice(-2500));
    }
    return m;
  }
  function netAvailable() {
    if (!window.mqtt) { toast('통신 라이브러리를 불러오지 못했습니다. 인터넷 연결을 확인하고 새로고침해주세요.'); return false; }
    return true;
  }
  // 모든 중계 서버에 접속. 첫 서버가 연결되면 resolve (나머지는 뒤에서 계속 연결)
  function netConnect(will, topics, onMsg) {
    const q = new URLSearchParams(location.search).get('broker');
    const list = q ? [{ u: q }] : BROKERS;
    NET.pending = [];
    return new Promise((res, rej) => {
      let left = list.length;
      let done = false;
      const failOne = () => { if (--left === 0 && !done) { done = true; rej(new Error('no broker')); } };
      for (const b of list) {
        let cli;
        let up = false;
        const opts = Object.assign({}, b.o || {}, {
          clientId: `av${PAGE_ID}${Math.random().toString(36).slice(2, 6)}`,
          clean: true, keepalive: 20, reconnectPeriod: 3000, connectTimeout: 9000,
        });
        if (will) opts.will = will;
        try { cli = mqtt.connect(b.u, opts); } catch (e) { failOne(); continue; }
        NET.pending.push(cli);
        cli.on('error', () => {});
        cli.on('connect', () => {
          if (!up) {
            up = true;
            NET.clis.push(cli);
            cli.subscribe(topics);
            cli.on('message', (tp, buf) => onMsg(tp, buf.toString()));
          }
          if (NET.onConnect) NET.onConnect();
          if (!done) { done = true; res(); }
        });
        setTimeout(() => { if (!up) { try { cli.end(true); } catch (e) { /* ignore */ } failOne(); } }, 10000);
      }
    });
  }

  /* ---------------- 공개 방 목록 ---------------- */
  const BROWSE = { clis: [], rooms: new Map(), on: false };
  function startBrowse() {
    if (BROWSE.on || !window.mqtt) return;
    BROWSE.on = true;
    BROWSE.rooms.clear();
    for (const b of BROKERS) {
      let cli;
      try {
        cli = mqtt.connect(b.u, Object.assign({}, b.o || {}, {
          clientId: `avb${Math.random().toString(36).slice(2, 10)}`, clean: true, keepalive: 30, reconnectPeriod: 4000, connectTimeout: 9000,
        }));
      } catch (e) { continue; }
      BROWSE.clis.push(cli);
      cli.on('error', () => {});
      cli.on('connect', () => { cli.subscribe(`${ROOMS}+`); renderRooms(); });
      cli.on('message', (tp, buf) => {
        const code = tp.slice(ROOMS.length);
        const txt = buf.toString();
        if (!txt) { BROWSE.rooms.delete(code); renderRooms(); return; }
        let m;
        try { m = JSON.parse(txt); } catch (e) { return; }
        if (!m || m.t !== 'room') return;
        const old = BROWSE.rooms.get(code);
        if (!old || m.ts >= old.ts) BROWSE.rooms.set(code, m);
        renderRooms();
      });
    }
  }
  function stopBrowse() {
    BROWSE.on = false;
    BROWSE.clis.forEach((c) => { try { c.end(true); } catch (e) { /* ignore */ } });
    BROWSE.clis = [];
  }
  setInterval(() => { if (BROWSE.on) renderRooms(); }, 2000);

  function renderRooms() {
    const el = $('#roomList');
    if (!el || app.mode !== 'home') return;
    const up = BROWSE.clis.filter((c) => c.connected).length;
    const now = Date.now();
    const list = [...BROWSE.rooms.values()]
      .filter((m) => m.v === PROTO && !m.gone && Math.abs(now - m.ts) < 20000)
      .sort((a, b) => (a.state === 'game') - (b.state === 'game') || b.ts - a.ts);
    const cnt = $('#roomCnt');
    if (cnt) cnt.textContent = up ? `${list.length}개` : '';
    if (!list.length) {
      el.innerHTML = `<div class="empty">${up ? '아직 열린 방이 없어요. 방을 만들어 친구들을 초대해보세요!' : '방 목록 서버에 연결하는 중...'}</div>`;
      return;
    }
    el.innerHTML = list.map((m) => {
      const full = m.n >= A.MAX_PLAYERS;
      const game = m.state === 'game';
      const need = Math.max(0, A.MIN_PLAYERS - m.n);
      return `<div class="roomrow">
        <div class="rinfo"><b>${esc(m.host)}의 방</b>
          <small>#${esc(m.code)} · ${game ? '⚔️ 게임 중' : (need ? `⏳ 대기 중 · ${need}명 더 필요` : '⏳ 대기 중 · 시작 가능')}</small></div>
        <span class="rcount ${m.n < A.MIN_PLAYERS ? 'short' : ''}">👥 ${m.n}/${A.MAX_PLAYERS}</span>
        <button class="mini ${game || full ? '' : 'primary'}" data-act="joinRoom" data-code="${esc(m.code)}" ${full && !game ? 'disabled' : ''}>${game ? '재입장' : (full ? '만석' : '입장')}</button>
      </div>`;
    }).join('');
  }

  /* ---------------- 공통 ---------------- */
  let toastTimer = null;
  function toast(msg) {
    const el = $('#toast');
    el.textContent = msg;
    el.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { el.hidden = true; }, 3500);
  }

  function teardown() {
    app.session += 1;
    netEnd();
    app.view = null;
    app.state = null;
    app.status = '';
    app.selection = [];
    app.clients = new Map();
    app.joined = false;
    app.info = null;
  }

  function goHome(msg) {
    teardown();
    app.mode = 'home';
    render();
    if (msg) toast(msg);
  }

  function inviteLink(code) {
    return `${location.origin}${location.pathname}?room=${code}`;
  }

  let wakeLock = null;
  async function keepAwake() {
    try {
      if ('wakeLock' in navigator && !document.hidden && app.mode !== 'home' && (!wakeLock || wakeLock.released)) wakeLock = await navigator.wakeLock.request('screen');
    } catch (e) { /* ignore */ }
  }

  /* ---------------- 호스트 ---------------- */
  function hostName() {
    const p = app.state && app.state.players.find((x) => x.id === app.state.hostId);
    return p ? p.name : app.name;
  }
  function pubInfo(gone) {
    if (app.mode !== 'host' || !app.state) return;
    const s = app.state;
    rawPub(ROOMS + app.code, JSON.stringify({
      t: 'room', code: app.code, host: hostName(), n: s.players.length,
      state: s.phase === 'lobby' ? 'lobby' : 'game', ts: Date.now(), gone: !!gone, v: PROTO,
    }), true);
  }

  async function hostStart(name, restore) {
    if (!netAvailable()) return;
    stopBrowse();
    teardown();
    const token = app.session;
    app.mode = 'host';
    app.name = name;
    app.epoch = newId().slice(0, 8);
    app.ver = 0;
    app.kicked = new Set();
    if (restore) {
      app.code = restore.code;
      app.state = restore.state;
      app.cid = app.state.hostId;
      store.set('avalon-cid', app.cid, sessionStorage);
      if (app.state.phase === 'lobby') app.state.players = app.state.players.filter((p) => p.id === app.state.hostId);
      app.state.players.forEach((p) => { p.connected = p.id === app.state.hostId; });
    } else {
      app.code = genCode();
      app.state = A.createRoom(app.code, app.cid, name);
    }
    app.status = '중계 서버에 연결하는 중...';
    hostPublish();
    NET.onConnect = () => pubInfo(false);
    try {
      await netConnect(
        { topic: ROOMS + app.code, payload: JSON.stringify({ t: 'room', code: app.code, ts: Date.now(), gone: true, v: PROTO }), qos: 0, retain: true },
        [`${roomBase(app.code)}/h`],
        (tp, txt) => { const m = parseMsg(txt); if (m && m.f) hostOnMsg(String(m.f), m); },
      );
    } catch (e) {
      if (token === app.session) goHome('중계 서버에 하나도 연결할 수 없습니다. 인터넷(와이파이/데이터) 연결을 확인해주세요.');
      return;
    }
    if (token !== app.session) return;
    app.status = '';
    pubInfo(false);
    hostPublish();
    keepAwake();
  }

  function sendTo(cid, m) {
    pub(`${roomBase(app.code)}/c/${cid}`, m);
  }

  function sendState(pid) {
    const c = app.clients.get(pid);
    if (c) sendTo(c.cid, { t: 'state', ep: app.epoch, ver: app.ver, view: A.viewFor(app.state, pid) });
  }

  function hostOnMsg(cid, m) {
    if (app.mode !== 'host' || !app.state) return;
    const s = app.state;
    const now = Date.now();
    if (m.t === 'hello') {
      if (m.v !== PROTO) { sendTo(cid, { t: 'reject', reason: '호스트와 버전이 다릅니다. 두 기기 모두 새로고침한 뒤 다시 시도해주세요.' }); return; }
      if (app.kicked.has(cid)) { sendTo(cid, { t: 'reject', reason: '호스트가 방에서 내보냈습니다.' }); return; }
      if (cid === app.cid) return;
      const before = JSON.stringify(s.players);
      const r = A.join(s, cid, m.name);
      if (r.error) { sendTo(cid, { t: 'reject', reason: r.error }); return; }
      const old = app.clients.get(r.pid);
      if (old && old.cid !== cid) sendTo(old.cid, { t: 'reject', reason: '다른 창에서 같은 플레이어로 접속했습니다.' });
      app.clients.set(r.pid, { cid, seen: now });
      if (JSON.stringify(s.players) !== before) hostPublish();
      else if (m.ep !== app.epoch || m.ver !== app.ver) sendState(r.pid);
      else sendTo(cid, { t: 'ok' });
      return;
    }
    const pid = A.resolveId(s, cid);
    const c = app.clients.get(pid);
    if (!c || c.cid !== cid) return;
    c.seen = now;
    if (m.t === 'action') {
      const err = runAction(pid, m.action, m.payload);
      if (err) sendTo(cid, { t: 'error', msg: err });
    } else if (m.t === 'bye') {
      app.clients.delete(pid);
      A.disconnect(s, pid);
      hostPublish();
    }
  }

  function runAction(pid, action, payload) {
    const err = A.applyAction(app.state, pid, action, payload || {});
    if (err) return err;
    if (action === 'kick') {
      const c = app.clients.get(payload.id);
      if (c) {
        app.kicked.add(c.cid);
        sendTo(c.cid, { t: 'reject', reason: '호스트가 방에서 내보냈습니다.' });
        app.clients.delete(payload.id);
      }
    }
    hostPublish();
    return null;
  }

  function saveHost() {
    if (app.mode === 'host' && app.state) store.set('avalon-host', { code: app.code, name: app.name, state: app.state, savedAt: Date.now() });
  }

  function hostPublish() {
    if (app.mode !== 'host') return;
    app.ver += 1;
    saveHost();
    for (const pid of app.clients.keys()) sendState(pid);
    app.view = A.viewFor(app.state, app.cid);
    pubInfo(false);
    render();
  }

  /* ---------------- 참가자 ---------------- */
  async function clientStart(code, name) {
    if (!netAvailable()) return;
    stopBrowse();
    teardown();
    const token = app.session;
    app.mode = 'client';
    app.code = code;
    app.name = name;
    app.viewEpoch = '';
    app.viewVer = -1;
    app.hostSeen = 0;
    app.t0 = Date.now();
    app.status = '중계 서버에 연결하는 중...';
    render();
    NET.onConnect = () => sendHello();
    try {
      await netConnect(null, [`${roomBase(code)}/c/${app.cid}`, ROOMS + code], clientOnMsg);
    } catch (e) {
      if (token === app.session) goHome('중계 서버에 하나도 연결할 수 없습니다. 인터넷(와이파이/데이터) 연결을 확인해주세요.');
      return;
    }
    if (token !== app.session) return;
    app.t0 = Date.now();
    app.status = '방을 찾는 중...';
    render();
    sendHello();
  }

  function sendHello() {
    if (app.mode !== 'client') return;
    app.lastHello = Date.now();
    pub(`${roomBase(app.code)}/h`, { t: 'hello', f: app.cid, name: app.name, v: PROTO, ep: app.viewEpoch, ver: app.viewVer });
  }

  function clientOnMsg(tp, txt) {
    if (app.mode !== 'client') return;
    if (tp.startsWith(ROOMS)) {
      if (!txt) { app.info = { gone: true, ts: Date.now() }; return; }
      try { const m = JSON.parse(txt); if (m && m.t === 'room') app.info = m; } catch (e) { /* ignore */ }
      return;
    }
    const m = parseMsg(txt);
    if (!m) return;
    app.hostSeen = Date.now();
    if (m.t === 'state') {
      if (m.ep === app.viewEpoch && m.ver <= app.viewVer) return; // 늦게 도착한 이전 상태
      app.viewEpoch = m.ep;
      app.viewVer = m.ver;
      app.view = m.view;
      if (!app.joined) { app.joined = true; keepAwake(); }
      app.status = '';
      store.set('avalon-client', { code: app.code, name: app.name, savedAt: Date.now() });
      render();
    } else if (m.t === 'error') {
      toast(m.msg);
    } else if (m.t === 'reject') {
      store.del('avalon-client');
      goHome(m.reason);
    } else if (m.t === 'ok' && app.status) {
      app.status = '';
      render();
    }
  }

  /* ---------------- 연결 유지 타이머 ---------------- */
  let lastBeat = Date.now();
  function resumed() {
    const now = Date.now();
    for (const c of app.clients.values()) c.seen = now;
    app.hostSeen = now;
    lastBeat = now;
    for (const c of NET.clis) if (!c.connected) { try { c.reconnect(); } catch (e) { /* ignore */ } }
    if (app.mode === 'client') sendHello();
    if (app.mode === 'host') pubInfo(false);
    keepAwake();
  }
  document.addEventListener('visibilitychange', () => { if (!document.hidden) resumed(); });

  setInterval(() => {
    const now = Date.now();
    const gap = now - lastBeat;
    lastBeat = now;
    if (gap > 5000) { resumed(); return; } // 화면이 꺼졌다 켜진 경우
    if (app.mode === 'host' && app.state) {
      let changed = false;
      for (const [pid, c] of app.clients) {
        if (now - c.seen > 15000) {
          app.clients.delete(pid);
          A.disconnect(app.state, pid);
          changed = true;
        }
      }
      if (changed) hostPublish();
      const st = netUp() ? '' : '중계 서버와 연결이 끊겼습니다. 다시 연결하는 중...';
      if (st !== app.status && (app.status === '' || app.status.startsWith('중계 서버와'))) { app.status = st; render(); }
    } else if (app.mode === 'client' && NET.clis.length) {
      const el = (now - app.t0) / 1000;
      if (!app.joined) {
        if (el > 10 && !app.info) { goHome('방을 찾을 수 없습니다. 방 코드를 확인하거나, 호스트에게 방이 열려 있는지 확인해주세요.'); return; }
        if (el > 90) { goHome('호스트가 응답하지 않습니다. 호스트 화면이 켜져 있는지 확인 후 다시 시도해주세요.'); return; }
        if (app.info && app.info.v && app.info.v !== PROTO) { goHome('호스트와 버전이 다릅니다. 두 기기 모두 새로고침한 뒤 다시 시도해주세요.'); return; }
        const away = app.info && (app.info.gone || now - app.info.ts > 20000);
        app.status = !app.info ? `방을 찾는 중... (${Math.floor(el)}초)` : (away ? '호스트 화면이 꺼져 있는 것 같아요. 호스트가 돌아오면 자동으로 입장합니다...' : '호스트의 응답을 기다리는 중...');
        render();
        if (now - app.lastHello > 1500) sendHello();
      } else {
        const lost = now - app.hostSeen > 9000;
        const st = lost ? '호스트와 연결이 끊겼어요. 다시 연결되면 자동으로 이어집니다...' : '';
        if (st !== app.status) { app.status = st; render(); }
        if (now - app.lastHello > 3000) sendHello();
      }
    }
  }, 1000);
  setInterval(() => { if (app.mode === 'host' && NET.clis.length) pubInfo(false); }, 5000);

  /* ---------------- 행동 ---------------- */
  function act(action, payload) {
    if (app.mode === 'host') {
      const err = runAction(app.cid, action, payload);
      if (err) toast(err);
    } else if (app.joined && netUp()) {
      pub(`${roomBase(app.code)}/h`, { t: 'action', f: app.cid, action, payload });
    } else {
      toast('연결 중입니다. 잠시 후 다시 시도해주세요.');
    }
  }

  function leave() {
    if (app.mode === 'host') {
      if (!confirm('방을 닫으면 모든 참가자의 연결이 끊어집니다. 정말 나가시겠습니까?')) return;
      for (const c of app.clients.values()) sendTo(c.cid, { t: 'reject', reason: '호스트가 방을 닫았습니다.' });
      rawPub(ROOMS + app.code, '', true);
      store.del('avalon-host');
      const s = app.session;
      setTimeout(() => { if (s === app.session) goHome(); }, 500);
      return;
    }
    if (app.view && app.view.phase !== 'lobby' && app.view.phase !== 'end' && !confirm('게임 중입니다. 나가시겠습니까? (같은 닉네임으로 다시 들어올 수 있습니다)')) return;
    if (app.joined) pub(`${roomBase(app.code)}/h`, { t: 'bye', f: app.cid });
    store.del('avalon-client');
    const s = app.session;
    setTimeout(() => { if (s === app.session) goHome(); }, 300);
  }

  window.addEventListener('beforeunload', () => {
    if (app.mode === 'host') pubInfo(true);
  });

  async function copyInvite() {
    const link = inviteLink(app.code);
    if (navigator.share && /Mobi|Android|iPhone/i.test(navigator.userAgent)) {
      try { await navigator.share({ title: '아발론 온라인', text: `아발론 방 코드: ${app.code}`, url: link }); return; } catch (e) { /* 취소 시 복사로 */ }
    }
    try { await navigator.clipboard.writeText(link); toast('초대 링크를 복사했습니다.'); }
    catch (e) { prompt('아래 링크를 복사해 공유하세요', link); }
  }

  /* ---------------- 화면 그리기 ---------------- */
  function roleSummary(list) {
    const count = {};
    list.forEach((r) => { count[r] = (count[r] || 0) + 1; });
    return Object.keys(count).map((r) => `${A.ROLES[r].name}${count[r] > 1 ? ` ×${count[r]}` : ''}`).join(', ');
  }

  function compositionTable(highlight) {
    const rows = [];
    for (let n = A.MIN_PLAYERS; n <= A.MAX_PLAYERS; n++) {
      const c = A.COMPOSITION[n];
      const q = A.QUEST_SIZES[n].map((s, i) => (A.failsNeeded(n, i) === 2 ? `${s}*` : `${s}`)).join(' · ');
      rows.push(`<tr class="${n === highlight ? 'hl' : ''}"><td>${n}명</td><td class="good-t">${c.good}</td><td class="evil-t">${c.evil}</td><td>${q}</td></tr>`);
    }
    return `<table class="comp"><thead><tr><th>인원</th><th>선</th><th>악</th><th>퀘스트별 원정대 인원</th></tr></thead><tbody>${rows.join('')}</tbody></table>
      <p class="hint">* 7명 이상일 때 4번째 퀘스트는 실패 카드가 2장 이상이어야 실패합니다.</p>`;
  }

  function renderHome() {
    const params = new URLSearchParams(location.search);
    const roomParam = (params.get('room') || '').toUpperCase();
    const host = store.get('avalon-host');
    const client = store.get('avalon-client');
    const fresh = (x) => x && Date.now() - x.savedAt < 12 * 3600 * 1000;
    let resume = '';
    if (fresh(host)) resume += `<button class="ghost" data-act="resumeHost">↩︎ 내가 만든 방 ${esc(host.code)} 다시 열기</button>`;
    if (fresh(client) && (!roomParam || roomParam === client.code)) resume += `<button class="ghost" data-act="resumeClient">↩︎ 방 ${esc(client.code)}에 다시 입장 (${esc(client.name)})</button>`;
    return `
      <header class="hero">
        <div class="crest">⚔️</div>
        <h1>아발론</h1>
        <p class="sub">The Resistance: Avalon · 온라인 보드게임</p>
        <div class="pills"><span class="pill">👥 최소 ${A.MIN_PLAYERS}명</span><span class="pill">최대 ${A.MAX_PLAYERS}명</span><span class="pill">⏱ 약 30분</span></div>
      </header>
      <section class="card">
        <label class="field">닉네임
          <input id="nameInput" maxlength="12" placeholder="예: 랜슬롯" value="${esc(app.name)}" autocomplete="off">
        </label>
        ${roomParam ? '' : `<button class="primary" data-act="create">🏰 방 만들기 (호스트)</button>
        <div class="divider"><span>또는 초대받은 방에 입장</span></div>`}
        <label class="field">방 코드
          <input id="codeInput" maxlength="5" placeholder="5자리 코드" value="${esc(roomParam)}" autocomplete="off" style="text-transform:uppercase;letter-spacing:.3em">
        </label>
        <button class="${roomParam ? 'primary' : ''}" data-act="join">🚪 입장하기</button>
        ${roomParam ? '<button class="link" data-act="clearRoom">새 방을 만들고 싶어요</button>' : ''}
        ${resume ? `<div class="resume">${resume}</div>` : ''}
      </section>
      <section class="card">
        <div class="row-between"><h2>열려 있는 방</h2><span class="muted" id="roomCnt"></span></div>
        <div id="roomList"><div class="empty">방 목록 서버에 연결하는 중...</div></div>
      </section>
      <section class="card">
        <h2>인원별 구성</h2>
        ${compositionTable()}
      </section>
      <section class="card rules">
        <h2>게임 방법</h2>
        <ol>
          <li>모두에게 비밀 역할이 주어집니다. <b class="good-t">선</b>은 퀘스트 성공, <b class="evil-t">악</b>은 실패가 목표입니다.</li>
          <li>👑 대표가 원정대를 고르면 전원이 찬성/반대 투표를 합니다. 과반 찬성이면 출발, 5번 연속 부결되면 악이 승리합니다.</li>
          <li>원정대원은 몰래 성공/실패 카드를 냅니다. 선은 성공만 낼 수 있고, 실패가 한 장이라도 있으면 퀘스트 실패입니다.</li>
          <li>퀘스트 3개를 먼저 성공/실패시키는 쪽이 이깁니다. 단, 선이 3번 성공해도 암살자가 멀린을 맞히면 악의 역전승!</li>
        </ol>
        <p class="hint">음성 채팅이나 같은 자리에서 대화하며 플레이하세요. 호스트의 브라우저가 게임 서버 역할을 하므로 호스트는 창을 닫지 마세요.</p>
      </section>`;
  }

  function renderConnecting() {
    return `<section class="card center">
      <div class="spinner"></div>
      <p>${esc(app.status || '연결 중...')}</p>
      <p class="hint">방 코드: <b>${esc(app.code)}</b></p>
      <button class="ghost" data-act="leave">취소</button>
    </section>`;
  }

  function renderLobby(v) {
    const n = v.n;
    const need = Math.max(0, A.MIN_PLAYERS - n);
    const pct = Math.min(100, (n / A.MAX_PLAYERS) * 100);
    const minPct = (A.MIN_PLAYERS / A.MAX_PLAYERS) * 100;
    const preview = A.buildRoles(Math.min(Math.max(n, A.MIN_PLAYERS), A.MAX_PLAYERS), v.settings);
    const previewN = Math.min(Math.max(n, A.MIN_PLAYERS), A.MAX_PLAYERS);
    const canStart = v.isHost && n >= A.MIN_PLAYERS && n <= A.MAX_PLAYERS && !preview.error;
    let statusMsg;
    if (need > 0) statusMsg = `게임 시작까지 <b>${need}명</b> 더 필요합니다 (최소 ${A.MIN_PLAYERS}명)`;
    else if (n >= A.MAX_PLAYERS) statusMsg = `최대 인원(${A.MAX_PLAYERS}명)이 모두 모였습니다`;
    else statusMsg = `시작할 수 있습니다 · ${A.MAX_PLAYERS - n}명 더 들어올 수 있어요`;

    const opt = (key, label, desc, extra) => `
      <label class="opt ${v.isHost ? '' : 'disabled'}">
        <input type="checkbox" data-setting="${key}" ${v.settings[key] ? 'checked' : ''} ${v.isHost ? '' : 'disabled'}>
        <span><b>${label}</b><small>${desc}${extra || ''}</small></span>
      </label>`;

    return `
      <section class="card room">
        <div class="room-label">방 코드</div>
        <div class="room-code">${esc(v.code)}</div>
        <button data-act="copyLink">🔗 초대 링크 공유</button>
      </section>
      <section class="card">
        <div class="row-between"><h2>참가자</h2><span class="count ${need > 0 ? 'short' : 'ok'}">${n} / ${A.MAX_PLAYERS}명</span></div>
        <div class="meter"><div class="fill" style="width:${pct}%"></div><div class="minmark" style="left:${minPct}%"><span>최소 ${A.MIN_PLAYERS}</span></div></div>
        <p class="hint status-line">${statusMsg}</p>
        <ul class="plist">
          ${v.players.map((p) => `<li>
            <span class="pname">${p.id === v.hostId ? '👑 ' : ''}${esc(p.name)}${p.id === v.me ? ' <em>(나)</em>' : ''}</span>
            ${v.isHost && p.id !== v.hostId ? `<button class="mini" data-act="kick" data-id="${esc(p.id)}">내보내기</button>` : ''}
          </li>`).join('')}
        </ul>
      </section>
      <section class="card">
        <h2>역할 설정 ${v.isHost ? '' : '<small class="muted">(호스트가 설정)</small>'}</h2>
        <div class="opts">
          <div class="opt fixed"><span><b>멀린 · 암살자</b><small>기본 역할 (항상 포함)</small></span></div>
          ${opt('percival', '퍼시발 <span class="good-t">선</span>', '멀린을 알아봅니다')}
          ${opt('morgana', '모르가나 <span class="evil-t">악</span>', '퍼시발에게 멀린처럼 보입니다')}
          ${opt('mordred', '모드레드 <span class="evil-t">악</span>', '멀린에게 보이지 않습니다')}
          ${opt('oberon', '오베론 <span class="evil-t">악</span>', '다른 악과 서로 모릅니다')}
          ${opt('lady', '호수의 여인 (확장)', '2·3·4번째 퀘스트 후 한 명의 진영을 확인', n < 7 ? ' · <b>7명 이상일 때만 적용</b>' : '')}
        </div>
        <div class="preview">
          ${preview.error ? `<p class="warn">⚠️ ${esc(preview.error)}</p>` : `
            <p><b>${previewN}명${n < A.MIN_PLAYERS ? ' 기준' : ''}</b> 구성</p>
            <p><span class="good-t">선 ${preview.good.length}명</span>: ${roleSummary(preview.good)}</p>
            <p><span class="evil-t">악 ${preview.evil.length}명</span>: ${roleSummary(preview.evil)}</p>`}
        </div>
      </section>
      ${v.isHost
        ? `<button class="primary big" data-act="start" ${canStart ? '' : 'disabled'}>${canStart ? '⚔️ 게임 시작' : (need > 0 ? `${need}명 더 필요 (최소 ${A.MIN_PLAYERS}명)` : '역할 설정을 확인하세요')}</button>`
        : '<p class="center muted waiting">호스트가 게임을 시작하기를 기다리는 중...</p>'}
      <button class="ghost" data-act="leave">${v.isHost ? '방 닫기' : '나가기'}</button>`;
  }

  const PHASE_LABEL = { propose: '원정대 구성', vote: '찬반 투표', quest: '퀘스트 진행', lady: '호수의 여인', assassin: '멀린 암살', end: '게임 종료' };

  function canPick(v, id) {
    if (v.phase === 'propose' && v.leaderId === v.me) return true;
    if (v.phase === 'lady' && v.lady && v.lady.holder === v.me) return id !== v.me && !v.lady.used.includes(id);
    if (v.phase === 'assassin' && v.myRole === 'assassin') return id !== v.me;
    return false;
  }

  function renderGame(v) {
    const nameOf = (id) => { const p = v.players.find((x) => x.id === id); return p ? p.name : '?'; };
    const key = `${v.gameNo}-${v.phase}-${v.questIdx}-${v.rejectCount}`;
    if (key !== app.selKey) { app.selKey = key; app.selection = []; }

    // 퀘스트 트랙
    const quests = v.sizes.map((size, i) => {
      const r = v.results[i];
      const cur = !r && i === v.questIdx && ['propose', 'vote', 'quest', 'lady'].includes(v.phase);
      const cls = r ? (r.success ? 'ok' : 'bad') : (cur ? 'cur' : '');
      const two = A.failsNeeded(v.n, i) === 2;
      return `<div class="quest ${cls}">
        <div class="qbig">${r ? (r.success ? '✓' : '✗') : size}</div>
        <div class="qsub">${r ? (r.success ? '성공' : `실패 ${r.fails}`) : `${size}명`}${two && !r ? '<br><b>실패2장</b>' : ''}</div>
      </div>`;
    }).join('');
    const rejects = [0, 1, 2, 3, 4].map((i) => `<span class="rdot ${i < v.rejectCount ? 'on' : ''} ${i === 4 ? 'last' : ''}"></span>`).join('');

    // 역할 카드
    let roleCard = '';
    if (v.myRole) {
      const R = A.ROLES[v.myRole];
      const know = v.knowledge.length
        ? `<ul class="know">${v.knowledge.map((k) => `<li><b>${esc(nameOf(k.id))}</b> — ${esc(k.label)}</li>`).join('')}</ul>`
        : (v.myRole === 'merlin' ? '<p class="muted">보이는 악이 없습니다.</p>' : '');
      const lady = v.ladyChecks && v.ladyChecks.length
        ? `<div class="ladyinfo">🌊 호수의 여인 결과:<ul>${v.ladyChecks.map((c) => `<li><b>${esc(nameOf(c.target))}</b> — <span class="${c.team}-t">${c.team === 'good' ? '선' : '악'}</span></li>`).join('')}</ul></div>` : '';
      roleCard = `<section class="card rolecard ${app.roleHidden ? 'hidden-role' : R.team}" data-act="toggleRole">
        ${app.roleHidden
          ? '<div class="reveal">🔒 내 역할 확인하기 <small>(탭해서 보기 · 다른 사람이 보지 않게 주의)</small></div>'
          : `<div class="role-top"><span class="role-team">${R.team === 'good' ? '🛡️ 선의 세력' : '🗡️ 악의 세력'}</span><small>탭해서 숨기기</small></div>
             <div class="role-name">${R.name}</div>
             <p>${R.desc}</p>${know}${lady}`}
      </section>`;
    }

    // 플레이어 목록
    const showLastVote = v.lastVote && ['propose', 'vote', 'quest', 'lady'].includes(v.phase);
    const plist = v.players.map((p) => {
      const pickable = canPick(v, p.id);
      const selected = app.selection.includes(p.id);
      const tags = [];
      if (p.id === v.leaderId && v.phase !== 'end') tags.push('<span class="tag lead">👑 대표</span>');
      if (v.lady && v.lady.holder === p.id && v.phase !== 'end') tags.push('<span class="tag lady">🌊</span>');
      if (v.team.includes(p.id)) tags.push('<span class="tag team">🛡 원정대</span>');
      if (v.phase === 'vote' && v.votedIds.includes(p.id)) tags.push('<span class="tag done">투표 완료</span>');
      if (v.phase === 'quest' && v.questDone.includes(p.id)) tags.push('<span class="tag done">카드 제출</span>');
      if (showLastVote && p.id in v.lastVote.votes && v.phase !== 'vote') tags.push(v.lastVote.votes[p.id] ? '<span class="tag yes">찬성</span>' : '<span class="tag no">반대</span>');
      if (!p.connected) tags.push('<span class="tag off">연결 끊김</span>');
      let roleTag = '';
      if (v.allRoles) {
        const R = A.ROLES[v.allRoles[p.id]];
        roleTag = `<span class="tag role ${R.team}">${R.name}</span>${v.assassinTarget === p.id ? '<span class="tag no">🗡 암살 지목</span>' : ''}`;
      }
      return `<li class="${pickable ? 'pickable' : ''} ${selected ? 'selected' : ''}" ${pickable ? `data-act="pick" data-id="${esc(p.id)}"` : ''}>
        <span class="pname">${pickable ? `<span class="check">${selected ? '☑' : '☐'}</span>` : ''}${esc(p.name)}${p.id === v.me ? ' <em>(나)</em>' : ''}</span>
        <span class="tags">${roleTag}${tags.join('')}</span>
      </li>`;
    }).join('');

    return `
      <div class="topbar">
        <span>방 <b>${esc(v.code)}</b> · ${v.n}명</span>
        <span class="phase">${PHASE_LABEL[v.phase] || ''}</span>
        <button class="mini ghost" data-act="${v.isHost ? 'menu' : 'leave'}">${v.isHost ? '⋯' : '나가기'}</button>
      </div>
      ${v.phase === 'end' ? renderEnd(v) : ''}
      <section class="card board">
        <div class="quests">${quests}</div>
        <div class="rejects"><span>부결</span>${rejects}<span class="muted">${v.rejectCount}/5</span></div>
      </section>
      ${v.phase !== 'end' ? renderAction(v, nameOf) : ''}
      ${roleCard}
      <section class="card">
        <h2>플레이어 <small class="muted">(앉은 순서 · 대표는 순서대로 넘어갑니다)</small></h2>
        <ul class="plist game">${plist}</ul>
        ${v.rolesInGame ? `<p class="hint">이번 게임 역할: ${roleSummary(v.rolesInGame)}</p>` : ''}
      </section>
      ${renderLog(v, nameOf)}`;
  }

  function renderAction(v, nameOf) {
    const leaderName = nameOf(v.leaderId);
    const size = v.sizes[v.questIdx];
    const teamNames = v.team.map((id) => `<b>${esc(nameOf(id))}</b>`).join(', ');
    let body = '';
    switch (v.phase) {
      case 'propose':
        if (v.leaderId === v.me) {
          const two = A.failsNeeded(v.n, v.questIdx) === 2;
          body = `<h3>👑 당신이 대표입니다</h3>
            <p>퀘스트 ${v.questIdx + 1}에 보낼 원정대 <b>${size}명</b>을 아래 플레이어 목록에서 고르세요. (${app.selection.length}/${size})${two ? '<br><small>이번 퀘스트는 실패 카드 2장이 있어야 실패합니다.</small>' : ''}</p>
            ${v.rejectCount === 4 ? '<p class="warn">⚠️ 이번 제안이 부결되면 악이 승리합니다!</p>' : ''}
            <button class="primary" data-act="propose" ${app.selection.length === size ? '' : 'disabled'}>원정대 제안하기</button>`;
        } else {
          body = `<h3>👑 ${esc(leaderName)} 님이 원정대를 고르는 중</h3><p>퀘스트 ${v.questIdx + 1} · 원정대 ${size}명 · ${v.rejectCount + 1}번째 제안</p>`;
        }
        break;
      case 'vote':
        body = `<h3>찬반 투표 (${v.votedIds.length}/${v.n})</h3>
          <p>${esc(leaderName)} 님의 제안: ${teamNames}</p>
          ${v.rejectCount === 4 ? '<p class="warn">⚠️ 이번 투표가 부결되면 악이 승리합니다!</p>' : ''}
          ${v.myVote === null
            ? '<div class="btns"><button class="yes" data-act="vote" data-v="1">👍 찬성</button><button class="no" data-act="vote" data-v="0">👎 반대</button></div>'
            : `<p class="muted">내 투표: <b>${v.myVote ? '찬성' : '반대'}</b> · 다른 사람을 기다리는 중...</p>`}`;
        break;
      case 'quest': {
        const onTeam = v.team.includes(v.me);
        const done = v.questDone.includes(v.me);
        const good = v.myRole && A.ROLES[v.myRole].team === 'good';
        body = `<h3>퀘스트 ${v.questIdx + 1} 진행 중 (${v.questDone.length}/${v.team.length})</h3>
          <p>원정대: ${teamNames}</p>
          ${onTeam && !done
            ? `<p>몰래 카드를 내세요.</p><div class="btns"><button class="yes" data-act="quest" data-v="1">✓ 성공</button><button class="no" data-act="quest" data-v="0" ${good ? 'disabled title="선은 성공만 낼 수 있습니다"' : ''}>✗ 실패</button></div>
               ${good ? '<p class="hint">선의 세력은 성공 카드만 낼 수 있습니다.</p>' : ''}`
            : `<p class="muted">${onTeam ? '카드를 냈습니다. ' : ''}원정대의 결과를 기다리는 중...</p>`}`;
        break;
      }
      case 'lady':
        if (v.lady.holder === v.me) {
          body = `<h3>🌊 호수의 여인</h3><p>진영을 확인할 사람을 한 명 고르세요. (이전에 호수의 여인을 가졌던 사람은 제외) 확인한 사람에게 호수의 여인이 넘어갑니다.</p>
            <button class="primary" data-act="lady" ${app.selection.length === 1 ? '' : 'disabled'}>확인하기</button>`;
        } else {
          body = `<h3>🌊 호수의 여인</h3><p>${esc(nameOf(v.lady.holder))} 님이 한 사람의 진영을 확인하는 중...</p>`;
        }
        break;
      case 'assassin':
        if (v.myRole === 'assassin') {
          body = `<h3>🗡️ 선이 퀘스트 3개를 성공했습니다</h3><p>멀린이라고 생각하는 사람을 지목하세요. 맞히면 악의 역전승입니다. (악의 동료와 상의해도 됩니다)</p>
            <button class="no" data-act="assassinate" ${app.selection.length === 1 ? '' : 'disabled'}>암살하기</button>`;
        } else {
          body = '<h3>🗡️ 선이 퀘스트 3개를 성공했습니다</h3><p>암살자가 멀린을 찾는 중입니다... 멀린을 숨겨주세요!</p>';
        }
        break;
      default:
    }
    let last = '';
    if (v.lastVote && ['propose', 'quest', 'lady'].includes(v.phase)) {
      const yes = v.players.filter((p) => v.lastVote.votes[p.id]).map((p) => esc(p.name));
      const no = v.players.filter((p) => p.id in v.lastVote.votes && !v.lastVote.votes[p.id]).map((p) => esc(p.name));
      last = `<div class="lastvote ${v.lastVote.approved ? 'ok' : 'bad'}">
        직전 투표 (퀘스트 ${v.lastVote.q}, ${v.lastVote.attempt}번째 제안): <b>${v.lastVote.approved ? '가결' : '부결'}</b><br>
        <span class="good-t">찬성 ${yes.length}</span>: ${yes.join(', ') || '-'}<br>
        <span class="evil-t">반대 ${no.length}</span>: ${no.join(', ') || '-'}</div>`;
    }
    return `<section class="card action">${body}${last}</section>`;
  }

  function renderEnd(v) {
    const good = v.winner === 'good';
    return `<section class="card endcard ${good ? 'good' : 'evil'}">
      <div class="end-title">${good ? '🛡️ 선의 세력 승리' : '🗡️ 악의 세력 승리'}</div>
      <p>${esc(v.reason)}</p>
      ${v.isHost ? '<button class="primary" data-act="restart">같은 멤버로 다시 하기</button>' : '<p class="muted">호스트가 새 게임을 준비하면 대기실로 이동합니다.</p>'}
      <button class="ghost" data-act="leave">${v.isHost ? '방 닫기' : '나가기'}</button>
    </section>`;
  }

  function renderLog(v, nameOf) {
    if (!v.log.length) return '';
    const items = v.log.slice().reverse().map((e) => {
      if (e.t === 'vote') {
        const yes = Object.values(e.votes).filter(Boolean).length;
        return `<li><span class="lq">Q${e.q}-${e.attempt}</span> ${esc(nameOf(e.leader))}의 제안 [${e.team.map((id) => esc(nameOf(id))).join(', ')}] → 찬성 ${yes} / 반대 ${Object.keys(e.votes).length - yes} <b class="${e.approved ? 'good-t' : 'evil-t'}">${e.approved ? '가결' : '부결'}</b></li>`;
      }
      if (e.t === 'quest') {
        return `<li class="qlog"><span class="lq">Q${e.q}</span> 퀘스트 <b class="${e.success ? 'good-t' : 'evil-t'}">${e.success ? '성공' : '실패'}</b> — 실패 카드 ${e.fails}장${e.need === 2 ? ' (2장 필요)' : ''}</li>`;
      }
      if (e.t === 'lady') return `<li><span class="lq">🌊</span> ${esc(nameOf(e.holder))} → ${esc(nameOf(e.target))} 진영 확인</li>`;
      return '';
    }).join('');
    return `<section class="card"><h2>기록</h2><ul class="log">${items}</ul></section>`;
  }

  function render() {
    const root = $('#app');
    const st = $('#status');
    const showStatus = app.mode !== 'home' && app.status && app.view;
    st.hidden = !showStatus;
    st.textContent = showStatus ? app.status : '';
    if (app.mode === 'home') { root.innerHTML = renderHome(); startBrowse(); renderRooms(); }
    else if (!app.view) root.innerHTML = renderConnecting();
    else if (app.view.phase === 'lobby') root.innerHTML = renderLobby(app.view);
    else root.innerHTML = renderGame(app.view);
  }

  /* ---------------- 이벤트 ---------------- */
  function readName() {
    const el = $('#nameInput');
    const name = (el ? el.value : app.name).replace(/\s+/g, ' ').trim().slice(0, 12);
    if (!name) { toast('닉네임을 입력해주세요.'); if (el) el.focus(); return null; }
    app.name = name;
    store.set('avalon-name', name);
    return name;
  }

  document.addEventListener('click', (e) => {
    const el = e.target.closest('[data-act]');
    if (!el || el.disabled) return;
    const a = el.dataset.act;
    const v = app.view;
    switch (a) {
      case 'create': { const name = readName(); if (name) hostStart(name); break; }
      case 'join': {
        const name = readName();
        if (!name) return;
        const code = ($('#codeInput').value || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
        if (code.length !== 5) { toast('5자리 방 코드를 입력해주세요.'); return; }
        clientStart(code, name);
        break;
      }
      case 'joinRoom': { const name = readName(); if (name) clientStart(el.dataset.code, name); break; }
      case 'resumeHost': { const h = store.get('avalon-host'); if (h) hostStart(h.name, h); break; }
      case 'resumeClient': { const c = store.get('avalon-client'); if (c) clientStart(c.code, c.name); break; }
      case 'clearRoom': history.replaceState(null, '', location.pathname); render(); break;
      case 'copyLink': copyInvite(); break;
      case 'leave': leave(); break;
      case 'menu':
        if (confirm('게임을 중단하고 대기실로 돌아갈까요?\n(취소를 누르면 방 닫기를 선택할 수 있습니다)')) act('restart');
        else leave();
        break;
      case 'kick': if (confirm('이 플레이어를 내보낼까요?')) act('kick', { id: el.dataset.id }); break;
      case 'start': act('start'); break;
      case 'restart': act('restart'); break;
      case 'toggleRole': app.roleHidden = !app.roleHidden; render(); break;
      case 'pick': {
        const id = el.dataset.id;
        if (v.phase === 'propose') {
          const size = v.sizes[v.questIdx];
          if (app.selection.includes(id)) app.selection = app.selection.filter((x) => x !== id);
          else if (app.selection.length < size) app.selection.push(id);
          else toast(`원정대는 ${size}명입니다. 다른 사람을 먼저 빼주세요.`);
        } else {
          app.selection = app.selection[0] === id ? [] : [id];
        }
        render();
        break;
      }
      case 'propose': act('propose', { team: app.selection.slice() }); break;
      case 'vote': act('vote', { approve: el.dataset.v === '1' }); break;
      case 'quest': act('quest', { success: el.dataset.v === '1' }); break;
      case 'lady':
        if (confirm(`${v.players.find((p) => p.id === app.selection[0]).name} 님의 진영을 확인할까요?`)) act('lady', { target: app.selection[0] });
        break;
      case 'assassinate':
        if (confirm(`${v.players.find((p) => p.id === app.selection[0]).name} 님을 암살할까요? 되돌릴 수 없습니다.`)) act('assassinate', { target: app.selection[0] });
        break;
      default:
    }
  });

  document.addEventListener('change', (e) => {
    const el = e.target.closest('[data-setting]');
    if (el) act('setting', { key: el.dataset.setting, value: el.checked });
  });

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter' || app.mode !== 'home') return;
    if (e.target.id === 'codeInput') $('[data-act="join"]').click();
  });

  window.addEventListener('beforeunload', (e) => {
    if (app.mode === 'host' && app.view && app.view.phase !== 'lobby') { e.preventDefault(); e.returnValue = ''; }
  });

  render();
})();
