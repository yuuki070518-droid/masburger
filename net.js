/* MAS BURGER — 通信部分（Claude の外で動かすとき）
   ゲーム本体が使う「room」（presence のキー値同期）と同じ窓口を用意する。

   しくみ
   ・ブラウザ同士を WebRTC の DataChannel で直接つなぐ（P2P）。遅延は数十ms。
   ・相手探しと接続の手続き（シグナリング）だけ公開 MQTT ブローカーを使う。数通なので流量制限に当たらない。
   ・P2P が張れない相手がいるときだけ、MQTT で中継する（フォールバック）。公開ブローカーは
     1接続あたり約10通/秒を超えると黙って捨てるので、変化したキーを1通にまとめ、最大8通/秒に抑える。
     retain は使わない（再接続時に古い状態が「新着」として届き、巻き戻るため）。2秒ごとに全キーを送り直す。
   ・送るたびに通し番号を付け、受け手は古い番号を捨てる（経路が切り替わっても新旧が逆転しない）。
   ・裏タブでもタイマーが絞られないよう、Worker の時計で送信を回す（window.BUNBUN_TICK）。 */
(function () {
  'use strict';
  // 裏タブでも止まらない時計。Worker 内のタイマーはページの裏タブ制限を受けにくく、
  // Worker からのメッセージはタイマー扱いされないので、ここを起点にすれば送信や進行が1Hzに落ちない。
  const tickFns = [];
  window.BUNBUN_TICK = fn => tickFns.push(fn);
  (function startTicker() {
    const run = () => { for (const f of tickFns) { try { f(); } catch (e) { console.error(e); } } };
    try {
      const src = 'setInterval(()=>postMessage(0),25);';
      const w = new Worker(URL.createObjectURL(new Blob([src], { type: 'text/javascript' })));
      w.onmessage = run;
    } catch (e) { setInterval(run, 25); }
  })();

  if (window.claude && window.claude.use) return; // Claude の中で開いたときは Claude の room を使う

  const SAFE = window.BUNBUN_SAFE;
  const params = new URLSearchParams(location.search);
  let code = (params.get('room') || '').replace(/[^a-z0-9]/gi, '').slice(0, 32).toLowerCase();
  if (!code) { code = SAFE.randomCode(); params.set('room', code); history.replaceState(null, '', location.pathname + '?' + params.toString() + location.hash); }
  window.BUNBUN_ROOM = code;
  const BASE = 'masburger/v4/' + code + '/';
  // 自前のサーバー（GitHub Pages 以外の http/https）から配信されているときは、まずそのサーバーの /mqtt を試す
  const SELF = /^https?:$/.test(location.protocol) && !/github\.io$/.test(location.hostname) && !/^(localhost|127\.0\.0\.1)$/.test(location.hostname) || params.get('own') === '1';
  const OWN = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/mqtt';
  const PUBLIC = ['wss://broker.emqx.io:8084/mqtt', 'wss://broker.hivemq.com:8884/mqtt'];
  const BROKERS = window.BUNBUN_BROKERS || (SELF ? [OWN].concat(PUBLIC) : PUBLIC);
  const ICE = window.BUNBUN_ICE || [{ urls: 'stun:stun.l.google.com:19302' }, { urls: 'stun:stun.cloudflare.com:3478' }];
  const ME = SAFE.randomCode();
  const DBG = params.get('netdebug') === '1' ? (...a) => console.log('[net]', ...a) : () => { };
  const now = () => Date.now();
  const mono = () => performance.now(); // RTT は時計の補正の影響を受けない時計で測る

  // ---- presence の状態 ----
  const pres = SAFE.dict(), mine = SAFE.dict(), handlers = SAFE.dict(), cache = SAFE.dict();
  const dirtyDC = new Set(), dirtyMQ = new Set(); // "ns\nkey"
  const snap = ns => Object.entries(pres[ns] || SAFE.dict()).map(([peer, p]) => Object.freeze({ peer, by: null, isMe: peer === ME, sameTab: peer === ME, kind: 'viewer', guest: false, presence: p, updatedAt: now() }));
  function fire(ns, joined, left) { cache[ns] = snap(ns); (handlers[ns] || []).forEach(h => { try { h({ peers: cache[ns], joined: joined || [], left: left || [], updated: [] }); } catch (e) { console.error(e); } }); }
  function setP(ns, peer, p) {
    pres[ns] = pres[ns] || SAFE.dict(); const was = peer in pres[ns];
    if (p == null) { if (was) { const old = pres[ns][peer]; delete pres[ns][peer]; fire(ns, [], [{ peer, presence: old }]); } return; }
    pres[ns][peer] = Object.freeze(p); fire(ns, was ? [] : [{ peer, presence: p }]);
  }

  // ---- 相手ごとの状態 ----
  // peers[id] = {seen, lastS, data:{ns:{k:v}}, alive, pc, dc, dcOpen, tries, nextTry, rtt, dcLast, conn}
  const peers = SAFE.dict();
  let seq = 0;
  const P = id => peers[id] || (peers[id] = { seen: 0, lastS: -1, data: SAFE.dict(), alive: false, pc: null, dc: null, dcOpen: false, tries: 0, nextTry: 0, rtt: null, rttAt: 0, dcProbe: null, mqRtt: null, mqRttAt: 0, mqEcho: null, mqAnswered: null, dcLast: 0, conn: 0 });
  function markAlive(id) {
    const p = P(id); p.seen = now();
    if (!p.alive) { p.alive = true; Object.keys(p.data).forEach(ns => setP(ns, id, Object.assign(SAFE.dict(), p.data[ns]))); fullMQ = true; }
  }
  function dropPeer(id, why) {
    const p = peers[id]; if (!p) return;
    closePC(p); delete peers[id];
    Object.keys(pres).forEach(ns => setP(ns, id, null));
  }
  // 受信：{s, f(全キー), d:{ns:{k:v|null}}}。古い番号は捨てる
  function onData(id, m, via) {
    if (!SAFE.record(m) || !SAFE.bounded(m)) return;
    const p = P(id);
    if (m.pi != null) { if(Number.isFinite(m.pi)&&m.pi>=0){sendDC(p, { po: m.pi }); markAlive(id);} return; } // ping
    if (m.po != null) {
      if(!Number.isFinite(m.po)||m.po<0)return;
      if (m.po === p.dcProbe) { const r = mono() - m.po; p.rtt = p.rtt == null ? r : p.rtt * .7 + r * .3; p.rttAt = mono(); p.dcProbe = null; }
      markAlive(id); return;
    }
    // 中継の ping/pong は通常の状態配信に同梱し、追加の MQTT メッセージを増やさない。
    if (via === 'mq') {
      if (Number.isFinite(m.rp) && m.rp !== p.mqEcho) { p.mqEcho = m.rp; mqEchoDirty = true; }
      const echo = m.ra && m.ra[ME];
      if (echo === mqProbe && echo != null && echo !== p.mqAnswered) {
        const r = mono() - echo;
        if (r >= 0 && r < 5000) { p.mqRtt = p.mqRtt == null ? r : p.mqRtt * .7 + r * .3; p.mqRttAt = mono(); }
        p.mqAnswered = echo;
      }
    }
    if (m.bye) { dropPeer(id); return; }
    if (typeof m.s !== 'number') { markAlive(id); return; }
    if (!SAFE.int(m.s,0) || m.s <= p.lastS || !SAFE.record(m.d)) return;
    if(Object.entries(m.d).some(([ns,v])=>!['lobby','lobby/world'].includes(ns)||!SAFE.record(v)))return;
    p.lastS = m.s;
    const changed = new Set();
    if (m.f) { // 全キー：持っていないキーは消す
      Object.keys(p.data).forEach(ns => { if (!m.d || !(ns in m.d)) { p.data[ns] = SAFE.dict(); changed.add(ns); } });
    }
    for (const ns in (m.d || {})) {
      const cur = m.f ? SAFE.dict() : Object.assign(SAFE.dict(), p.data[ns] || {}), inc = m.d[ns];
      for (const k in inc) { if (inc[k] === null) delete cur[k]; else cur[k] = inc[k]; }
      if(Object.keys(cur).length>32||!SAFE.bounded(cur)||JSON.stringify(cur).length>131072)return;
      p.data[ns] = cur; changed.add(ns);
    }
    const wasAlive = p.alive; markAlive(id);
    if (wasAlive) changed.forEach(ns => setP(ns, id, Object.assign(SAFE.dict(), p.data[ns])));
  }

  // ---- 送信 ----
  function payload(keys, full) {
    const d = SAFE.dict();
    if (full) { for (const ns in mine) d[ns] = Object.assign(SAFE.dict(), mine[ns]); }
    else keys.forEach(id => { const [ns, k] = id.split('\n'); (d[ns] = d[ns] || {})[k] = mine[ns] && k in mine[ns] ? mine[ns][k] : null; });
    return { s: ++seq, f: full ? 1 : 0, d };
  }
  function sendDC(p, obj) { if (!p.dcOpen) return false; try { p.dc.send(typeof obj === 'string' ? obj : JSON.stringify(obj)); return true; } catch (e) { return false; } }
  let lastDC = 0, lastMQ = 0, lastFullMQ = 0, fullMQ = true, lastHB = 0, lastPing = 0;
  let mqProbe = null, mqEchoDirty = false;
  // P2P が「開いていて、しかも最近なにか届いている」ときだけ健全とみなす（open だけでは詰まりを見抜けない）
  const dcOk = p => p.dcOpen && now() - p.dcLast < 3500;
  // MQTT で届ける必要がある相手（P2P が健全でない生きている相手）がいるか
  const needMQ = () => Object.values(peers).some(p => p.alive && !dcOk(p));
  function pumpDC(force) {
    const t = now();
    if (!dirtyDC.size || (!force && t - lastDC < 15)) return;
    const open = Object.values(peers).filter(p => p.dcOpen);
    if (open.some(p => p.dc.bufferedAmount > 256 * 1024) && !force) return; // 詰まっているときは待つ（その間の変化は最新値にまとまる）
    const str = JSON.stringify(payload([...dirtyDC], false)); dirtyDC.clear(); lastDC = t;
    open.forEach(p => sendDC(p, str));
  }
  function pumpMQ() {
    if (!client || !connected) return;
    const t = now();
    if (t - lastHB > 2500) { lastHB = t; pub('hb/' + ME, String(t), true); }
    if (!needMQ()) { dirtyMQ.clear(); fullMQ = true; return; }
    if (t - lastMQ < 130) return; // 最大 約8通/秒
    const full = fullMQ || t - lastFullMQ > 2000;
    const probeDue = mqProbe == null || mono() - mqProbe >= 2000;
    if (!full && !dirtyMQ.size && !probeDue && !mqEchoDirty) return;
    const m = payload([...dirtyMQ], full); dirtyMQ.clear(); lastMQ = t;
    if (probeDue) mqProbe = mono();
    m.rp = mqProbe;
    m.ra = Object.fromEntries(Object.entries(peers).filter(([, p]) => p.alive && !dcOk(p) && p.mqEcho != null).map(([id, p]) => [id, p.mqEcho]));
    mqEchoDirty = false;
    if (full) { fullMQ = false; lastFullMQ = t; }
    pub('p/' + ME, JSON.stringify(m), false);
  }
  function tick() {
    const t = now();
    pumpDC(false); pumpMQ();
    if (t - lastPing > 1000) { lastPing = t; Object.values(peers).forEach(p => { p.dcProbe = mono(); sendDC(p, { pi: p.dcProbe }); }); }
    for (const id in peers) {
      const p = peers[id];
      if (p.alive && t - p.seen > 10000) { dropPeer(id, 'timeout'); continue; }
      const ok = dcOk(p);
      if (p.wasOk && !ok) fullMQ = true; // P2P が止まった → MQTT で全キーを送り直す
      p.wasOk = ok;
      maybeConnect(id, p, t);
    }
  }
  window.BUNBUN_TICK(tick);

  // ---- MQTT（相手探し・シグナリング・中継） ----
  let client = null, connected = false, stopped = false;
  function pub(sub, str, retain) { try { client.publish(BASE + sub, str, { qos: 0, retain: !!retain }); } catch (e) { } }
  function onMsg(topic, buf) { try { onMsg2(topic, buf); } catch (e) { console.error(e); } } // ここで例外を出すと MQTT の受信が止まる
  function onMsg2(topic, buf) {
    const rest = topic.slice(BASE.length), [kind, id] = rest.split('/');
    const s = buf.toString();
    if (s.length > 131072 || !topic.startsWith(BASE)) return;
    if (id && (!/^[a-z0-9]{6,32}$/.test(id) || !SAFE.own(peers,id) && Object.keys(peers).length >= 32)) return;
    // シグナリングは「宛先＝自分」のトピックに届く
    if (kind === 's') { if (id === ME) { let m; try { m = JSON.parse(s); } catch (e) { return; } onSignal(m); } return; }
    if (!id || id === ME) return;
    if (kind === 'hb') {
      if (!s) { if (peers[id] && !peers[id].dcOpen) dropPeer(id); return; }
      if (now() - (+s || 0) > 15000) return; // 古い retained
      const fresh = !peers[id] || !peers[id].alive;
      markAlive(id);
      if (fresh) { lastHB = 0; fullMQ = true; } // 新しい人には早めに自分を知らせる
      return;
    }
    if (kind === 'p') {
      const p = P(id);
      if (dcOk(p)) { p.seen = now(); return; } // P2P で受けている相手の中継は読まない（番号で新旧は判定済み）
      let m; try { m = JSON.parse(s); } catch (e) { return; }
      onData(id, m, 'mq');
    }
  }
  function connect(i) {
    if (stopped) return;
    if (typeof mqtt === 'undefined') { console.warn('通信ライブラリを読み込めませんでした'); return; }
    const url = BROKERS[i % BROKERS.length]; let ok = false;
    const c = mqtt.connect(url, {
      clientId: 'mb_' + ME + '_' + i, keepalive: 20, connectTimeout: 6000, reconnectPeriod: 2000, clean: true,
      will: { topic: BASE + 'hb/' + ME, payload: '', qos: 0, retain: true }
    });
    client = c;
    c.on('connect', () => {
      ok = true; connected = true;
      c.subscribe([BASE + 'hb/+', BASE + 'p/+', BASE + 's/' + ME], { qos: 0 });
      lastHB = 0; fullMQ = true; tick();
      window.BUNBUN_NET = url === OWN ? '自前サーバー' : '公開サーバー';
    });
    c.on('message', onMsg);
    c.on('close', () => { connected = false; });
    setTimeout(() => { if (!ok && !stopped) { try { c.end(true); } catch (e) { } connect(i + 1); } }, url === OWN ? 3500 : 8000);
  }

  // ---- WebRTC（P2P）----
  // id が小さい方が申し込む（同時に申し込み合うのを防ぐ）。ICE は全部集めてから1通で送る（シグナリングを2通に抑える）
  function maybeConnect(id, p, t) {
    if (typeof RTCPeerConnection === 'undefined' || !p.alive || p.dcOpen || ME > id) return;
    if (p.pc && t - p.conn < 12000) return; // 接続中
    if (t < p.nextTry || p.tries >= 4) return;
    p.tries++; p.nextTry = t + 4000 * p.tries;
    startPC(id, true);
  }
  function closePC(p) { if (p.pc) { try { p.pc.close(); } catch (e) { } } p.pc = null; p.dc = null; if (p.dcOpen) { p.dcOpen = false; fullMQ = true; } }
  function gathered(pc) {
    return new Promise(res => {
      if (pc.iceGatheringState === 'complete') return res();
      const done = () => { if (pc.iceGatheringState === 'complete') res(); };
      pc.addEventListener('icegatheringstatechange', done);
      setTimeout(res, 2500); // 集め終わらなくても、それまでの候補で送る
    });
  }
  function startPC(id, offerer, offer) {
    const p = P(id); closePC(p);
    const pc = new RTCPeerConnection({ iceServers: ICE });
    DBG('startPC', id, offerer ? 'offer' : 'answer');
    pc.oniceconnectionstatechange = () => DBG('ice', id, pc.iceConnectionState);
    const connId = offerer ? SAFE.randomCode() : offer.c;
    p.pc = pc; p.conn = now(); p.connId = connId;
    const dc = pc.createDataChannel('mb', { negotiated: true, id: 0, ordered: true });
    p.dc = dc;
    dc.onopen = () => {
      DBG('dc open', id);
      if (p.dc !== dc) return;
      p.dcOpen = true; p.tries = 0; p.dcLast = now();
      sendDC(p, payload([], true)); // 開いたらまず全キーを送る
    };
    dc.onmessage = e => { if(typeof e.data!=='string'||e.data.length>131072)return; p.dcLast = now(); let m; try { m = JSON.parse(e.data); } catch (_) { return; } onData(id, m, 'dc'); };
    dc.onclose = () => { if (p.dc === dc) { p.dcOpen = false; fullMQ = true; p.nextTry = now() + 1500; } };
    pc.onconnectionstatechange = () => {
      if (p.pc !== pc) return;
      if (pc.connectionState === 'failed' || pc.connectionState === 'closed') { p.dcOpen = false; fullMQ = true; p.nextTry = now() + 3000; }
    };
    (async () => {
      try {
        if (offerer) {
          await pc.setLocalDescription(await pc.createOffer());
          await gathered(pc);
          if (p.pc !== pc) return;
          pub('s/' + id, JSON.stringify({ t: 'offer', from: ME, c: connId, sdp: pc.localDescription.sdp }), false);
        } else {
          await pc.setRemoteDescription({ type: 'offer', sdp: offer.sdp });
          await pc.setLocalDescription(await pc.createAnswer());
          await gathered(pc);
          if (p.pc !== pc) return;
          pub('s/' + id, JSON.stringify({ t: 'answer', from: ME, c: connId, sdp: pc.localDescription.sdp }), false);
        }
      } catch (e) { console.warn('P2P 接続の準備に失敗', e); }
    })();
  }
  function onSignal(m) {
    if (!SAFE.record(m) || !SAFE.bounded(m) || typeof m.from !== 'string' || !/^[a-z0-9]{6,32}$/.test(m.from) || m.from === ME || !SAFE.own(peers,m.from)&&Object.keys(peers).length>=32) return;
    DBG('signal', m.t, 'from', m.from, (m.sdp || '').split('a=candidate').length - 1, 'candidates');
    const p = P(m.from); markAlive(m.from);
    if (m.t === 'offer') { if (typeof RTCPeerConnection !== 'undefined') startPC(m.from, false, m); }
    else if (m.t === 'answer' && p.pc && p.connId === m.c && p.pc.signalingState === 'have-local-offer') {
      p.pc.setRemoteDescription({ type: 'answer', sdp: m.sdp }).catch(e => console.warn('P2P 応答の適用に失敗', e));
    }
  }

  window.addEventListener('pagehide', () => {
    stopped = true;
    Object.values(peers).forEach(p => sendDC(p, { bye: 1 }));
    try { pub('hb/' + ME, '', true); client.end(); } catch (e) { }
  });

  // ---- ゲームから見える窓口 ----
  function api(ns) {
    if(typeof ns!=='string'||!/^[-a-z0-9_/]{1,80}$/i.test(ns))throw new Error('不正なnamespace');
    pres[ns] = pres[ns] || SAFE.dict();
    return {
      presence(patch) {
        if(!SAFE.record(patch)||!SAFE.bounded(patch))return Promise.reject(new Error('presenceの形式が不正です'));
        if(JSON.stringify(patch).length>131072)return Promise.reject(new Error('presenceが大きすぎます'));
        mine[ns] = Object.assign(SAFE.dict(), mine[ns] || {});
        for (const k in patch) { if (patch[k] === null) delete mine[ns][k]; else mine[ns][k] = patch[k]; dirtyDC.add(ns + '\n' + k); dirtyMQ.add(ns + '\n' + k); }
        setP(ns, ME, Object.assign(SAFE.dict(), mine[ns])); return Promise.resolve();
      },
      peers() { return cache[ns] || (cache[ns] = snap(ns)); },
      onPeers(h) { (handlers[ns] = handlers[ns] || []).push(h); setTimeout(() => h({ peers: snap(ns), joined: snap(ns), left: [], updated: [] }), 0); return () => { handlers[ns] = handlers[ns].filter(x => x !== h); }; },
      join(name) { return Promise.resolve(api(ns + '/' + name)); },
      connected() { return connected || Object.values(peers).some(p => p.dcOpen); },
      flush() { pumpDC(true); },
      // 相手ごとの経路と往復時間（表示用）
      link(id) {
        const p = peers[id]; if (!p) return null;
        const p2p = dcOk(p), rtt = p2p ? p.rtt : p.mqRtt, at = p2p ? p.rttAt : p.mqRttAt;
        return { p2p, rtt: rtt != null && mono() - at < 5000 ? Math.round(rtt) : null };
      },
      emit() { return Promise.resolve(); }, on() { return () => { }; }
    };
  }
  const lobby = api('lobby'); setP('lobby', ME, {});
  window.claude = { use: n => Promise.resolve(n === 'room' ? lobby : null) };
  connect(0);
})();
