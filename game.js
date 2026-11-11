/* 아발론 게임 규칙 엔진 (호스트 브라우저에서 실행) */
(function (root) {
  'use strict';

  const MIN_PLAYERS = 5;
  const MAX_PLAYERS = 10;

  // 인원별 선/악 인원
  const COMPOSITION = {
    5: { good: 3, evil: 2 },
    6: { good: 4, evil: 2 },
    7: { good: 4, evil: 3 },
    8: { good: 5, evil: 3 },
    9: { good: 6, evil: 3 },
    10: { good: 6, evil: 4 },
  };

  // 인원별 퀘스트 원정대 인원
  const QUEST_SIZES = {
    5: [2, 3, 2, 3, 3],
    6: [2, 3, 4, 3, 4],
    7: [2, 3, 3, 4, 4],
    8: [3, 4, 4, 5, 5],
    9: [3, 4, 4, 5, 5],
    10: [3, 4, 4, 5, 5],
  };

  const ROLES = {
    merlin: { name: '멀린', team: 'good', desc: '악의 세력이 누구인지 알고 있습니다(모드레드 제외). 게임 끝에 암살자에게 정체가 들키면 악이 승리하니 조심스럽게 선을 이끄세요.' },
    percival: { name: '퍼시발', team: 'good', desc: '멀린이 누구인지 압니다. 단, 모르가나도 멀린처럼 보이므로 둘 중 진짜를 가려내야 합니다.' },
    servant: { name: '아서의 충신', team: 'good', desc: '특별한 정보는 없습니다. 투표와 퀘스트 결과로 악의 세력을 추리하세요.' },
    assassin: { name: '암살자', team: 'evil', desc: '선이 퀘스트 3개를 성공하면, 멀린으로 의심되는 사람을 지목해 암살합니다. 맞히면 악의 역전승!' },
    morgana: { name: '모르가나', team: 'evil', desc: '퍼시발에게 멀린처럼 보입니다. 멀린 행세로 퍼시발을 혼란에 빠뜨리세요.' },
    mordred: { name: '모드레드', team: 'evil', desc: '멀린에게도 정체가 보이지 않는 숨은 악의 우두머리입니다.' },
    oberon: { name: '오베론', team: 'evil', desc: '악의 세력이지만 다른 악을 모르고, 다른 악도 당신을 모릅니다.' },
    minion: { name: '모드레드의 하수인', team: 'evil', desc: '다른 악의 세력을 알고 있습니다(오베론 제외). 정체를 숨기고 퀘스트를 실패시키세요.' },
  };

  const OPTIONAL_GOOD = ['percival'];
  const OPTIONAL_EVIL = ['morgana', 'mordred', 'oberon'];

  function failsNeeded(n, questIdx) {
    return questIdx === 3 && n >= 7 ? 2 : 1;
  }

  function shuffle(arr) {
    const a = arr.slice();
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
  }

  function cleanName(name) {
    return String(name || '').replace(/\s+/g, ' ').trim().slice(0, 12);
  }

  function createRoom(code, hostId, hostName) {
    return {
      code,
      hostId,
      phase: 'lobby',
      players: [{ id: hostId, name: cleanName(hostName) || '호스트', connected: true }],
      settings: { percival: true, morgana: true, mordred: false, oberon: false, lady: false },
      aliases: {},
      roles: {},
      leaderIdx: 0,
      questIdx: 0,
      rejectCount: 0,
      team: [],
      votes: {},
      questCards: {},
      results: [],
      log: [],
      lastVote: null,
      lady: null,
      winner: null,
      reason: '',
      assassinTarget: null,
      gameNo: 0,
    };
  }

  function buildRoles(n, settings) {
    const c = COMPOSITION[n];
    if (!c) return { error: `아발론은 최소 ${MIN_PLAYERS}명, 최대 ${MAX_PLAYERS}명이 필요합니다.` };
    const good = ['merlin'];
    OPTIONAL_GOOD.forEach((r) => settings[r] && good.push(r));
    const evil = ['assassin'];
    OPTIONAL_EVIL.forEach((r) => settings[r] && evil.push(r));
    if (evil.length > c.evil) {
      return { error: `${n}명 게임의 악 진영은 ${c.evil}명이라, 선택한 악 역할(암살자 포함 ${evil.length}개)을 모두 넣을 수 없습니다. 특수 악 역할을 줄여주세요.` };
    }
    while (good.length < c.good) good.push('servant');
    while (evil.length < c.evil) evil.push('minion');
    return { good, evil, roles: good.concat(evil) };
  }

  function resolveId(s, id) {
    return (s.aliases && s.aliases[id]) || id;
  }

  // 참가 처리. 성공 시 {pid}, 실패 시 {error}
  function join(s, rawId, rawName) {
    if (!rawId) return { error: '잘못된 접속 정보입니다.' };
    const id = resolveId(s, rawId);
    const name = cleanName(rawName) || '플레이어';
    const existing = s.players.find((p) => p.id === id);
    if (existing) {
      existing.connected = true;
      return { pid: id };
    }
    if (s.phase !== 'lobby') {
      const ghost = s.players.find((p) => !p.connected && p.name === name && p.id !== s.hostId);
      if (ghost) {
        s.aliases[rawId] = ghost.id;
        ghost.connected = true;
        return { pid: ghost.id };
      }
      return { error: '이미 게임이 진행 중입니다. (연결이 끊긴 플레이어는 같은 닉네임으로 다시 들어올 수 있습니다)' };
    }
    if (s.players.length >= MAX_PLAYERS) return { error: `방이 가득 찼습니다. (최대 ${MAX_PLAYERS}명)` };
    let finalName = name;
    let k = 2;
    while (s.players.some((p) => p.name === finalName)) finalName = `${name}${k++}`;
    s.players.push({ id, name: finalName, connected: true });
    return { pid: id };
  }

  function disconnect(s, pid) {
    const i = s.players.findIndex((p) => p.id === pid);
    if (i < 0) return;
    if (s.phase === 'lobby' && pid !== s.hostId) s.players.splice(i, 1);
    else s.players[i].connected = false;
  }

  function leaderId(s) {
    const p = s.players[s.leaderIdx];
    return p ? p.id : null;
  }

  function nextLeader(s) {
    s.leaderIdx = (s.leaderIdx + 1) % s.players.length;
  }

  function nameOf(s, id) {
    const p = s.players.find((x) => x.id === id);
    return p ? p.name : '?';
  }

  function end(s, winner, reason) {
    s.phase = 'end';
    s.winner = winner;
    s.reason = reason;
    s.team = [];
    return null;
  }

  function start(s) {
    if (s.phase !== 'lobby') return '이미 게임이 시작되었습니다.';
    const n = s.players.length;
    if (n < MIN_PLAYERS) return `최소 ${MIN_PLAYERS}명이 필요합니다. (현재 ${n}명)`;
    if (n > MAX_PLAYERS) return `최대 ${MAX_PLAYERS}명까지 플레이할 수 있습니다.`;
    const b = buildRoles(n, s.settings);
    if (b.error) return b.error;
    const roles = shuffle(b.roles);
    s.roles = {};
    s.players.forEach((p, i) => { s.roles[p.id] = roles[i]; });
    s.leaderIdx = Math.floor(Math.random() * n);
    s.questIdx = 0;
    s.rejectCount = 0;
    s.team = [];
    s.votes = {};
    s.questCards = {};
    s.results = [];
    s.log = [];
    s.lastVote = null;
    s.winner = null;
    s.reason = '';
    s.assassinTarget = null;
    s.gameNo += 1;
    if (s.settings.lady && n >= 7) {
      const holder = s.players[(s.leaderIdx - 1 + n) % n].id;
      s.lady = { holder, used: [holder], checks: {} };
    } else {
      s.lady = null;
    }
    s.phase = 'propose';
    return null;
  }

  function toLobby(s) {
    s.players = s.players.filter((p) => p.connected || p.id === s.hostId);
    s.phase = 'lobby';
    s.roles = {};
    s.team = [];
    s.votes = {};
    s.questCards = {};
    s.results = [];
    s.log = [];
    s.lastVote = null;
    s.lady = null;
    s.winner = null;
    s.reason = '';
    s.assassinTarget = null;
    s.rejectCount = 0;
    s.questIdx = 0;
    s.aliases = {};
    return null;
  }

  function resolveVote(s) {
    const n = s.players.length;
    const approvals = Object.values(s.votes).filter(Boolean).length;
    const approved = approvals > n / 2;
    const rec = {
      t: 'vote', q: s.questIdx + 1, attempt: s.rejectCount + 1,
      leader: leaderId(s), team: s.team.slice(), votes: Object.assign({}, s.votes), approved,
    };
    s.log.push(rec);
    s.lastVote = rec;
    s.votes = {};
    if (approved) {
      s.rejectCount = 0;
      s.questCards = {};
      s.phase = 'quest';
      return null;
    }
    s.rejectCount += 1;
    if (s.rejectCount >= 5) return end(s, 'evil', '원정대 제안이 5번 연속 부결되어 악의 세력이 승리했습니다.');
    nextLeader(s);
    s.team = [];
    s.phase = 'propose';
    return null;
  }

  function resolveQuest(s) {
    const n = s.players.length;
    const fails = Object.values(s.questCards).filter((v) => !v).length;
    const need = failsNeeded(n, s.questIdx);
    const success = fails < need;
    const rec = { t: 'quest', q: s.questIdx + 1, team: s.team.slice(), fails, need, success };
    s.results.push(rec);
    s.log.push(rec);
    s.questCards = {};
    const wins = s.results.filter((r) => r.success).length;
    const losses = s.results.length - wins;
    if (losses >= 3) return end(s, 'evil', '퀘스트 3개가 실패하여 악의 세력이 승리했습니다.');
    if (wins >= 3) {
      s.phase = 'assassin';
      s.team = [];
      return null;
    }
    s.questIdx += 1;
    nextLeader(s);
    s.team = [];
    s.phase = s.lady && s.results.length >= 2 ? 'lady' : 'propose';
    return null;
  }

  function applyAction(s, pid, type, p) {
    p = p || {};
    const n = s.players.length;
    const isHost = pid === s.hostId;
    if (!s.players.some((x) => x.id === pid)) return '참가자가 아닙니다.';
    const isPlayer = (id) => s.players.some((x) => x.id === id);

    switch (type) {
      case 'setting': {
        if (!isHost) return '호스트만 설정을 바꿀 수 있습니다.';
        if (s.phase !== 'lobby') return '게임 중에는 설정을 바꿀 수 없습니다.';
        if (!(p.key in s.settings)) return '알 수 없는 설정입니다.';
        s.settings[p.key] = !!p.value;
        return null;
      }
      case 'kick': {
        if (!isHost) return '호스트만 내보낼 수 있습니다.';
        if (s.phase !== 'lobby') return '대기실에서만 내보낼 수 있습니다.';
        if (p.id === s.hostId) return '호스트는 내보낼 수 없습니다.';
        const i = s.players.findIndex((x) => x.id === p.id);
        if (i < 0) return '없는 플레이어입니다.';
        s.players.splice(i, 1);
        return null;
      }
      case 'start':
        if (!isHost) return '호스트만 시작할 수 있습니다.';
        return start(s);
      case 'restart':
        if (!isHost) return '호스트만 할 수 있습니다.';
        return toLobby(s);
      case 'propose': {
        if (s.phase !== 'propose') return '지금은 원정대를 제안할 수 없습니다.';
        if (leaderId(s) !== pid) return '대표만 원정대를 제안할 수 있습니다.';
        const size = QUEST_SIZES[n][s.questIdx];
        const team = Array.from(new Set(Array.isArray(p.team) ? p.team : []));
        if (team.length !== size) return `원정대원 ${size}명을 선택해야 합니다.`;
        if (!team.every(isPlayer)) return '잘못된 선택입니다.';
        s.team = team;
        s.votes = {};
        s.phase = 'vote';
        return null;
      }
      case 'vote': {
        if (s.phase !== 'vote') return '지금은 투표 시간이 아닙니다.';
        if (pid in s.votes) return '이미 투표했습니다.';
        s.votes[pid] = !!p.approve;
        if (Object.keys(s.votes).length >= n) return resolveVote(s);
        return null;
      }
      case 'quest': {
        if (s.phase !== 'quest') return '지금은 퀘스트 진행 중이 아닙니다.';
        if (!s.team.includes(pid)) return '원정대원만 카드를 낼 수 있습니다.';
        if (pid in s.questCards) return '이미 카드를 냈습니다.';
        const success = !!p.success;
        if (!success && ROLES[s.roles[pid]].team === 'good') return '선의 세력은 성공 카드만 낼 수 있습니다.';
        s.questCards[pid] = success;
        if (Object.keys(s.questCards).length >= s.team.length) return resolveQuest(s);
        return null;
      }
      case 'lady': {
        if (s.phase !== 'lady' || !s.lady) return '지금은 호수의 여인을 사용할 수 없습니다.';
        if (s.lady.holder !== pid) return '호수의 여인을 가진 사람만 사용할 수 있습니다.';
        const t = p.target;
        if (!isPlayer(t) || t === pid) return '확인할 사람을 골라주세요.';
        if (s.lady.used.includes(t)) return '이미 호수의 여인을 가졌던 사람은 확인할 수 없습니다.';
        (s.lady.checks[pid] = s.lady.checks[pid] || []).push({ target: t, team: ROLES[s.roles[t]].team });
        s.log.push({ t: 'lady', holder: pid, target: t });
        s.lady.used.push(t);
        s.lady.holder = t;
        s.phase = 'propose';
        return null;
      }
      case 'assassinate': {
        if (s.phase !== 'assassin') return '지금은 암살 단계가 아닙니다.';
        if (s.roles[pid] !== 'assassin') return '암살자만 지목할 수 있습니다.';
        const t = p.target;
        if (!isPlayer(t) || t === pid) return '암살할 사람을 골라주세요.';
        s.assassinTarget = t;
        if (s.roles[t] === 'merlin') return end(s, 'evil', `암살자가 멀린(${nameOf(s, t)})을 찾아내 악의 세력이 역전승했습니다!`);
        return end(s, 'good', `암살자가 ${nameOf(s, t)}을(를) 지목했지만 멀린이 아니었습니다. 선의 세력 승리!`);
      }
      default:
        return '알 수 없는 행동입니다.';
    }
  }

  function knowledgeFor(s, pid) {
    const role = s.roles[pid];
    const out = [];
    if (!role) return out;
    const others = s.players.filter((p) => p.id !== pid);
    if (role === 'merlin') {
      others.forEach((p) => {
        const r = s.roles[p.id];
        if (ROLES[r].team === 'evil' && r !== 'mordred') out.push({ id: p.id, label: '악의 세력' });
      });
    } else if (role === 'percival') {
      const hasMorgana = Object.values(s.roles).includes('morgana');
      others.forEach((p) => {
        const r = s.roles[p.id];
        if (r === 'merlin' || r === 'morgana') out.push({ id: p.id, label: hasMorgana ? '멀린 또는 모르가나' : '멀린' });
      });
    } else if (ROLES[role].team === 'evil' && role !== 'oberon') {
      others.forEach((p) => {
        const r = s.roles[p.id];
        if (ROLES[r].team === 'evil' && r !== 'oberon') out.push({ id: p.id, label: '악의 동료' });
      });
    }
    return out;
  }

  // 각 플레이어에게 보낼 화면 정보 (비밀 정보는 본인 것만)
  function viewFor(s, pid) {
    const n = s.players.length;
    const inGame = s.phase !== 'lobby';
    const v = {
      code: s.code,
      hostId: s.hostId,
      me: pid,
      isHost: pid === s.hostId,
      phase: s.phase,
      gameNo: s.gameNo,
      settings: Object.assign({}, s.settings),
      players: s.players.map((p) => ({ id: p.id, name: p.name, connected: p.connected })),
      n,
      leaderId: inGame ? leaderId(s) : null,
      questIdx: s.questIdx,
      rejectCount: s.rejectCount,
      team: s.team.slice(),
      votedIds: Object.keys(s.votes),
      myVote: pid in s.votes ? s.votes[pid] : null,
      questDone: Object.keys(s.questCards),
      results: s.results,
      log: s.log,
      lastVote: s.lastVote,
      lady: s.lady ? { holder: s.lady.holder, used: s.lady.used.slice() } : null,
      winner: s.winner,
      reason: s.reason,
      assassinTarget: s.assassinTarget,
      sizes: QUEST_SIZES[n] || null,
    };
    if (inGame && s.roles[pid]) {
      v.myRole = s.roles[pid];
      v.knowledge = knowledgeFor(s, pid);
      v.ladyChecks = s.lady ? (s.lady.checks[pid] || []) : [];
      v.rolesInGame = Object.values(s.roles).slice().sort();
    }
    if (s.phase === 'end') v.allRoles = Object.assign({}, s.roles);
    return JSON.parse(JSON.stringify(v));
  }

  const api = {
    MIN_PLAYERS, MAX_PLAYERS, COMPOSITION, QUEST_SIZES, ROLES,
    failsNeeded, buildRoles, createRoom, join, disconnect, applyAction, viewFor, resolveId,
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.Avalon = api;
})(this);
