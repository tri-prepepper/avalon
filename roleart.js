/* 역할 카드 일러스트 (직접 그린 SVG) */
(function (root) {
  'use strict';

  function star(cx, cy, r, fill) {
    const pts = [];
    for (let i = 0; i < 10; i++) {
      const a = (Math.PI / 5) * i - Math.PI / 2;
      const rr = i % 2 ? r * 0.45 : r;
      pts.push(`${(cx + rr * Math.cos(a)).toFixed(1)},${(cy + rr * Math.sin(a)).toFixed(1)}`);
    }
    return `<polygon points="${pts.join(' ')}" fill="${fill}"/>`;
  }

  const SHIELD = 'M60 30 C70 36 82 38 90 38 L90 68 C90 92 76 106 60 114 C44 106 30 92 30 68 L30 38 C38 38 50 36 60 30 Z';

  const ART = {
    merlin: {
      bg: ['#1f3f86', '#0e1838'], glow: '#7fb4ff',
      emblem: `
        <path d="M60 24 L84 94 L36 94 Z" fill="#3156b8" stroke="#d9b45a" stroke-width="2" stroke-linejoin="round"/>
        <path d="M60 24 C66 40 70 52 74 66" stroke="#5f86e0" stroke-width="3" fill="none" opacity=".6"/>
        <ellipse cx="60" cy="95" rx="36" ry="8" fill="#284a9e" stroke="#d9b45a" stroke-width="2"/>
        ${star(57, 58, 6, '#ffe39a')}${star(68, 78, 4, '#ffe39a')}${star(50, 80, 3.5, '#ffe39a')}
        <path d="M86 40 a10 10 0 1 0 6 18 a8 8 0 1 1 -6 -18 Z" fill="#ffe39a"/>
        ${star(30, 44, 2.5, '#cfe0ff')}${star(92, 70, 2, '#cfe0ff')}${star(26, 70, 2, '#cfe0ff')}`,
    },
    percival: {
      bg: ['#1d4f7a', '#0d1a34'], glow: '#8fd0ff',
      emblem: `
        <path d="${SHIELD}" fill="#2a6aa8" stroke="#d9b45a" stroke-width="3"/>
        <path d="M60 36 L60 108" stroke="#d9b45a" stroke-width="2" opacity=".35"/>
        <path d="M38 70 Q60 50 82 70 Q60 90 38 70 Z" fill="#eaf4ff" stroke="#0d1a34" stroke-width="2"/>
        <circle cx="60" cy="70" r="9" fill="#3f8de0"/><circle cx="60" cy="70" r="4.5" fill="#0d1a34"/>
        <circle cx="57" cy="67" r="2" fill="#fff"/>
        ${star(60, 42, 4, '#ffe39a')}`,
    },
    servant: {
      bg: ['#24527a', '#101a33'], glow: '#9fd3ff',
      emblem: `
        <path d="${SHIELD}" fill="#2b5f93" stroke="#d9b45a" stroke-width="3"/>
        <rect x="55" y="40" width="10" height="64" rx="2" fill="#d9b45a"/>
        <rect x="38" y="58" width="44" height="10" rx="2" fill="#d9b45a"/>
        <circle cx="60" cy="63" r="6" fill="#ffe39a" stroke="#8a6a1e" stroke-width="1.5"/>`,
    },
    assassin: {
      bg: ['#5a1820', '#170a12'], glow: '#ff6b6b',
      emblem: `
        <path d="M32 100 C30 60 44 34 60 28 C76 34 90 60 88 100 C78 92 70 90 60 90 C50 90 42 92 32 100 Z" fill="#2a1016" stroke="#7a2a33" stroke-width="2"/>
        <path d="M46 64 Q53 58 58 64" stroke="#ff5a5a" stroke-width="3" fill="none" stroke-linecap="round"/>
        <path d="M62 64 Q67 58 74 64" stroke="#ff5a5a" stroke-width="3" fill="none" stroke-linecap="round"/>
        <g transform="rotate(-35 60 80)">
          <path d="M60 46 L66 84 L54 84 Z" fill="#dfe6ee" stroke="#8b96a3" stroke-width="1.5"/>
          <path d="M60 50 L60 82" stroke="#9aa6b3" stroke-width="1.2"/>
          <rect x="46" y="84" width="28" height="6" rx="2" fill="#d9b45a"/>
          <rect x="56.5" y="90" width="7" height="18" rx="2" fill="#4a2a18"/>
          <circle cx="60" cy="111" r="4" fill="#d9b45a"/>
        </g>`,
    },
    morgana: {
      bg: ['#4b1a5e', '#140a1f'], glow: '#d68bff',
      emblem: `
        <path d="M74 34 A38 38 0 1 0 74 108 A30 30 0 1 1 74 34 Z" fill="#c9a6ff" stroke="#d9b45a" stroke-width="2"/>
        <path d="M58 44 L62 54 L70 46 L72 58 L80 52 L78 64 L56 64 Z" fill="#d9b45a" transform="translate(4 -14)"/>
        ${star(84, 60, 4, '#f3d6ff')}${star(90, 80, 3, '#f3d6ff')}${star(80, 96, 2.5, '#f3d6ff')}
        <circle cx="48" cy="72" r="3" fill="#ff5a8a"/>`,
    },
    mordred: {
      bg: ['#3a0f14', '#0c0608'], glow: '#ff3b3b',
      emblem: `
        <path d="M30 92 L34 50 L46 66 L53 40 L60 60 L67 40 L74 66 L86 50 L90 92 Z" fill="#2b2b33" stroke="#d9b45a" stroke-width="2.5" stroke-linejoin="round"/>
        <rect x="30" y="92" width="60" height="12" rx="2" fill="#1c1c22" stroke="#d9b45a" stroke-width="2"/>
        <path d="M60 70 L67 80 L60 90 L53 80 Z" fill="#e0262e" stroke="#ff9a9a" stroke-width="1"/>
        <circle cx="42" cy="98" r="2.5" fill="#e0262e"/><circle cx="78" cy="98" r="2.5" fill="#e0262e"/>
        <circle cx="60" cy="98" r="2.5" fill="#e0262e"/>`,
    },
    oberon: {
      bg: ['#16424a', '#071417'], glow: '#5fe0d0',
      emblem: `
        <path d="M30 56 C30 40 46 32 60 32 C74 32 90 40 90 56 C90 84 76 106 60 110 C44 106 30 84 30 56 Z" fill="#cfd9d6" stroke="#d9b45a" stroke-width="2"/>
        <path d="M60 32 C74 32 90 40 90 56 C90 84 76 106 60 110 Z" fill="#1f3a3e"/>
        <path d="M40 62 Q47 68 54 62" stroke="#1f3a3e" stroke-width="3" fill="none" stroke-linecap="round"/>
        <path d="M66 62 Q73 56 80 62 Q73 68 66 62 Z" fill="#5fe0d0"/>
        <path d="M48 88 Q54 92 60 88" stroke="#1f3a3e" stroke-width="2.5" fill="none" stroke-linecap="round"/>
        <text x="60" y="30" text-anchor="middle" font-size="16" font-weight="800" fill="#5fe0d0">?</text>`,
    },
    minion: {
      bg: ['#4a1a1a', '#120808'], glow: '#ff7a5a',
      emblem: `
        <path d="M34 98 L34 60 C34 40 46 30 60 30 C74 30 86 40 86 60 L86 98 Z" fill="#3a3a44" stroke="#d9b45a" stroke-width="2.5"/>
        <path d="M60 30 L60 98" stroke="#55555f" stroke-width="3"/>
        <rect x="40" y="58" width="16" height="5" rx="2" fill="#ff5a3a"/>
        <rect x="64" y="58" width="16" height="5" rx="2" fill="#ff5a3a"/>
        <path d="M44 74 L76 74 M44 82 L76 82" stroke="#1c1c22" stroke-width="3"/>
        <path d="M60 30 C66 22 76 20 84 24 C76 26 70 30 66 36" fill="#b0272b"/>`,
    },
  };

  function roleArt(role, name) {
    const a = ART[role];
    if (!a) return '';
    const id = `ra-${role}`;
    return `<svg class="roleart" viewBox="0 0 120 170" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="${name}">
      <defs>
        <linearGradient id="${id}-bg" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${a.bg[0]}"/><stop offset="1" stop-color="${a.bg[1]}"/></linearGradient>
        <radialGradient id="${id}-gl"><stop offset="0" stop-color="${a.glow}" stop-opacity=".55"/><stop offset="1" stop-color="${a.glow}" stop-opacity="0"/></radialGradient>
      </defs>
      <rect x="2" y="2" width="116" height="166" rx="10" fill="url(#${id}-bg)" stroke="#d9b45a" stroke-width="2"/>
      <rect x="8" y="8" width="104" height="154" rx="7" fill="none" stroke="#d9b45a" stroke-opacity=".4"/>
      <path d="M14 14 l6 0 M14 14 l0 6 M106 14 l-6 0 M106 14 l0 6 M14 156 l6 0 M14 156 l0 -6 M106 156 l-6 0 M106 156 l0 -6" stroke="#d9b45a" stroke-width="1.5"/>
      <circle cx="60" cy="70" r="50" fill="url(#${id}-gl)"/>
      ${a.emblem}
      <rect x="14" y="128" width="92" height="26" rx="5" fill="rgba(0,0,0,.4)" stroke="#d9b45a" stroke-opacity=".5"/>
      <text x="60" y="146" text-anchor="middle" font-size="${name.length > 6 ? 10 : 13}" font-weight="800" fill="#f5e7bd">${name}</text>
    </svg>`;
  }

  root.RoleArt = roleArt;
})(this);
