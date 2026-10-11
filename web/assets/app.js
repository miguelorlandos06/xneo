const XNEO = (() => {
  const API_BASE = window.XNEO_API_BASE || '';
  function token() { return localStorage.getItem('xneo_token'); }
  function user() { try { return JSON.parse(localStorage.getItem('xneo_user')); } catch { return null; } }
  function isLogged() { return !!token(); }
  function setSession(t, u) { localStorage.setItem('xneo_token', t); localStorage.setItem('xneo_user', JSON.stringify(u)); }
  function clearSession() { localStorage.removeItem('xneo_token'); localStorage.removeItem('xneo_user'); }
  function requireLogin() { if (!isLogged()) { location.href = 'auth.html'; return false; } return true; }
  function logout() { clearSession(); location.href = 'auth.html'; }
  async function api(path, opts = {}) {
    const headers = { 'Content-Type': 'application/json', ...(opts.headers || {}) };
    const t = token(); if (t) headers['Authorization'] = `Bearer ${t}`;
    const res = await fetch(API_BASE + path, { ...opts, headers });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || `HTTP ${res.status}`);
    return data;
  }
  function fmtNum(n) { n = Number(n) || 0;
    if (n >= 1e6) return (n/1e6).toFixed(1).replace('.0','') + 'M';
    if (n >= 1e3) return (n/1e3).toFixed(1).replace('.0','') + 'K'; return String(n); }
  function fmtDuration(sec) { sec = Math.floor(Number(sec) || 0);
    const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60), s = sec%60;
    const pad = x => String(x).padStart(2, '0');
    return h > 0 ? `${h}:${pad(m)}:${pad(s)}` : `${m}:${pad(s)}`; }
  function escapeHtml(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }
  function timeAgo(ts) { const d = new Date(ts);
    const s = Math.floor((Date.now() - d.getTime()) / 1000);
    if (s < 60) return 'ahora';
    const m = Math.floor(s/60); if (m < 60) return `hace ${m} min`;
    const h = Math.floor(m/60); if (h < 24) return `hace ${h} h`;
    const dd = Math.floor(h/24); if (dd < 7) return `hace ${dd} d`;
    const w = Math.floor(dd/7); if (w < 5) return `hace ${w} sem`;
    const mo = Math.floor(dd/30); if (mo < 12) return `hace ${mo} mes${mo>1?'es':''}`;
    return `hace ${Math.floor(dd/365)} año(s)`; }
  let toastTimer;
  function toast(msg) { let el = document.getElementById('xneo-toast');
    if (!el) { el = document.createElement('div'); el.id = 'xneo-toast'; el.className = 'toast'; document.body.appendChild(el); }
    el.textContent = msg; el.classList.add('show');
    clearTimeout(toastTimer); toastTimer = setTimeout(() => el.classList.remove('show'), 2200); }
  return { API_BASE, token, user, isLogged, setSession, clearSession, requireLogin, logout, api, fmtNum, fmtDuration, escapeHtml, timeAgo, toast };
})();
