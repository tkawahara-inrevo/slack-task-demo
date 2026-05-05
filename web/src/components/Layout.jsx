import { NavLink, useLocation } from 'react-router-dom';
import { useEffect, useRef, useState } from 'react';
import { api } from '../api/client';

function DropdownNav({ label, items, matchPaths, onNavigate }) {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const location = useLocation();
  const isActive = matchPaths.some(p => location.pathname.startsWith(p));

  useEffect(() => {
    const handler = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  return (
    <div ref={ref} style={{ position: 'relative' }}>
      <button onClick={() => setOpen(v => !v)}
        className={isActive ? 'nav-link active' : 'nav-link'}
        style={{ background: 'none', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
        {label}
        <span style={{ fontSize: 9, opacity: 0.6 }}>{open ? '▲' : '▼'}</span>
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: '100%', left: 0, zIndex: 200,
          background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8,
          boxShadow: '0 4px 16px rgba(0,0,0,0.1)', minWidth: 160, padding: '4px 0',
        }}
          onClick={() => { setOpen(false); onNavigate?.(); }}>
          {items.map(({ to, label: l }) => (
            <NavLink key={to} to={to}
              style={({ isActive }) => ({
                display: 'block', padding: '8px 16px', fontSize: '0.85rem', textDecoration: 'none',
                color: isActive ? '#1d4ed8' : '#374151', fontWeight: isActive ? 700 : 400,
                background: isActive ? '#eff6ff' : 'transparent',
              })}>
              {l}
            </NavLink>
          ))}
        </div>
      )}
    </div>
  );
}

// Slackのin-app browserかどうかを検出
const isSlackBrowser = typeof navigator !== 'undefined' &&
  /SlackWebClient|Slack\//.test(navigator.userAgent);

function SlackBrowserBanner() {
  const [state, setState] = useState('idle'); // idle | loading | ready | copied
  const [transferUrl, setTransferUrl] = useState('');

  const handleGenerate = async () => {
    setState('loading');
    try {
      const data = await api.authTransferToken();
      const currentPath = window.location.pathname + window.location.search;
      const url = `${window.location.origin}/api/auth/transfer?token=${data.token}&redirect=${encodeURIComponent(currentPath)}`;
      setTransferUrl(url);
      setState('ready');
    } catch { setState('idle'); }
  };

  const handleCopy = () => {
    navigator.clipboard?.writeText(transferUrl).then(() => {
      setState('copied');
      setTimeout(() => setState('ready'), 2500);
    }).catch(() => {
      // fallback
      const el = document.createElement('textarea');
      el.value = transferUrl; document.body.appendChild(el); el.select();
      document.execCommand('copy'); document.body.removeChild(el);
      setState('copied'); setTimeout(() => setState('ready'), 2500);
    });
  };

  return (
    <div style={{ background:'#f59e0b', padding:'10px 16px', display:'flex', alignItems:'flex-start', gap:10, flexWrap:'wrap' }}>
      <div style={{ flex:1, minWidth:200 }}>
        <div style={{ fontWeight:700, fontSize:'0.85rem', color:'#1a1a1a' }}>Slackブラウザで開いています</div>
        <div style={{ fontSize:'0.75rem', color:'#3b2a00', marginTop:2 }}>
          セッションを引き継いでSafari/Chromeで開けます
        </div>
        {state === 'ready' && (
          <div style={{ marginTop:6, padding:'6px 10px', background:'rgba(0,0,0,0.1)', borderRadius:6, fontSize:'0.7rem', color:'#1a1a1a', wordBreak:'break-all', lineHeight:1.4 }}>
            {transferUrl}
          </div>
        )}
      </div>
      <div style={{ display:'flex', gap:6, flexShrink:0, alignItems:'center' }}>
        {state === 'idle' && (
          <button onClick={handleGenerate}
            style={{ padding:'6px 14px', background:'#1a1a1a', color:'#fff', border:'none', borderRadius:7, fontSize:'0.8rem', fontWeight:700, cursor:'pointer' }}>
            ブラウザで開く準備
          </button>
        )}
        {state === 'loading' && (
          <span style={{ fontSize:'0.78rem', color:'#3b2a00' }}>生成中…</span>
        )}
        {state === 'ready' && (
          <button onClick={handleCopy}
            style={{ padding:'6px 14px', background:'#1a1a1a', color:'#fff', border:'none', borderRadius:7, fontSize:'0.8rem', fontWeight:700, cursor:'pointer' }}>
            URLをコピー
          </button>
        )}
        {state === 'copied' && (
          <span style={{ fontSize:'0.82rem', fontWeight:700, color:'#1a1a1a' }}>✓ コピーしました！SafariのURL欄に貼り付けてください</span>
        )}
      </div>
    </div>
  );
}

export default function Layout({ children }) {
  const [user,      setUser]      = useState(null);
  const [rpoAccess, setRpoAccess] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const location = useLocation();

  useEffect(() => {
    api.me().then(setUser).catch(() => {});
    api.rpoAccess().then(r => setRpoAccess(!!r.canAccess)).catch(() => setRpoAccess(false));
  }, []);

  // ページ遷移時にモバイルメニューを閉じる
  useEffect(() => { setMobileOpen(false); }, [location.pathname]);

  const hitotoreItems = [
    { to: '/crm', label: 'CRM' },
    ...(rpoAccess ? [{ to: '/rpo', label: '案件管理' }] : []),
  ];

  const navLinks = [
    { to: '/', label: 'ホーム', end: true },
    { to: '/workload', label: '業務ガント' },
    { to: '/org-chart', label: 'チーム設定' },
  ];

  const adminLink = user && ['admin','corp','it','personnel'].includes(user?.role) ? (
    user?.role === 'admin' ? '/admin' : user?.role === 'personnel' ? '/admin/recruitment' : '/admin/daily-report'
  ) : null;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <nav className="global-nav">
        <div className="global-nav-inner">
          <div className="global-nav-brand">TaskHub</div>

          {/* デスクトップナビ */}
          <div className="global-nav-links">
            {navLinks.map(({ to, label, end }) => (
              <NavLink key={to} to={to} end={end} className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>{label}</NavLink>
            ))}
            <DropdownNav label="ヒトトレ" matchPaths={['/crm', '/rpo']} items={hitotoreItems} />
            <NavLink to="/analytics" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>分析</NavLink>
            {adminLink && <NavLink to={adminLink} className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>Corp</NavLink>}
          </div>

          {user && (
            <div className="global-nav-user">
              {user.displayName}
              {user.role === 'admin' && <span className="nav-role-badge">admin</span>}
            </div>
          )}

          {/* ハンバーガーボタン（モバイルのみ） */}
          <button className="mobile-menu-btn" onClick={() => setMobileOpen(v => !v)}
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#c9cdd5', fontSize: 22, padding: '4px 8px', display: 'none' }}>
            {mobileOpen ? '✕' : '☰'}
          </button>
        </div>

        {/* モバイルメニュー展開 */}
        {mobileOpen && (
          <div className="mobile-menu" style={{ background: '#1e2127', borderTop: '1px solid #2d3139', padding: '8px 0' }}>
            {navLinks.map(({ to, label, end }) => (
              <NavLink key={to} to={to} end={end}
                className={({ isActive }) => isActive ? 'mobile-nav-link active' : 'mobile-nav-link'}
                onClick={() => setMobileOpen(false)}>
                {label}
              </NavLink>
            ))}
            {hitotoreItems.map(({ to, label }) => (
              <NavLink key={to} to={to}
                className={({ isActive }) => isActive ? 'mobile-nav-link active' : 'mobile-nav-link'}
                onClick={() => setMobileOpen(false)}>
                {label}
              </NavLink>
            ))}
            <NavLink to="/analytics" className={({ isActive }) => isActive ? 'mobile-nav-link active' : 'mobile-nav-link'}
              onClick={() => setMobileOpen(false)}>分析</NavLink>
            {adminLink && (
              <NavLink to={adminLink} className={({ isActive }) => isActive ? 'mobile-nav-link active' : 'mobile-nav-link'}
                onClick={() => setMobileOpen(false)}>Corp</NavLink>
            )}
            {user && (
              <div style={{ padding: '10px 20px', fontSize: 13, color: '#9ba1ad', borderTop: '1px solid #2d3139', marginTop: 4 }}>
                {user.displayName}
                {user.role === 'admin' && <span className="nav-role-badge" style={{ marginLeft: 8 }}>admin</span>}
              </div>
            )}
          </div>
        )}
      </nav>
      {isSlackBrowser && <SlackBrowserBanner />}
      <div className="global-content">
        {children}
      </div>
    </div>
  );
}
