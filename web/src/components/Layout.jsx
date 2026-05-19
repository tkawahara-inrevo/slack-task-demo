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
        style={{ background: 'none', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4, color: 'inherit' }}>
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

// モバイル・タブレット判定
// iPadOS 13+ は UA が Mac と同じになるため maxTouchPoints で補完
const isMobileDevice = typeof navigator !== 'undefined' && (
  /iPhone|iPad|iPod|Android/i.test(navigator.userAgent) ||
  (navigator.userAgent.includes('Mac') && navigator.maxTouchPoints > 1)
);

// ブラウザ転送ボタン（モバイルナビに常時表示）
function BrowserTransferButton({ onClose, floating } = {}) {
  const [state, setState] = useState('idle'); // idle | loading | ready | copied
  const [transferUrl, setTransferUrl] = useState('');
  const [showModal, setShowModal] = useState(false);

  const handleGenerate = async () => {
    setState('loading');
    onClose?.();
    try {
      const data = await api.authTransferToken();
      const currentPath = window.location.pathname;
      const url = `${window.location.origin}/api/auth/adopt?token=${data.token}&redirect=${encodeURIComponent(currentPath)}`;
      setTransferUrl(url);
      setShowModal(true);
      setState('ready');
    } catch { setState('idle'); }
  };

  const handleCopy = () => {
    const copy = (text) => {
      navigator.clipboard?.writeText(text).catch(() => {
        const el = document.createElement('textarea');
        el.value = text; document.body.appendChild(el); el.select();
        document.execCommand('copy'); document.body.removeChild(el);
      });
    };
    copy(transferUrl);
    setState('copied');
    setTimeout(() => setState('ready'), 3000);
  };

  const handleClose = () => { setShowModal(false); setState('idle'); setTransferUrl(''); };

  return (
    <>
      {floating ? (
        <button onClick={handleGenerate}
          style={{ background:'#1e40af', border:'none', borderRadius:50, color:'#fff', fontSize:'0.8rem', fontWeight:700, padding:'10px 16px', cursor:'pointer', boxShadow:'0 4px 16px rgba(30,64,175,0.5)', display:'flex', alignItems:'center', gap:6, whiteSpace:'nowrap' }}>
          🔗 Safariで開く
        </button>
      ) : (
        <button onClick={handleGenerate}
          style={{ width:'100%', background:'#f1f5f9', border:'1px solid #e2e8f0', borderRadius:9, color:'#334155', fontSize:'0.85rem', fontWeight:600, padding:'10px 16px', cursor:'pointer', textAlign:'left', display:'flex', alignItems:'center', gap:8 }}>
          🔗 <span>Safariで開く（セッション引き継ぎ）</span>
        </button>
      )}

      {showModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.6)', zIndex:2000, display:'flex', alignItems:'flex-end', justifyContent:'center', padding:'0 0 20px' }}
          onClick={handleClose}>
          <div style={{ background:'#fff', borderRadius:16, width:'min(480px,94vw)', padding:'20px', boxShadow:'0 -8px 32px rgba(0,0,0,0.2)' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:14 }}>
              <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>ブラウザで開く</div>
              <button onClick={handleClose} style={{ background:'#f1f5f9', border:'none', borderRadius:8, width:28, height:28, cursor:'pointer', color:'#64748b', fontSize:16 }}>×</button>
            </div>
            {state === 'loading' && (
              <div style={{ textAlign:'center', padding:'20px', color:'#94a3b8' }}>生成中…</div>
            )}
            {(state === 'ready' || state === 'copied') && (
              <button onClick={handleCopy}
                style={{ width:'100%', padding:'14px', background: state === 'copied' ? '#059669' : '#1e40af', color:'#fff', border:'none', borderRadius:10, fontSize:'1rem', fontWeight:700, cursor:'pointer' }}>
                {state === 'copied' ? '✓ コピーしました　ブラウザのURL欄に貼り付けてください' : 'URLをコピー'}
              </button>
            )}
          </div>
        </div>
      )}
    </>
  );
}

export default function Layout({ children }) {
  const [user,      setUser]      = useState(null);
  const [rpoAccess, setRpoAccess] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [isDark,    setIsDark]    = useState(false);
  const location = useLocation();

  // テーマ初期化
  useEffect(() => {
    const saved = localStorage.getItem('th-theme');
    const dark = saved === 'dark';
    setIsDark(dark);
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
  }, []);

  const toggleTheme = () => {
    setIsDark(v => {
      const next = !v;
      localStorage.setItem('th-theme', next ? 'dark' : 'light');
      document.documentElement.setAttribute('data-theme', next ? 'dark' : 'light');
      return next;
    });
  };

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const authToken = params.get('auth');

    if (authToken) {
      // URLからトークンをすぐ消す（見た目をきれいに）
      window.history.replaceState({}, '', window.location.pathname);

      // まず現在のセッションが有効か確認
      fetch('/api/dashboard/me', { credentials: 'include' })
        .then(r => {
          if (r.ok) {
            // Slackブラウザ：既にcookieがある → そのまま使う
            api.me().then(setUser).catch(() => {});
            api.rpoAccess().then(res => setRpoAccess(!!res.canAccess)).catch(() => setRpoAccess(false));
          } else {
            // Safari等：cookieなし → adopt でセッションをもらう
            const redirect = encodeURIComponent(window.location.pathname);
            window.location.replace(`/api/auth/adopt?token=${authToken}&redirect=${redirect}`);
          }
        })
        .catch(() => {
          const redirect = encodeURIComponent(window.location.pathname);
          window.location.replace(`/api/auth/adopt?token=${authToken}&redirect=${redirect}`);
        });
      return;
    }

    api.me().then(setUser).catch(() => {});
    api.rpoAccess().then(r => setRpoAccess(!!r.canAccess)).catch(() => setRpoAccess(false));
  }, []);

  // ページ遷移時にモバイルメニューを閉じる
  useEffect(() => { setMobileOpen(false); }, [location.pathname]);

  const hitotoreItems = [
    { to: '/crm', label: 'CRM' },
    { to: '/crm/leads', label: 'リード管理' },
    ...(rpoAccess ? [{ to: '/rpo', label: '案件管理' }] : []),
  ];

  const navLinks = [
    { to: '/', label: 'ホーム', end: true },
    { to: '/workload', label: '業務ガント' },
    { to: '/workflows', label: 'WF' },
    { to: '/analytics', label: '分析' },
    { to: '/org-chart', label: 'チーム設定' },
    { to: '/legal', label: '法務' },
  ];

  const adminLink = user && ['admin','corp','it','personnel'].includes(user?.role) ? (
    user?.role === 'admin'     ? '/admin' :
    user?.role === 'it'        ? '/admin/slack-groups' :
    user?.role === 'personnel' ? '/admin/recruitment' :
    '/admin'  // corp
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
            {adminLink && <NavLink to={adminLink} className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>Corp</NavLink>}
          </div>

          {/* ダークモード切り替え */}
          <button onClick={toggleTheme} title={isDark ? 'ライトモード' : 'ダークモード'}
            style={{ background:'none', border:'1px solid var(--gray-200)', borderRadius:8, width:32, height:32, cursor:'pointer', color:'var(--gray-500)', fontSize:15, display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0, marginLeft:8 }}>
            {isDark ? '☀' : '🌙'}
          </button>

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
          <div className="mobile-menu" style={{ background: '#fff', borderTop: '1px solid #e2e8f0', padding: '4px 0', boxShadow: '0 8px 24px rgba(0,0,0,0.08)' }}>
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
            {adminLink && (
              <NavLink to={adminLink} className={({ isActive }) => isActive ? 'mobile-nav-link active' : 'mobile-nav-link'}
                onClick={() => setMobileOpen(false)}>Corp</NavLink>
            )}
            {/* Safariで開くボタン（モバイルメニュー内） */}
            {user && (
              <div style={{ padding: '12px 16px', borderTop: '1px solid #f1f5f9', marginTop: 4 }}>
                <BrowserTransferButton onClose={() => setMobileOpen(false)} />
              </div>
            )}
            {user && (
              <div style={{ padding: '10px 20px', fontSize: 13, color: '#64748b', borderTop: '1px solid #f1f5f9' }}>
                {user.displayName}
                {user.role === 'admin' && <span className="nav-role-badge" style={{ marginLeft: 8 }}>admin</span>}
              </div>
            )}
          </div>
        )}
      </nav>
      <div className="global-content">
        {children}
      </div>
      {/* モバイル: フローティングSafariボタン */}
      {user && isMobileDevice && (
        <div style={{ position:'fixed', bottom:20, right:16, zIndex:500 }}>
          <BrowserTransferButton floating />
        </div>
      )}
    </div>
  );
}
