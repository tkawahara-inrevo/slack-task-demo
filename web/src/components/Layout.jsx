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

// モバイルかどうかの判定（ボタン表示に使用）
const isMobileDevice = typeof navigator !== 'undefined' && /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);

// ブラウザ転送ボタン（モバイルナビに常時表示）
function BrowserTransferButton({ onClose, floating } = {}) {
  const [state, setState] = useState('idle'); // idle | loading | ready | copied
  const [transferUrl, setTransferUrl] = useState('');
  const [showModal, setShowModal] = useState(false);

  const handleGenerate = async () => {
    setState('loading');
    setShowModal(true);
    onClose?.();
    try {
      const data = await api.authTransferToken();
      const currentPath = window.location.pathname + window.location.search;
      const url = `${window.location.origin}/api/auth/transfer?token=${data.token}&redirect=${encodeURIComponent(currentPath)}`;
      setTransferUrl(url);
      setState('ready');
    } catch { setState('idle'); setShowModal(false); }
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
          style={{ width:'100%', background:'rgba(255,255,255,0.12)', border:'1px solid rgba(255,255,255,0.2)', borderRadius:9, color:'#fff', fontSize:'0.85rem', fontWeight:600, padding:'10px 16px', cursor:'pointer', textAlign:'left', display:'flex', alignItems:'center', gap:8 }}>
          🔗 <span>Safariで開く（セッション引き継ぎ）</span>
        </button>
      )}

      {showModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.6)', zIndex:2000, display:'flex', alignItems:'flex-end', justifyContent:'center', padding:'0 0 20px' }}
          onClick={handleClose}>
          <div style={{ background:'#fff', borderRadius:16, width:'min(480px,94vw)', padding:'20px', boxShadow:'0 -8px 32px rgba(0,0,0,0.2)' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:16 }}>
              <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>Safariで開く</div>
              <button onClick={handleClose} style={{ background:'#f1f5f9', border:'none', borderRadius:8, width:28, height:28, cursor:'pointer', color:'#64748b', fontSize:16 }}>×</button>
            </div>
            {state === 'loading' && (
              <div style={{ textAlign:'center', padding:'20px', color:'#94a3b8' }}>URLを生成中…</div>
            )}
            {(state === 'ready' || state === 'copied') && (<>
              <div style={{ fontSize:'0.82rem', color:'#374151', marginBottom:12, lineHeight:1.6 }}>
                このURLをコピーして、<strong>SafariのURL欄に貼り付け</strong>てください。<br />
                <span style={{ fontSize:'0.75rem', color:'#94a3b8' }}>※ 有効期限は90秒です</span>
              </div>
              <div style={{ background:'#f8fafc', border:'1px solid #e2e8f0', borderRadius:8, padding:'10px 12px', fontSize:'0.72rem', color:'#374151', wordBreak:'break-all', lineHeight:1.5, marginBottom:12 }}>
                {transferUrl}
              </div>
              <button onClick={handleCopy}
                style={{ width:'100%', padding:'12px', background: state === 'copied' ? '#059669' : '#1e40af', color:'#fff', border:'none', borderRadius:10, fontSize:'0.9rem', fontWeight:700, cursor:'pointer' }}>
                {state === 'copied' ? '✓ コピーしました！ SafariのURL欄に貼り付けてください' : 'URLをコピーする'}
              </button>
            </>)}
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
  const location = useLocation();

  useEffect(() => {
    // URLに ?auth=TOKEN があれば自動でセッション引き継ぎ（Slack → ネイティブブラウザ）
    const params = new URLSearchParams(window.location.search);
    const authToken = params.get('auth');
    if (authToken) {
      // トークンをサーバーに渡してセッションcookieを取得し、URLをクリーン
      const redirect = encodeURIComponent(window.location.pathname);
      window.location.replace(`/api/auth/adopt?token=${authToken}&redirect=${redirect}`);
      return; // リダイレクト中なので以降は実行しない
    }
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
            {/* Safariで開くボタン（モバイルメニュー内） */}
            {user && (
              <div style={{ padding: '12px 16px', borderTop: '1px solid #2d3139', marginTop: 4 }}>
                <BrowserTransferButton onClose={() => setMobileOpen(false)} />
              </div>
            )}
            {user && (
              <div style={{ padding: '10px 20px', fontSize: 13, color: '#9ba1ad', borderTop: '1px solid #2d3139' }}>
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
