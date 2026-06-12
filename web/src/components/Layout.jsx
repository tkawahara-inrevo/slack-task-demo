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
  const [featureAccess, setFeatureAccess] = useState({});
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
    api.featureAccess().then(r => setFeatureAccess(r.access || {})).catch(() => setFeatureAccess({}));
  }, []);

  // ページ遷移時にモバイルメニューを閉じる
  useEffect(() => { setMobileOpen(false); }, [location.pathname]);

  const hitotoreItems = [
    { to: '/crm', label: 'CRM' },
    { to: '/crm/leads', label: 'リード管理' },
    ...(rpoAccess ? [{ to: '/rpo', label: '案件管理' }] : []),
    { to: '/an', label: 'AN一覧' },
    ...(featureAccess.legal ? [{ to: '/legal', label: '法務' }] : []),
  ];

  const navLinks = [
    { to: '/', label: 'ホーム', end: true },
    { to: '/workload', label: '業務ガント' },
    { to: '/workflows', label: 'WF' },
    { to: '/analytics', label: '分析' },
    { to: '/org-chart', label: 'チーム設定' },
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
            <DropdownNav label="ヒトトレ" matchPaths={['/crm', '/rpo', '/legal', '/an']} items={hitotoreItems} />
            {adminLink && <NavLink to={adminLink} className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>Corp</NavLink>}
          </div>

          {/* ダークモード切り替え */}
          <button onClick={toggleTheme} title={isDark ? 'ライトモード' : 'ダークモード'}
            style={{ background:'none', border:'1px solid var(--gray-200)', borderRadius:8, width:32, height:32, cursor:'pointer', color:'var(--gray-500)', fontSize:15, display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0, marginLeft:8 }}>
            {isDark ? '☀' : '🌙'}
          </button>

          {user && (user.realRole === 'admin' || user.role === 'admin') && (
            <ViewAsWidget user={user} onChange={() => api.me().then(setUser)} />
          )}
          {user && (
            <NavLink to="/my-settings" title="個人設定"
              style={({ isActive }) => ({
                background: isActive ? 'rgba(99,102,241,0.15)' : 'transparent',
                border: '1px solid var(--gray-200)', borderRadius: 8, width: 32, height: 32,
                color: isActive ? '#4f46e5' : 'var(--gray-500)',
                fontSize: 15, display: 'flex', alignItems: 'center', justifyContent: 'center',
                textDecoration: 'none', flexShrink: 0,
              })}>
              ⚙
            </NavLink>
          )}
          {user && (
            <div className="global-nav-user">
              {user.displayName}
              {user.role === 'admin' && !user.viewingAs && <span className="nav-role-badge">admin</span>}
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
      {/* View As バナー */}
      {user?.viewingAs && (
        <div style={{
          background: 'linear-gradient(90deg, #f59e0b, #ea580c)',
          color: '#fff', padding: '6px 16px', display: 'flex', alignItems: 'center', gap: 10,
          fontSize: '0.78rem', fontWeight: 600, boxShadow: '0 2px 6px rgba(234,88,12,0.3)',
        }}>
          <span style={{ fontSize: '0.92rem' }}>👁️</span>
          <span>
            <b>View As 中</b>:
            {user.viewingAs.userName && ` 👤 ${user.viewingAs.userName.split('/')[0].trim()}`}
            {user.viewingAs.role    && ` 🎭 ${user.viewingAs.role}`}
            <span style={{ marginLeft: 8, opacity: 0.85, fontWeight: 500, fontSize: '0.7rem' }}>
              （実: {user.realName?.split('/')[0].trim() || 'admin'} / {user.realRole}）
            </span>
          </span>
          <button onClick={async () => { await api.viewAsClear(); window.location.reload(); }}
            style={{ marginLeft: 'auto', background: 'rgba(255,255,255,0.2)', border: '1px solid rgba(255,255,255,0.4)', color: '#fff', padding: '3px 12px', borderRadius: 6, cursor: 'pointer', fontSize: '0.72rem', fontWeight: 700 }}>
            ✕ 元のadminに戻す
          </button>
        </div>
      )}
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

// 👁️ View As widget（adminのみ）
function ViewAsWidget({ user, onChange }) {
  const [open, setOpen] = useState(false);
  const [users, setUsers] = useState(null);
  const [busy, setBusy] = useState(false);
  const [search, setSearch] = useState('');

  const loadUsers = () => {
    if (users !== null) return;
    api.viewAsUsers().then(r => setUsers(r.users || [])).catch(() => setUsers([]));
  };

  const apply = async (u) => {
    setBusy(true);
    try {
      await api.viewAsSet({ asUserId: u.user_id, asUserName: u.real_name || u.display_name || null });
      onChange && onChange();
      setOpen(false);
      setTimeout(() => window.location.reload(), 150);
    } catch (e) { alert('失敗: ' + e.message); }
    finally { setBusy(false); }
  };

  const clear = async () => {
    setBusy(true);
    try {
      await api.viewAsClear();
      onChange && onChange();
      setTimeout(() => window.location.reload(), 150);
    } catch (e) { alert('失敗: ' + e.message); }
    finally { setBusy(false); }
  };

  const filteredUsers = (users || []).filter(u => {
    if (!search) return true;
    const s = search.toLowerCase();
    return (u.real_name || '').toLowerCase().includes(s) || (u.display_name || '').toLowerCase().includes(s);
  });

  return (
    <div style={{ position: 'relative' }}>
      <button onClick={() => { setOpen(v => !v); loadUsers(); }}
        title="View As（管理者向けテスト機能）"
        style={{ background: user?.viewingAs ? '#f59e0b' : 'rgba(99,102,241,0.1)', border: `1px solid ${user?.viewingAs ? '#f59e0b' : '#c7d2fe'}`, color: user?.viewingAs ? '#fff' : '#4f46e5', borderRadius: 8, padding: '4px 10px', cursor: 'pointer', fontSize: '0.72rem', fontWeight: 700, display: 'flex', alignItems: 'center', gap: 4 }}>
        👁️ {user?.viewingAs ? 'View As中' : 'View As'}
      </button>
      {open && (
        <>
          <div onClick={() => setOpen(false)} style={{ position: 'fixed', inset: 0, zIndex: 1000 }} />
          <div style={{ position: 'absolute', top: 36, right: 0, width: 280, background: '#fff', border: '1px solid #e2e8f0', borderRadius: 10, boxShadow: '0 10px 32px rgba(0,0,0,0.15)', padding: 12, zIndex: 1001 }}>
            <div style={{ fontWeight: 800, fontSize: '0.84rem', color: '#0f172a', marginBottom: 4 }}>👁️ View As</div>
            <div style={{ fontSize: '0.66rem', color: '#94a3b8', marginBottom: 10 }}>選んだユーザーの権限・データで画面を確認</div>

            <input type="search" placeholder="名前で検索…" value={search} onChange={e => setSearch(e.target.value)} autoFocus
              style={{ width: '100%', boxSizing: 'border-box', padding: '6px 10px', border: '1px solid #cbd5e1', borderRadius: 6, fontSize: '0.82rem', marginBottom: 8 }} />
            <div style={{ maxHeight: 260, overflowY: 'auto', border: '1px solid #f1f5f9', borderRadius: 6 }}>
              {users === null && <div style={{ padding: 16, textAlign: 'center', color: '#94a3b8', fontSize: '0.72rem' }}>読み込み中…</div>}
              {users?.length === 0 && <div style={{ padding: 16, textAlign: 'center', color: '#94a3b8', fontSize: '0.72rem' }}>ユーザーなし</div>}
              {filteredUsers.slice(0, 60).map(u => {
                const isCurrent = user?.viewingAs?.userId === u.user_id;
                return (
                  <button key={u.user_id} onClick={() => !isCurrent && apply(u)} disabled={busy || isCurrent}
                    style={{ display: 'block', width: '100%', textAlign: 'left', padding: '7px 10px', fontSize: '0.78rem', cursor: isCurrent ? 'default' : 'pointer', background: isCurrent ? '#fef3c7' : '#fff', border: 'none', borderBottom: '1px solid #f8fafc', color: '#0f172a' }}
                    onMouseEnter={e => { if (!isCurrent) e.currentTarget.style.background = '#f1f5f9'; }}
                    onMouseLeave={e => { if (!isCurrent) e.currentTarget.style.background = '#fff'; }}>
                    {(u.real_name || u.display_name || u.user_id).split('/')[0].trim()}
                    {isCurrent && <span style={{ marginLeft: 6, fontSize: '0.66rem', color: '#92400e', fontWeight: 700 }}>← 選択中</span>}
                  </button>
                );
              })}
            </div>

            {user?.viewingAs && (
              <button onClick={clear} disabled={busy}
                style={{ marginTop: 10, width: '100%', padding: '6px 12px', border: '1px solid #fca5a5', background: '#fef2f2', color: '#dc2626', borderRadius: 6, cursor: 'pointer', fontSize: '0.74rem', fontWeight: 700 }}>
                ✕ 元のadminに戻す
              </button>
            )}
          </div>
        </>
      )}
    </div>
  );
}
