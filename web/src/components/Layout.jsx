import { NavLink, useLocation, useNavigate } from 'react-router-dom';
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
      <div className="global-content">
        {children}
      </div>
    </div>
  );
}
