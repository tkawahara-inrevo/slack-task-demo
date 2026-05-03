import { NavLink, useLocation } from 'react-router-dom';
import { useEffect, useRef, useState } from 'react';
import { api } from '../api/client';

function DropdownNav({ label, items, matchPaths }) {
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
          position: 'absolute', top: '100%', left: 0, zIndex: 100,
          background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8,
          boxShadow: '0 4px 16px rgba(0,0,0,0.1)', minWidth: 160, padding: '4px 0',
        }}
          onClick={() => setOpen(false)}>
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

  useEffect(() => {
    api.me().then(setUser).catch(() => {});
    api.rpoAccess().then(r => setRpoAccess(!!r.canAccess)).catch(() => setRpoAccess(false));
  }, []);

  const hitotoreItems = [
    { to: '/crm', label: 'CRM' },
    ...(rpoAccess ? [{ to: '/rpo', label: '案件管理' }] : []),
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <nav className="global-nav">
        <div className="global-nav-inner">
          <div className="global-nav-brand">TaskHub</div>
          <div className="global-nav-links">
            <NavLink to="/" end className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>ホーム</NavLink>
            <NavLink to="/workload" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>業務ガント</NavLink>
            <NavLink to="/org-chart" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>チーム設定</NavLink>
            <DropdownNav
              label="ヒトトレ"
              matchPaths={['/crm', '/rpo']}
              items={hitotoreItems}
            />
            <NavLink to="/analytics" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>分析</NavLink>
            {['admin','corp','it','personnel'].includes(user?.role) && (
              <NavLink to={user?.role === 'admin' ? '/admin' : user?.role === 'personnel' ? '/admin/recruitment' : '/admin/daily-report'}
                className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
                Corp
              </NavLink>
            )}
          </div>
          {user && (
            <div className="global-nav-user">
              {user.displayName}
              {user.role === 'admin' && <span className="nav-role-badge">admin</span>}
            </div>
          )}
        </div>
      </nav>
      <div className="global-content">
        {children}
      </div>
    </div>
  );
}
