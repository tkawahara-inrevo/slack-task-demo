import { NavLink } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { api } from '../api/client';

export default function Layout({ children }) {
  const [user, setUser] = useState(null);

  useEffect(() => {
    api.me().then(setUser).catch(() => {});
  }, []);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <nav className="global-nav">
        <div className="global-nav-inner">
          <div className="global-nav-brand">TaskHub</div>
          <div className="global-nav-links">
            <NavLink to="/" end className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
              ダッシュボード
            </NavLink>
            <NavLink to="/workload" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
              業務ガント
            </NavLink>
            <NavLink to="/org-chart" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
              チーム設定
            </NavLink>
            <NavLink to="/crm/clients" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
              顧客
            </NavLink>
            <NavLink to="/crm/deals" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
              案件
            </NavLink>
            <NavLink to="/analytics" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
              分析
            </NavLink>
            {user?.role === 'admin' && (
              <NavLink to="/admin" className={({ isActive }) => isActive ? 'nav-link active' : 'nav-link'}>
                管理設定
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
