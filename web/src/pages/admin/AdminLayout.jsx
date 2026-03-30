import { NavLink, Outlet } from 'react-router-dom';

export default function AdminLayout() {
  return (
    <div className="admin-layout">
      <nav className="admin-nav">
        <h2>管理メニュー</h2>
        <NavLink to="/admin/teams" className={({ isActive }) => isActive ? 'active' : ''}>チーム管理</NavLink>
        <NavLink to="/admin/projects" className={({ isActive }) => isActive ? 'active' : ''}>プロジェクト管理</NavLink>
        <NavLink to="/admin/roles" className={({ isActive }) => isActive ? 'active' : ''}>権限管理</NavLink>
        <NavLink to="/admin/integrations" className={({ isActive }) => isActive ? 'active' : ''}>外部連携</NavLink>
        <NavLink to="/admin/formulas" className={({ isActive }) => isActive ? 'active' : ''}>計算フィールド</NavLink>
        <NavLink to="/" className="back-link">← ダッシュボード</NavLink>
      </nav>
      <div className="admin-content">
        <Outlet />
      </div>
    </div>
  );
}
