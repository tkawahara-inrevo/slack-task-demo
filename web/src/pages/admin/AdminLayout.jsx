import { NavLink, Outlet } from 'react-router-dom';

export default function AdminLayout() {
  return (
    <div className="admin-layout">
      <nav className="admin-nav">
        <h2>管理設定</h2>
        <NavLink to="/admin/roles" className={({ isActive }) => (isActive ? 'active' : '')}>管理者権限</NavLink>
        <NavLink to="/admin/user-mapping" className={({ isActive }) => (isActive ? 'active' : '')}>ユーザーマッピング</NavLink>
        <NavLink to="/admin/integrations" className={({ isActive }) => (isActive ? 'active' : '')}>外部連携</NavLink>
        <NavLink to="/admin/formulas" className={({ isActive }) => (isActive ? 'active' : '')}>計算式</NavLink>
        <NavLink to="/admin/rpo" className={({ isActive }) => (isActive ? 'active' : '')}>案件管理権限</NavLink>
      </nav>
      <div className="admin-content">
        <Outlet />
      </div>
    </div>
  );
}
