import { NavLink, Outlet } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { api } from '../../api/client';

export default function AdminLayout() {
  const [role, setRole] = useState('');
  const [collapsed, setCollapsed] = useState(false);
  useEffect(() => { api.me().then(r => setRole(r.role || '')).catch(() => {}); }, []);

  const isAdmin     = role === 'admin';
  const isIT        = role === 'admin' || role === 'it';
  const isPersonnel = role === 'admin' || role === 'personnel';

  const navStyle = (isActive) => ({
    display: 'block', padding: collapsed ? '8px 0' : '8px 16px', textAlign: collapsed ? 'center' : 'left',
    fontSize: '0.85rem', color: isActive ? '#1d4ed8' : '#374151', fontWeight: isActive ? 700 : 400,
    background: isActive ? '#eff6ff' : 'transparent', textDecoration: 'none',
    borderLeft: isActive && !collapsed ? '2px solid #3b82f6' : '2px solid transparent',
    overflow: 'hidden', textOverflow: 'ellipsis',
  });

  return (
    <div style={{ display: 'flex', minHeight: '100%' }}>
      <nav style={{
        width: collapsed ? 40 : 200,
        flexShrink: 0,
        transition: 'width 0.2s ease',
        borderRight: '1px solid #e5e7eb',
        background: '#f8fafc',
        padding: collapsed ? '12px 4px' : '12px 0',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
        whiteSpace: 'nowrap',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: collapsed ? 'center' : 'space-between', padding: collapsed ? '0' : '0 16px', marginBottom: 12 }}>
          {!collapsed && <h2 style={{ margin: 0, fontSize: '0.88rem', fontWeight: 800, color: '#374151' }}>Corp</h2>}
          <button onClick={() => setCollapsed(v => !v)}
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#9ca3af', fontSize: 14, padding: 2, lineHeight: 1, flexShrink: 0 }}>
            {collapsed ? '▶' : '◀'}
          </button>
        </div>

        {/* 管理者権限: admin のみ */}
        {isAdmin && (
          <NavLink to="/admin/roles" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '⚙' : '管理者権限'}
          </NavLink>
        )}
        {isAdmin && (
          <NavLink to="/admin/org-chart" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '🏢' : '組織・役職（新）'}
          </NavLink>
        )}
        {isAdmin && (
          <NavLink to="/admin/crm-permissions" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '🔐' : 'CRM権限管理'}
          </NavLink>
        )}
        {isAdmin && (
          <NavLink to="/admin/feature-access" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '🛡️' : 'アクセス権限'}
          </NavLink>
        )}
        {isAdmin && (
          <NavLink to="/admin/overdue-report" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '📊' : '期限切れタスク'}
          </NavLink>
        )}

        {/* IT 所属のみ */}
        {isIT && (
          <NavLink to="/admin/channel-mapping" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '📡' : 'チャンネルマッピング'}
          </NavLink>
        )}
        {isIT && (
          <NavLink to="/admin/slack-groups" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '💬' : 'Slackメンション管理'}
          </NavLink>
        )}
        {isIT && (
          <NavLink to="/admin/daily-report" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '📋' : '日報管理'}
          </NavLink>
        )}
        {isIT && (
          <NavLink to="/admin/ranking" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '🏆' : 'ランキング管理'}
          </NavLink>
        )}

        {/* Personnel 所属のみ */}
        {isPersonnel && (
          <NavLink to="/admin/hrmos-check" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '🕒' : 'HRMOS勤怠チェック'}
          </NavLink>
        )}
        {isPersonnel && (
          <NavLink to="/admin/recruitment" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '📋' : '採用管理（自社）'}
          </NavLink>
        )}
        {isPersonnel && (
          <NavLink to="/admin/hrmos-recruitment" style={({ isActive }) => navStyle(isActive)}>
            {collapsed ? '📊' : 'HRMOS採用分析'}
          </NavLink>
        )}
      </nav>

      <div style={{ flex: 1, padding: '24px', minWidth: 0 }}>
        <Outlet />
      </div>
    </div>
  );
}
