import { useEffect, useMemo, useState } from 'react';
import { api } from '../../api/client';

export default function RolesAdmin() {
  const [admins, setAdmins] = useState([]);
  const [members, setMembers] = useState([]);
  const [query, setQuery] = useState('');
  const [newUserId, setNewUserId] = useState('');

  const load = async () => {
    const [adminRes, memberRes] = await Promise.all([
      api.adminRoles(),
      api.adminUserMapping(''),
    ]);
    setAdmins(adminRes.admins || []);
    setMembers(memberRes.members || []);
  };

  useEffect(() => {
    load().catch(console.error);
  }, []);

  const candidateMembers = useMemo(() => {
    const adminIds = new Set(admins.map((item) => item.user_id));
    const normalizedQuery = query.trim().toLowerCase();
    return members.filter((member) => {
      if (adminIds.has(member.user_id)) return false;
      if (!normalizedQuery) return true;
      return [member.display_name, member.real_name, member.user_id]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalizedQuery));
    });
  }, [admins, members, query]);

  const handleAdd = async () => {
    if (!newUserId) return;
    await api.adminSetRole(newUserId, 'admin');
    setNewUserId('');
    await load();
  };

  const handleRemove = async (userId) => {
    if (!window.confirm('この管理者権限を解除しますか？')) return;
    await api.adminSetRole(userId, 'member');
    await load();
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>管理者権限</h2>
          <p className="page-subtitle">管理者は全チームの設定変更と閲覧権限の管理ができます。</p>
        </div>
      </div>

      <div className="admin-form-row">
        <input
          type="text"
          placeholder="ユーザーを検索"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <select value={newUserId} onChange={(event) => setNewUserId(event.target.value)}>
          <option value="">管理者にするユーザーを選択</option>
          {candidateMembers.map((member) => (
            <option key={member.user_id} value={member.user_id}>
              {member.display_name || member.real_name || member.user_id} ({member.user_id})
            </option>
          ))}
        </select>
        <button className="btn-primary" onClick={handleAdd} disabled={!newUserId}>追加</button>
      </div>

      <div className="admin-list">
        {admins.map((admin) => (
          <div key={admin.user_id} className="admin-card">
            <div className="admin-card-header">
              <span className="admin-card-title">
                {admin.displayName}
                <span className="badge">admin</span>
                <span className="user-id-hint">{admin.user_id}</span>
              </span>
              <div className="admin-card-actions">
                <button className="btn-sm btn-danger" onClick={() => handleRemove(admin.user_id)}>解除</button>
              </div>
            </div>
          </div>
        ))}
        {admins.length === 0 && <p className="empty-text">管理者はまだいません</p>}
      </div>

    </div>
  );
}
