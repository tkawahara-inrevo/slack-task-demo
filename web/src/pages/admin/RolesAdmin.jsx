import { useEffect, useState } from 'react';
import { api } from '../../api/client';

export default function RolesAdmin() {
  const [admins, setAdmins] = useState([]);
  const [newUserId, setNewUserId] = useState('');

  const load = () => api.adminRoles().then((r) => setAdmins(r.admins)).catch(console.error);
  useEffect(() => { load(); }, []);

  const handleAdd = async () => {
    if (!newUserId.trim()) return;
    await api.adminSetRole(newUserId.trim(), 'admin');
    setNewUserId('');
    load();
  };

  const handleRemove = async (userId) => {
    if (!confirm('この管理者を解除しますか？')) return;
    await api.adminSetRole(userId, 'member');
    load();
  };

  return (
    <div>
      <h2>権限管理</h2>
      <p className="hint-text">admin権限を持つユーザーは、全チームのタスクを閲覧でき、チーム・プロジェクト管理ができます。</p>

      <div className="admin-form-row">
        <input
          type="text"
          placeholder="Slack User ID (例: U0A6JPMKVRR)"
          value={newUserId}
          onChange={(e) => setNewUserId(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleAdd()}
        />
        <button className="btn-primary" onClick={handleAdd}>admin追加</button>
      </div>

      <div className="admin-list">
        {admins.map((a) => (
          <div key={a.user_id} className="admin-card">
            <div className="admin-card-header">
              <span className="admin-card-title">
                {a.displayName}
                <span className="badge">admin</span>
                <span className="user-id-hint">{a.user_id}</span>
              </span>
              <div className="admin-card-actions">
                <button className="btn-sm btn-danger" onClick={() => handleRemove(a.user_id)}>解除</button>
              </div>
            </div>
          </div>
        ))}
        {admins.length === 0 && <p className="empty-text">管理者がいません</p>}
      </div>
    </div>
  );
}
