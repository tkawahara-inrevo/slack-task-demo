import { useEffect, useState } from 'react';
import { api } from '../../api/client';

export default function TeamsAdmin() {
  const [teams, setTeams] = useState([]);
  const [newName, setNewName] = useState('');
  const [expandedId, setExpandedId] = useState(null);
  const [members, setMembers] = useState([]);
  const [newMemberId, setNewMemberId] = useState('');
  const [editingId, setEditingId] = useState(null);
  const [editName, setEditName] = useState('');

  const loadTeams = () => api.adminTeams().then((r) => setTeams(r.teams)).catch(console.error);

  useEffect(() => { loadTeams(); }, []);

  const handleCreate = async () => {
    if (!newName.trim()) return;
    await api.adminCreateTeam(newName.trim());
    setNewName('');
    loadTeams();
  };

  const handleDelete = async (id) => {
    if (!confirm('このチームを削除しますか？')) return;
    await api.adminDeleteTeam(id);
    if (expandedId === id) setExpandedId(null);
    loadTeams();
  };

  const handleRename = async (id) => {
    if (!editName.trim()) return;
    await api.adminUpdateTeam(id, editName.trim());
    setEditingId(null);
    loadTeams();
  };

  const toggleMembers = async (id) => {
    if (expandedId === id) { setExpandedId(null); return; }
    setExpandedId(id);
    const r = await api.adminTeamMembers(id);
    setMembers(r.members);
  };

  const handleAddMember = async () => {
    if (!newMemberId.trim() || !expandedId) return;
    await api.adminAddTeamMember(expandedId, newMemberId.trim());
    setNewMemberId('');
    const r = await api.adminTeamMembers(expandedId);
    setMembers(r.members);
    loadTeams();
  };

  const handleRemoveMember = async (userId) => {
    if (!expandedId) return;
    await api.adminRemoveTeamMember(expandedId, userId);
    const r = await api.adminTeamMembers(expandedId);
    setMembers(r.members);
    loadTeams();
  };

  return (
    <div>
      <h2>チーム管理</h2>

      <div className="admin-form-row">
        <input
          type="text"
          placeholder="新しいチーム名"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
        />
        <button className="btn-primary" onClick={handleCreate}>作成</button>
      </div>

      <div className="admin-list">
        {teams.map((t) => (
          <div key={t.id} className="admin-card">
            <div className="admin-card-header">
              {editingId === t.id ? (
                <div className="admin-form-row">
                  <input value={editName} onChange={(e) => setEditName(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleRename(t.id)} />
                  <button className="btn-sm" onClick={() => handleRename(t.id)}>保存</button>
                  <button className="btn-sm btn-ghost" onClick={() => setEditingId(null)}>取消</button>
                </div>
              ) : (
                <>
                  <span className="admin-card-title" onClick={() => toggleMembers(t.id)}>
                    {t.name}
                    <span className="badge">{t.memberCount}名</span>
                  </span>
                  <div className="admin-card-actions">
                    <button className="btn-sm" onClick={() => { setEditingId(t.id); setEditName(t.name); }}>編集</button>
                    <button className="btn-sm btn-danger" onClick={() => handleDelete(t.id)}>削除</button>
                  </div>
                </>
              )}
            </div>

            {expandedId === t.id && (
              <div className="admin-card-body">
                <h4>メンバー</h4>
                <div className="admin-form-row">
                  <input
                    type="text"
                    placeholder="Slack User ID (例: U0A6JPMKVRR)"
                    value={newMemberId}
                    onChange={(e) => setNewMemberId(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleAddMember()}
                  />
                  <button className="btn-sm btn-primary" onClick={handleAddMember}>追加</button>
                </div>
                {members.length === 0 && <p className="empty-text">メンバーなし</p>}
                <ul className="member-ul">
                  {members.map((m) => (
                    <li key={m.user_id}>
                      <span>{m.displayName} ({m.user_id})</span>
                      <button className="btn-sm btn-danger" onClick={() => handleRemoveMember(m.user_id)}>外す</button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        ))}
        {teams.length === 0 && <p className="empty-text">チームがまだありません</p>}
      </div>
    </div>
  );
}
