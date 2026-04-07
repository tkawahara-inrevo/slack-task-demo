import { useEffect, useMemo, useState } from 'react';
import { api } from '../../api/client';

export default function TeamsAdmin() {
  const [teams, setTeams] = useState([]);
  const [directory, setDirectory] = useState([]);
  const [newName, setNewName] = useState('');
  const [expandedId, setExpandedId] = useState(null);
  const [members, setMembers] = useState([]);
  const [memberQuery, setMemberQuery] = useState('');
  const [newMemberId, setNewMemberId] = useState('');
  const [editingId, setEditingId] = useState(null);
  const [editName, setEditName] = useState('');

  const loadTeams = async () => {
    const response = await api.adminTeams();
    setTeams(response.teams || []);
  };

  const loadDirectory = async () => {
    const response = await api.adminUserMapping('');
    setDirectory(response.members || []);
  };

  useEffect(() => {
    loadTeams().catch(console.error);
    loadDirectory().catch(console.error);
  }, []);

  const handleCreate = async () => {
    if (!newName.trim()) return;
    await api.adminCreateTeam(newName.trim());
    setNewName('');
    await loadTeams();
  };

  const handleDelete = async (id) => {
    if (!window.confirm('このチームを削除しますか？')) return;
    await api.adminDeleteTeam(id);
    if (expandedId === id) {
      setExpandedId(null);
      setMembers([]);
    }
    await loadTeams();
  };

  const handleRename = async (id) => {
    if (!editName.trim()) return;
    await api.adminUpdateTeam(id, editName.trim());
    setEditingId(null);
    await loadTeams();
  };

  const toggleMembers = async (id) => {
    if (expandedId === id) {
      setExpandedId(null);
      setMembers([]);
      setMemberQuery('');
      setNewMemberId('');
      return;
    }
    setExpandedId(id);
    setMemberQuery('');
    setNewMemberId('');
    const response = await api.adminTeamMembers(id);
    setMembers(response.members || []);
  };

  const handleAddMember = async () => {
    if (!expandedId || !newMemberId) return;
    await api.adminAddTeamMember(expandedId, newMemberId);
    setNewMemberId('');
    const response = await api.adminTeamMembers(expandedId);
    setMembers(response.members || []);
    await loadTeams();
  };

  const handleRemoveMember = async (userId) => {
    if (!expandedId) return;
    await api.adminRemoveTeamMember(expandedId, userId);
    const response = await api.adminTeamMembers(expandedId);
    setMembers(response.members || []);
    await loadTeams();
  };

  const availableMembers = useMemo(() => {
    const memberIds = new Set(members.map((member) => member.user_id));
    const normalizedQuery = memberQuery.trim().toLowerCase();
    return directory.filter((member) => {
      if (memberIds.has(member.user_id)) return false;
      if (!normalizedQuery) return true;
      return [member.display_name, member.real_name, member.user_id]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalizedQuery));
    });
  }, [directory, memberQuery, members]);

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>チーム管理</h2>
          <p className="page-subtitle">細かいチームを作成して、業務ガントやダッシュボードの可視範囲に使います。</p>
        </div>
      </div>

      <div className="admin-form-row">
        <input
          type="text"
          placeholder="新しいチーム名"
          value={newName}
          onChange={(event) => setNewName(event.target.value)}
          onKeyDown={(event) => event.key === 'Enter' && handleCreate()}
        />
        <button className="btn-primary" onClick={handleCreate}>作成</button>
      </div>

      <div className="admin-list">
        {teams.map((team) => (
          <div key={team.id} className="admin-card">
            <div className="admin-card-header">
              {editingId === team.id ? (
                <div className="admin-form-row">
                  <input
                    value={editName}
                    onChange={(event) => setEditName(event.target.value)}
                    onKeyDown={(event) => event.key === 'Enter' && handleRename(team.id)}
                  />
                  <button className="btn-sm btn-primary" onClick={() => handleRename(team.id)}>保存</button>
                  <button className="btn-sm btn-ghost" onClick={() => setEditingId(null)}>キャンセル</button>
                </div>
              ) : (
                <>
                  <span className="admin-card-title" onClick={() => toggleMembers(team.id)}>
                    {team.name}
                    <span className="badge">{team.memberCount}名</span>
                  </span>
                  <div className="admin-card-actions">
                    <button className="btn-sm" onClick={() => { setEditingId(team.id); setEditName(team.name); }}>編集</button>
                    <button className="btn-sm btn-danger" onClick={() => handleDelete(team.id)}>削除</button>
                  </div>
                </>
              )}
            </div>

            {expandedId === team.id && (
              <div className="admin-card-body">
                <h4>メンバー</h4>
                <div className="admin-form-row">
                  <input
                    type="text"
                    placeholder="追加するメンバーを検索"
                    value={memberQuery}
                    onChange={(event) => setMemberQuery(event.target.value)}
                  />
                  <select value={newMemberId} onChange={(event) => setNewMemberId(event.target.value)}>
                    <option value="">追加するユーザーを選択</option>
                    {availableMembers.map((member) => (
                      <option key={member.user_id} value={member.user_id}>
                        {member.display_name || member.real_name || member.user_id} ({member.user_id})
                      </option>
                    ))}
                  </select>
                  <button className="btn-sm btn-primary" onClick={handleAddMember} disabled={!newMemberId}>追加</button>
                </div>
                {members.length === 0 && <p className="empty-text">メンバーはまだいません</p>}
                <ul className="member-ul">
                  {members.map((member) => (
                    <li key={member.user_id}>
                      <span>{member.displayName} ({member.user_id})</span>
                      <button className="btn-sm btn-danger" onClick={() => handleRemoveMember(member.user_id)}>外す</button>
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
