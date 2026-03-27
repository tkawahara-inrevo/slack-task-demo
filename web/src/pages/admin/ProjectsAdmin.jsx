import { useEffect, useState } from 'react';
import { api } from '../../api/client';

export default function ProjectsAdmin() {
  const [projects, setProjects] = useState([]);
  const [teams, setTeams] = useState([]);
  const [newName, setNewName] = useState('');
  const [newTeamId, setNewTeamId] = useState('');
  const [editingId, setEditingId] = useState(null);
  const [editName, setEditName] = useState('');
  const [editTeamId, setEditTeamId] = useState('');

  const load = () => {
    api.adminProjects().then((r) => setProjects(r.projects)).catch(console.error);
    api.adminTeams().then((r) => setTeams(r.teams)).catch(console.error);
  };

  useEffect(() => { load(); }, []);

  const teamName = (id) => teams.find((t) => t.id === id)?.name || '-';

  const handleCreate = async () => {
    if (!newName.trim()) return;
    await api.adminCreateProject(newName.trim(), newTeamId || null);
    setNewName('');
    setNewTeamId('');
    load();
  };

  const handleDelete = async (id) => {
    if (!confirm('このプロジェクトを削除しますか？')) return;
    await api.adminDeleteProject(id);
    load();
  };

  const handleUpdate = async (id) => {
    if (!editName.trim()) return;
    await api.adminUpdateProject(id, editName.trim(), editTeamId || null);
    setEditingId(null);
    load();
  };

  return (
    <div>
      <h2>プロジェクト管理</h2>

      <div className="admin-form-row">
        <input
          type="text"
          placeholder="新しいプロジェクト名"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
        />
        <select value={newTeamId} onChange={(e) => setNewTeamId(e.target.value)}>
          <option value="">チーム指定なし</option>
          {teams.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
        </select>
        <button className="btn-primary" onClick={handleCreate}>作成</button>
      </div>

      <div className="admin-list">
        {projects.map((p) => (
          <div key={p.id} className="admin-card">
            <div className="admin-card-header">
              {editingId === p.id ? (
                <div className="admin-form-row">
                  <input value={editName} onChange={(e) => setEditName(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleUpdate(p.id)} />
                  <select value={editTeamId} onChange={(e) => setEditTeamId(e.target.value)}>
                    <option value="">チーム指定なし</option>
                    {teams.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
                  </select>
                  <button className="btn-sm" onClick={() => handleUpdate(p.id)}>保存</button>
                  <button className="btn-sm btn-ghost" onClick={() => setEditingId(null)}>取消</button>
                </div>
              ) : (
                <>
                  <span className="admin-card-title">
                    {p.name}
                    {p.dash_team_id && <span className="badge">{teamName(p.dash_team_id)}</span>}
                  </span>
                  <div className="admin-card-actions">
                    <button className="btn-sm" onClick={() => {
                      setEditingId(p.id);
                      setEditName(p.name);
                      setEditTeamId(p.dash_team_id || '');
                    }}>編集</button>
                    <button className="btn-sm btn-danger" onClick={() => handleDelete(p.id)}>削除</button>
                  </div>
                </>
              )}
            </div>
          </div>
        ))}
        {projects.length === 0 && <p className="empty-text">プロジェクトがまだありません</p>}
      </div>
    </div>
  );
}
