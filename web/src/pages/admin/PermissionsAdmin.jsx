import { useEffect, useMemo, useState } from 'react';
import { api } from '../../api/client';

function toggleId(values, targetId) {
  return values.includes(targetId)
    ? values.filter((value) => value !== targetId)
    : [...values, targetId];
}

export default function PermissionsAdmin() {
  const [teams, setTeams] = useState([]);
  const [members, setMembers] = useState([]);
  const [viewerQuery, setViewerQuery] = useState('');
  const [targetQuery, setTargetQuery] = useState('');
  const [viewerUserId, setViewerUserId] = useState('');
  const [selectedTeamIds, setSelectedTeamIds] = useState([]);
  const [selectedUserIds, setSelectedUserIds] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  const loadBase = async () => {
    setLoading(true);
    try {
      const [teamRes, memberRes] = await Promise.all([
        api.adminTeams(),
        api.adminUserMapping(''),
      ]);
      const nextTeams = teamRes.teams || [];
      const nextMembers = memberRes.members || [];
      setTeams(nextTeams);
      setMembers(nextMembers);
      if (!viewerUserId && nextMembers[0]?.user_id) {
        setViewerUserId(nextMembers[0].user_id);
      }
    } finally {
      setLoading(false);
    }
  };

  const loadVisibility = async (nextViewerUserId) => {
    if (!nextViewerUserId) {
      setSelectedTeamIds([]);
      setSelectedUserIds([]);
      return;
    }
    const response = await api.adminVisibility(nextViewerUserId);
    setSelectedTeamIds(response.visibleDashTeamIds || []);
    setSelectedUserIds(response.visibleUserIds || []);
  };

  useEffect(() => {
    loadBase().catch(console.error);
  }, []);

  useEffect(() => {
    loadVisibility(viewerUserId).catch(console.error);
  }, [viewerUserId]);

  const visibleViewerCandidates = useMemo(() => {
    const normalizedQuery = viewerQuery.trim().toLowerCase();
    return members.filter((member) => {
      if (!normalizedQuery) return true;
      return [member.display_name, member.real_name, member.user_id]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalizedQuery));
    });
  }, [members, viewerQuery]);

  const visibleTargetCandidates = useMemo(() => {
    const normalizedQuery = targetQuery.trim().toLowerCase();
    return members.filter((member) => {
      if (member.user_id === viewerUserId) return false;
      if (!normalizedQuery) return true;
      return [member.display_name, member.real_name, member.user_id]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalizedQuery));
    });
  }, [members, targetQuery, viewerUserId]);

  const selectedViewer = members.find((member) => member.user_id === viewerUserId);

  const handleSave = async () => {
    if (!viewerUserId) return;
    setSaving(true);
    try {
      await api.adminSetVisibility(viewerUserId, selectedTeamIds, selectedUserIds);
      await loadVisibility(viewerUserId);
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return <p className="empty-text">読み込み中...</p>;
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>閲覧権限</h2>
          <p className="page-subtitle">ユーザーごとに、見られるチームと個別に見られる人を設定します。</p>
        </div>
        <button className="btn-primary" onClick={handleSave} disabled={!viewerUserId || saving}>
          {saving ? '保存中...' : '保存'}
        </button>
      </div>

      <div className="permissions-layout">
        <section className="permissions-panel">
          <h3>権限を設定する人</h3>
          <input
            type="text"
            className="permissions-search"
            placeholder="ユーザーを検索"
            value={viewerQuery}
            onChange={(event) => setViewerQuery(event.target.value)}
          />
          <div className="permissions-user-list">
            {visibleViewerCandidates.map((member) => (
              <button
                key={member.user_id}
                type="button"
                className={`permissions-user-row${member.user_id === viewerUserId ? ' active' : ''}`}
                onClick={() => setViewerUserId(member.user_id)}
              >
                <strong>{member.display_name || member.real_name || member.user_id}</strong>
                <span>{member.real_name || member.user_id}</span>
              </button>
            ))}
          </div>
        </section>

        <section className="permissions-panel permissions-panel-wide">
          <h3>設定内容</h3>
          {selectedViewer ? (
            <>
              <div className="permissions-summary">
                <strong>{selectedViewer.display_name || selectedViewer.real_name || selectedViewer.user_id}</strong>
                <span>{selectedViewer.user_id}</span>
              </div>

              <div className="permissions-section">
                <h4>見られるチーム</h4>
                <p className="hint-text">ここで選んだチームに所属する人のタスクを見られるようにします。</p>
                <div className="permissions-checkbox-grid">
                  {teams.map((team) => (
                    <label key={team.id} className="permissions-checkbox-card">
                      <input
                        type="checkbox"
                        checked={selectedTeamIds.includes(team.id)}
                        onChange={() => setSelectedTeamIds((current) => toggleId(current, team.id))}
                      />
                      <span>{team.name}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div className="permissions-section">
                <h4>個別に見られる人</h4>
                <p className="hint-text">チームに関係なく、個別に見せたい人だけを追加できます。</p>
                <input
                  type="text"
                  className="permissions-search"
                  placeholder="対象ユーザーを検索"
                  value={targetQuery}
                  onChange={(event) => setTargetQuery(event.target.value)}
                />
                <div className="permissions-checkbox-grid">
                  {visibleTargetCandidates.map((member) => (
                    <label key={member.user_id} className="permissions-checkbox-card">
                      <input
                        type="checkbox"
                        checked={selectedUserIds.includes(member.user_id)}
                        onChange={() => setSelectedUserIds((current) => toggleId(current, member.user_id))}
                      />
                      <span>{member.display_name || member.real_name || member.user_id}</span>
                    </label>
                  ))}
                </div>
              </div>
            </>
          ) : (
            <p className="empty-text">対象ユーザーを選んでください</p>
          )}
        </section>
      </div>
    </div>
  );
}
