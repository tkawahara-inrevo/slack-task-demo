import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../../api/client';

function MemberAvatar({ name }) {
  const letter = (name || '?')[0].toUpperCase();
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
      width: 28, height: 28, borderRadius: '50%',
      background: 'var(--primary-light)', color: 'var(--primary)',
      fontSize: 12, fontWeight: 700, flexShrink: 0,
    }}>
      {letter}
    </span>
  );
}

export default function TeamsAdmin() {
  const [teams, setTeams] = useState([]);
  const [directory, setDirectory] = useState([]);

  // New team inline creation
  const [newName, setNewName] = useState('');

  // Team edit modal
  const [modalTeam, setModalTeam] = useState(null); // { id, name, memberCount }
  const [modalName, setModalName] = useState('');
  const [modalMembers, setModalMembers] = useState([]);
  const [memberQuery, setMemberQuery] = useState('');
  const [savingName, setSavingName] = useState(false);
  const searchRef = useRef(null);

  const loadTeams = async () => {
    const res = await api.adminTeams();
    setTeams(res.teams || []);
  };

  const loadDirectory = async () => {
    const res = await api.adminUserMapping('');
    setDirectory(res.members || []);
  };

  useEffect(() => {
    loadTeams().catch(console.error);
    loadDirectory().catch(console.error);
  }, []);

  // ── Team creation ──────────────────────────────────────
  const handleCreate = async () => {
    if (!newName.trim()) return;
    await api.adminCreateTeam(newName.trim());
    setNewName('');
    await loadTeams();
  };

  const handleDelete = async (id) => {
    if (!window.confirm('このチームを削除しますか？')) return;
    await api.adminDeleteTeam(id);
    await loadTeams();
  };

  // ── Modal open/close ───────────────────────────────────
  const openModal = async (team) => {
    setModalTeam(team);
    setModalName(team.name);
    setMemberQuery('');
    const res = await api.adminTeamMembers(team.id);
    setModalMembers(res.members || []);
    setTimeout(() => searchRef.current?.focus(), 80);
  };

  const closeModal = () => {
    setModalTeam(null);
    setModalName('');
    setModalMembers([]);
    setMemberQuery('');
  };

  // ── Name save ─────────────────────────────────────────
  const handleSaveName = async () => {
    if (!modalTeam || !modalName.trim() || modalName.trim() === modalTeam.name) return;
    setSavingName(true);
    await api.adminUpdateTeam(modalTeam.id, modalName.trim());
    setSavingName(false);
    setModalTeam((prev) => ({ ...prev, name: modalName.trim() }));
    await loadTeams();
  };

  // ── Member add/remove ──────────────────────────────────
  const handleAddMember = async (userId) => {
    await api.adminAddTeamMember(modalTeam.id, userId);
    const res = await api.adminTeamMembers(modalTeam.id);
    setModalMembers(res.members || []);
    await loadTeams();
  };

  const handleRemoveMember = async (userId) => {
    await api.adminRemoveTeamMember(modalTeam.id, userId);
    const res = await api.adminTeamMembers(modalTeam.id);
    setModalMembers(res.members || []);
    await loadTeams();
  };

  // ── Filtered candidate list ────────────────────────────
  const memberSet = useMemo(() => new Set(modalMembers.map((m) => m.user_id)), [modalMembers]);

  const candidates = useMemo(() => {
    const q = memberQuery.trim().toLowerCase();
    return directory.filter((m) => {
      if (memberSet.has(m.user_id)) return false;
      if (!q) return true;
      return [m.display_name, m.real_name, m.user_id].filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q));
    });
  }, [directory, memberQuery, memberSet]);

  const displayName = (m) => m.display_name || m.real_name || m.user_id;

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>チーム管理</h2>
          <p className="page-subtitle">細かいチームを作成して、業務ガントやダッシュボードの可視範囲に使います。</p>
        </div>
      </div>

      {/* New team */}
      <div className="admin-form-row">
        <input
          type="text"
          placeholder="新しいチーム名"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
        />
        <button className="btn-primary" onClick={handleCreate} disabled={!newName.trim()}>作成</button>
      </div>

      {/* Team list */}
      <div className="admin-list">
        {teams.map((team) => (
          <div key={team.id} className="admin-card">
            <div className="admin-card-header">
              <button
                className="admin-card-title"
                style={{ background: 'none', border: 'none', padding: 0, textAlign: 'left', cursor: 'pointer' }}
                onClick={() => openModal(team)}
              >
                {team.name}
                <span className="badge">{team.memberCount}名</span>
              </button>
              <div className="admin-card-actions">
                <button className="btn-sm" onClick={() => openModal(team)}>メンバー編集</button>
                <button className="btn-sm btn-danger" onClick={() => handleDelete(team.id)}>削除</button>
              </div>
            </div>
          </div>
        ))}
        {teams.length === 0 && <p className="empty-text">チームがまだありません</p>}
      </div>

      {/* Edit modal */}
      {modalTeam && (
        <div className="modal-overlay" onClick={closeModal}>
          <div
            className="modal-content"
            onClick={(e) => e.stopPropagation()}
            style={{ maxWidth: 520, display: 'flex', flexDirection: 'column', gap: 0 }}
          >
            {/* Team name */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20 }}>
              <input
                type="text"
                value={modalName}
                onChange={(e) => setModalName(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSaveName()}
                style={{
                  flex: 1, fontSize: 18, fontWeight: 700,
                  border: 'none', borderBottom: '2px solid var(--gray-200)',
                  borderRadius: 0, padding: '4px 0', outline: 'none',
                  background: 'transparent',
                }}
                onFocus={(e) => e.target.style.borderBottomColor = 'var(--primary)'}
                onBlur={(e) => { e.target.style.borderBottomColor = 'var(--gray-200)'; handleSaveName(); }}
              />
              {savingName && <span style={{ fontSize: 12, color: 'var(--gray-400)' }}>保存中…</span>}
            </div>

            {/* Current members */}
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 12, color: 'var(--gray-500)', marginBottom: 8, fontWeight: 600 }}>
                メンバー {modalMembers.length > 0 && <span className="badge">{modalMembers.length}名</span>}
              </div>
              {modalMembers.length === 0 ? (
                <p style={{ fontSize: 13, color: 'var(--gray-400)', margin: 0 }}>メンバーはまだいません</p>
              ) : (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                  {modalMembers.map((m) => (
                    <div
                      key={m.user_id}
                      style={{
                        display: 'inline-flex', alignItems: 'center', gap: 6,
                        background: 'var(--gray-100)', borderRadius: 20,
                        padding: '4px 10px 4px 6px', fontSize: 13,
                      }}
                    >
                      <MemberAvatar name={m.displayName || m.user_id} />
                      <span>{m.displayName || m.user_id}</span>
                      <button
                        onClick={() => handleRemoveMember(m.user_id)}
                        style={{
                          background: 'none', border: 'none', cursor: 'pointer',
                          color: 'var(--gray-400)', fontSize: 14, padding: '0 0 0 2px',
                          lineHeight: 1,
                        }}
                        title="外す"
                      >×</button>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* Add member search */}
            <div style={{ borderTop: '1px solid var(--gray-100)', paddingTop: 14 }}>
              <div style={{ fontSize: 12, color: 'var(--gray-500)', marginBottom: 8, fontWeight: 600 }}>メンバーを追加</div>
              <input
                ref={searchRef}
                type="text"
                placeholder="名前で検索…"
                value={memberQuery}
                onChange={(e) => setMemberQuery(e.target.value)}
                style={{
                  width: '100%', boxSizing: 'border-box',
                  fontSize: 13, padding: '8px 12px',
                  border: '1px solid var(--gray-200)', borderRadius: 8,
                  marginBottom: 8, outline: 'none',
                }}
              />
              {candidates.length === 0 ? (
                <p style={{ fontSize: 13, color: 'var(--gray-400)', margin: 0 }}>
                  {memberQuery ? '一致するユーザーが見つかりません' : '追加できるユーザーがいません'}
                </p>
              ) : (
                <div style={{ maxHeight: 220, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 2 }}>
                  {candidates.map((m) => (
                    <button
                      key={m.user_id}
                      onClick={() => handleAddMember(m.user_id)}
                      style={{
                        display: 'flex', alignItems: 'center', gap: 10,
                        padding: '7px 10px', borderRadius: 8,
                        border: 'none', background: 'none', cursor: 'pointer',
                        textAlign: 'left', width: '100%',
                      }}
                      onMouseEnter={(e) => e.currentTarget.style.background = 'var(--primary-light)'}
                      onMouseLeave={(e) => e.currentTarget.style.background = 'none'}
                    >
                      <MemberAvatar name={displayName(m)} />
                      <div style={{ lineHeight: 1.4 }}>
                        <div style={{ fontSize: 13, fontWeight: 500 }}>{displayName(m)}</div>
                        {m.real_name && m.real_name !== m.display_name && (
                          <div style={{ fontSize: 11, color: 'var(--gray-400)' }}>{m.real_name}</div>
                        )}
                      </div>
                    </button>
                  ))}
                </div>
              )}
            </div>

            <div style={{ marginTop: 20, display: 'flex', justifyContent: 'flex-end' }}>
              <button className="btn-secondary" onClick={closeModal}>閉じる</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
