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

// ── Member edit modal ────────────────────────────────────────────────
function MemberModal({ team, directory, onClose, onReload }) {
  const [members, setMembers] = useState([]);
  const [modalName, setModalName] = useState(team.name);
  const [memberQuery, setMemberQuery] = useState('');
  const [savingName, setSavingName] = useState(false);
  const searchRef = useRef(null);

  useEffect(() => {
    api.adminTeamMembers(team.id).then((r) => setMembers(r.members || [])).catch(console.error);
    setTimeout(() => searchRef.current?.focus(), 80);
  }, [team.id]);

  const memberSet = useMemo(() => new Set(members.map((m) => m.user_id)), [members]);
  const candidates = useMemo(() => {
    const q = memberQuery.trim().toLowerCase();
    return directory.filter((m) => {
      if (memberSet.has(m.user_id)) return false;
      if (!q) return true;
      return [m.display_name, m.real_name, m.user_id].filter(Boolean)
        .some((v) => String(v).toLowerCase().includes(q));
    });
  }, [directory, memberQuery, memberSet]);

  const reload = async () => {
    const r = await api.adminTeamMembers(team.id);
    setMembers(r.members || []);
    onReload();
  };

  const handleSaveName = async () => {
    const trimmed = modalName.trim();
    if (!trimmed || trimmed === team.name) return;
    setSavingName(true);
    await api.adminUpdateTeam(team.id, trimmed);
    setSavingName(false);
    onReload();
  };

  const handleAdd = async (userId) => {
    await api.adminAddTeamMember(team.id, userId);
    await reload();
  };

  const handleRemove = async (userId) => {
    await api.adminRemoveTeamMember(team.id, userId);
    await reload();
  };

  const dn = (m) => m.display_name || m.real_name || m.user_id;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 520 }}>
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
              borderRadius: 0, padding: '4px 0', outline: 'none', background: 'transparent',
            }}
            onFocus={(e) => (e.target.style.borderBottomColor = 'var(--primary)')}
            onBlur={(e) => { e.target.style.borderBottomColor = 'var(--gray-200)'; handleSaveName(); }}
          />
          {savingName && <span style={{ fontSize: 12, color: 'var(--gray-400)' }}>保存中…</span>}
        </div>

        {/* Current members */}
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 12, color: 'var(--gray-500)', marginBottom: 8, fontWeight: 600 }}>
            メンバー {members.length > 0 && <span className="badge">{members.length}名</span>}
          </div>
          {members.length === 0 ? (
            <p style={{ fontSize: 13, color: 'var(--gray-400)', margin: 0 }}>メンバーはまだいません</p>
          ) : (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {members.map((m) => (
                <div key={m.user_id} style={{
                  display: 'inline-flex', alignItems: 'center', gap: 6,
                  background: 'var(--gray-100)', borderRadius: 20,
                  padding: '4px 10px 4px 6px', fontSize: 13,
                }}>
                  <MemberAvatar name={m.displayName || m.user_id} />
                  <span>{m.displayName || m.user_id}</span>
                  <button
                    onClick={() => handleRemove(m.user_id)}
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-400)', fontSize: 14, padding: '0 0 0 2px', lineHeight: 1 }}
                    title="外す"
                  >×</button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Add member */}
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
                  onClick={() => handleAdd(m.user_id)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 10,
                    padding: '7px 10px', borderRadius: 8,
                    border: 'none', background: 'none', cursor: 'pointer', textAlign: 'left', width: '100%',
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.background = 'var(--primary-light)')}
                  onMouseLeave={(e) => (e.currentTarget.style.background = 'none')}
                >
                  <MemberAvatar name={dn(m)} />
                  <div style={{ lineHeight: 1.4 }}>
                    <div style={{ fontSize: 13, fontWeight: 500 }}>{dn(m)}</div>
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
          <button className="btn-secondary" onClick={onClose}>閉じる</button>
        </div>
      </div>
    </div>
  );
}

// ── Sub-team row ─────────────────────────────────────────────────────
function SubTeamRow({ team, onEdit, onDelete }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8,
      padding: '8px 12px 8px 36px',
      borderTop: '1px solid var(--gray-100)',
      fontSize: 14,
    }}>
      <span style={{ color: 'var(--gray-400)', fontSize: 12, marginRight: 2 }}>└</span>
      <span style={{ flex: 1, fontWeight: 500 }}>{team.name}</span>
      <span className="badge">{team.member_count ?? 0}名</span>
      <button className="btn-sm" onClick={() => onEdit(team)}>メンバー編集</button>
      <button className="btn-sm btn-danger" onClick={() => onDelete(team.id)}>削除</button>
    </div>
  );
}

// ── Parent team card ─────────────────────────────────────────────────
function ParentTeamCard({ parent, children, onEdit, onDelete, onAddChild }) {
  const [expanded, setExpanded] = useState(true);
  const [newChildName, setNewChildName] = useState('');
  const [addingChild, setAddingChild] = useState(false);

  const handleAddChild = async () => {
    const name = newChildName.trim();
    if (!name) return;
    await onAddChild(parent.id, name);
    setNewChildName('');
    setAddingChild(false);
  };

  return (
    <div className="admin-card">
      {/* Parent header */}
      <div className="admin-card-header" style={{ paddingLeft: 16 }}>
        <button
          style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8 }}
          onClick={() => setExpanded((v) => !v)}
        >
          <span style={{ fontSize: 12, color: 'var(--gray-400)', width: 14 }}>{expanded ? '▾' : '▸'}</span>
          <span className="admin-card-title" style={{ cursor: 'pointer' }}>
            {parent.name}
            <span className="badge">{children.length}部署</span>
          </span>
        </button>
        <div className="admin-card-actions">
          <button className="btn-sm" onClick={() => onEdit(parent)}>メンバー編集</button>
          <button
            className="btn-sm"
            onClick={() => setAddingChild((v) => !v)}
            title="部署を追加"
          >＋ 部署</button>
          <button className="btn-sm btn-danger" onClick={() => onDelete(parent.id)}>削除</button>
        </div>
      </div>

      {/* Add child inline */}
      {addingChild && (
        <div style={{ padding: '8px 12px 8px 36px', borderTop: '1px solid var(--gray-100)', display: 'flex', gap: 8 }}>
          <input
            type="text"
            autoFocus
            placeholder="部署名"
            value={newChildName}
            onChange={(e) => setNewChildName(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') handleAddChild(); if (e.key === 'Escape') { setAddingChild(false); setNewChildName(''); } }}
            style={{ flex: 1, fontSize: 13, padding: '6px 10px', border: '1px solid var(--gray-300)', borderRadius: 6 }}
          />
          <button className="btn-sm btn-primary" onClick={handleAddChild} disabled={!newChildName.trim()}>追加</button>
          <button className="btn-sm btn-ghost" onClick={() => { setAddingChild(false); setNewChildName(''); }}>キャンセル</button>
        </div>
      )}

      {/* Child teams */}
      {expanded && children.map((child) => (
        <SubTeamRow key={child.id} team={child} onEdit={onEdit} onDelete={onDelete} />
      ))}

      {expanded && children.length === 0 && !addingChild && (
        <div style={{ padding: '8px 12px 8px 36px', borderTop: '1px solid var(--gray-100)', fontSize: 13, color: 'var(--gray-400)' }}>
          部署がありません —
          <button
            style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--primary)', fontSize: 13, padding: '0 4px' }}
            onClick={() => setAddingChild(true)}
          >＋ 部署を追加</button>
        </div>
      )}
    </div>
  );
}

// ── Main component ───────────────────────────────────────────────────
export default function TeamsAdmin() {
  const [teams, setTeams] = useState([]);
  const [directory, setDirectory] = useState([]);
  const [newParentName, setNewParentName] = useState('');
  const [modalTeam, setModalTeam] = useState(null);

  const loadTeams = async () => {
    const res = await api.adminTeams();
    setTeams(res.teams || []);
  };

  useEffect(() => {
    loadTeams().catch(console.error);
    api.adminUserMapping('').then((r) => setDirectory(r.members || [])).catch(console.error);
  }, []);

  const parents = useMemo(() => teams.filter((t) => !t.parent_id), [teams]);
  const childrenOf = useMemo(() => {
    const map = {};
    for (const t of teams) {
      if (t.parent_id) {
        if (!map[t.parent_id]) map[t.parent_id] = [];
        map[t.parent_id].push(t);
      }
    }
    return map;
  }, [teams]);
  // Standalone teams: no parent AND no children
  const standaloneTeams = useMemo(
    () => parents.filter((t) => !(childrenOf[t.id]?.length)),
    [parents, childrenOf],
  );
  const parentTeams = useMemo(
    () => parents.filter((t) => childrenOf[t.id]?.length > 0),
    [parents, childrenOf],
  );

  const handleCreateParent = async () => {
    const name = newParentName.trim();
    if (!name) return;
    await api.adminCreateTeam(name);
    setNewParentName('');
    await loadTeams();
  };

  const handleAddChild = async (parentId, name) => {
    await api.adminCreateTeam(name, parentId);
    await loadTeams();
  };

  const handleDelete = async (id) => {
    const hasChildren = childrenOf[id]?.length > 0;
    const msg = hasChildren
      ? 'このチームを削除すると、配下の部署も全て削除されます。よろしいですか？'
      : 'このチームを削除しますか？';
    if (!window.confirm(msg)) return;
    // Delete children first
    for (const child of (childrenOf[id] || [])) await api.adminDeleteTeam(child.id);
    await api.adminDeleteTeam(id);
    await loadTeams();
  };

  const openModal = (team) => setModalTeam(team);
  const closeModal = () => setModalTeam(null);

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>チーム管理</h2>
          <p className="page-subtitle">事業部・チームを作成し、配下に部署を追加できます。部署メンバーは親チームのメンバーからも引き継がれます。</p>
        </div>
      </div>

      {/* New parent team */}
      <div className="admin-form-row">
        <input
          type="text"
          placeholder="新しいチーム・事業部名"
          value={newParentName}
          onChange={(e) => setNewParentName(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleCreateParent()}
        />
        <button className="btn-primary" onClick={handleCreateParent} disabled={!newParentName.trim()}>作成</button>
      </div>

      {/* Teams with children = "parent groups" */}
      {parentTeams.length > 0 && (
        <div style={{ marginBottom: 24 }}>
          <div style={{ fontSize: 12, color: 'var(--gray-500)', fontWeight: 600, marginBottom: 8 }}>事業部 / 大チーム</div>
          <div className="admin-list">
            {parentTeams.map((parent) => (
              <ParentTeamCard
                key={parent.id}
                parent={parent}
                children={childrenOf[parent.id] || []}
                onEdit={openModal}
                onDelete={handleDelete}
                onAddChild={handleAddChild}
              />
            ))}
          </div>
        </div>
      )}

      {/* Standalone teams (no children) */}
      {standaloneTeams.length > 0 && (
        <div>
          <div style={{ fontSize: 12, color: 'var(--gray-500)', fontWeight: 600, marginBottom: 8 }}>
            {parentTeams.length > 0 ? 'その他のチーム' : 'チーム'}
          </div>
          <div className="admin-list">
            {standaloneTeams.map((team) => (
              <div key={team.id} className="admin-card">
                <div className="admin-card-header">
                  <button
                    className="admin-card-title"
                    style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer' }}
                    onClick={() => openModal(team)}
                  >
                    {team.name}
                    <span className="badge">{team.member_count ?? 0}名</span>
                  </button>
                  <div className="admin-card-actions">
                    <button className="btn-sm" onClick={() => openModal(team)}>メンバー編集</button>
                    <button className="btn-sm btn-danger" onClick={() => handleDelete(team.id)}>削除</button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {teams.length === 0 && <p className="empty-text">チームがまだありません</p>}

      {/* Member edit modal */}
      {modalTeam && (
        <MemberModal
          team={modalTeam}
          directory={directory}
          onClose={closeModal}
          onReload={loadTeams}
        />
      )}
    </div>
  );
}
