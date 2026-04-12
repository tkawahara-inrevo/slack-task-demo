import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api/client';

// ─── Shared avatar ────────────────────────────────────────────────────────────
function Avatar({ member, size = 48 }) {
  const name = member.display_name || member.real_name || member.user_id;
  if (member.avatar_url) {
    return (
      <img
        src={member.avatar_url}
        alt={name}
        style={{ width: size, height: size, borderRadius: '50%', objectFit: 'cover', flexShrink: 0 }}
      />
    );
  }
  return (
    <div style={{
      width: size, height: size, borderRadius: '50%',
      background: 'var(--primary-light)', color: 'var(--primary)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      fontSize: Math.round(size * 0.38), fontWeight: 700, flexShrink: 0,
    }}>
      {name[0]?.toUpperCase()}
    </div>
  );
}

// ─── Member card (org chart left panel) — draggable in build mode ──────────────
function MemberCard({ member, buildMode }) {
  const name = member.display_name || member.real_name || member.user_id;
  const title = member.title || '';

  const handleDragStart = (e) => {
    e.dataTransfer.setData('member', JSON.stringify({
      userId: member.user_id,
      displayName: name,
      avatarUrl: member.avatar_url || null,
      title: title || null,
    }));
    e.dataTransfer.effectAllowed = 'copy';
  };

  return (
    <div
      draggable={buildMode}
      onDragStart={buildMode ? handleDragStart : undefined}
      title={buildMode ? `${name}をドラッグしてチームに追加` : name}
      style={{
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        width: 90, flexShrink: 0,
        cursor: buildMode ? 'grab' : 'default',
        userSelect: 'none',
      }}
    >
      <div style={{ position: 'relative' }}>
        <Avatar member={member} size={52} />
        {buildMode && (
          <div style={{
            position: 'absolute', inset: 0, borderRadius: '50%',
            border: '2px dashed var(--primary)', opacity: 0.5,
            pointerEvents: 'none',
          }} />
        )}
      </div>
      <div style={{
        marginTop: 6, textAlign: 'center', background: '#fff',
        border: '1px solid var(--gray-300)', borderRadius: 6,
        padding: '4px 7px', width: '100%', boxSizing: 'border-box',
      }}>
        <div style={{ fontSize: 12, fontWeight: 600, lineHeight: 1.3, wordBreak: 'break-all' }}>{name}</div>
        {title && <div style={{ fontSize: 10, color: 'var(--gray-500)', marginTop: 1, lineHeight: 1.3 }}>{title}</div>}
      </div>
    </div>
  );
}

// ─── Team box (org chart left panel) ─────────────────────────────────────────
function OrgTeamBox({ team, members, buildMode, style }) {
  const [collapsed, setCollapsed] = useState(false);
  const isChild = !!team.parent_id;

  return (
    <div style={{
      border: `1.5px solid ${isChild ? '#d4c89a' : '#c8b96e'}`,
      borderRadius: 10,
      background: isChild ? '#f9f7f0' : '#f5f3ec',
      padding: '12px 16px',
      ...style,
    }}>
      <div
        style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', marginBottom: collapsed ? 0 : 12 }}
        onClick={() => setCollapsed(v => !v)}
      >
        <span style={{ fontSize: 11, color: '#9a8c5a', width: 14 }}>{collapsed ? '▸' : '▾'}</span>
        <span style={{ fontWeight: 700, fontSize: 13, color: '#5a4e2a' }}>{team.name}</span>
        {members.length > 0 && (
          <span style={{ fontSize: 11, background: '#e8dfa8', color: '#7a6a30', padding: '1px 7px', borderRadius: 10 }}>
            {members.length}名
          </span>
        )}
      </div>
      {!collapsed && (
        members.length === 0
          ? <p style={{ fontSize: 12, color: 'var(--gray-400)', margin: 0 }}>メンバーなし</p>
          : <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
              {members.map(m => <MemberCard key={m.user_id} member={m} buildMode={buildMode} />)}
            </div>
      )}
    </div>
  );
}

// ─── Parent section (org chart left panel) ────────────────────────────────────
function OrgParentSection({ parent, children, membersMap, buildMode }) {
  const [collapsed, setCollapsed] = useState(false);
  const parentMembers = membersMap[parent.id] || [];

  return (
    <div style={{
      border: '2px solid #c8b96e', borderRadius: 12, background: '#fdf9ee',
      padding: '14px 18px', marginBottom: 20,
    }}>
      <div
        style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', marginBottom: collapsed ? 0 : 14 }}
        onClick={() => setCollapsed(v => !v)}
      >
        <span style={{ fontSize: 12, color: '#9a8c5a', width: 16 }}>{collapsed ? '▸' : '▾'}</span>
        <span style={{ fontWeight: 800, fontSize: 15, color: '#3d3520' }}>{parent.name}</span>
        {children.length > 0 && <span style={{ fontSize: 11, color: '#9a8c5a' }}>{children.length}部署</span>}
      </div>
      {!collapsed && (
        <>
          {parentMembers.length > 0 && (
            <div style={{ marginBottom: 12 }}>
              <div style={{ fontSize: 11, color: '#9a8c5a', fontWeight: 600, marginBottom: 8 }}>共通メンバー</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
                {parentMembers.map(m => <MemberCard key={m.user_id} member={m} buildMode={buildMode} />)}
              </div>
            </div>
          )}
          {children.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 14 }}>
              {children.map(child => (
                <OrgTeamBox
                  key={child.id}
                  team={child}
                  members={membersMap[child.id] || []}
                  buildMode={buildMode}
                  style={{ flex: '1 1 240px', minWidth: 240 }}
                />
              ))}
            </div>
          )}
          {parentMembers.length === 0 && children.length === 0 && (
            <p style={{ fontSize: 12, color: 'var(--gray-400)', margin: 0 }}>メンバーなし</p>
          )}
        </>
      )}
    </div>
  );
}

// ─── Working team card (right panel) — droppable ──────────────────────────────
function WorkingTeamCard({ team, members, onDrop, onRemoveMember, onDelete, onRename }) {
  const [dragOver, setDragOver] = useState(false);
  const [editing, setEditing] = useState(!team.id); // new team starts in edit mode
  const [nameVal, setNameVal] = useState(team.name);
  const inputRef = useRef(null);

  const handleDragOver = (e) => { e.preventDefault(); setDragOver(true); };
  const handleDragLeave = (e) => {
    // only clear if leaving the card entirely (not entering a child)
    if (!e.currentTarget.contains(e.relatedTarget)) setDragOver(false);
  };
  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    try {
      const data = JSON.parse(e.dataTransfer.getData('member'));
      if (data?.userId) onDrop(team.id, data);
    } catch (_) {}
  };

  const commitRename = () => {
    setEditing(false);
    const v = nameVal.trim();
    if (v && v !== team.name) onRename(team.id, v);
    else setNameVal(team.name);
  };

  return (
    <div
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
      style={{
        border: dragOver ? '2px dashed var(--primary)' : '1.5px solid var(--gray-300)',
        borderRadius: 10,
        background: dragOver ? 'var(--primary-light)' : '#fff',
        padding: '12px 14px',
        transition: 'border 0.12s, background 0.12s',
        minHeight: 70,
      }}
    >
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
        {editing ? (
          <input
            ref={inputRef}
            value={nameVal}
            onChange={e => setNameVal(e.target.value)}
            onBlur={commitRename}
            onKeyDown={e => {
              if (e.key === 'Enter') { e.preventDefault(); commitRename(); }
              if (e.key === 'Escape') { setEditing(false); setNameVal(team.name); }
            }}
            autoFocus
            placeholder="チーム名を入力"
            style={{
              flex: 1, fontSize: 14, fontWeight: 700,
              border: 'none', borderBottom: '1.5px solid var(--primary)',
              outline: 'none', background: 'transparent', padding: '2px 0',
            }}
          />
        ) : (
          <span
            onClick={() => { setEditing(true); setTimeout(() => inputRef.current?.select(), 0); }}
            title="クリックで名前を編集"
            style={{ flex: 1, fontWeight: 700, fontSize: 14, cursor: 'text', color: 'var(--gray-800)' }}
          >
            {team.name}
          </span>
        )}
        <span style={{
          fontSize: 11, color: 'var(--gray-500)', background: 'var(--gray-100)',
          padding: '2px 7px', borderRadius: 10,
        }}>{members.length}名</span>
        <button
          onClick={() => onDelete(team.id)}
          title="このチームを削除"
          style={{
            background: 'none', border: 'none', cursor: 'pointer',
            color: 'var(--gray-400)', fontSize: 18, lineHeight: 1, padding: 0,
          }}
        >×</button>
      </div>

      {/* Member list */}
      {members.length === 0 ? (
        <div style={{
          textAlign: 'center', fontSize: 12,
          color: dragOver ? 'var(--primary)' : 'var(--gray-400)',
          fontWeight: dragOver ? 600 : 400,
          padding: '10px 0',
          border: `1.5px dashed ${dragOver ? 'var(--primary)' : 'var(--gray-200)'}`,
          borderRadius: 7,
          transition: 'all 0.12s',
        }}>
          {dragOver ? 'ここに追加' : 'メンバーをここにドロップ'}
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
          {members.map((m, i) => {
            const mName = m.display_name || m.real_name || m.user_id;
            const mTitle = m.title || '';
            return (
              <div
                key={m.user_id}
                style={{
                  display: 'flex', alignItems: 'center', gap: 8,
                  padding: '4px 8px', borderRadius: 7,
                  background: i === 0 ? '#fffbeb' : 'var(--gray-50)',
                  border: i === 0 ? '1px solid #fde68a' : '1px solid transparent',
                }}
              >
                <Avatar member={m} size={28} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {mName}
                  </div>
                  {mTitle && (
                    <div style={{ fontSize: 10, color: 'var(--gray-500)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {mTitle}
                    </div>
                  )}
                </div>
                {i === 0 && (
                  <span style={{
                    fontSize: 10, color: '#92400e', background: '#fde68a',
                    padding: '1px 6px', borderRadius: 10, flexShrink: 0,
                  }}>リーダー</span>
                )}
                <button
                  onClick={() => onRemoveMember(team.id, m.user_id)}
                  title={`${mName}をチームから外す`}
                  style={{
                    background: 'none', border: 'none', cursor: 'pointer',
                    color: 'var(--gray-400)', fontSize: 15, lineHeight: 1,
                    padding: '0 2px', flexShrink: 0,
                  }}
                >×</button>
              </div>
            );
          })}
          {/* Drop hint at bottom when already has members */}
          {dragOver && (
            <div style={{
              textAlign: 'center', fontSize: 11, color: 'var(--primary)',
              padding: '6px 0', fontWeight: 600,
            }}>＋ここに追加</div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Main OrgChart page ────────────────────────────────────────────────────────
export default function OrgChart() {
  const [teams, setTeams] = useState([]);
  const [membersMap, setMembersMap] = useState({});   // teamId → [memberObj]
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [buildMode, setBuildMode] = useState(false);
  const [saving, setSaving] = useState(false);

  const load = () => {
    api.orgChart()
      .then(r => { setTeams(r.teams || []); setMembersMap(r.membersMap || {}); })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  // Derived structure
  const parents = useMemo(() => teams.filter(t => !t.parent_id), [teams]);
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
  const standalones = useMemo(() => parents.filter(p => !childrenOf[p.id]?.length), [parents, childrenOf]);
  const parentGroups = useMemo(() => parents.filter(p => childrenOf[p.id]?.length > 0), [parents, childrenOf]);

  // Search filter (for left panel org chart)
  const filteredMembersMap = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return membersMap;
    const out = {};
    for (const [id, members] of Object.entries(membersMap)) {
      out[id] = members.filter(m =>
        [m.display_name, m.real_name, m.title].filter(Boolean).some(v => v.toLowerCase().includes(q))
      );
    }
    return out;
  }, [membersMap, search]);

  // ── Build-mode handlers ─────────────────────────────────────────────────────
  const handleDrop = async (teamId, memberData) => {
    const { userId } = memberData;
    // Optimistic: add locally
    const existing = membersMap[teamId] || [];
    if (existing.find(m => m.user_id === userId)) return; // already in team

    const newMember = {
      user_id: userId,
      display_name: memberData.displayName,
      avatar_url: memberData.avatarUrl,
      title: memberData.title || '',
    };
    setMembersMap(prev => ({
      ...prev,
      [teamId]: [...(prev[teamId] || []), newMember],
    }));

    setSaving(true);
    try {
      await api.adminAddTeamMember(teamId, userId);
    } catch (e) {
      console.error(e);
      // Rollback
      setMembersMap(prev => ({
        ...prev,
        [teamId]: (prev[teamId] || []).filter(m => m.user_id !== userId),
      }));
    } finally {
      setSaving(false);
    }
  };

  const handleRemoveMember = async (teamId, userId) => {
    const prev = membersMap[teamId] || [];
    setMembersMap(prevMap => ({
      ...prevMap,
      [teamId]: prev.filter(m => m.user_id !== userId),
    }));

    setSaving(true);
    try {
      await api.adminRemoveTeamMember(teamId, userId);
    } catch (e) {
      console.error(e);
      setMembersMap(prevMap => ({ ...prevMap, [teamId]: prev }));
    } finally {
      setSaving(false);
    }
  };

  const handleCreateTeam = async () => {
    const name = `新しいチーム ${teams.filter(t => !t.parent_id).length + 1}`;
    setSaving(true);
    try {
      const created = await api.adminCreateTeam(name);
      const newTeam = { id: created.team.id, name: created.team.name, parent_id: null };
      setTeams(prev => [...prev, newTeam]);
      setMembersMap(prev => ({ ...prev, [created.id]: [] }));
    } catch (e) {
      console.error(e);
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteTeam = async (teamId) => {
    if (!window.confirm('このチームを削除しますか？メンバー情報も削除されます。')) return;
    const prevTeams = teams;
    const prevMap = membersMap;
    setTeams(prev => prev.filter(t => t.id !== teamId));
    setMembersMap(prev => { const n = { ...prev }; delete n[teamId]; return n; });

    setSaving(true);
    try {
      await api.adminDeleteTeam(teamId);
    } catch (e) {
      console.error(e);
      setTeams(prevTeams);
      setMembersMap(prevMap);
    } finally {
      setSaving(false);
    }
  };

  const handleRenameTeam = async (teamId, newName) => {
    setTeams(prev => prev.map(t => t.id === teamId ? { ...t, name: newName } : t));
    setSaving(true);
    try {
      await api.adminUpdateTeam(teamId, newName);
    } catch (e) {
      console.error(e);
      load(); // reload to sync
    } finally {
      setSaving(false);
    }
  };

  // All standalone teams shown in the builder panel
  // (leaf teams: no children, which are the "working" teams)
  const builderTeams = useMemo(
    () => teams.filter(t => !childrenOf[t.id]?.length),
    [teams, childrenOf],
  );

  return (
    <div className="workload-page">
      {/* ── Page header ── */}
      <div className="page-header" style={{ marginBottom: 16 }}>
        <div>
          <h1>組織図</h1>
          <p className="page-subtitle">チームと部署のメンバー構成を表示します。</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {saving && <span style={{ fontSize: 12, color: 'var(--gray-400)' }}>保存中…</span>}
          <button
            onClick={() => setBuildMode(v => !v)}
            className={buildMode ? 'btn btn-primary' : 'btn btn-secondary'}
            style={{ display: 'flex', alignItems: 'center', gap: 6 }}
          >
            {buildMode ? '✓ 編集完了' : '✏️ チームを組む'}
          </button>
        </div>
      </div>

      {buildMode && (
        <div style={{
          background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 8,
          padding: '8px 14px', fontSize: 13, color: '#92400e', marginBottom: 16,
        }}>
          左の組織図からメンバーカードをドラッグして、右のチームカードにドロップするとメンバーを追加できます。
          最初に追加したメンバーが「リーダー」になります。
        </div>
      )}

      {loading ? (
        <p className="empty-text">読み込み中…</p>
      ) : teams.length === 0 ? (
        <p className="empty-text">チームがまだ作成されていません。</p>
      ) : buildMode ? (
        /* ── Build mode: split layout ─────────────────────────────────────── */
        <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start' }}>

          {/* Left: org chart reference */}
          <div style={{ flex: '0 0 44%', minWidth: 0 }}>
            <div style={{
              fontSize: 12, fontWeight: 700, color: 'var(--gray-500)',
              textTransform: 'uppercase', letterSpacing: 0.8,
              marginBottom: 12, padding: '0 2px',
            }}>
              組織図（ドラッグ元）
            </div>

            <div style={{ marginBottom: 12 }}>
              <input
                type="text"
                placeholder="名前・役職で検索…"
                value={search}
                onChange={e => setSearch(e.target.value)}
                style={{
                  fontSize: 13, padding: '7px 12px',
                  border: '1px solid var(--gray-300)', borderRadius: 8,
                  width: '100%', boxSizing: 'border-box', outline: 'none',
                }}
              />
            </div>

            <div style={{ maxHeight: 'calc(100vh - 260px)', overflowY: 'auto', paddingRight: 4 }}>
              {parentGroups.map(parent => (
                <OrgParentSection
                  key={parent.id}
                  parent={parent}
                  children={childrenOf[parent.id] || []}
                  membersMap={filteredMembersMap}
                  buildMode
                />
              ))}
              {standalones.length > 0 && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
                  {standalones.map(team => (
                    <OrgTeamBox
                      key={team.id}
                      team={team}
                      members={filteredMembersMap[team.id] || []}
                      buildMode
                    />
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Right: team builder */}
          <div style={{ flex: '1 1 0', minWidth: 0 }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
              <div style={{
                fontSize: 12, fontWeight: 700, color: 'var(--gray-500)',
                textTransform: 'uppercase', letterSpacing: 0.8,
              }}>
                チーム編集（ドロップ先）
              </div>
              <button
                onClick={handleCreateTeam}
                className="btn btn-secondary"
                style={{ fontSize: 13, padding: '5px 12px' }}
              >
                ＋ チームを追加
              </button>
            </div>

            <div style={{
              display: 'flex', flexDirection: 'column', gap: 12,
              maxHeight: 'calc(100vh - 260px)', overflowY: 'auto', paddingRight: 4,
            }}>
              {builderTeams.length === 0 ? (
                <p style={{ fontSize: 13, color: 'var(--gray-400)', textAlign: 'center', padding: 24 }}>
                  「＋ チームを追加」でチームを作成してメンバーをドロップしてください。
                </p>
              ) : (
                builderTeams.map(team => (
                  <WorkingTeamCard
                    key={team.id}
                    team={team}
                    members={membersMap[team.id] || []}
                    onDrop={handleDrop}
                    onRemoveMember={handleRemoveMember}
                    onDelete={handleDeleteTeam}
                    onRename={handleRenameTeam}
                  />
                ))
              )}
            </div>
          </div>
        </div>

      ) : (
        /* ── View mode: regular org chart ─────────────────────────────────── */
        <>
          <div style={{ marginBottom: 20 }}>
            <input
              type="text"
              placeholder="名前・役職で検索…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              style={{
                fontSize: 14, padding: '8px 14px',
                border: '1px solid var(--gray-300)', borderRadius: 8,
                width: 260, outline: 'none',
              }}
            />
          </div>

          {parentGroups.map(parent => (
            <OrgParentSection
              key={parent.id}
              parent={parent}
              children={childrenOf[parent.id] || []}
              membersMap={filteredMembersMap}
              buildMode={false}
            />
          ))}

          {standalones.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
              {standalones.map(team => (
                <OrgTeamBox
                  key={team.id}
                  team={team}
                  members={filteredMembersMap[team.id] || []}
                  buildMode={false}
                  style={{ flex: '1 1 300px', minWidth: 300 }}
                />
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}
