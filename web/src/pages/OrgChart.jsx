import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api/client';

// ─── Role definitions ────────────────────────────────────────────────────────
const ROLES = [
  { value: 'dept_leader',  label: '部署リーダー',       color: '#6d28d9', bg: '#ede9fe' },
  { value: 'team_leader',  label: 'チームリーダー',     color: '#1d4ed8', bg: '#dbeafe' },
  { value: 'sub_leader',   label: 'サブリーダー',       color: '#0369a1', bg: '#e0f2fe' },
  { value: 'member',       label: 'メンバー',           color: '#374151', bg: '#f3f4f6' },
];
const ROLE_MAP = Object.fromEntries(ROLES.map(r => [r.value, r]));
const roleOf = (v) => ROLE_MAP[v] || ROLE_MAP['member'];

// ─── Avatar ──────────────────────────────────────────────────────────────────
function Avatar({ member, size = 48 }) {
  const name = member.display_name || member.real_name || member.user_id;
  if (member.avatar_url) {
    return (
      <img src={member.avatar_url} alt={name}
        style={{ width: size, height: size, borderRadius: '50%', objectFit: 'cover', flexShrink: 0 }} />
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

// ─── Member card: view mode (existing org chart style) ───────────────────────
function MemberCardView({ member }) {
  const name = member.display_name || member.real_name || member.user_id;
  const title = member.title || '';
  const role = roleOf(member.role);
  const showRoleBadge = member.role && member.role !== 'member';
  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', width: 90, flexShrink: 0 }}>
      <Avatar member={member} size={54} />
      <div style={{
        marginTop: 6, textAlign: 'center', background: '#fff',
        border: '1px solid var(--gray-300)', borderRadius: 6,
        padding: '4px 7px', width: '100%', boxSizing: 'border-box',
      }}>
        <div style={{ fontSize: 12, fontWeight: 600, lineHeight: 1.3, wordBreak: 'break-all' }}>{name}</div>
        {title && <div style={{ fontSize: 10, color: 'var(--gray-500)', marginTop: 1 }}>{title}</div>}
        {showRoleBadge && (
          <div style={{
            marginTop: 4, fontSize: 9, fontWeight: 700,
            color: role.color, background: role.bg,
            borderRadius: 8, padding: '1px 5px', display: 'inline-block',
          }}>{role.label}</div>
        )}
      </div>
    </div>
  );
}

// ─── Member chip: build mode (compact row, draggable + role selector) ────────
function MemberChip({ member, isDragging, onDragStart, onRemove, onRoleChange, showRemove = false }) {
  const name = member.display_name || member.real_name || member.user_id;
  const title = member.title || '';
  const role = roleOf(member.role);
  const [roleOpen, setRoleOpen] = useState(false);
  const dropRef = useRef(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    if (!roleOpen) return;
    const handler = (e) => { if (!dropRef.current?.contains(e.target)) setRoleOpen(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [roleOpen]);

  return (
    <div
      draggable
      onDragStart={onDragStart}
      style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '6px 8px', borderRadius: 8,
        background: role.bg,
        border: `1px solid ${role.color}30`,
        cursor: 'grab',
        opacity: isDragging ? 0.4 : 1,
        userSelect: 'none',
        transition: 'opacity 0.1s',
        position: 'relative',
      }}
    >
      <Avatar member={member} size={30} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 13, fontWeight: 600, lineHeight: 1.3, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {name}
        </div>
        {title && (
          <div style={{ fontSize: 10, color: 'var(--gray-500)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
            {title}
          </div>
        )}
      </div>

      {/* Role badge — click to open dropdown */}
      <div ref={dropRef} style={{ position: 'relative', flexShrink: 0 }}>
        <button
          onMouseDown={e => { e.stopPropagation(); e.preventDefault(); }}
          onClick={e => { e.stopPropagation(); if (onRoleChange) setRoleOpen(v => !v); }}
          title="役割を変更"
          style={{
            fontSize: 10, fontWeight: 600,
            color: role.color, background: role.bg,
            border: `1px solid ${role.color}60`,
            borderRadius: 10, padding: '2px 7px',
            cursor: onRoleChange ? 'pointer' : 'default',
            whiteSpace: 'nowrap',
          }}
        >
          {role.label} {onRoleChange ? '▾' : ''}
        </button>

        {roleOpen && (
          <div style={{
            position: 'absolute', right: 0, top: '100%', marginTop: 4, zIndex: 100,
            background: '#fff', border: '1px solid var(--gray-200)',
            borderRadius: 8, boxShadow: '0 4px 12px rgba(0,0,0,0.12)',
            minWidth: 140, overflow: 'hidden',
          }}>
            {ROLES.map(r => (
              <button
                key={r.value}
                onMouseDown={e => e.stopPropagation()}
                onClick={e => {
                  e.stopPropagation();
                  setRoleOpen(false);
                  if (r.value !== member.role) onRoleChange(r.value);
                }}
                style={{
                  display: 'block', width: '100%', textAlign: 'left',
                  padding: '8px 12px', fontSize: 13, border: 'none', cursor: 'pointer',
                  background: r.value === member.role ? r.bg : '#fff',
                  color: r.value === member.role ? r.color : 'var(--gray-700)',
                  fontWeight: r.value === member.role ? 700 : 400,
                }}
              >
                <span style={{ display: 'inline-block', width: 8, height: 8, borderRadius: '50%', background: r.color, marginRight: 6 }} />
                {r.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {showRemove && (
        <button
          onClick={e => { e.stopPropagation(); onRemove(); }}
          title="チームから外す"
          style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-400)', fontSize: 16, lineHeight: 1, padding: '0 2px', flexShrink: 0 }}
        >×</button>
      )}
    </div>
  );
}

// ─── View mode: member card (with role badge) ────────────────────────────────
function OrgTeamBox({ team, members, style }) {
  const [collapsed, setCollapsed] = useState(false);
  const isChild = !!team.parent_id;
  return (
    <div style={{
      border: `1.5px solid ${isChild ? '#d4c89a' : '#c8b96e'}`,
      borderRadius: 10, background: isChild ? '#f9f7f0' : '#f5f3ec',
      padding: '12px 16px', ...style,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', marginBottom: collapsed ? 0 : 12 }}
        onClick={() => setCollapsed(v => !v)}>
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
              {members.map(m => <MemberCardView key={m.user_id} member={m} />)}
            </div>
      )}
    </div>
  );
}

// ─── View mode: parent section ───────────────────────────────────────────────
function OrgParentSection({ parent, children, membersMap }) {
  const [collapsed, setCollapsed] = useState(false);
  const parentMembers = membersMap[parent.id] || [];
  return (
    <div style={{ border: '2px solid #c8b96e', borderRadius: 12, background: '#fdf9ee', padding: '14px 18px', marginBottom: 20 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', marginBottom: collapsed ? 0 : 14 }}
        onClick={() => setCollapsed(v => !v)}>
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
                {parentMembers.map(m => <MemberCardView key={m.user_id} member={m} />)}
              </div>
            </div>
          )}
          {children.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 14 }}>
              {children.map(child => (
                <OrgTeamBox key={child.id} team={child} members={membersMap[child.id] || []} style={{ flex: '1 1 260px', minWidth: 260 }} />
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

// ─── Build mode: droppable team card ─────────────────────────────────────────
function EditableTeamCard({ team, members, dragState, onDragStart, onDrop, onRemoveMember, onRoleChange, onDelete, onRename }) {
  const [dragOver, setDragOver] = useState(false);
  const [editing, setEditing] = useState(false);
  const [nameVal, setNameVal] = useState(team.name);
  const inputRef = useRef(null);
  const [collapsed, setCollapsed] = useState(false);

  const handleDragOver = (e) => { e.preventDefault(); setDragOver(true); };
  const handleDragLeave = (e) => { if (!e.currentTarget.contains(e.relatedTarget)) setDragOver(false); };
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
        borderRadius: 12,
        background: dragOver ? 'var(--primary-light)' : '#fff',
        padding: '14px 16px',
        transition: 'border 0.1s, background 0.1s',
        display: 'flex', flexDirection: 'column', gap: 0,
      }}
    >
      {/* Card header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <span
          onClick={() => setCollapsed(v => !v)}
          style={{ fontSize: 11, color: 'var(--gray-400)', cursor: 'pointer', width: 14 }}
        >{collapsed ? '▸' : '▾'}</span>

        {editing ? (
          <input ref={inputRef} value={nameVal}
            onChange={e => setNameVal(e.target.value)}
            onBlur={commitRename}
            onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); commitRename(); } if (e.key === 'Escape') { setEditing(false); setNameVal(team.name); } }}
            autoFocus
            style={{ flex: 1, fontSize: 14, fontWeight: 700, border: 'none', borderBottom: '2px solid var(--primary)', outline: 'none', background: 'transparent', padding: '2px 0' }}
          />
        ) : (
          <span onClick={() => setEditing(true)} title="クリックで名前を編集"
            style={{ flex: 1, fontWeight: 700, fontSize: 14, cursor: 'text', color: 'var(--gray-800)' }}>
            {team.name}
          </span>
        )}

        <span style={{ fontSize: 11, color: 'var(--gray-500)', background: 'var(--gray-100)', padding: '2px 8px', borderRadius: 10 }}>
          {members.length}名
        </span>
        <button onClick={() => onDelete(team.id)} title="チームを削除"
          style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-300)', fontSize: 18, lineHeight: 1, padding: 0 }}>×</button>
      </div>

      {/* Member list */}
      {!collapsed && (
        <>
          {members.length === 0 ? (
            <div style={{
              border: `1.5px dashed ${dragOver ? 'var(--primary)' : 'var(--gray-200)'}`,
              borderRadius: 8, padding: '16px 12px', textAlign: 'center',
              fontSize: 12, color: dragOver ? 'var(--primary)' : 'var(--gray-400)',
              fontWeight: dragOver ? 600 : 400, transition: 'all 0.1s',
            }}>
              {dragOver ? 'ここに追加' : 'メンバーをドロップ'}
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {members.map((m) => (
                <MemberChip
                  key={m.user_id}
                  member={m}
                  isDragging={dragState?.userId === m.user_id}
                  onDragStart={(e) => onDragStart(e, m)}
                  onRemove={() => onRemoveMember(team.id, m.user_id)}
                  onRoleChange={(role) => onRoleChange(team.id, m.user_id, role)}
                  showRemove
                />
              ))}
              {dragOver && (
                <div style={{ textAlign: 'center', fontSize: 11, color: 'var(--primary)', fontWeight: 600, padding: '4px 0' }}>
                  ＋ここに追加
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

// ─── Build mode: unassigned members source panel ─────────────────────────────
function UnassignedPanel({ members, dragState, onDragStart }) {
  const [collapsed, setCollapsed] = useState(false);
  const [search, setSearch] = useState('');
  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return members;
    return members.filter(m =>
      [m.display_name, m.real_name, m.title].filter(Boolean).some(v => v.toLowerCase().includes(q))
    );
  }, [members, search]);

  return (
    <div style={{
      border: '1.5px solid var(--gray-300)', borderRadius: 12,
      background: '#f8f8f8', padding: '14px 16px', marginBottom: 24,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: collapsed ? 0 : 12 }}>
        <span onClick={() => setCollapsed(v => !v)}
          style={{ fontSize: 12, color: 'var(--gray-500)', cursor: 'pointer', width: 16 }}>
          {collapsed ? '▸' : '▾'}
        </span>
        <span style={{ fontWeight: 700, fontSize: 14, color: 'var(--gray-700)' }}>未所属</span>
        <span style={{ fontSize: 11, background: 'var(--gray-200)', color: 'var(--gray-600)', padding: '2px 8px', borderRadius: 10 }}>
          {members.length}名
        </span>
        {!collapsed && (
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="絞り込み…"
            onClick={e => e.stopPropagation()}
            style={{
              marginLeft: 'auto', fontSize: 12, padding: '4px 10px',
              border: '1px solid var(--gray-300)', borderRadius: 6,
              outline: 'none', width: 160,
            }}
          />
        )}
      </div>
      {!collapsed && (
        members.length === 0 ? (
          <p style={{ fontSize: 12, color: 'var(--gray-400)', margin: 0 }}>全員いずれかのチームに所属しています</p>
        ) : (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
            {filtered.map(m => (
              <MemberChip
                key={m.user_id}
                member={m}
                isDragging={dragState?.userId === m.user_id}
                onDragStart={(e) => onDragStart(e, m)}
                showRemove={false}
              />
            ))}
          </div>
        )
      )}
    </div>
  );
}

// ─── Main ─────────────────────────────────────────────────────────────────────
export default function OrgChart() {
  const [teams, setTeams] = useState([]);
  const [membersMap, setMembersMap] = useState({});
  const [unassigned, setUnassigned] = useState([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [buildMode, setBuildMode] = useState(false);
  const [saving, setSaving] = useState(false);
  const [dragState, setDragState] = useState(null); // { userId, displayName, ... }

  const load = () => {
    api.orgChart()
      .then(r => {
        setTeams(r.teams || []);
        setMembersMap(r.membersMap || {});
        setUnassigned(r.unassigned || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  // Structure
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

  // Search filter (view mode)
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

  // All unassigned, kept in sync as members are added/removed
  const liveUnassigned = useMemo(() => {
    const assigned = new Set(
      Object.values(membersMap).flatMap(ms => ms.map(m => m.user_id))
    );
    return unassigned.filter(u => !assigned.has(u.user_id));
  }, [membersMap, unassigned]);

  // ── Drag ──────────────────────────────────────────────────────────────────
  const handleDragStart = (e, member) => {
    const data = {
      userId: member.user_id,
      displayName: member.display_name || member.real_name || member.user_id,
      avatarUrl: member.avatar_url || null,
      title: member.title || null,
    };
    e.dataTransfer.setData('member', JSON.stringify(data));
    e.dataTransfer.effectAllowed = 'copy';
    setDragState(data);
  };

  // ── Drop: add member to team ───────────────────────────────────────────────
  const handleDrop = async (teamId, memberData) => {
    setDragState(null);
    const { userId } = memberData;
    const existing = membersMap[teamId] || [];
    if (existing.find(m => m.user_id === userId)) return;

    const newMember = {
      user_id: userId,
      display_name: memberData.displayName,
      avatar_url: memberData.avatarUrl,
      title: memberData.title || '',
    };
    setMembersMap(prev => ({ ...prev, [teamId]: [...(prev[teamId] || []), newMember] }));

    setSaving(true);
    try {
      await api.adminAddTeamMember(teamId, userId);
    } catch (e) {
      console.error(e);
      setMembersMap(prev => ({ ...prev, [teamId]: (prev[teamId] || []).filter(m => m.user_id !== userId) }));
    } finally {
      setSaving(false);
    }
  };

  // ── Remove member from team ────────────────────────────────────────────────
  const handleRemoveMember = async (teamId, userId) => {
    const prev = membersMap[teamId] || [];
    setMembersMap(prevMap => ({ ...prevMap, [teamId]: prev.filter(m => m.user_id !== userId) }));
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

  // ── Create team ────────────────────────────────────────────────────────────
  const handleCreateTeam = async () => {
    const name = `新しいチーム ${teams.length + 1}`;
    setSaving(true);
    try {
      const res = await api.adminCreateTeam(name);
      const newTeam = { id: res.team.id, name: res.team.name, parent_id: null };
      setTeams(prev => [...prev, newTeam]);
      setMembersMap(prev => ({ ...prev, [res.team.id]: [] }));
    } catch (e) {
      console.error(e);
    } finally {
      setSaving(false);
    }
  };

  // ── Delete team ────────────────────────────────────────────────────────────
  const handleDeleteTeam = async (teamId) => {
    if (!window.confirm('このチームを削除しますか？')) return;
    const prevTeams = teams;
    const prevMap = membersMap;
    setTeams(prev => prev.filter(t => t.id !== teamId && t.parent_id !== teamId));
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

  // ── Rename team ────────────────────────────────────────────────────────────
  const handleRenameTeam = async (teamId, newName) => {
    setTeams(prev => prev.map(t => t.id === teamId ? { ...t, name: newName } : t));
    setSaving(true);
    try {
      await api.adminUpdateTeam(teamId, newName);
    } catch (e) {
      console.error(e);
      load();
    } finally {
      setSaving(false);
    }
  };

  const handleRoleChange = async (teamId, userId, role) => {
    // Optimistic update
    setMembersMap(prev => {
      const members = (prev[teamId] || []).map(m =>
        m.user_id === userId ? { ...m, role } : m
      );
      // Re-sort by role order then added_at
      const ORDER = { dept_leader: 1, team_leader: 2, sub_leader: 3, member: 4 };
      members.sort((a, b) => (ORDER[a.role] ?? 4) - (ORDER[b.role] ?? 4));
      return { ...prev, [teamId]: members };
    });
    setSaving(true);
    try {
      await api.adminUpdateTeamMemberRole(teamId, userId, role);
    } catch (e) {
      console.error(e);
      load(); // rollback via reload
    } finally {
      setSaving(false);
    }
  };

  const [buildSearch, setBuildSearch] = useState('');

  // Teams shown in build mode grid — filter by team name or member name
  const filteredBuildTeams = useMemo(() => {
    const q = buildSearch.trim().toLowerCase();
    if (!q) return teams;
    return teams.filter(t => {
      if (t.name.toLowerCase().includes(q)) return true;
      return (membersMap[t.id] || []).some(m =>
        [m.display_name, m.real_name, m.title].filter(Boolean).some(v => v.toLowerCase().includes(q))
      );
    });
  }, [teams, membersMap, buildSearch]);

  return (
    <div className="workload-page">
      {/* ── Header ── */}
      <div className="page-header" style={{ marginBottom: 16 }}>
        <div>
          <h1>組織図</h1>
          <p className="page-subtitle">チームと部署のメンバー構成を表示・編集できます。</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {saving && <span style={{ fontSize: 12, color: 'var(--gray-400)' }}>保存中…</span>}
          <button
            onClick={() => { setBuildMode(v => !v); setSearch(''); setBuildSearch(''); }}
            className={buildMode ? 'btn btn-primary' : 'btn btn-secondary'}
          >
            {buildMode ? '✓ 編集完了' : '✏️ チームを組む'}
          </button>
        </div>
      </div>

      {loading ? (
        <p className="empty-text">読み込み中…</p>
      ) : buildMode ? (
        /* ══════════════ BUILD MODE ══════════════ */
        <div onDragEnd={() => setDragState(null)}>
          {/* Hint banner */}
          <div style={{
            background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 8,
            padding: '8px 14px', fontSize: 13, color: '#92400e', marginBottom: 20,
            display: 'flex', alignItems: 'center', gap: 8,
          }}>
            <span>💡</span>
            <span>メンバーチップをドラッグしてチームカードにドロップするとメンバーを追加できます。最初に追加したメンバーが「リーダー」になります。</span>
          </div>

          {/* Unassigned section (drag source) */}
          {liveUnassigned.length > 0 && (
            <UnassignedPanel
              members={liveUnassigned}
              dragState={dragState}
              onDragStart={handleDragStart}
            />
          )}

          {/* Team grid */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 14 }}>
            <input
              type="text"
              placeholder="チーム名・メンバー名で絞り込み…"
              value={buildSearch}
              onChange={e => setBuildSearch(e.target.value)}
              style={{
                flex: 1, maxWidth: 300, fontSize: 13, padding: '7px 12px',
                border: '1px solid var(--gray-300)', borderRadius: 8, outline: 'none',
              }}
            />
            <span style={{ fontSize: 13, color: 'var(--gray-500)' }}>
              {filteredBuildTeams.length} / {teams.length}チーム
            </span>
            <button onClick={handleCreateTeam} className="btn btn-secondary" style={{ fontSize: 13, padding: '6px 14px', marginLeft: 'auto' }}>
              ＋ チームを追加
            </button>
          </div>

          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
            gap: 16,
          }}>
            {filteredBuildTeams.map(team => (
              <EditableTeamCard
                key={team.id}
                team={team}
                members={membersMap[team.id] || []}
                dragState={dragState}
                onDragStart={handleDragStart}
                onDrop={handleDrop}
                onRemoveMember={handleRemoveMember}
                onRoleChange={handleRoleChange}
                onDelete={handleDeleteTeam}
                onRename={handleRenameTeam}
              />
            ))}
          </div>
        </div>

      ) : (
        /* ══════════════ VIEW MODE ══════════════ */
        <>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 20 }}>
            <input
              type="text" placeholder="名前・役職で検索…" value={search}
              onChange={e => setSearch(e.target.value)}
              style={{ fontSize: 14, padding: '8px 14px', border: '1px solid var(--gray-300)', borderRadius: 8, width: 260, outline: 'none' }}
            />
            {liveUnassigned.length > 0 && (
              <span style={{ fontSize: 12, color: 'var(--gray-500)' }}>
                未所属: {liveUnassigned.length}名 — 「チームを組む」で割り当てできます
              </span>
            )}
          </div>

          {parentGroups.map(parent => (
            <OrgParentSection key={parent.id} parent={parent} children={childrenOf[parent.id] || []} membersMap={filteredMembersMap} />
          ))}

          {standalones.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
              {standalones.map(team => (
                <OrgTeamBox key={team.id} team={team} members={filteredMembersMap[team.id] || []} style={{ flex: '1 1 300px', minWidth: 300 }} />
              ))}
            </div>
          )}

          {/* Unassigned section in view mode */}
          {liveUnassigned.length > 0 && (
            <div style={{ marginTop: 28, border: '1.5px solid var(--gray-200)', borderRadius: 12, background: '#f8f8f8', padding: '14px 18px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                <span style={{ fontWeight: 700, fontSize: 14, color: 'var(--gray-600)' }}>未所属</span>
                <span style={{ fontSize: 11, background: 'var(--gray-200)', color: 'var(--gray-600)', padding: '2px 8px', borderRadius: 10 }}>
                  {liveUnassigned.length}名
                </span>
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12 }}>
                {liveUnassigned.map(m => <MemberCardView key={m.user_id} member={m} />)}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
