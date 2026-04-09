import { useEffect, useMemo, useState } from 'react';
import { api } from '../api/client';

function MemberCard({ member }) {
  const name = member.display_name || member.real_name || member.user_id;
  const title = member.title || '';
  const avatar = member.avatar_url;

  return (
    <div style={{
      display: 'flex', flexDirection: 'column', alignItems: 'center',
      width: 100, flexShrink: 0,
    }}>
      {avatar ? (
        <img
          src={avatar}
          alt={name}
          style={{ width: 64, height: 64, borderRadius: '50%', objectFit: 'cover', border: '2px solid var(--gray-200)' }}
        />
      ) : (
        <div style={{
          width: 64, height: 64, borderRadius: '50%',
          background: 'var(--primary-light)', color: 'var(--primary)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: 22, fontWeight: 700, border: '2px solid var(--gray-200)',
        }}>
          {name[0]?.toUpperCase()}
        </div>
      )}
      <div style={{
        marginTop: 8, textAlign: 'center',
        background: '#fff', border: '1px solid var(--gray-300)',
        borderRadius: 6, padding: '5px 8px', width: '100%', boxSizing: 'border-box',
      }}>
        <div style={{ fontSize: 13, fontWeight: 600, lineHeight: 1.3, wordBreak: 'break-all' }}>{name}</div>
        {title && (
          <div style={{ fontSize: 10, color: 'var(--gray-500)', marginTop: 2, lineHeight: 1.3 }}>{title}</div>
        )}
      </div>
    </div>
  );
}

function TeamBox({ team, members, style }) {
  const [collapsed, setCollapsed] = useState(false);
  const bgColor = team.parent_id ? '#f9f7f0' : '#f5f3ec';
  const borderColor = team.parent_id ? '#d4c89a' : '#c8b96e';

  return (
    <div style={{
      border: `1.5px solid ${borderColor}`,
      borderRadius: 10,
      background: bgColor,
      padding: '12px 16px',
      ...style,
    }}>
      {/* Team header */}
      <div
        style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', marginBottom: collapsed ? 0 : 14 }}
        onClick={() => setCollapsed((v) => !v)}
      >
        <span style={{ fontSize: 12, color: '#9a8c5a', width: 14 }}>{collapsed ? '▸' : '▾'}</span>
        <span style={{ fontWeight: 700, fontSize: 14, color: '#5a4e2a' }}>{team.name}</span>
        {members.length > 0 && (
          <span style={{
            fontSize: 11, background: '#e8dfa8', color: '#7a6a30',
            padding: '1px 7px', borderRadius: 10,
          }}>{members.length}名</span>
        )}
      </div>

      {!collapsed && (
        members.length === 0 ? (
          <p style={{ fontSize: 12, color: 'var(--gray-400)', margin: 0 }}>メンバーなし</p>
        ) : (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
            {members.map((m) => <MemberCard key={m.user_id} member={m} />)}
          </div>
        )
      )}
    </div>
  );
}

function ParentSection({ parent, children, membersMap }) {
  const [collapsed, setCollapsed] = useState(false);
  const parentMembers = membersMap[parent.id] || [];
  const childCount = children.length;

  return (
    <div style={{
      border: '2px solid #c8b96e',
      borderRadius: 12,
      background: '#fdf9ee',
      padding: '16px 20px',
      marginBottom: 28,
    }}>
      {/* Parent header */}
      <div
        style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', marginBottom: collapsed ? 0 : 16 }}
        onClick={() => setCollapsed((v) => !v)}
      >
        <span style={{ fontSize: 13, color: '#9a8c5a', width: 16 }}>{collapsed ? '▸' : '▾'}</span>
        <span style={{ fontWeight: 800, fontSize: 16, color: '#3d3520', letterSpacing: 0.3 }}>{parent.name}</span>
        {childCount > 0 && (
          <span style={{ fontSize: 12, color: '#9a8c5a' }}>{childCount}部署</span>
        )}
      </div>

      {!collapsed && (
        <>
          {/* Parent-level members (if any) */}
          {parentMembers.length > 0 && (
            <div style={{ marginBottom: 16 }}>
              <div style={{ fontSize: 11, color: '#9a8c5a', fontWeight: 600, marginBottom: 8 }}>共通メンバー</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
                {parentMembers.map((m) => <MemberCard key={m.user_id} member={m} />)}
              </div>
            </div>
          )}

          {/* Child teams */}
          {childCount > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
              {children.map((child) => (
                <TeamBox
                  key={child.id}
                  team={child}
                  members={membersMap[child.id] || []}
                  style={{ flex: '1 1 280px', minWidth: 280 }}
                />
              ))}
            </div>
          )}

          {childCount === 0 && parentMembers.length === 0 && (
            <p style={{ fontSize: 12, color: 'var(--gray-400)', margin: 0 }}>メンバーなし</p>
          )}
        </>
      )}
    </div>
  );
}

export default function OrgChart() {
  const [teams, setTeams] = useState([]);
  const [membersMap, setMembersMap] = useState({});
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');

  useEffect(() => {
    api.orgChart()
      .then((r) => { setTeams(r.teams || []); setMembersMap(r.membersMap || {}); })
      .catch(console.error)
      .finally(() => setLoading(false));
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
  const standalones = useMemo(
    () => parents.filter((p) => !(childrenOf[p.id]?.length)),
    [parents, childrenOf],
  );
  const parentGroups = useMemo(
    () => parents.filter((p) => childrenOf[p.id]?.length > 0),
    [parents, childrenOf],
  );

  // Search filter: find teams/members matching query
  const filteredMembersMap = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return membersMap;
    const filtered = {};
    for (const [id, members] of Object.entries(membersMap)) {
      const hits = members.filter((m) =>
        [m.display_name, m.real_name, m.title].filter(Boolean)
          .some((v) => v.toLowerCase().includes(q)),
      );
      filtered[id] = hits;
    }
    return filtered;
  }, [membersMap, search]);

  return (
    <div className="workload-page">
      <div className="page-header">
        <div>
          <h1>組織図</h1>
          <p className="page-subtitle">チームと部署のメンバー構成を表示します。</p>
        </div>
      </div>

      <div style={{ marginBottom: 24 }}>
        <input
          type="text"
          placeholder="名前・役職で検索…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            fontSize: 14, padding: '8px 14px',
            border: '1px solid var(--gray-300)', borderRadius: 8,
            width: 260, outline: 'none',
          }}
        />
      </div>

      {loading ? (
        <p className="empty-text">読み込み中…</p>
      ) : teams.length === 0 ? (
        <p className="empty-text">チームがまだ作成されていません。管理 → チーム管理でチームを作成してください。</p>
      ) : (
        <>
          {/* Parent groups with sub-teams */}
          {parentGroups.map((parent) => (
            <ParentSection
              key={parent.id}
              parent={parent}
              children={childrenOf[parent.id] || []}
              membersMap={filteredMembersMap}
            />
          ))}

          {/* Standalone teams */}
          {standalones.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
              {standalones.map((team) => (
                <TeamBox
                  key={team.id}
                  team={team}
                  members={filteredMembersMap[team.id] || []}
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
