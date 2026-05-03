import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api/client';

// ─── Role definitions ────────────────────────────────────────────────────────
const ROLES = [
  { value: 'admin',     label: 'アドミン',       color: '#991b1b', bg: '#fee2e2' },
  { value: 'manager',   label: 'マネージャー',   color: '#6d28d9', bg: '#ede9fe' },
  { value: 'chief',     label: 'チーフ',         color: '#1d4ed8', bg: '#dbeafe' },
  { value: 'sub_chief', label: 'サブチーフ',     color: '#0369a1', bg: '#e0f2fe' },
  { value: 'lead',      label: 'リード',         color: '#047857', bg: '#d1fae5' },
  { value: 'member',    label: 'メンバー',       color: '#374151', bg: '#f3f4f6' },
];
const ROLE_MAP = Object.fromEntries(ROLES.map(r => [r.value, r]));
const roleOf = (v) => ROLE_MAP[v] || ROLE_MAP['member'];

// Slackプロフィールtitleからロールを自動導出（サーバーサイドと同じロジック）
function roleTitleFromSlack(title) {
  if (!title) return 'member';
  if (/sub\s+(manager|expert)/i.test(title)) return 'manager';
  if (/\bmanager\b/i.test(title)) return 'manager';
  if (/sub\s*chief/i.test(title)) return 'sub_chief';
  if (/\bchief\b/i.test(title)) return 'chief';
  if (/\blead\b/i.test(title)) return 'lead';
  return 'member';
}
// memberのロールをtitleから導出（titleがなければDBのroleを使用）
const derivedRole = (member) => roleOf(member.title ? roleTitleFromSlack(member.title) : (member.role || 'member'));

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

// ─── OrgChartView: 組織図表示 ───────────────────────────────────────────────

function MemberChipSmall({ member }) {
  const name = member.display_name || member.real_name || member.user_id;
  const r = derivedRole(member);
  return (
    <div style={{ display:'flex', alignItems:'center', gap:6, padding:'5px 8px', background:'#f9fafb', borderRadius:8, border:'1px solid #f3f4f6' }}>
      <Avatar member={member} size={28} />
      <div>
        <div style={{ fontSize:'0.78rem', fontWeight:600, color:'#111827', whiteSpace:'nowrap' }}>{name.split('/')[0].trim()}</div>
        {r.value !== 'member' && <div style={{ fontSize:'0.65rem', color:r.color, fontWeight:700 }}>{r.label}</div>}
        {member.title && <div style={{ fontSize:'0.65rem', color:'#9ca3af' }}>{member.title}</div>}
      </div>
    </div>
  );
}

// 全メンバー数を再帰集計
function countAll(teamId, membersMap, childrenOf) {
  const direct = (membersMap[teamId] || []).length;
  return direct + (childrenOf[teamId] || []).reduce((s, c) => s + countAll(c.id, membersMap, childrenOf), 0);
}

// チームノード（再帰・display_rowグルーピング対応）
function TeamTreeNode({ team, membersMap, childrenOf, selectedId, onSelect }) {
  const children = childrenOf[team.id] || [];
  const memberCount = (membersMap[team.id] || []).length;

  const byRow = {};
  for (const c of children) {
    const row = c.display_row || 1;
    if (!byRow[row]) byRow[row] = [];
    byRow[row].push(c);
  }
  const rows = Object.keys(byRow).map(Number).sort((a, b) => a - b);

  return (
    <div style={{ display:'flex', flexDirection:'column', alignItems:'center' }}>
      <div
        className={`org-chart-node org-chart-node--child${selectedId === team.id ? ' org-chart-node--selected' : ''}`}
        onClick={() => onSelect(team.id)}>
        <div>{team.name}</div>
        <div style={{ fontSize:'0.68rem', color:'#9ca3af', marginTop:2 }}>{memberCount}名</div>
      </div>
      {rows.map(row => (
        <div key={row} style={{ display:'flex', flexDirection:'column', alignItems:'center' }}>
          <div style={{ width:1, height:16, background:'#c8b96e' }} />
          <div className="org-children">
            {byRow[row].map(child => (
              <div key={child.id} className="org-child-col">
                <TeamTreeNode team={child} membersMap={membersMap} childrenOf={childrenOf} selectedId={selectedId} onSelect={onSelect} />
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function UnassignedCollapsible({ members }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ marginTop:20, border:'1px solid #e5e7eb', borderRadius:10, background:'#f9fafb', overflow:'hidden' }}>
      <button onClick={() => setOpen(v => !v)}
        style={{ width:'100%', display:'flex', alignItems:'center', justifyContent:'space-between', padding:'10px 16px', background:'none', border:'none', cursor:'pointer', textAlign:'left' }}>
        <span style={{ fontWeight:700, fontSize:'0.82rem', color:'#6b7280' }}>未所属 ({members.length}名)</span>
        <span style={{ fontSize:11, color:'#9ca3af' }}>{open ? '▲' : '▼'}</span>
      </button>
      {open && (
        <div style={{ padding:'0 16px 14px', display:'flex', flexWrap:'wrap', gap:6 }}>
          {members.map(m => <MemberChipSmall key={m.user_id} member={m} />)}
        </div>
      )}
    </div>
  );
}

// 階層型チームピッカー
function TeamPicker({ allTeams, childrenOf, excludeId, onSelect }) {
  const [openParent, setOpenParent] = useState(null);
  const topLevel = allTeams.filter(t => !t.parent_id && t.id !== excludeId);
  return (
    <div style={{ marginTop:6, border:'1px solid #e5e7eb', borderRadius:6, overflow:'hidden', background:'#fff', maxHeight:200, overflowY:'auto' }}>
      {topLevel.map(parent => {
        const children = (childrenOf[parent.id] || []).filter(c => c.id !== excludeId);
        const isOpen = openParent === parent.id;
        if (children.length === 0) {
          return (
            <button key={parent.id} onClick={() => onSelect(parent.id)}
              style={{ display:'block', width:'100%', padding:'6px 10px', background:'none', border:'none', borderBottom:'1px solid #f3f4f6', cursor:'pointer', textAlign:'left', fontSize:12, color:'#374151' }}
              onMouseEnter={e => e.currentTarget.style.background='#f9fafb'}
              onMouseLeave={e => e.currentTarget.style.background='none'}>
              {parent.name}
            </button>
          );
        }
        return (
          <div key={parent.id}>
            <button onClick={() => setOpenParent(isOpen ? null : parent.id)}
              style={{ display:'flex', alignItems:'center', gap:6, width:'100%', padding:'6px 10px', background: isOpen ? '#fffef0' : 'none', border:'none', borderBottom:'1px solid #f3f4f6', cursor:'pointer', textAlign:'left', fontSize:12, fontWeight:600, color:'#374151' }}>
              <span style={{ fontSize:9, color:'#c8b96e' }}>{isOpen ? '▼' : '▶'}</span>
              {parent.name}
            </button>
            {isOpen && children.map(child => (
              <button key={child.id} onClick={() => onSelect(child.id)}
                style={{ display:'block', width:'100%', padding:'5px 10px 5px 24px', background:'none', border:'none', borderBottom:'1px solid #f9fafb', cursor:'pointer', textAlign:'left', fontSize:11, color:'#6b7280' }}
                onMouseEnter={e => e.currentTarget.style.background='#f9fafb'}
                onMouseLeave={e => e.currentTarget.style.background='none'}>
                {child.name}
              </button>
            ))}
          </div>
        );
      })}
    </div>
  );
}

function OrgChartView({ parentGroups, childrenOf, filteredMembersMap, standalones, liveUnassigned, search, setSearch, ceo, allTeams, membersMap, onMemberAdd, onMemberRemove, onMemberMove }) {
  const [selectedId, setSelectedId] = useState(null);
  const [addSearch, setAddSearch] = useState('');
  const [moveTarget, setMoveTarget] = useState('');
  const [movingMemberId, setMovingMemberId] = useState(null);

  const allNodes = Object.values(childrenOf).flat().concat(parentGroups, standalones);
  const selectedTeam = allNodes.find(t => t.id === selectedId);
  const selectedMembers = selectedId ? (filteredMembersMap[selectedId] || []) : [];
  const select = (id) => { setSelectedId(prev => prev === id ? null : id); setAddSearch(''); setMoveTarget(''); setMovingMemberId(null); };
  const allDepts = [...parentGroups, ...standalones];
  const ceoName = ceo ? (ceo.display_name || ceo.real_name || '').split('/')[0].trim() : '南 晴仁';

  return (
    <div>
      {/* 検索 */}
      <div style={{ display:'flex', alignItems:'center', gap:12, marginBottom:20 }}>
        <input type="text" placeholder="名前・役職で検索…" value={search} onChange={e => setSearch(e.target.value)}
          style={{ fontSize:14, padding:'7px 14px', border:'1px solid #e5e7eb', borderRadius:8, width:240, outline:'none' }} />
        {liveUnassigned.length > 0 && (
          <span style={{ fontSize:12, color:'#9ca3af' }}>未所属: {liveUnassigned.length}名</span>
        )}
      </div>

      {/* 組織図 + 右サイドパネル */}
      <div style={{ display:'flex', alignItems:'flex-start', gap:0 }}>

        {/* ───── 組織図エリア（横スクロール） ───── */}
        <div style={{ flex:1, overflowX:'auto', minWidth:0 }}>
          <div className="org-tree" style={{ minWidth:'fit-content', padding:'8px 24px 32px' }}>

            {/* CEO ノード */}
            <div className="org-chart-node org-chart-node--root">
              <div style={{ padding:'5px 24px 3px', borderBottom:'1px solid #c8b96e30', fontSize:'0.62rem', fontWeight:700, color:'#c8b96e70', letterSpacing:'0.16em' }}>CEO</div>
              <div style={{ padding:'5px 20px 8px', display:'flex', alignItems:'center', gap:8, justifyContent:'center' }}>
                {ceo?.avatar_url && (
                  <img src={ceo.avatar_url} alt={ceoName}
                    style={{ width:26, height:26, borderRadius:'50%', objectFit:'cover', border:'1.5px solid #c8b96e55' }} />
                )}
                <span style={{ fontSize:'0.88rem', fontWeight:800, color:'#c8b96e', whiteSpace:'nowrap' }}>{ceoName}</span>
              </div>
            </div>

            {/* 部署群 */}
            {allDepts.length > 0 && <>
              <div style={{ width:1, height:20, background:'#c8b96e' }} />
              <div className="org-children">
                {allDepts.map(dept => {
                  const directChildren = childrenOf[dept.id] || [];
                  const total = countAll(dept.id, filteredMembersMap, childrenOf);
                  const directMembers = (filteredMembersMap[dept.id] || []).length;
                  const isClickable = directMembers > 0;
                  const byRow = {};
                  for (const c of directChildren) {
                    const row = c.display_row || 1;
                    if (!byRow[row]) byRow[row] = [];
                    byRow[row].push(c);
                  }
                  const rows = Object.keys(byRow).map(Number).sort((a, b) => a - b);
                  return (
                    <div key={dept.id} className="org-child-col">
                      <div
                        className={`org-chart-node org-chart-node--dept${selectedId===dept.id?' org-chart-node--selected':''}`}
                        onClick={() => isClickable && select(dept.id)}
                        style={{ cursor: isClickable ? 'pointer' : 'default' }}>
                        <div>{dept.name}</div>
                        <div style={{ fontSize:'0.68rem', color:'#9ca3af', marginTop:2 }}>{total}名</div>
                      </div>
                      {rows.map(row => (
                        <div key={row} style={{ display:'flex', flexDirection:'column', alignItems:'center' }}>
                          <div style={{ width:1, height:20, background:'#c8b96e' }} />
                          <div className="org-children">
                            {byRow[row].map(child => (
                              <div key={child.id} className="org-child-col">
                                <TeamTreeNode team={child} membersMap={filteredMembersMap} childrenOf={childrenOf} selectedId={selectedId} onSelect={select} />
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  );
                })}
              </div>
            </>}
          </div>
        </div>

        {/* ───── 右サイドパネル（選択チームのメンバー一覧） ───── */}
        <div style={{
          width: selectedTeam ? 260 : 0,
          overflow: 'hidden',
          transition: 'width 0.22s ease',
          flexShrink: 0,
          borderLeft: selectedTeam ? '1px solid #e8dfa8' : 'none',
          background: '#fffef9',
        }}>
          {selectedTeam && (
            <div style={{ padding:'16px 14px', width:260, boxSizing:'border-box' }}>
              {/* ヘッダー */}
              <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:12 }}>
                <div>
                  <div style={{ fontWeight:800, fontSize:'0.9rem', color:'#1e1a10' }}>{selectedTeam.name}</div>
                  <div style={{ fontSize:'0.72rem', color:'#9ca3af', marginTop:1 }}>{selectedMembers.length}名</div>
                </div>
                <button onClick={() => select(null)}
                  style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:16, padding:'2px 4px', lineHeight:1 }}>✕</button>
              </div>

              {/* メンバー追加（全メンバー検索・兼任OK） */}
              {(() => {
                const alreadyIn = new Set((membersMap[selectedId] || []).map(m => m.user_id));
                const allWorkspace = Object.values({
                  ...Object.fromEntries(
                    Object.values(membersMap).flat().map(m => [m.user_id, m])
                  ),
                  ...Object.fromEntries(liveUnassigned.map(m => [m.user_id, m])),
                });
                const candidates = allWorkspace.filter(m => {
                  if (alreadyIn.has(m.user_id)) return false;
                  const n = (m.display_name || m.real_name || '').toLowerCase();
                  return addSearch && n.includes(addSearch.toLowerCase());
                });
                return (
                  <div style={{ marginBottom:12 }}>
                    <div style={{ fontSize:'0.75rem', fontWeight:700, color:'#6b7280', marginBottom:4 }}>メンバーを追加</div>
                    <input
                      placeholder="名前で検索（兼任可）…"
                      value={addSearch}
                      onChange={e => setAddSearch(e.target.value)}
                      style={{ width:'100%', fontSize:12, padding:'5px 8px', border:'1px solid #e5e7eb', borderRadius:6, outline:'none', boxSizing:'border-box' }}
                    />
                    {addSearch && (
                      <div style={{ border:'1px solid #e5e7eb', borderRadius:6, marginTop:2, maxHeight:140, overflowY:'auto', background:'#fff', zIndex:10, position:'relative' }}>
                        {candidates.length === 0
                          ? <div style={{ padding:'8px', fontSize:12, color:'#9ca3af' }}>見つかりません</div>
                          : candidates.slice(0, 20).map(m => {
                              const name = (m.display_name || m.real_name || '').split('/')[0].trim();
                              return (
                                <button key={m.user_id}
                                  onClick={() => { onMemberAdd(selectedId, m); setAddSearch(''); }}
                                  style={{ display:'flex', alignItems:'center', gap:6, width:'100%', padding:'6px 8px', background:'none', border:'none', cursor:'pointer', textAlign:'left', fontSize:12, color:'#374151' }}
                                  onMouseEnter={e => e.currentTarget.style.background='#f9fafb'}
                                  onMouseLeave={e => e.currentTarget.style.background='none'}>
                                  <Avatar member={m} size={20} />
                                  <span>{name}</span>
                                </button>
                              );
                            })
                        }
                      </div>
                    )}
                  </div>
                );
              })()}

              {/* メンバー一覧 */}
              <div style={{ display:'flex', flexDirection:'column', gap:4, maxHeight:'calc(100vh - 360px)', overflowY:'auto' }}>
                {selectedMembers.length === 0 ? (
                  <div style={{ fontSize:'0.8rem', color:'#d1d5db', fontStyle:'italic', paddingTop:4 }}>メンバーなし</div>
                ) : selectedMembers.map(m => {
                  const name = (m.display_name || m.real_name || '').split('/')[0].trim();
                  const isMoving = movingMemberId === m.user_id;
                  return (
                    <div key={m.user_id} style={{ background:'#f9fafb', borderRadius:7, padding:'5px 8px', border:'1px solid #f3f4f6' }}>
                      <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                        <Avatar member={m} size={24} />
                        <span style={{ flex:1, fontSize:'0.77rem', fontWeight:600, color:'#111827', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{name}</span>
                        <button onClick={() => setMovingMemberId(isMoving ? null : m.user_id)}
                          title="別チームへ移動"
                          style={{ background:'none', border:'none', cursor:'pointer', fontSize:13, color: isMoving ? '#c8b96e' : '#9ca3af', padding:'0 2px', lineHeight:1 }}>⇄</button>
                        <button onClick={() => onMemberRemove(selectedId, m.user_id)}
                          title="チームから外す"
                          style={{ background:'none', border:'none', cursor:'pointer', fontSize:13, color:'#9ca3af', padding:'0 2px', lineHeight:1 }}>×</button>
                      </div>
                      {isMoving && (
                        <TeamPicker
                          allTeams={allTeams}
                          childrenOf={childrenOf}
                          excludeId={selectedId}
                          onSelect={toId => { onMemberMove(selectedId, toId, m); setMovingMemberId(null); }}
                        />
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* 未所属（折りたたみ可） */}
      {liveUnassigned.length > 0 && <UnassignedCollapsible members={liveUnassigned} />}
    </div>
  );
}

// ─── Member card: view mode (existing org chart style) ───────────────────────
function MemberCardView({ member }) {
  const name = member.display_name || member.real_name || member.user_id;
  const title = member.title || '';
  const role = derivedRole(member);
  const showRoleBadge = role.value !== 'member';
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
  const role = derivedRole(member);
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
function EditableTeamCard({ team, members, parentTeams, dragState, onDragStart, onDrop, onRemoveMember, onRoleChange, onDelete, onRename, onSetParent }) {
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

      {/* Parent team selector */}
      {parentTeams && onSetParent && (
        <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid var(--gray-100)', display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ fontSize: 11, color: 'var(--gray-500)', whiteSpace: 'nowrap' }}>所属部署</span>
          <select
            value={team.parent_id || ''}
            onChange={e => onSetParent(team.id, e.target.value || null)}
            style={{
              flex: 1, fontSize: 12, padding: '3px 6px',
              border: '1px solid var(--gray-300)', borderRadius: 6,
              background: '#fff', color: 'var(--gray-700)', cursor: 'pointer',
            }}
          >
            <option value="">なし（独立チーム）</option>
            {parentTeams.filter(p => p.id !== team.id).map(p => (
              <option key={p.id} value={p.id}>{p.name}</option>
            ))}
          </select>
        </div>
      )}
    </div>
  );
}

// ─── Build mode: department section header (replaces parent EditableTeamCard) ─
function DeptSectionHeader({ team, onRename, onDelete, onCreateChild }) {
  const [editing, setEditing] = useState(false);
  const [nameVal, setNameVal] = useState(team.name);

  const commit = () => {
    setEditing(false);
    const v = nameVal.trim();
    if (v && v !== team.name) onRename(team.id, v);
    else setNameVal(team.name);
  };

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      {editing ? (
        <input
          value={nameVal}
          onChange={e => setNameVal(e.target.value)}
          onBlur={commit}
          onKeyDown={e => { if (e.key === 'Enter') commit(); if (e.key === 'Escape') { setEditing(false); setNameVal(team.name); } }}
          autoFocus
          style={{ flex: 1, fontSize: 15, fontWeight: 800, border: 'none', borderBottom: '2px solid #c8b96e', outline: 'none', background: 'transparent', color: '#3d3520', padding: '2px 0' }}
        />
      ) : (
        <span
          onClick={() => setEditing(true)}
          title="クリックで名前を編集"
          style={{ flex: 1, fontWeight: 800, fontSize: 15, color: '#3d3520', cursor: 'text' }}
        >
          {team.name}
        </span>
      )}
      <button
        onClick={() => onCreateChild(team.id)}
        title="この部署にチームを追加"
        style={{ fontSize: 12, padding: '3px 10px', borderRadius: 6, border: '1px solid #c8b96e', background: '#fff8e1', color: '#7a6a30', cursor: 'pointer', whiteSpace: 'nowrap', flexShrink: 0 }}
      >＋ チームを追加</button>
      <button
        onClick={() => onDelete(team.id)}
        title="部署を削除"
        style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#c8b96e', fontSize: 20, lineHeight: 1, padding: '0 2px', flexShrink: 0 }}
      >×</button>
    </div>
  );
}

// ─── Build mode: unassigned members source panel ─────────────────────────────
function UnassignedPanel({ members, dragState, onDragStart, onHide }) {
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
            {filtered.map(m => {
              const name = m.display_name || m.real_name || m.user_id;
              return (
                <div key={m.user_id} style={{ position: 'relative' }}>
                  <MemberChip
                    member={m}
                    isDragging={dragState?.userId === m.user_id}
                    onDragStart={(e) => onDragStart(e, m)}
                    showRemove={false}
                  />
                  <button
                    onClick={() => onHide(m.user_id)}
                    title={`${name}を組織図から非表示にする`}
                    style={{
                      position: 'absolute', top: -6, right: -6,
                      width: 18, height: 18, borderRadius: '50%',
                      background: 'var(--gray-500)', color: '#fff',
                      border: 'none', cursor: 'pointer',
                      fontSize: 11, lineHeight: '18px', textAlign: 'center',
                      padding: 0, display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}
                  >×</button>
                </div>
              );
            })}
          </div>
        )
      )}
    </div>
  );
}

// ─── TeamXX管理 ──────────────────────────────────────────────────────────────
const TEAM_COLORS = ['#6d28d9','#2563eb','#059669','#d97706','#dc2626','#0891b2','#7c3aed','#64748b'];

function TeamXXManager({ teams, membersMap, onCreateTeam, onDeleteTeam, onRenameTeam, onAddMember, onRemoveMember }) {
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [selectedTeamId, setSelectedTeamId] = useState(null);
  const [newName, setNewName] = useState('チーム');
  const [createMembers, setCreateMembers] = useState(new Set());
  const [creating, setCreating] = useState(false);
  // 右パネル内の状態
  const [editingName, setEditingName] = useState(false);
  const [editNameVal, setEditNameVal] = useState('');
  const [addTab, setAddTab] = useState('dr'); // 'dr' | 'cs'
  const [panelAddMembers, setPanelAddMembers] = useState(new Set());

  const xxTeams = teams.filter(t => t.show_in_orgchart === false);
  const drTeam = teams.find(t => t.name.includes('Direction'));
  const csTeam = teams.find(t => t.name.includes('Customer Success'));
  const drMembers = drTeam ? (membersMap[drTeam.id] || []) : [];
  const csMembers = csTeam ? (membersMap[csTeam.id] || []) : [];

  const selectedTeam = selectedTeamId ? xxTeams.find(t => t.id === selectedTeamId) : null;
  const selectedMembers = selectedTeamId ? (membersMap[selectedTeamId] || []) : [];
  const selectedMemberIds = new Set(selectedMembers.map(m => m.user_id));

  const selectTeam = (id) => {
    setSelectedTeamId(id);
    setEditingName(false);
    setPanelAddMembers(new Set());
    setAddTab('dr');
  };

  // モーダル用チェックボックス
  const toggleCreate = (uid) => setCreateMembers(prev => { const n = new Set(prev); n.has(uid) ? n.delete(uid) : n.add(uid); return n; });
  // パネル追加用チェックボックス（DR/CS から未所属を選ぶ）
  const togglePanelAdd = (uid) => setPanelAddMembers(prev => { const n = new Set(prev); n.has(uid) ? n.delete(uid) : n.add(uid); return n; });

  const handleCreate = async () => {
    if (!newName.trim()) return;
    setCreating(true);
    try {
      const hrTeam = teams.find(t => t.name === 'HR');
      const created = await onCreateTeam({ name: newName.trim(), parentId: hrTeam?.id, showInOrgchart: false });
      const allSel = [...drMembers, ...csMembers].filter(m => createMembers.has(m.user_id));
      for (const m of allSel) {
        await onAddMember(created?.id, { userId: m.user_id, displayName: m.display_name, avatarUrl: m.avatar_url, title: m.title });
      }
      setShowCreateModal(false);
      setNewName('チーム');
      setCreateMembers(new Set());
      selectTeam(created?.id);
    } catch (e) { console.error(e); }
    finally { setCreating(false); }
  };

  const handleSaveName = async () => {
    if (!editNameVal.trim() || !selectedTeamId) return;
    await onRenameTeam(selectedTeamId, editNameVal.trim());
    setEditingName(false);
  };

  const handlePanelAdd = async () => {
    const candidates = (addTab === 'dr' ? drMembers : csMembers).filter(m => panelAddMembers.has(m.user_id));
    for (const m of candidates) {
      await onAddMember(selectedTeamId, { userId: m.user_id, displayName: m.display_name, avatarUrl: m.avatar_url, title: m.title });
    }
    setPanelAddMembers(new Set());
  };

  const MemberCheckRow = ({ member, checked, onToggle }) => {
    const name = (member.display_name || member.real_name || '').split('/')[0].trim();
    return (
      <label style={{ display:'flex', alignItems:'center', gap:8, padding:'5px 8px', borderRadius:6, cursor:'pointer', background: checked ? '#f5f3ff' : 'transparent' }}>
        <input type="checkbox" checked={checked} onChange={() => onToggle(member.user_id)} style={{ accentColor:'#6d28d9' }} />
        <Avatar member={member} size={22} />
        <span style={{ fontSize:13, color:'#374151' }}>{name}</span>
      </label>
    );
  };

  return (
    <div>
      {/* ── カードグリッド ── */}
      <div style={{ marginBottom:20, display:'flex', alignItems:'center', justifyContent:'space-between' }}>
        <p style={{ margin:0, fontSize:13, color:'#6b7280' }}>DR＋CSメンバーを組み合わせた業務チームを管理します</p>
        <button onClick={() => setShowCreateModal(true)} className="btn btn-secondary" style={{ fontSize:13 }}>＋ チームを追加</button>
      </div>
      {xxTeams.length === 0 ? (
        <div style={{ textAlign:'center', padding:'60px 20px', color:'#9ca3af' }}>
          <div style={{ fontSize:'2rem', marginBottom:8 }}>👥</div>
          <div style={{ fontSize:14 }}>「＋ チームを追加」で作成してください</div>
        </div>
      ) : (
        <div className="rpo-team-picker-grid">
          {xxTeams.map((team, idx) => {
            const members = membersMap[team.id] || [];
            const color = TEAM_COLORS[idx % TEAM_COLORS.length];
            return (
              <button key={team.id} className="rpo-team-picker-card" onClick={() => selectTeam(team.id)}>
                <span className="rpo-team-picker-icon" style={{ background: color + '22', color }}>{team.name[0]}</span>
                <span className="rpo-team-picker-name">{team.name}</span>
                <span style={{ fontSize:11, color:'#9ca3af' }}>{members.length}名</span>
                {members.length > 0 && (
                  <div style={{ display:'flex' }}>
                    {members.slice(0, 5).map((m, i) => (
                      <div key={m.user_id} style={{ marginLeft: i > 0 ? -8 : 0, zIndex: 5 - i }}>
                        <Avatar member={m} size={22} />
                      </div>
                    ))}
                    {members.length > 5 && <span style={{ fontSize:10, color:'#9ca3af', alignSelf:'center', marginLeft:6 }}>+{members.length-5}</span>}
                  </div>
                )}
              </button>
            );
          })}
        </div>
      )}

      {/* チーム編集モーダル */}
      {selectedTeam && (
        <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.4)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
          onClick={e => { if (e.target === e.currentTarget) setSelectedTeamId(null); }}>
          <div style={{ background:'#fff', borderRadius:12, padding:28, width:500, maxWidth:'90vw', maxHeight:'85vh', overflowY:'auto', boxShadow:'0 20px 60px rgba(0,0,0,0.2)' }}>
            {/* ヘッダー */}
            <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:20 }}>
              {editingName ? (
                <div style={{ display:'flex', gap:6, flex:1, marginRight:8 }}>
                  <input value={editNameVal} onChange={e => setEditNameVal(e.target.value)}
                    autoFocus onKeyDown={e => { if (e.key==='Enter') handleSaveName(); if (e.key==='Escape') setEditingName(false); }}
                    style={{ flex:1, fontSize:15, fontWeight:700, padding:'5px 10px', border:'1.5px solid #c8b96e', borderRadius:8, outline:'none' }} />
                  <button onClick={handleSaveName} style={{ fontSize:12, background:'#c8b96e', color:'#fff', border:'none', borderRadius:8, padding:'5px 12px', cursor:'pointer', fontWeight:700 }}>保存</button>
                  <button onClick={() => setEditingName(false)} style={{ fontSize:12, background:'#f3f4f6', color:'#6b7280', border:'none', borderRadius:8, padding:'5px 10px', cursor:'pointer' }}>取消</button>
                </div>
              ) : (
                <div style={{ display:'flex', alignItems:'center', gap:8, flex:1 }}>
                  <h3 style={{ margin:0, fontSize:'1rem', fontWeight:800 }}>{selectedTeam.name}</h3>
                  <button onClick={() => { setEditNameVal(selectedTeam.name); setEditingName(true); }}
                    title="名前を変更" style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:14, padding:'0 2px', lineHeight:1 }}>✎</button>
                </div>
              )}
              <button onClick={() => setSelectedTeamId(null)} style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:20, lineHeight:1, flexShrink:0 }}>✕</button>
            </div>

            {/* 現在のメンバー */}
            <div style={{ fontSize:12, fontWeight:700, color:'#6b7280', marginBottom:8 }}>メンバー（{selectedMembers.length}名）</div>
            <div style={{ display:'flex', flexDirection:'column', gap:4, marginBottom:20 }}>
              {selectedMembers.length === 0
                ? <div style={{ fontSize:12, color:'#d1d5db', fontStyle:'italic', padding:'8px 0' }}>メンバーなし</div>
                : selectedMembers.map(m => {
                    const name = (m.display_name || m.real_name || '').split('/')[0].trim();
                    return (
                      <div key={m.user_id} style={{ display:'flex', alignItems:'center', gap:8, padding:'6px 8px', background:'#f9fafb', borderRadius:8, border:'1px solid #f3f4f6' }}>
                        <Avatar member={m} size={28} />
                        <span style={{ flex:1, fontSize:13, fontWeight:600 }}>{name}</span>
                        <button onClick={() => onRemoveMember(selectedTeamId, m.user_id)}
                          style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:16, lineHeight:1 }}>×</button>
                      </div>
                    );
                  })
              }
            </div>

            {/* メンバー追加（DR/CSタブ） */}
            <div style={{ borderTop:'1px solid #f3f4f6', paddingTop:16, marginBottom:20 }}>
              <div style={{ fontSize:12, fontWeight:700, color:'#6b7280', marginBottom:10 }}>メンバーを追加</div>
              <div style={{ display:'flex', gap:6, marginBottom:10 }}>
                {[['dr','DR','#dbeafe','#1d4ed8'],['cs','CS','#dcfce7','#15803d']].map(([key,label,bg,col]) => (
                  <button key={key} onClick={() => { setAddTab(key); setPanelAddMembers(new Set()); }}
                    style={{ flex:1, padding:'6px 0', borderRadius:8, border:'none', cursor:'pointer', fontSize:13, fontWeight:700,
                      background: addTab===key ? bg : '#f3f4f6', color: addTab===key ? col : '#9ca3af' }}>
                    {label}
                  </button>
                ))}
              </div>
              <div style={{ border:'1px solid #e5e7eb', borderRadius:8, padding:'4px', maxHeight:180, overflowY:'auto', marginBottom:10 }}>
                {(addTab==='dr' ? drMembers : csMembers).filter(m => !selectedMemberIds.has(m.user_id)).length === 0
                  ? <div style={{ padding:'10px', fontSize:12, color:'#9ca3af' }}>追加できるメンバーなし</div>
                  : (addTab==='dr' ? drMembers : csMembers)
                      .filter(m => !selectedMemberIds.has(m.user_id))
                      .map(m => <MemberCheckRow key={m.user_id} member={m} checked={panelAddMembers.has(m.user_id)} onToggle={togglePanelAdd} />)
                }
              </div>
              {panelAddMembers.size > 0 && (
                <button onClick={handlePanelAdd} className="btn btn-primary" style={{ width:'100%', fontSize:13 }}>
                  {panelAddMembers.size}名を追加
                </button>
              )}
            </div>

            {/* 削除 */}
            <div style={{ borderTop:'1px solid #f3f4f6', paddingTop:16, display:'flex', justifyContent:'flex-end' }}>
              <button onClick={() => { if (window.confirm(`「${selectedTeam.name}」を削除しますか？`)) { onDeleteTeam(selectedTeam.id); setSelectedTeamId(null); } }}
                style={{ background:'none', border:'1px solid #fee2e2', color:'#ef4444', borderRadius:8, padding:'6px 16px', fontSize:12, cursor:'pointer' }}>
                チームを削除
              </button>
            </div>
          </div>
        </div>
      )}

      {/* チーム作成モーダル */}
      {showCreateModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.4)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
          onClick={e => { if (e.target === e.currentTarget) setShowCreateModal(false); }}>
          <div style={{ background:'#fff', borderRadius:12, padding:28, width:500, maxWidth:'90vw', maxHeight:'85vh', overflowY:'auto', boxShadow:'0 20px 60px rgba(0,0,0,0.2)' }}>
            <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:20 }}>
              <h3 style={{ margin:0, fontSize:'1rem', fontWeight:800 }}>チームを追加</h3>
              <button onClick={() => setShowCreateModal(false)} style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:20, lineHeight:1 }}>✕</button>
            </div>
            <div style={{ marginBottom:20 }}>
              <label style={{ fontSize:12, fontWeight:700, color:'#6b7280', display:'block', marginBottom:6 }}>チーム名</label>
              <input value={newName} onChange={e => setNewName(e.target.value)} placeholder="チーム〇〇"
                style={{ width:'100%', fontSize:14, padding:'8px 12px', border:'1px solid #e5e7eb', borderRadius:8, outline:'none', boxSizing:'border-box' }} />
            </div>
            {[['dr','DR','#dbeafe','#1d4ed8', drMembers, 'Direction Team'],['cs','CS','#dcfce7','#15803d', csMembers, 'Customer Success Team']].map(([key,label,bg,col,members,teamName]) => (
              <div key={key} style={{ marginBottom:16 }}>
                <div style={{ fontSize:12, fontWeight:700, color:'#6b7280', marginBottom:6, display:'flex', alignItems:'center', gap:6 }}>
                  <span style={{ background:bg, color:col, padding:'1px 8px', borderRadius:99, fontSize:11 }}>{label}</span>
                  {teamName}（{members.length}名）
                </div>
                <div style={{ border:'1px solid #e5e7eb', borderRadius:8, padding:'4px' }}>
                  {members.length === 0
                    ? <div style={{ padding:'8px', fontSize:12, color:'#9ca3af' }}>メンバーなし</div>
                    : members.map(m => <MemberCheckRow key={m.user_id} member={m} checked={createMembers.has(m.user_id)} onToggle={toggleCreate} />)}
                </div>
              </div>
            ))}
            <div style={{ display:'flex', justifyContent:'flex-end', gap:8 }}>
              <button onClick={() => setShowCreateModal(false)} className="btn btn-secondary">キャンセル</button>
              <button onClick={handleCreate} disabled={creating || !newName.trim()} className="btn btn-primary">
                {creating ? '作成中…' : `作成（${createMembers.size}名選択）`}
              </button>
            </div>
          </div>
        </div>
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

  // ビルドモード用: 全チーム
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

  // ビューモード用: show_in_orgchart=true のチームのみ
  const orgTeams = useMemo(() => teams.filter(t => t.show_in_orgchart !== false), [teams]);
  const orgChildrenOf = useMemo(() => {
    const map = {};
    for (const t of orgTeams) {
      if (t.parent_id) {
        if (!map[t.parent_id]) map[t.parent_id] = [];
        map[t.parent_id].push(t);
      }
    }
    return map;
  }, [orgTeams]);
  const orgParents = useMemo(() => orgTeams.filter(t => !t.parent_id), [orgTeams]);
  const standalones = useMemo(() => orgParents.filter(p => !orgChildrenOf[p.id]?.length), [orgParents, orgChildrenOf]);
  const parentGroups = useMemo(() => orgParents.filter(p => orgChildrenOf[p.id]?.length > 0), [orgParents, orgChildrenOf]);

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

  // CEO: title に「代表」を含むメンバーを全データから探す
  const ceo = useMemo(() => {
    const all = [...Object.values(membersMap).flat(), ...unassigned];
    return all.find(m => m.title && (m.title.includes('代表') || m.title.toLowerCase().includes('ceo'))) || null;
  }, [membersMap, unassigned]);

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

  // ── Add member to team (from unassigned) ──────────────────────────────────
  const handleMemberAdd = async (teamId, member) => {
    const existing = membersMap[teamId] || [];
    if (existing.find(m => m.user_id === member.user_id)) return;
    setMembersMap(prev => ({ ...prev, [teamId]: [...existing, member] }));
    setSaving(true);
    try {
      await api.adminAddTeamMember(teamId, member.user_id);
    } catch (e) {
      console.error(e);
      setMembersMap(prev => ({ ...prev, [teamId]: existing }));
    } finally { setSaving(false); }
  };

  // ── Move member between teams ──────────────────────────────────────────────
  const handleMemberMove = async (fromId, toId, member) => {
    const fromPrev = membersMap[fromId] || [];
    const toPrev = membersMap[toId] || [];
    if (toPrev.find(m => m.user_id === member.user_id)) {
      setMembersMap(prev => ({ ...prev, [fromId]: fromPrev.filter(m => m.user_id !== member.user_id) }));
    } else {
      setMembersMap(prev => ({
        ...prev,
        [fromId]: fromPrev.filter(m => m.user_id !== member.user_id),
        [toId]: [...toPrev, member],
      }));
    }
    setSaving(true);
    try {
      await api.adminRemoveTeamMember(fromId, member.user_id);
      await api.adminAddTeamMember(toId, member.user_id);
    } catch (e) {
      console.error(e);
      setMembersMap(prev => ({ ...prev, [fromId]: fromPrev, [toId]: toPrev }));
    } finally { setSaving(false); }
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
  const handleCreateTeam = async (opts = {}) => {
    const name = opts.name || `新しいチーム ${teams.length + 1}`;
    const parentId = opts.parentId || null;
    const showInOrgchart = opts.showInOrgchart !== false;
    setSaving(true);
    try {
      const res = await api.adminCreateTeam(name, parentId, { show_in_orgchart: showInOrgchart });
      const newTeam = { id: res.team.id, name: res.team.name, parent_id: parentId, show_in_orgchart: showInOrgchart };
      setTeams(prev => [...prev, newTeam]);
      setMembersMap(prev => ({ ...prev, [res.team.id]: [] }));
      return newTeam;
    } catch (e) {
      console.error(e);
    } finally {
      setSaving(false);
    }
  };

  // ── Create child team inside a department ─────────────────────────────────
  const handleCreateChildTeam = async (parentId) => {
    const name = `新しいチーム ${teams.length + 1}`;
    setSaving(true);
    try {
      const res = await api.adminCreateTeam(name, parentId);
      const newTeam = { id: res.team.id, name: res.team.name, parent_id: parentId };
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

  const handleHideUser = async (userId) => {
    // Optimistic: remove from unassigned list
    setUnassigned(prev => prev.filter(u => u.user_id !== userId));
    try {
      await api.adminHideDirectoryUser(userId);
    } catch (e) {
      console.error(e);
      load(); // rollback
    }
  };

  const handleSetParent = async (teamId, parentId) => {
    setTeams(prev => prev.map(t => t.id === teamId ? { ...t, parent_id: parentId } : t));
    setSaving(true);
    try {
      await api.adminUpdateTeamParent(teamId, parentId);
    } catch (e) {
      console.error(e);
      load();
    } finally {
      setSaving(false);
    }
  };

  // Parent teams = teams that have (or could have) children — used in dropdown
  const parentCandidates = useMemo(
    () => teams.filter(t => !t.parent_id),
    [teams],
  );

  const [buildSearch1, setBuildSearch1] = useState('');
  const [buildSearch2, setBuildSearch2] = useState('');

  // Teams shown in build mode — OR filter: matches search1 OR search2
  const filteredBuildTeams = useMemo(() => {
    const q1 = buildSearch1.trim().toLowerCase();
    const q2 = buildSearch2.trim().toLowerCase();
    if (!q1 && !q2) return teams;
    const matchesQuery = (t, q) => {
      if (!q) return false;
      if (t.name.toLowerCase().includes(q)) return true;
      return (membersMap[t.id] || []).some(m =>
        [m.display_name, m.real_name, m.title].filter(Boolean).some(v => v.toLowerCase().includes(q))
      );
    };
    return teams.filter(t => matchesQuery(t, q1) || matchesQuery(t, q2));
  }, [teams, membersMap, buildSearch1, buildSearch2]);

  // Build mode: group filtered teams by parent-child structure
  const buildGroups = useMemo(() => {
    const filteredIds = new Set(filteredBuildTeams.map(t => t.id));
    const childrenOf = {};
    for (const t of filteredBuildTeams) {
      if (t.parent_id) {
        if (!childrenOf[t.parent_id]) childrenOf[t.parent_id] = [];
        childrenOf[t.parent_id].push(t);
      }
    }
    // Parents in filtered set
    const parentSections = filteredBuildTeams
      .filter(t => !t.parent_id)
      .map(parent => ({ parent, children: childrenOf[parent.id] || [] }));
    // Child teams whose parent isn't in the filtered set (show as loose cards)
    const orphans = filteredBuildTeams.filter(t => t.parent_id && !filteredIds.has(t.parent_id));
    return { parentSections, orphans };
  }, [filteredBuildTeams]);

  return (
    <div className="workload-page">
      {/* ── Header ── */}
      <div className="page-header" style={{ marginBottom: 16 }}>
        <div>
          <h1>チーム設定</h1>
          <p className="page-subtitle">チームと部署のメンバー構成を表示・編集できます。</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {saving && <span style={{ fontSize: 12, color: 'var(--gray-400)' }}>保存中…</span>}
          <button
            onClick={() => { setBuildMode(v => !v); setSearch(''); setBuildSearch1(''); setBuildSearch2(''); }}
            className={buildMode ? 'btn btn-primary' : 'btn btn-secondary'}
          >
            {buildMode ? '✓ 完了' : '👥 チームXX管理'}
          </button>
        </div>
      </div>

      {loading ? (
        <p className="empty-text">読み込み中…</p>
      ) : buildMode ? (
        /* ══════════════ チームXX管理 ══════════════ */
        <TeamXXManager
          teams={teams}
          membersMap={membersMap}
          onCreateTeam={handleCreateTeam}
          onDeleteTeam={handleDeleteTeam}
          onRenameTeam={handleRenameTeam}
          onAddMember={handleDrop}
          onRemoveMember={handleRemoveMember}
        />

      ) : (
        /* ══════════════ VIEW MODE: 組織図 ══════════════ */
        <OrgChartView
          parentGroups={parentGroups}
          childrenOf={orgChildrenOf}
          filteredMembersMap={filteredMembersMap}
          standalones={standalones}
          liveUnassigned={liveUnassigned}
          search={search}
          setSearch={setSearch}
          ceo={ceo}
          allTeams={teams}
          membersMap={membersMap}
          onMemberAdd={handleMemberAdd}
          onMemberRemove={handleRemoveMember}
          onMemberMove={handleMemberMove}
        />
      )}
    </div>
  );
}
