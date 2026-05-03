import { useEffect, useState } from 'react';
import { api } from '../../api/client';

const CATEGORY_LABELS = { role: '役職', dept: '部署', other: 'その他' };
const CATEGORY_COLORS = { role: '#6d28d9', dept: '#0891b2', other: '#64748b' };
const CATEGORY_BG = { role: '#f5f3ff', dept: '#ecfeff', other: '#f8fafc' };

function Avatar({ user, size = 28 }) {
  const name = user.display_name || user.real_name || user.user_id;
  if (user.avatar_url) {
    return <img src={user.avatar_url} alt={name} style={{ width: size, height: size, borderRadius: '50%', objectFit: 'cover', flexShrink: 0 }} />;
  }
  return (
    <div style={{ width: size, height: size, borderRadius: '50%', background: '#e0e7ff', color: '#4338ca', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 700, fontSize: Math.round(size * 0.4), flexShrink: 0 }}>
      {name[0]?.toUpperCase()}
    </div>
  );
}

// ───── グループ一覧タブ ─────
function GroupList({ groups, users, onMemberChange }) {
  const [search, setSearch] = useState('');
  const [expandedId, setExpandedId] = useState(null);
  const [adding, setAdding] = useState(null); // groupId being added to
  const [addSearch, setAddSearch] = useState('');
  const [saving, setSaving] = useState(false);

  const filtered = groups.filter(g =>
    !search || g.handle.toLowerCase().includes(search.toLowerCase())
  );

  const handleToggleMember = async (groupId, userId, action) => {
    setSaving(true);
    try {
      const res = await api.slackGroupUpdateMember(groupId, action, userId);
      onMemberChange(groupId, res.members);
    } catch (e) { console.error(e); alert('更新に失敗しました'); }
    finally { setSaving(false); }
  };

  return (
    <div>
      <input placeholder="グループを検索… (@handle)"
        value={search} onChange={e => setSearch(e.target.value)}
        style={{ width: 260, fontSize: 13, padding: '7px 12px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', marginBottom: 16 }} />

      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {filtered.map(group => {
          const isOpen = expandedId === group.id;
          const addablUsers = users.filter(u => !group.members.find(m => m.user_id === u.user_id));
          const addFiltered = addSearch ? addablUsers.filter(u => (u.display_name || u.real_name || '').toLowerCase().includes(addSearch.toLowerCase())) : addablUsers;

          return (
            <div key={group.id} style={{ border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden', background: '#fff' }}>
              <button onClick={() => { setExpandedId(isOpen ? null : group.id); setAdding(null); setAddSearch(''); }}
                style={{ width: '100%', display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left' }}>
                <span style={{ fontWeight: 700, fontSize: 13, color: '#111827', flex: 1 }}>@{group.handle}</span>
                <span style={{ fontSize: 12, color: '#9ca3af' }}>{group.members.length}名</span>
                <span style={{ fontSize: 10, color: '#9ca3af' }}>{isOpen ? '▲' : '▼'}</span>
              </button>

              {isOpen && (
                <div style={{ borderTop: '1px solid #f3f4f6', padding: '10px 14px' }}>
                  {/* メンバー一覧 */}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 10 }}>
                    {group.members.length === 0
                      ? <span style={{ fontSize: 12, color: '#9ca3af', fontStyle: 'italic' }}>メンバーなし</span>
                      : group.members.map(m => {
                          const name = (m.display_name || m.real_name || '').split('/')[0].trim();
                          return (
                            <div key={m.user_id} style={{ display: 'flex', alignItems: 'center', gap: 5, background: '#f9fafb', border: '1px solid #f3f4f6', borderRadius: 7, padding: '3px 8px 3px 4px' }}>
                              <Avatar user={m} size={22} />
                              <span style={{ fontSize: 12, fontWeight: 600 }}>{name}</span>
                              <button onClick={() => handleToggleMember(group.id, m.user_id, 'remove')} disabled={saving}
                                style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#d1d5db', fontSize: 14, lineHeight: 1, padding: '0 1px' }}>×</button>
                            </div>
                          );
                        })}
                  </div>

                  {/* メンバー追加 */}
                  {adding === group.id ? (
                    <div>
                      <input autoFocus placeholder="名前で検索…" value={addSearch} onChange={e => setAddSearch(e.target.value)}
                        style={{ width: '100%', fontSize: 12, padding: '5px 8px', border: '1px solid #e5e7eb', borderRadius: 6, outline: 'none', boxSizing: 'border-box', marginBottom: 4 }} />
                      <div style={{ border: '1px solid #e5e7eb', borderRadius: 6, maxHeight: 160, overflowY: 'auto', background: '#fff' }}>
                        {addFiltered.slice(0, 20).map(u => {
                          const name = (u.display_name || u.real_name || '').split('/')[0].trim();
                          return (
                            <button key={u.user_id} onClick={() => { handleToggleMember(group.id, u.user_id, 'add'); setAdding(null); setAddSearch(''); }}
                              style={{ display: 'flex', alignItems: 'center', gap: 6, width: '100%', padding: '6px 8px', background: 'none', border: 'none', cursor: 'pointer', textAlign: 'left', fontSize: 12 }}
                              onMouseEnter={e => e.currentTarget.style.background = '#f9fafb'}
                              onMouseLeave={e => e.currentTarget.style.background = 'none'}>
                              <Avatar user={u} size={20} />
                              <span>{name}</span>
                              {u.title && <span style={{ fontSize: 11, color: '#9ca3af', marginLeft: 'auto' }}>{u.title}</span>}
                            </button>
                          );
                        })}
                        {addFiltered.length === 0 && <div style={{ padding: '8px', fontSize: 12, color: '#9ca3af' }}>見つかりません</div>}
                      </div>
                      <button onClick={() => { setAdding(null); setAddSearch(''); }}
                        style={{ marginTop: 4, fontSize: 11, color: '#9ca3af', background: 'none', border: 'none', cursor: 'pointer' }}>キャンセル</button>
                    </div>
                  ) : (
                    <button onClick={() => setAdding(group.id)}
                      style={{ fontSize: 12, color: '#6b7280', background: '#f9fafb', border: '1px dashed #e5e7eb', borderRadius: 6, padding: '4px 12px', cursor: 'pointer' }}>
                      ＋ メンバーを追加
                    </button>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ───── メンバー別管理タブ（入退社/異動） ─────
function MemberView({ groups, users, userGroups, onMemberChange, teams, userTeamMap }) {
  const [search, setSearch] = useState('');
  const [selectedUser, setSelectedUser] = useState(null);
  const [pending, setPending] = useState({});
  const [saving, setSaving] = useState(false);
  const [showWizard, setShowWizard] = useState(false);

  const filteredUsers = search
    ? users.filter(u => (u.display_name || u.real_name || '').toLowerCase().includes(search.toLowerCase()))
    : [];

  const currentGroups = selectedUser ? (userGroups[selectedUser.user_id] || []) : [];
  const currentGroupIds = new Set(currentGroups.map(g => g.id));

  const selectUser = (u) => {
    setSelectedUser(u);
    setPending({});
    setSearch('');
  };

  const toggleGroup = (groupId) => {
    const isIn = currentGroupIds.has(groupId);
    const hasPending = pending[groupId];
    if (hasPending) {
      setPending(p => { const n = {...p}; delete n[groupId]; return n; });
    } else {
      setPending(p => ({ ...p, [groupId]: isIn ? 'remove' : 'add' }));
    }
  };

  const pendingCount = Object.keys(pending).length;

  const handleApply = async () => {
    if (!selectedUser || pendingCount === 0) return;
    setSaving(true);
    try {
      for (const [groupId, action] of Object.entries(pending)) {
        const res = await api.slackGroupUpdateMember(groupId, action, selectedUser.user_id);
        onMemberChange(groupId, res.members);
      }
      // userGroups を更新するため再フェッチは親で行う
      setPending({});
      alert(`${pendingCount}件の変更を適用しました`);
    } catch (e) { console.error(e); alert('一部の変更に失敗しました'); }
    finally { setSaving(false); }
  };

  // 退社: 全グループから削除
  const handleLeave = async () => {
    if (!selectedUser || !window.confirm(`「${(selectedUser.display_name||selectedUser.real_name||'').split('/')[0].trim()}」を全グループから削除しますか？`)) return;
    setSaving(true);
    try {
      for (const g of currentGroups) {
        const res = await api.slackGroupUpdateMember(g.id, 'remove', selectedUser.user_id);
        onMemberChange(g.id, res.members);
      }
      setPending({});
      alert(`全グループから削除しました`);
    } catch (e) { console.error(e); alert('失敗しました'); }
    finally { setSaving(false); }
  };

  return (
    <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start' }}>
      {/* ウィザードモーダル */}
      {showWizard && selectedUser && (
        <ChangeWizard
          user={selectedUser}
          currentGroupIds={currentGroupIds}
          currentTeamName={userTeamMap[selectedUser.user_id] || null}
          groups={groups}
          teams={teams}
          onClose={() => setShowWizard(false)}
          onApplied={() => {}}
        />
      )}

      {/* 左: ユーザー検索 */}
      <div style={{ width: 220, flexShrink: 0 }}>
        <div style={{ fontWeight: 700, fontSize: 12, color: '#6b7280', marginBottom: 8 }}>対象者を選択</div>
        <input placeholder="名前で検索…" value={search} onChange={e => setSearch(e.target.value)}
          style={{ width: '100%', fontSize: 13, padding: '7px 10px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', boxSizing: 'border-box', marginBottom: 6 }} />
        {filteredUsers.length > 0 && (
          <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, maxHeight: 300, overflowY: 'auto', background: '#fff' }}>
            {filteredUsers.slice(0, 20).map(u => {
              const name = (u.display_name || u.real_name || '').split('/')[0].trim();
              const isSelected = selectedUser?.user_id === u.user_id;
              return (
                <button key={u.user_id} onClick={() => selectUser(u)}
                  style={{ display: 'flex', alignItems: 'center', gap: 8, width: '100%', padding: '7px 10px', background: isSelected ? '#eff6ff' : 'none', border: 'none', cursor: 'pointer', textAlign: 'left' }}
                  onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = '#f9fafb'; }}
                  onMouseLeave={e => { if (!isSelected) e.currentTarget.style.background = 'none'; }}>
                  <Avatar user={u} size={24} />
                  <div>
                    <div style={{ fontSize: 13, fontWeight: 600, color: '#111827' }}>{name}</div>
                    {u.title && <div style={{ fontSize: 11, color: '#9ca3af' }}>{u.title}</div>}
                  </div>
                </button>
              );
            })}
          </div>
        )}

        {/* 選択中ユーザー情報 */}
        {selectedUser && (
          <div style={{ marginTop: 12, padding: '10px 12px', background: '#eff6ff', borderRadius: 8, border: '1px solid #bfdbfe' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Avatar user={selectedUser} size={32} />
              <div>
                <div style={{ fontWeight: 700, fontSize: 13 }}>{(selectedUser.display_name || selectedUser.real_name || '').split('/')[0].trim()}</div>
                {selectedUser.title && <div style={{ fontSize: 11, color: '#6b7280' }}>{selectedUser.title}</div>}
              </div>
            </div>
            <div style={{ fontSize: 11, color: '#6b7280', marginTop: 6 }}>{currentGroups.length}グループ所属</div>
          </div>
        )}
      </div>

      {/* 右: グループ一覧 + 変更 */}
      {selectedUser ? (
        <div style={{ flex: 1 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
            <div style={{ fontWeight: 700, fontSize: 13, color: '#374151' }}>グループのメンバーシップを編集</div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button onClick={() => setShowWizard(true)}
                style={{ fontSize: 12, padding: '5px 12px', borderRadius: 6, border: '1px solid #c8b96e', background: '#fffef0', color: '#b45309', cursor: 'pointer', fontWeight: 700 }}>
                🔄 役職・部署変更
              </button>
              <button onClick={handleLeave} disabled={saving || currentGroups.length === 0}
                style={{ fontSize: 12, padding: '5px 12px', borderRadius: 6, border: '1px solid #fee2e2', background: 'none', color: '#ef4444', cursor: 'pointer' }}>
                退社（全グループから削除）
              </button>
              {pendingCount > 0 && (
                <button onClick={handleApply} disabled={saving}
                  className="btn btn-primary" style={{ fontSize: 12, padding: '5px 14px' }}>
                  {saving ? '適用中…' : `${pendingCount}件を適用`}
                </button>
              )}
            </div>
          </div>

          <div style={{ fontSize: 11, color: '#9ca3af', marginBottom: 8 }}>✓ = 現在所属 | クリックで追加/削除をマーク → 「適用」で確定</div>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: 6 }}>
            {groups.map(g => {
              const isIn = currentGroupIds.has(g.id);
              const pAction = pending[g.id];
              const willBeIn = pAction === 'add' ? true : pAction === 'remove' ? false : isIn;
              return (
                <button key={g.id} onClick={() => toggleGroup(g.id)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 6, padding: '7px 10px',
                    border: `1.5px solid ${pAction ? '#f59e0b' : willBeIn ? '#3b82f6' : '#e5e7eb'}`,
                    borderRadius: 8, background: pAction ? '#fffbeb' : willBeIn ? '#eff6ff' : '#fff',
                    cursor: 'pointer', textAlign: 'left', fontSize: 12,
                  }}>
                  <span style={{ fontWeight: 700, color: pAction ? '#d97706' : willBeIn ? '#1d4ed8' : '#9ca3af', fontSize: 14, width: 14 }}>
                    {pAction === 'add' ? '+' : pAction === 'remove' ? '−' : willBeIn ? '✓' : ''}
                  </span>
                  <span style={{ color: willBeIn ? '#111827' : '#6b7280' }}>@{g.handle}</span>
                  <span style={{ fontSize: 10, color: '#9ca3af', marginLeft: 'auto' }}>{g.members.length}</span>
                </button>
              );
            })}
          </div>
        </div>
      ) : (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#9ca3af', padding: '60px 0' }}>
          <div style={{ textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', marginBottom: 8 }}>👤</div>
            <div>左の検索から対象者を選択してください</div>
          </div>
        </div>
      )}
    </div>
  );
}

// ───── 変更ウィザード ─────
function ChangeWizard({ user, currentGroupIds, currentTeamName, groups, teams, onClose, onApplied }) {
  const [rules, setRules] = useState(null);
  const [roleSelection, setRoleSelection] = useState(null); // role name | 'none'
  const [deptSelection, setDeptSelection] = useState(null); // dept rule id
  const [applying, setApplying] = useState(false);

  useEffect(() => {
    api.slackGroupRules().then(r => setRules(r.rules)).catch(console.error);
  }, []);

  const userName = (user.display_name || user.real_name || '').split('/')[0].trim();
  const groupHandleMap = Object.fromEntries(groups.map(g => [g.id, g.handle]));

  // チーム階層ヘルパー
  const getAncestors = (teamName) => {
    const result = [];
    let cur = teamName;
    for (let i = 0; i < 5; i++) {
      const team = teams.find(t => t.name === cur);
      if (!team?.parent_id) break;
      const parent = teams.find(t => t.id === team.parent_id);
      if (!parent) break;
      result.push(parent.name);
      cur = parent.name;
    }
    return result;
  };
  const getTopLevelParent = (teamName) => {
    const ancs = getAncestors(teamName);
    return ancs.length > 0 ? ancs[ancs.length - 1] : teamName;
  };

  const roleRules = (rules || []).filter(r => r.category === 'role');
  const deptRules = (rules || []).filter(r => r.category === 'dept');
  const autoRules = (rules || []).filter(r => r.category === 'auto');

  // 現在の役職を推定（全変種のいずれかのグループに属していれば一致）
  const currentRoleName = (() => {
    const seen = new Set();
    let best = null, bestCount = 0;
    for (const r of roleRules) {
      if (seen.has(r.name)) continue;
      seen.add(r.name);
      const allIds = roleRules.filter(x => x.name === r.name).flatMap(x => x.group_ids || []);
      if (allIds.length === 0) continue;
      const count = allIds.filter(gid => currentGroupIds.has(gid)).length;
      // 一致率 = マッチ数 / ユニーク数（重複排除後）
      const unique = new Set(allIds).size;
      const ratio = count / unique;
      if (ratio > 0 && count > bestCount) { bestCount = count; best = r.name; }
    }
    return best;
  })();

  // 現在の部署: チーム設定の所属チーム名を正とする
  // deptRule との名前一致は不要 - currentTeamName を直接使う
  const currentDeptRule = currentTeamName
    ? (deptRules.find(r => r.name === currentTeamName) || { name: currentTeamName, group_ids: [] })
    : null;

  // 適用対象の部署名（変更先 or 現在）
  const targetDeptRule = deptSelection ? deptRules.find(r => r.id === deptSelection) : null;
  // 変更先があればそれ、なければチーム設定の現在チーム名（teams の階層から親を辿れる）
  const effectiveDeptName = targetDeptRule?.name || currentTeamName;
  const effectiveTopLevel = effectiveDeptName ? getTopLevelParent(effectiveDeptName) : null;

  const preview = (() => {
    if (!rules) return { add: [], remove: [] };
    const addSet = new Set(), removeSet = new Set();

    // 役職次元
    if (roleSelection) {
      const allRoleGroups = new Set(roleRules.flatMap(r => r.group_ids || []));
      if (roleSelection === 'none') {
        // なし → 全役職グループを外す
        [...allRoleGroups].filter(gid => currentGroupIds.has(gid)).forEach(gid => removeSet.add(gid));
      } else {
        const shouldBeIn = new Set();
        // 全部署共通ルール (dept_name=null)
        roleRules.filter(r => r.name === roleSelection && !r.dept_name)
          .forEach(r => (r.group_ids || []).forEach(gid => shouldBeIn.add(gid)));
        // 部署固有ルール (dept_name=effectiveTopLevel)
        if (effectiveTopLevel) {
          roleRules.filter(r => r.name === roleSelection && r.dept_name === effectiveTopLevel)
            .forEach(r => (r.group_ids || []).forEach(gid => shouldBeIn.add(gid)));
        }
        [...shouldBeIn].filter(gid => !currentGroupIds.has(gid)).forEach(gid => addSet.add(gid));
        [...allRoleGroups].filter(gid => !shouldBeIn.has(gid) && currentGroupIds.has(gid)).forEach(gid => removeSet.add(gid));
      }
    }

    // 部署次元
    if (deptSelection && targetDeptRule) {
      const allDeptGroups = new Set(deptRules.flatMap(r => r.group_ids || []));
      const shouldBeIn = new Set(targetDeptRule.group_ids || []);
      // 先祖部署のルールも含める
      for (const anc of getAncestors(targetDeptRule.name)) {
        const ancRule = deptRules.find(r => r.name === anc);
        (ancRule?.group_ids || []).forEach(gid => shouldBeIn.add(gid));
      }
      [...shouldBeIn].filter(gid => !currentGroupIds.has(gid)).forEach(gid => addSet.add(gid));
      [...allDeptGroups].filter(gid => !shouldBeIn.has(gid) && currentGroupIds.has(gid)).forEach(gid => removeSet.add(gid));
    }

    // 共通条件 (auto):
    // 「全員」ルールは常に適用、部署名ルールは対象部署が一致する場合のみ適用
    for (const rule of autoRules) {
      const isAllRule = rule.name.includes('全員') || rule.name.includes('@inrevo');
      const isDeptMatch = effectiveTopLevel && rule.name === effectiveTopLevel;
      if (!isAllRule && !isDeptMatch) continue;
      (rule.group_ids || []).forEach(gid => {
        if (!currentGroupIds.has(gid)) addSet.add(gid);
        removeSet.delete(gid);
      });
    }

    return {
      add: [...addSet].map(gid => ({ id: gid, handle: groupHandleMap[gid] || gid })),
      remove: [...removeSet].map(gid => ({ id: gid, handle: groupHandleMap[gid] || gid })),
    };
  })();

  const hasSelections = roleSelection || deptSelection;
  const hasChanges = preview.add.length > 0 || preview.remove.length > 0;

  const handleApply = async () => {
    setApplying(true);
    try {
      await api.slackGroupChangeApply(user.user_id, preview.add, preview.remove);
      onApplied();
      onClose();
    } catch (e) { console.error(e); alert('適用に失敗しました'); }
    finally { setApplying(false); }
  };

  const BtnSet = ({ selected, isCurrent, label, onClick, color }) => (
    <button onClick={onClick}
      style={{ fontSize: 12, padding: '4px 12px', borderRadius: 99,
        border: `1.5px solid ${selected ? (color || '#6d28d9') : '#e5e7eb'}`,
        background: selected ? '#f5f3ff' : '#fff',
        color: selected ? (color || '#6d28d9') : isCurrent ? '#9ca3af' : '#374151',
        cursor: 'pointer', fontWeight: selected ? 700 : 400, opacity: isCurrent && !selected ? 0.6 : 1 }}>
      {isCurrent ? `✓ ${label}（現在）` : label}
    </button>
  );

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', zIndex: 1000, display: 'flex', alignItems: 'center', justifyContent: 'center' }}
      onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ background: '#fff', borderRadius: 12, padding: 28, width: 560, maxWidth: '92vw', maxHeight: '88vh', overflowY: 'auto', boxShadow: '0 20px 60px rgba(0,0,0,0.2)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
          <div>
            <h3 style={{ margin: '0 0 3px', fontSize: '1rem', fontWeight: 800 }}>変更ウィザード</h3>
            <div style={{ fontSize: 13, color: '#6b7280' }}>{userName} のグループを変更します</div>
          </div>
          <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#9ca3af', fontSize: 20 }}>✕</button>
        </div>

        {!rules ? (
          <div style={{ color: '#9ca3af', padding: 24, textAlign: 'center' }}>読み込み中…</div>
        ) : (
          <>
            {/* 役職セクション */}
            <div style={{ marginBottom: 16, border: '1px solid #ede9fe', borderRadius: 10, overflow: 'hidden' }}>
              <div style={{ background: '#f5f3ff', padding: '8px 14px', display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: 11, fontWeight: 800, color: '#6d28d9', textTransform: 'uppercase', letterSpacing: '0.06em' }}>役職</span>
                {currentRoleName && <span style={{ fontSize: 11, color: '#6b7280' }}>現在: {currentRoleName}</span>}
                {roleSelection && roleSelection !== 'change_none' && <span style={{ fontSize: 11, color: '#6d28d9', fontWeight: 700 }}>→ {roleSelection === 'none' ? 'なし' : roleSelection}</span>}
              </div>
              <div style={{ padding: '8px 10px', display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                <BtnSet selected={!roleSelection} label="変更なし" onClick={() => setRoleSelection(null)} />
                {ROLE_NAMES.filter(n => n !== 'なし').map(name => (
                  <BtnSet key={name} selected={roleSelection === name} isCurrent={name === currentRoleName} label={name}
                    onClick={() => setRoleSelection(roleSelection === name ? null : name)} color="#6d28d9" />
                ))}
                <BtnSet selected={roleSelection === 'none'} label="なし（全役職グループ外す）"
                  onClick={() => setRoleSelection(roleSelection === 'none' ? null : 'none')} color="#ef4444" />
              </div>
            </div>

            {/* 部署セクション */}
            <div style={{ marginBottom: 16, border: '1px solid #cffafe', borderRadius: 10, overflow: 'hidden' }}>
              <div style={{ background: '#ecfeff', padding: '8px 14px', display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: 11, fontWeight: 800, color: '#0891b2', textTransform: 'uppercase', letterSpacing: '0.06em' }}>部署</span>
                {currentDeptRule && <span style={{ fontSize: 11, color: '#6b7280' }}>現在: {currentDeptRule.name}</span>}
                {targetDeptRule && <span style={{ fontSize: 11, color: '#0891b2', fontWeight: 700 }}>→ {targetDeptRule.name}</span>}
              </div>
              <div style={{ padding: '8px 10px', display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                <BtnSet selected={!deptSelection} label="変更なし" onClick={() => setDeptSelection(null)} />
                {deptRules.map(r => (
                  <BtnSet key={r.id} selected={deptSelection === r.id} isCurrent={r.id === currentDeptRule?.id} label={r.name}
                    onClick={() => setDeptSelection(deptSelection === r.id ? null : r.id)} color="#0891b2" />
                ))}
              </div>
            </div>

            {/* プレビュー */}
            {hasSelections && (
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 12, fontWeight: 700, color: '#6b7280', marginBottom: 8 }}>変更内容プレビュー</div>
                {!hasChanges ? (
                  <div style={{ padding: '10px 14px', background: '#f9fafb', borderRadius: 8, fontSize: 13, color: '#6b7280' }}>
                    変更なし（既に正しいグループに所属しています）
                  </div>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                    {preview.add.length > 0 && (
                      <div style={{ background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, padding: '10px 14px' }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color: '#15803d', marginBottom: 6 }}>追加</div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                          {preview.add.map(g => <span key={g.id} style={{ fontSize: 12, background: '#dcfce7', color: '#15803d', padding: '3px 10px', borderRadius: 99, fontWeight: 700 }}>＋@{g.handle}</span>)}
                        </div>
                      </div>
                    )}
                    {preview.remove.length > 0 && (
                      <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8, padding: '10px 14px' }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color: '#dc2626', marginBottom: 6 }}>削除</div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                          {preview.remove.map(g => <span key={g.id} style={{ fontSize: 12, background: '#fee2e2', color: '#dc2626', padding: '3px 10px', borderRadius: 99, fontWeight: 700 }}>−@{g.handle}</span>)}
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
              <button onClick={onClose} className="btn btn-secondary" style={{ fontSize: 12 }}>キャンセル</button>
              <button onClick={handleApply} disabled={applying || !hasChanges} className="btn btn-primary" style={{ fontSize: 12 }}>
                {applying ? '適用中…' : hasChanges ? `${preview.add.length + preview.remove.length}件を適用` : '変更なし'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ───── マスタ設定タブ ─────
const ROLE_NAMES = ['なし', 'lead', 'sub chief', 'chief', 'sub expert', 'expert', 'sub manager', 'manager'];

function RulesTab({ groups, teams }) {
  const [rules, setRules] = useState([]);
  const [visibility, setVisibility] = useState({}); // groupId -> string[]
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState({});
  const [tab, setTab] = useState('role');
  const [expandedRoles, setExpandedRoles] = useState(new Set());
  const [showGroupManager, setShowGroupManager] = useState(false);
  const [visSaving, setVisSaving] = useState({});

  useEffect(() => {
    Promise.all([api.slackGroupRules(), api.slackGroupVisibility()])
      .then(([rulesRes, visRes]) => {
        setRules(rulesRes.rules || []);
        setVisibility(visRes.visibility || {});
      }).catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const toggleTabVisibility = async (gid, tabName) => {
    const cur = visibility[gid] || [];
    const next = cur.includes(tabName) ? cur.filter(t => t !== tabName) : [...cur, tabName];
    setVisibility(prev => ({ ...prev, [gid]: next }));
    setVisSaving(prev => ({ ...prev, [gid]: true }));
    try { await api.slackGroupVisibilityUpdate(gid, next); }
    catch (e) {
      console.error(e);
      setVisibility(prev => ({ ...prev, [gid]: cur }));
    } finally { setVisSaving(prev => { const n = {...prev}; delete n[gid]; return n; }); }
  };

  const isHiddenInTab = (gid, tabName) => (visibility[gid] || []).includes(tabName);

  // ルール取得（dept_name 対応）
  const getRule = (name, category, deptName = null) =>
    rules.find(r => r.name === name && r.category === category && (r.dept_name || null) === (deptName || null)) || null;

  // チェック変更 → ルール自動作成 or 更新
  const handleToggle = async (rowName, category, groupId, deptName = null) => {
    const key = `${category}:${rowName}:${deptName}:${groupId}`;
    setSaving(prev => ({ ...prev, [key]: true }));
    try {
      let rule = getRule(rowName, category, deptName);
      if (!rule) {
        const res = await api.slackGroupRuleCreate({ name: rowName, category, deptName, groupIds: [groupId] });
        setRules(prev => [...prev, res.rule]);
      } else {
        const cur = rule.group_ids || [];
        const isIn = cur.includes(groupId);
        const next = isIn ? cur.filter(id => id !== groupId) : [...cur, groupId];
        setRules(prev => prev.map(r => r.id === rule.id ? { ...r, group_ids: next } : r));
        await api.slackGroupRuleUpdate(rule.id, { name: rule.name, category, deptName, groupIds: next });
      }
    } catch (e) { console.error(e); }
    finally { setSaving(prev => { const n = { ...prev }; delete n[key]; return n; }); }
  };

  const isChecked = (name, category, groupId, deptName = null) =>
    (getRule(name, category, deptName)?.group_ids || []).includes(groupId);
  const isSavingKey = (key) => !!saving[key];

  // 部署フラットリスト（階層順）
  const topLevelTeams = teams.filter(t => !t.parent_id);
  const childrenOf = {};
  for (const t of teams) {
    if (t.parent_id) { if (!childrenOf[t.parent_id]) childrenOf[t.parent_id] = []; childrenOf[t.parent_id].push(t); }
  }
  const deptRows = [];
  for (const p of topLevelTeams) {
    deptRows.push({ ...p, indent: 0 });
    for (const c of (childrenOf[p.id] || [])) deptRows.push({ ...c, indent: 1 });
  }

  // 共通条件の行
  const autoRows = [
    { name: '全員 (@inrevo.jp)', category: 'auto', indent: 0 },
    ...topLevelTeams.map(t => ({ name: t.name, category: 'auto', indent: 0 })),
  ];

  // 共通条件グループは role/dept タブで非表示
  const autoGroupIds = new Set(rules.filter(r => r.category === 'auto').flatMap(r => r.group_ids || []));
  const sortedGroups = [...groups].sort((a, b) => a.handle.localeCompare(b.handle));
  const visibleGroups = sortedGroups
    .filter(g => tab === 'auto' ? true : !autoGroupIds.has(g.id))
    .filter(g => !isHiddenInTab(g.id, tab));

  // テーブル共通スタイル
  const stickyTh = { padding: '10px 16px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', minWidth: 200, position: 'sticky', left: 0, background: '#f8fafc', zIndex: 2, whiteSpace: 'nowrap', boxShadow: '2px 0 4px rgba(0,0,0,0.06)' };
  const stickyTd = (indent = 0, bold = true) => ({ padding: `8px ${16 + indent * 14}px`, fontWeight: bold ? 700 : 400, color: '#111827', position: 'sticky', left: 0, background: '#fff', zIndex: 1, borderRight: '2px solid #e5e7eb', whiteSpace: 'nowrap', boxShadow: '2px 0 4px rgba(0,0,0,0.06)' });
  const cellTd = { textAlign: 'center', borderLeft: '1px solid #f3f4f6', padding: 0, minWidth: 90 };
  const labelSt = { display: 'flex', alignItems: 'center', justifyContent: 'center', height: 36, cursor: 'pointer', margin: 0 };

  if (loading) return <div style={{ color: '#9ca3af', padding: 24 }}>読み込み中…</div>;

  return (
    <div>
      {/* タブ + 表示切替 */}
      <div style={{ display: 'flex', alignItems: 'center', marginBottom: 16, gap: 12 }}>
        <div style={{ display: 'flex', gap: 4, padding: '3px', background: '#f1f5f9', borderRadius: 8 }}>
          {[['role', '役職'], ['dept', '部署'], ['auto', '共通条件']].map(([v, l]) => (
            <button key={v} onClick={() => setTab(v)}
              style={{ padding: '5px 16px', borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: '0.82rem',
                fontWeight: v === tab ? 700 : 400, background: v === tab ? '#fff' : 'transparent',
                color: v === tab ? (v === 'auto' ? '#059669' : '#374151') : '#9ca3af',
                boxShadow: v === tab ? '0 1px 3px rgba(0,0,0,0.1)' : 'none' }}>
              {l}
            </button>
          ))}
        </div>
        <button onClick={() => setShowGroupManager(v => !v)}
          style={{ fontSize: 12, padding: '4px 12px', borderRadius: 6, border: '1px solid #e5e7eb',
            background: '#fff', color: '#6b7280', cursor: 'pointer', marginLeft: 'auto' }}>
          グループ表示設定
        </button>
      </div>

      {/* グループ表示設定パネル */}
      {showGroupManager && (
        <div style={{ marginBottom: 16, border: '1px solid #e5e7eb', borderRadius: 10, background: '#fff', padding: 16 }}>
          <div style={{ marginBottom: 10 }}>
            <span style={{ fontWeight: 700, fontSize: 13 }}>グループ表示設定</span>
            <span style={{ fontSize: 12, color: '#9ca3af', marginLeft: 8 }}>各タブでの表示/非表示をDB保存</span>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ borderCollapse: 'collapse', fontSize: 12, width: '100%' }}>
              <thead>
                <tr style={{ background: '#f8fafc' }}>
                  <th style={{ padding: '6px 12px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', whiteSpace: 'nowrap' }}>グループ</th>
                  {[['role','役職'], ['dept','部署'], ['auto','共通条件']].map(([v, l]) => (
                    <th key={v} style={{ padding: '6px 16px', textAlign: 'center', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', whiteSpace: 'nowrap', borderLeft: '1px solid #f3f4f6' }}>{l}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedGroups.map(g => {
                  const isSav = !!visSaving[g.id];
                  return (
                    <tr key={g.id} style={{ borderBottom: '1px solid #f9fafb' }}>
                      <td style={{ padding: '5px 12px', fontWeight: 500, color: '#374151', whiteSpace: 'nowrap' }}>@{g.handle}</td>
                      {[['role','#6d28d9'], ['dept','#0891b2'], ['auto','#059669']].map(([v, col]) => {
                        const hidden = isHiddenInTab(g.id, v);
                        return (
                          <td key={v} style={{ textAlign: 'center', borderLeft: '1px solid #f9fafb' }}>
                            <label style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 30, cursor: isSav ? 'wait' : 'pointer', margin: 0 }}>
                              <input type="checkbox" checked={!hidden} disabled={isSav}
                                onChange={() => toggleTabVisibility(g.id, v)}
                                style={{ accentColor: col, width: 14, height: 14, margin: 0 }} />
                            </label>
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab === 'auto' && (
        <div style={{ marginBottom: 12, padding: '10px 14px', background: '#f0fdf4', border: '1px solid #bbf7d0', borderRadius: 8, fontSize: 12, color: '#15803d' }}>
          役職・部署に関わらず<strong>常に</strong>適用される条件です。変更ウィザードでも自動で組み込まれます。
        </div>
      )}
      <p style={{ margin: '0 0 12px', fontSize: 12, color: '#9ca3af' }}>変更は即座に保存されます</p>

      <div style={{ overflowX: 'auto', border: '1px solid #e5e7eb', borderRadius: 10 }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ background: '#f8fafc' }}>
              <th style={stickyTh}>{tab === 'role' ? '役職' : tab === 'auto' ? '共通条件' : '部署'}</th>
              {visibleGroups.map(g => (
                <th key={g.id} style={{ padding: '8px 10px', textAlign: 'center', borderBottom: '1px solid #e5e7eb', borderLeft: '1px solid #f3f4f6', minWidth: 90, whiteSpace: 'nowrap' }}>
                  <span style={{ fontSize: 12, color: '#374151', fontWeight: 600 }}>@{g.handle}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {/* ───── 役職タブ: 展開式 ───── */}
            {tab === 'role' && ROLE_NAMES.map(roleName => {
              const isNone = roleName === 'なし';
              const isExpanded = expandedRoles.has(roleName);
              return [
                /* 役職ヘッダー行 */
                <tr key={`hdr-${roleName}`} style={{ borderBottom: '1px solid #f3f4f6', background: '#f9fafb', cursor: isNone ? 'default' : 'pointer' }}
                  onClick={() => !isNone && setExpandedRoles(prev => { const n = new Set(prev); n.has(roleName) ? n.delete(roleName) : n.add(roleName); return n; })}>
                  <td style={{ ...stickyTd(0, true), background: '#f9fafb' }}>
                    {!isNone && <span style={{ display: 'inline-block', width: 14, fontSize: 10, color: '#9ca3af' }}>{isExpanded ? '▼' : '▶'}</span>}
                    {isNone ? <span style={{ color: '#9ca3af', fontStyle: 'italic', fontSize: 12 }}>なし（全役職グループ外す）</span> : <strong>{roleName}</strong>}
                  </td>
                  {visibleGroups.map(g => <td key={g.id} style={cellTd} />)}
                </tr>,
                /* 展開時: 共通サブ行 */
                isExpanded && !isNone && (
                  <tr key={`${roleName}:common`} style={{ borderBottom: '1px solid #f3f4f6' }}>
                    <td style={{ ...stickyTd(1, false), color: '#6b7280', fontSize: 12 }}>共通（全部署）</td>
                    {visibleGroups.map(g => {
                      const key = `role:${roleName}:null:${g.id}`;
                      const checked = isChecked(roleName, 'role', g.id, null);
                      return (
                        <td key={g.id} style={cellTd}>
                          <label style={labelSt}>
                            <input type="checkbox" checked={checked} disabled={isSavingKey(key)}
                              onChange={() => handleToggle(roleName, 'role', g.id, null)}
                              style={{ accentColor: CATEGORY_COLORS.role, width: 15, height: 15, margin: 0 }} />
                          </label>
                        </td>
                      );
                    })}
                  </tr>
                ),
                /* 展開時: 部署別サブ行 */
                ...(isExpanded && !isNone ? topLevelTeams.filter(t => t.name === 'HR').map(dept => (
                  <tr key={`${roleName}:${dept.name}`} style={{ borderBottom: '1px solid #f3f4f6' }}>
                    <td style={{ ...stickyTd(1, false), fontSize: 12 }}>{dept.name}</td>
                    {visibleGroups.map(g => {
                      const key = `role:${roleName}:${dept.name}:${g.id}`;
                      const checked = isChecked(roleName, 'role', g.id, dept.name);
                      return (
                        <td key={g.id} style={cellTd}>
                          <label style={labelSt}>
                            <input type="checkbox" checked={checked} disabled={isSavingKey(key)}
                              onChange={() => handleToggle(roleName, 'role', g.id, dept.name)}
                              style={{ accentColor: CATEGORY_COLORS.dept, width: 15, height: 15, margin: 0 }} />
                          </label>
                        </td>
                      );
                    })}
                  </tr>
                )) : []),
              ];
            })}

            {/* ───── 部署タブ ───── */}
            {tab === 'dept' && deptRows.map(row => (
              <tr key={row.id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                <td style={stickyTd(row.indent, row.indent === 0)}>
                  {row.indent > 0 && <span style={{ color: '#d1d5db', marginRight: 4 }}>└</span>}
                  {row.name}
                </td>
                {visibleGroups.map(g => {
                  const key = `dept:${row.name}:null:${g.id}`;
                  const checked = isChecked(row.name, 'dept', g.id);
                  return (
                    <td key={g.id} style={cellTd}>
                      <label style={labelSt}>
                        <input type="checkbox" checked={checked} disabled={isSavingKey(key)}
                          onChange={() => handleToggle(row.name, 'dept', g.id)}
                          style={{ accentColor: CATEGORY_COLORS.dept, width: 15, height: 15, margin: 0 }} />
                      </label>
                    </td>
                  );
                })}
              </tr>
            ))}

            {/* ───── 共通条件タブ ───── */}
            {tab === 'auto' && autoRows.map(row => (
              <tr key={row.name} style={{ borderBottom: '1px solid #f3f4f6' }}>
                <td style={stickyTd(row.indent, true)}>{row.name}</td>
                {visibleGroups.map(g => {
                  const key = `auto:${row.name}:null:${g.id}`;
                  const checked = isChecked(row.name, 'auto', g.id);
                  return (
                    <td key={g.id} style={cellTd}>
                      <label style={labelSt}>
                        <input type="checkbox" checked={checked} disabled={isSavingKey(key)}
                          onChange={() => handleToggle(row.name, 'auto', g.id)}
                          style={{ accentColor: '#059669', width: 15, height: 15, margin: 0 }} />
                      </label>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ───── メインページ ─────
export default function SlackGroups() {
  const [data, setData] = useState(null);
  const [teams, setTeams] = useState([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState('member');

  const [userTeamMap, setUserTeamMap] = useState({}); // userId → teamName

  const load = async () => {
    setLoading(true);
    try {
      const [d, orgRes] = await Promise.all([api.slackGroups(), api.orgChart()]);
      setData(d);
      const filteredTeams = (orgRes.teams || []).filter(t => t.show_in_orgchart !== false);
      setTeams(filteredTeams);
      // userId → 所属チーム名のマップを構築
      const map = {};
      for (const [teamId, members] of Object.entries(orgRes.membersMap || {})) {
        const team = filteredTeams.find(t => t.id === teamId);
        if (!team) continue;
        for (const m of members) map[m.user_id] = team.name;
      }
      setUserTeamMap(map);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, []);

  const handleMemberChange = (groupId, newMemberIds) => {
    setData(prev => {
      if (!prev) return prev;
      // userGroups マップを再構築
      const newGroups = prev.groups.map(g => {
        if (g.id !== groupId) return g;
        const members = newMemberIds.map(uid => {
          const existing = g.members.find(m => m.user_id === uid);
          if (existing) return existing;
          const u = prev.users.find(u => u.user_id === uid);
          return u ? { user_id: uid, display_name: u.display_name, real_name: u.real_name, avatar_url: u.avatar_url, title: u.title } : { user_id: uid, display_name: uid, real_name: uid };
        });
        return { ...g, members };
      });
      // userGroups 再構築
      const userGroups = {};
      for (const g of newGroups) {
        for (const m of g.members) {
          if (!userGroups[m.user_id]) userGroups[m.user_id] = [];
          userGroups[m.user_id].push({ id: g.id, handle: g.handle });
        }
      }
      return { ...prev, groups: newGroups, userGroups };
    });
  };

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: '0 0 4px', fontSize: '1rem', fontWeight: 700 }}>Slackメンション管理</h2>
          <p style={{ margin: 0, fontSize: '0.82rem', color: '#6b7280' }}>
            ユーザーグループのメンバーを管理・更新します。変更は即時Slackに反映されます。
          </p>
        </div>
        <button onClick={load} disabled={loading}
          style={{ fontSize: 12, color: '#6b7280', background: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: 6, padding: '5px 12px', cursor: 'pointer' }}>
          {loading ? '読み込み中…' : '🔄 再読み込み'}
        </button>
      </div>

      {/* タブ */}
      <div style={{ display: 'flex', gap: 4, padding: '3px', background: '#f1f5f9', borderRadius: 8, marginBottom: 20, width: 'fit-content' }}>
        {[['member', '👤 メンバー別管理'], ['group', '📋 グループ一覧'], ['rules', '⚙️ ルール設定']].map(([v, l]) => (
          <button key={v} onClick={() => setTab(v)}
            style={{ padding: '5px 16px', borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: '0.82rem',
              fontWeight: v === tab ? 700 : 400, background: v === tab ? '#fff' : 'transparent',
              color: v === tab ? '#374151' : '#9ca3af', boxShadow: v === tab ? '0 1px 3px rgba(0,0,0,0.1)' : 'none' }}>
            {l}
          </button>
        ))}
      </div>

      {loading ? (
        <div style={{ color: '#9ca3af', padding: 24 }}>読み込み中…</div>
      ) : !data ? (
        <div style={{ color: '#ef4444', padding: 24 }}>データの取得に失敗しました</div>
      ) : tab === 'group' ? (
        <GroupList groups={data.groups} users={data.users} onMemberChange={handleMemberChange} />
      ) : tab === 'rules' ? (
        <RulesTab groups={data.groups} teams={teams} />
      ) : (
        <MemberView groups={data.groups} users={data.users} userGroups={data.userGroups} onMemberChange={handleMemberChange} teams={teams} userTeamMap={userTeamMap} />
      )}
    </div>
  );
}
