import { useEffect, useState, useMemo } from 'react';
import { api } from '../../api/client';
import { Lock, User, Tag, Users, Plus, X, ChevronRight } from 'lucide-react';

const ROLE_OPTIONS = [
  { value: 'admin',     label: 'admin（管理者）' },
  { value: 'corp',      label: 'corp（Corporate）' },
  { value: 'it',        label: 'it（ITチーム）' },
  { value: 'personnel', label: 'personnel（Personnelチーム）' },
  { value: 'manager',   label: 'manager（マネージャー）' },
  { value: 'bc_manager',label: 'bc_manager（BC管理職）' },
  { value: 'member',    label: 'member（一般）' },
];

const SUBJECT_LABELS = {
  user:            { label: 'ユーザー個別', Icon: User,  color: '#3b82f6' },
  role:            { label: '役職ロール',   Icon: Tag,   color: '#a855f7' },
  slack_usergroup: { label: 'Slackユーザーグループ', Icon: Users, color: '#10b981' },
};

export default function FeatureAccessAdmin() {
  const [features, setFeatures]   = useState([]);
  const [selected, setSelected]   = useState(null);
  const [grants, setGrants]       = useState([]);
  const [loading, setLoading]     = useState(true);
  const [adding, setAdding]       = useState(false);
  const [usergroups, setUsergroups] = useState([]);
  const [userDir, setUserDir]     = useState([]);
  const [showAddForm, setShowAddForm] = useState(false);
  const [addType, setAddType] = useState('user');
  const [addValue, setAddValue] = useState('');
  const [userQuery, setUserQuery] = useState('');

  useEffect(() => {
    Promise.all([
      api.featureAccessCatalog().catch(() => ({ features: [] })),
      api.usergroups().catch(() => ({ usergroups: [] })),
      api.adminUserMapping().catch(() => ({ members: [] })),
    ]).then(([cat, ug, dir]) => {
      setFeatures(Array.isArray(cat?.features) ? cat.features : []);
      setUsergroups(Array.isArray(ug?.usergroups) ? ug.usergroups : Array.isArray(ug) ? ug : []);
      setUserDir(Array.isArray(dir?.members) ? dir.members : Array.isArray(dir?.items) ? dir.items : Array.isArray(dir) ? dir : []);
      if (cat?.features?.length) setSelected(cat.features[0]);
    }).finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (!selected) return;
    setLoading(true);
    api.featureAccessGrants(selected.key)
      .then(r => setGrants(r.grants || []))
      .catch(() => setGrants([]))
      .finally(() => setLoading(false));
  }, [selected?.key]);

  const refreshGrants = () => {
    if (!selected) return;
    api.featureAccessGrants(selected.key).then(r => setGrants(r.grants || []));
  };

  const handleAdd = async () => {
    if (!addValue || !selected) return;
    setAdding(true);
    try {
      await api.featureAccessAdd(selected.key, { subject_type: addType, subject_id: addValue });
      setShowAddForm(false);
      setAddValue('');
      setUserQuery('');
      refreshGrants();
    } catch { alert('追加に失敗しました'); }
    finally { setAdding(false); }
  };

  const handleDelete = async (g) => {
    if (!window.confirm('この許可を削除しますか？')) return;
    try {
      await api.featureAccessDelete(selected.key, g.subject_type, g.subject_id);
      refreshGrants();
    } catch { alert('削除に失敗しました'); }
  };

  const userDirMap = useMemo(() => {
    const m = new Map();
    for (const u of userDir) m.set(u.user_id || u.userId, u);
    return m;
  }, [userDir]);

  const subjectLabel = (g) => {
    if (g.subject_type === 'user') {
      const u = userDirMap.get(g.subject_id);
      return u ? `${u.display_name || u.real_name || g.subject_id}（${g.subject_id}）` : g.subject_id;
    }
    if (g.subject_type === 'role') {
      const r = ROLE_OPTIONS.find(o => o.value === g.subject_id);
      return r ? r.label : g.subject_id;
    }
    if (g.subject_type === 'slack_usergroup') {
      const ug = usergroups.find(u => (u.id || u.usergroup_id) === g.subject_id);
      return ug ? `@${ug.handle || ug.name || g.subject_id}` : g.subject_id;
    }
    return g.subject_id;
  };

  const filteredUsers = useMemo(() => {
    if (!userQuery) return userDir.slice(0, 20);
    const q = userQuery.toLowerCase();
    return userDir.filter(u =>
      (u.display_name || '').toLowerCase().includes(q) ||
      (u.real_name || '').toLowerCase().includes(q) ||
      (u.user_id || u.userId || '').toLowerCase().includes(q)
    ).slice(0, 20);
  }, [userDir, userQuery]);

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ margin: '0 0 4px', fontSize: '1rem', fontWeight: 700, display: 'flex', alignItems: 'center', gap: 8 }}>
          <Lock size={18} color="#dc2626"/> アクセス権限管理
        </h2>
        <p style={{ margin: 0, fontSize: '0.82rem', color: '#6b7280' }}>
          各機能の閲覧／操作を、ユーザー個別 / Slackユーザーグループ単位で許可します。<br/>
          <strong>admin ロールは常にすべての機能にアクセスできます。</strong>誰も許可がない機能は admin 以外見えません。
        </p>
      </div>

      <div style={{ display: 'flex', gap: 16, alignItems: 'flex-start' }}>
        {/* 左: 機能リスト */}
        <div style={{ width: 280, background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 8, flexShrink: 0 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#6b7280', padding: '6px 10px', textTransform: 'uppercase', letterSpacing: 1 }}>機能</div>
          {features.map(f => (
            <button key={f.key} onClick={() => setSelected(f)}
              style={{ width: '100%', display: 'flex', alignItems: 'center', gap: 8, padding: '8px 10px', borderRadius: 8, border: 'none', background: selected?.key === f.key ? '#eff6ff' : 'transparent', cursor: 'pointer', textAlign: 'left', marginBottom: 2 }}>
              <ChevronRight size={14} color={selected?.key === f.key ? '#2563eb' : '#9ca3af'}/>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: selected?.key === f.key ? '#1d4ed8' : '#111827', marginLeft: f.parent ? 12 : 0 }}>{f.label}</div>
                <div style={{ fontSize: 11, color: '#6b7280', marginLeft: f.parent ? 12 : 0 }}>{f.desc}</div>
              </div>
              {f.sensitive && <span style={{ fontSize: 10, padding: '1px 5px', background: '#fef2f2', color: '#dc2626', borderRadius: 4, fontWeight: 700 }}>機密</span>}
            </button>
          ))}
        </div>

        {/* 右: 選択した機能の許可リスト */}
        <div style={{ flex: 1, background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: 18 }}>
          {!selected ? (
            <div style={{ color: '#9ca3af', textAlign: 'center', padding: 40 }}>機能を選択してください</div>
          ) : (
            <>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16, paddingBottom: 12, borderBottom: '1px solid #f3f4f6' }}>
                <div>
                  <div style={{ fontSize: 16, fontWeight: 800, color: '#111827' }}>{selected.label}</div>
                  <div style={{ fontSize: 12, color: '#6b7280' }}>{selected.desc}</div>
                </div>
                {!showAddForm && (
                  <button onClick={() => setShowAddForm(true)}
                    style={{ background: '#2563eb', color: '#fff', border: 'none', padding: '7px 14px', borderRadius: 8, fontSize: 12, fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 4 }}>
                    <Plus size={14}/> 許可を追加
                  </button>
                )}
              </div>

              {/* 追加フォーム */}
              {showAddForm && (
                <div style={{ background: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: 8, padding: 14, marginBottom: 14 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
                    <label style={{ fontSize: 12, fontWeight: 700, color: '#374151' }}>種別:</label>
                    <select value={addType} onChange={e => { setAddType(e.target.value); setAddValue(''); setUserQuery(''); }}
                      style={{ padding: '5px 8px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: 12 }}>
                      <option value="user">ユーザー個別</option>
                      <option value="slack_usergroup">Slackユーザーグループ</option>
                    </select>
                  </div>
                  {addType === 'user' && (
                    <div style={{ marginBottom: 10 }}>
                      <input value={userQuery} onChange={e => setUserQuery(e.target.value)}
                        placeholder="名前またはユーザーIDで検索（U08...）"
                        style={{ width: '100%', padding: '6px 10px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: 12, marginBottom: 6 }}/>
                      <div style={{ maxHeight: 200, overflowY: 'auto', border: '1px solid #e5e7eb', borderRadius: 6, background: '#fff' }}>
                        {filteredUsers.map(u => {
                          const uid = u.user_id || u.userId;
                          return (
                            <button key={uid} onClick={() => setAddValue(uid)}
                              style={{ display: 'block', width: '100%', textAlign: 'left', padding: '6px 10px', background: addValue === uid ? '#eff6ff' : 'transparent', border: 'none', cursor: 'pointer', fontSize: 12, borderBottom: '1px solid #f3f4f6' }}>
                              <strong>{u.display_name || u.real_name || uid}</strong>
                              <span style={{ color: '#9ca3af', marginLeft: 8 }}>{uid}</span>
                            </button>
                          );
                        })}
                        {filteredUsers.length === 0 && <div style={{ padding: 12, fontSize: 12, color: '#9ca3af', textAlign: 'center' }}>該当ユーザーなし</div>}
                      </div>
                    </div>
                  )}
                  {addType === 'slack_usergroup' && (
                    <select value={addValue} onChange={e => setAddValue(e.target.value)}
                      style={{ width: '100%', padding: '6px 10px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: 12, marginBottom: 10 }}>
                      <option value="">グループを選択…</option>
                      {usergroups.map(g => {
                        const id = g.id || g.usergroup_id;
                        return <option key={id} value={id}>@{g.handle || g.name} ({id})</option>;
                      })}
                    </select>
                  )}
                  <div style={{ display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
                    <button onClick={() => { setShowAddForm(false); setAddValue(''); setUserQuery(''); }}
                      style={{ background: '#fff', border: '1px solid #d1d5db', color: '#374151', padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 700, cursor: 'pointer' }}>
                      キャンセル
                    </button>
                    <button onClick={handleAdd} disabled={!addValue || adding}
                      style={{ background: addValue ? '#2563eb' : '#9ca3af', color: '#fff', border: 'none', padding: '6px 14px', borderRadius: 6, fontSize: 12, fontWeight: 700, cursor: addValue ? 'pointer' : 'not-allowed' }}>
                      {adding ? '追加中…' : '追加'}
                    </button>
                  </div>
                </div>
              )}

              {/* 許可リスト */}
              {loading ? (
                <div style={{ color: '#9ca3af', textAlign: 'center', padding: 30 }}>読み込み中…</div>
              ) : grants.length === 0 ? (
                <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8, padding: 14, fontSize: 13, color: '#991b1b' }}>
                  <strong>⚠ 許可がありません。</strong>現在この機能には admin ロール以外のユーザーはアクセスできません。
                </div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {grants.map(g => {
                    const t = SUBJECT_LABELS[g.subject_type] || { label: g.subject_type, Icon: User, color: '#9ca3af' };
                    const Icon = t.Icon;
                    return (
                      <div key={`${g.subject_type}:${g.subject_id}`} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', background: '#f9fafb', border: '1px solid #f3f4f6', borderRadius: 8 }}>
                        <div style={{ background: t.color + '20', color: t.color, padding: 6, borderRadius: 6, display: 'flex' }}>
                          <Icon size={14}/>
                        </div>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontSize: 11, color: '#6b7280', fontWeight: 700 }}>{t.label}</div>
                          <div style={{ fontSize: 13, color: '#111827', fontWeight: 600 }}>{subjectLabel(g)}</div>
                        </div>
                        <button onClick={() => handleDelete(g)} title="削除"
                          style={{ background: 'none', border: 'none', color: '#9ca3af', cursor: 'pointer', padding: 4 }}>
                          <X size={16}/>
                        </button>
                      </div>
                    );
                  })}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
