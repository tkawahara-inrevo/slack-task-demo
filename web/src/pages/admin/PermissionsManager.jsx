import { useEffect, useState } from 'react';
import { api } from '../../api/client';

const SCOPE_COLORS = {
  none: { bg: '#f3f4f6', text: '#9ca3af', border: '#e5e7eb' },
  own:  { bg: '#fef3c7', text: '#d97706', border: '#fde68a' },
  all:  { bg: '#dcfce7', text: '#15803d', border: '#86efac' },
};

function ScopeButton({ scope, current, onClick, label }) {
  const active = scope === current;
  const c = SCOPE_COLORS[scope] || SCOPE_COLORS.none;
  return (
    <button onClick={() => onClick(scope)}
      style={{
        padding: '3px 10px', borderRadius: 6, border: `1.5px solid ${active ? c.border : '#e5e7eb'}`,
        background: active ? c.bg : '#fff', color: active ? c.text : '#9ca3af',
        fontSize: '0.72rem', fontWeight: active ? 700 : 400, cursor: 'pointer', transition: 'all 0.1s',
      }}>
      {label}
    </button>
  );
}

export default function PermissionsManager() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState({});
  const [viewType, setViewType] = useState('role'); // 'role' | 'dept'

  const load = async () => {
    setLoading(true);
    try { setData(await api.permissionsGet()); } catch {}
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, []);

  const getScope = (subjectType, subjectId, featureId) => {
    return data?.permissions?.[subjectType]?.[subjectId]?.[featureId]
      || data?.defaults?.[subjectType]?.[subjectId]?.[featureId]
      || 'none';
  };

  const handleChange = async (subjectType, subjectId, featureId, scope) => {
    const key = `${subjectType}:${subjectId}:${featureId}`;
    setSaving(p => ({ ...p, [key]: true }));
    try {
      await api.permissionsSet({ subjectType, subjectId, feature: featureId, scope });
      setData(prev => {
        const next = { ...prev, permissions: { ...prev.permissions } };
        if (!next.permissions[subjectType]) next.permissions[subjectType] = {};
        if (!next.permissions[subjectType][subjectId]) next.permissions[subjectType][subjectId] = {};
        next.permissions[subjectType][subjectId][featureId] = scope;
        return next;
      });
    } catch {}
    finally { setSaving(p => { const n = { ...p }; delete n[key]; return n; }); }
  };

  if (loading) return <div style={{ padding: 24, color: '#9ca3af' }}>読み込み中...</div>;
  if (!data) return null;

  const subjects = viewType === 'role' ? data.roles : data.departments;
  const subjectType = viewType === 'role' ? 'role' : 'dept';

  // カテゴリ別にフィーチャーをグループ化
  const categories = [...new Set(data.features.map(f => f.category))];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: '0 0 4px', fontSize: '1rem', fontWeight: 700 }}>権限管理</h2>
          <p style={{ margin: 0, fontSize: '0.82rem', color: '#6b7280' }}>
            ロール・部署ごとに各機能へのアクセス権限を設定します
          </p>
        </div>
        <div style={{ display: 'flex', gap: 4, padding: '3px', background: '#f1f5f9', borderRadius: 8 }}>
          {[['role','ロール別'], ['dept','部署別']].map(([v, l]) => (
            <button key={v} onClick={() => setViewType(v)}
              style={{ padding: '5px 14px', borderRadius: 6, border: 'none', cursor: 'pointer', fontSize: '0.82rem', fontWeight: v === viewType ? 700 : 400,
                background: v === viewType ? '#fff' : 'transparent', color: v === viewType ? '#374151' : '#9ca3af',
                boxShadow: v === viewType ? '0 1px 3px rgba(0,0,0,0.1)' : 'none', transition: 'all 0.15s' }}>
              {l}
            </button>
          ))}
        </div>
      </div>

      {/* 凡例 */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 16, fontSize: '0.75rem' }}>
        {Object.entries(data.scopeLabels).map(([k, v]) => (
          <span key={k} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ width: 10, height: 10, borderRadius: 3, background: SCOPE_COLORS[k]?.bg, border: `1px solid ${SCOPE_COLORS[k]?.border}` }} />
            <span style={{ color: SCOPE_COLORS[k]?.text, fontWeight: 600 }}>{v}</span>
          </span>
        ))}
      </div>

      {/* 権限マトリクス */}
      <div style={{ border: '1px solid #e5e7eb', borderRadius: 10, overflow: 'hidden', background: '#fff' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.82rem' }}>
          <thead>
            <tr style={{ background: '#f8fafc' }}>
              <th style={{ padding: '10px 16px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', minWidth: 160 }}>
                機能
              </th>
              {subjects.map(s => (
                <th key={s} style={{ padding: '10px 12px', textAlign: 'center', fontWeight: 700, color: '#374151', borderBottom: '1px solid #e5e7eb', minWidth: 120, borderLeft: '1px solid #f3f4f6' }}>
                  {s}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {categories.map(cat => {
              const catFeatures = data.features.filter(f => f.category === cat);
              return [
                <tr key={`cat-${cat}`}>
                  <td colSpan={subjects.length + 1}
                    style={{ padding: '8px 16px', background: '#f9fafb', fontSize: '0.72rem', fontWeight: 800, color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.06em', borderBottom: '1px solid #f3f4f6' }}>
                    {cat}
                  </td>
                </tr>,
                ...catFeatures.map(feature => (
                  <tr key={feature.id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                    <td style={{ padding: '10px 16px', color: '#374151', fontWeight: 500 }}>
                      {feature.label}
                    </td>
                    {subjects.map(s => {
                      const current = getScope(subjectType, s, feature.id);
                      const key = `${subjectType}:${s}:${feature.id}`;
                      const isSaving = saving[key];
                      return (
                        <td key={s} style={{ padding: '8px 12px', textAlign: 'center', borderLeft: '1px solid #f3f4f6' }}>
                          {isSaving ? (
                            <span style={{ fontSize: '0.72rem', color: '#9ca3af' }}>保存中...</span>
                          ) : (
                            <div style={{ display: 'flex', gap: 4, justifyContent: 'center', flexWrap: 'wrap' }}>
                              {feature.scopes.map(scope => (
                                <ScopeButton key={scope} scope={scope} current={current}
                                  label={data.scopeLabels[scope]}
                                  onClick={newScope => handleChange(subjectType, s, feature.id, newScope)} />
                              ))}
                            </div>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                )),
              ];
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
