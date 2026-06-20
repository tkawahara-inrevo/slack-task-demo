import { useEffect, useState } from 'react';
import { api } from '../../api/client';

// 権限再設計 Phase 3: 組織・役職・所属の読み取り専用 UI
// docs/permission-redesign.md

export default function OrgChart() {
  const [tab, setTab] = useState('chart');
  const [units, setUnits] = useState([]);
  const [positions, setPositions] = useState([]);
  const [members, setMembers] = useState([]);
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [migrating, setMigrating] = useState(false);
  const [migrateResult, setMigrateResult] = useState(null);

  async function reload() {
    setLoading(true);
    try {
      const [u, p, m, s] = await Promise.all([
        api.orgUnits().catch(() => ({ units: [] })),
        api.orgPositions().catch(() => ({ positions: [] })),
        api.orgMembers().catch(() => ({ members: [] })),
        api.orgMigrationStatus().catch(() => null),
      ]);
      setUnits(u.units || []);
      setPositions(p.positions || []);
      setMembers(m.members || []);
      setStatus(s);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { reload(); }, []);

  async function handleMigrate() {
    if (!confirm('既存の dash_teams / dash_team_members から新テーブルへデータを複製します。\n冪等なので何度実行しても安全ですが、進めますか？')) return;
    setMigrating(true);
    setMigrateResult(null);
    try {
      const r = await api.orgMigrateFromLegacy();
      setMigrateResult(r);
      await reload();
    } catch (e) {
      setMigrateResult({ error: e.message });
    } finally {
      setMigrating(false);
    }
  }

  // ツリー構造に組み立て
  function buildTree(units) {
    const byId = new Map(units.map(u => [u.id, { ...u, children: [] }]));
    const roots = [];
    for (const u of byId.values()) {
      if (u.parent_id && byId.has(u.parent_id)) byId.get(u.parent_id).children.push(u);
      else roots.push(u);
    }
    return roots;
  }
  function renderNode(node, depth = 0) {
    return (
      <div key={node.id} style={{ marginLeft: depth * 20, padding: '4px 0' }}>
        <span style={{
          display: 'inline-block', minWidth: 60, fontSize: 11, color: '#6b7280',
          background: '#f3f4f6', borderRadius: 4, padding: '2px 6px', marginRight: 8,
        }}>{node.type}</span>
        <span style={{ fontWeight: node.type === 'ceo' ? 700 : 500 }}>{node.name}</span>
        <span style={{ fontSize: 11, color: '#9ca3af', marginLeft: 8 }}>#{node.id}</span>
        {node.children?.map(c => renderNode(c, depth + 1))}
      </div>
    );
  }

  const tree = buildTree(units);

  if (loading) return <div style={{ padding: 20 }}>読み込み中...</div>;

  return (
    <div style={{ padding: 20, maxWidth: 1000 }}>
      <h2 style={{ margin: '0 0 16px', fontSize: '1.2rem' }}>組織・役職管理（読み取り専用）</h2>

      {/* 移行状態カード */}
      {status && (
        <div style={{
          background: '#f8fafc', border: '1px solid #e5e7eb', borderRadius: 8,
          padding: 12, marginBottom: 16,
        }}>
          <div style={{ display: 'flex', gap: 24, fontSize: 13 }}>
            <div>
              <div style={{ color: '#6b7280', fontSize: 11 }}>新スキーマ</div>
              <div>org_units: <strong>{status.newSchema.orgUnits}</strong></div>
              <div>有効 assignments: <strong>{status.newSchema.activeAssignments}</strong></div>
            </div>
            <div>
              <div style={{ color: '#6b7280', fontSize: 11 }}>旧スキーマ（参考）</div>
              <div>dash_teams: {status.legacy.dashTeams}</div>
              <div>dash_team_members: {status.legacy.dashTeamMembers}</div>
            </div>
            <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center' }}>
              <button
                onClick={handleMigrate}
                disabled={migrating}
                style={{
                  padding: '6px 14px', background: '#3b82f6', color: 'white',
                  border: 'none', borderRadius: 6, cursor: migrating ? 'wait' : 'pointer',
                  fontSize: 12,
                }}
              >{migrating ? '移行中...' : '旧データから移行'}</button>
            </div>
          </div>
          {migrateResult?.summary && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#059669' }}>
              ✓ unit: 作成 {migrateResult.summary.createdUnits} / スキップ {migrateResult.summary.skippedUnits} ／
              assignment: 作成 {migrateResult.summary.createdAssignments} / スキップ {migrateResult.summary.skippedAssignments}
            </div>
          )}
          {migrateResult?.error && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#dc2626' }}>エラー: {migrateResult.error}</div>
          )}
        </div>
      )}

      {/* タブ */}
      <div style={{ display: 'flex', gap: 4, borderBottom: '1px solid #e5e7eb', marginBottom: 16 }}>
        {[
          { key: 'chart',     label: `組織ツリー (${units.length})` },
          { key: 'positions', label: `役職 (${positions.length})` },
          { key: 'members',   label: `メンバー (${members.length})` },
        ].map(t => (
          <button key={t.key} onClick={() => setTab(t.key)} style={{
            padding: '8px 16px', border: 'none', background: 'none', cursor: 'pointer',
            fontSize: 13, fontWeight: tab === t.key ? 700 : 400,
            color: tab === t.key ? '#1d4ed8' : '#6b7280',
            borderBottom: tab === t.key ? '2px solid #3b82f6' : '2px solid transparent',
          }}>{t.label}</button>
        ))}
      </div>

      {/* タブ内容 */}
      {tab === 'chart' && (
        <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8, padding: 16 }}>
          {tree.length === 0 ? (
            <div style={{ color: '#9ca3af', fontSize: 13 }}>組織がまだ設定されていません</div>
          ) : tree.map(node => renderNode(node))}
        </div>
      )}

      {tab === 'positions' && (
        <table style={{ width: '100%', borderCollapse: 'collapse', background: '#fff' }}>
          <thead>
            <tr style={{ background: '#f9fafb', borderBottom: '1px solid #e5e7eb' }}>
              <th style={{ padding: 10, textAlign: 'left', fontSize: 12 }}>ID</th>
              <th style={{ padding: 10, textAlign: 'left', fontSize: 12 }}>名前</th>
              <th style={{ padding: 10, textAlign: 'left', fontSize: 12 }}>Level</th>
            </tr>
          </thead>
          <tbody>
            {positions.map(p => (
              <tr key={p.id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                <td style={{ padding: 10, fontSize: 12, color: '#6b7280' }}>{p.id}</td>
                <td style={{ padding: 10, fontSize: 13 }}>{p.name}</td>
                <td style={{ padding: 10, fontSize: 13 }}>{p.level}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {tab === 'members' && (
        <div>
          <div style={{ marginBottom: 8, fontSize: 12, color: '#6b7280' }}>
            所属がない = まだ移行されていない or 直接登録されていない
          </div>
          <table style={{ width: '100%', borderCollapse: 'collapse', background: '#fff' }}>
            <thead>
              <tr style={{ background: '#f9fafb', borderBottom: '1px solid #e5e7eb' }}>
                <th style={{ padding: 10, textAlign: 'left', fontSize: 12 }}>名前</th>
                <th style={{ padding: 10, textAlign: 'left', fontSize: 12 }}>所属（役職）</th>
              </tr>
            </thead>
            <tbody>
              {members.map(m => (
                <tr key={m.user_id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                  <td style={{ padding: 10, fontSize: 13 }}>
                    {m.display_name || m.real_name || m.user_id}
                  </td>
                  <td style={{ padding: 10, fontSize: 12 }}>
                    {m.assignments.length === 0 ? (
                      <span style={{ color: '#9ca3af' }}>—</span>
                    ) : m.assignments.map((a, i) => (
                      <span key={i} style={{
                        display: 'inline-block', marginRight: 6, marginBottom: 2,
                        padding: '2px 6px', borderRadius: 3,
                        background: a.is_primary ? '#dbeafe' : '#f3f4f6',
                        color: a.is_primary ? '#1e40af' : '#374151',
                        fontSize: 11,
                      }}>
                        {a.org_unit_name} ({a.position_name})
                        {a.is_primary && ' ★'}
                      </span>
                    ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div style={{ marginTop: 16, fontSize: 11, color: '#9ca3af' }}>
        ※ Phase 3（読み取り専用）。編集機能は次フェーズで実装予定。詳細: docs/permission-redesign.md
      </div>
    </div>
  );
}
