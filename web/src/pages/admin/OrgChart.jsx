import { useEffect, useState } from 'react';
import { api } from '../../api/client';

// 権限再設計 Phase 2-5: 組織・役職・所属・権限の管理 UI
// docs/permission-redesign.md

export default function OrgChartAdmin() {
  const [tab, setTab] = useState('chart');
  const [units, setUnits] = useState([]);
  const [positions, setPositions] = useState([]);
  const [members, setMembers] = useState([]);
  const [status, setStatus] = useState(null);
  const [features, setFeatures] = useState([]);
  const [grants, setGrants] = useState([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState(null);

  // モーダル状態
  const [unitModal, setUnitModal] = useState(null);       // {mode:'create'|'edit', data}
  const [positionModal, setPositionModal] = useState(null);
  const [memberModal, setMemberModal] = useState(null);   // {user_id, display_name}
  const [assignModal, setAssignModal] = useState(null);   // 所属追加モーダル
  const [grantModal, setGrantModal] = useState(null);     // 権限グラント追加モーダル

  function notify(message, type = 'success') {
    setToast({ message, type });
    setTimeout(() => setToast(null), 3000);
  }

  async function reload() {
    setLoading(true);
    try {
      const [u, p, m, s, fc, gr] = await Promise.all([
        api.orgUnits().catch(() => ({ units: [] })),
        api.orgPositions().catch(() => ({ positions: [] })),
        api.orgMembers().catch(() => ({ members: [] })),
        api.orgMigrationStatus().catch(() => null),
        api.orgFeatureCatalog().catch(() => ({ features: [] })),
        api.orgPermissionGrants().catch(() => ({ grants: [] })),
      ]);
      setUnits(u.units || []);
      setPositions(p.positions || []);
      setMembers(m.members || []);
      setStatus(s);
      setFeatures(fc.features || []);
      setGrants(gr.grants || []);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { reload(); }, []);

  // ─ 移行 ─
  async function handleMigrate() {
    if (!confirm('既存 dash_teams を新テーブルにコピーします（冪等）。実行しますか？')) return;
    setBusy(true);
    try {
      const r = await api.orgMigrateFromLegacy();
      notify(`移行完了 unit:+${r.summary.createdUnits} assignment:+${r.summary.createdAssignments}`);
      await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
    finally { setBusy(false); }
  }
  async function handleMigratePermissions() {
    if (!confirm('dashboard_roles + feature_access を permission_grants に複製します（冪等）。実行しますか？')) return;
    setBusy(true);
    try {
      const r = await api.orgMigratePermissions();
      notify(`権限移行 admin:${r.summary.admins} corp:${r.summary.corps} fa:${r.summary.featureAccessCopied}`);
      await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
    finally { setBusy(false); }
  }

  // ─ unit CRUD ─
  async function saveUnit(form) {
    setBusy(true);
    try {
      if (unitModal.mode === 'create') await api.orgUnitCreate(form);
      else await api.orgUnitUpdate(unitModal.data.id, form);
      setUnitModal(null);
      notify('保存しました');
      await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
    finally { setBusy(false); }
  }
  async function deleteUnit(unit) {
    if (!confirm(`「${unit.name}」を削除しますか？`)) return;
    setBusy(true);
    try {
      try { await api.orgUnitDelete(unit.id); }
      catch (e) {
        const msg = JSON.parse(e.message?.match(/\{.*\}/)?.[0] || '{}');
        if (msg.error === 'has active assignments') {
          if (!confirm(`${msg.assignments}件の所属があります。全部終了して削除しますか？`)) { setBusy(false); return; }
          await api.orgUnitDelete(unit.id, true);
        } else throw e;
      }
      notify('削除しました');
      await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
    finally { setBusy(false); }
  }

  // ─ position CRUD ─
  async function savePosition(form) {
    setBusy(true);
    try {
      if (positionModal.mode === 'create') await api.orgPositionCreate(form);
      else await api.orgPositionUpdate(positionModal.data.id, form);
      setPositionModal(null);
      notify('保存しました');
      await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
    finally { setBusy(false); }
  }
  async function deletePosition(p) {
    if (!confirm(`役職「${p.name}」を削除しますか？`)) return;
    try {
      await api.orgPositionDelete(p.id);
      notify('削除しました'); await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
  }

  // ─ assignment CRUD ─
  async function saveAssignment(form) {
    setBusy(true);
    try {
      await api.orgAssignmentCreate({ ...form, user_id: assignModal.user_id });
      setAssignModal(null);
      notify('所属追加しました');
      await reload();
      // メンバーモーダル再表示
      const m = (await api.orgMembers()).members.find(x => x.user_id === assignModal.user_id);
      if (m) setMemberModal(m);
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
    finally { setBusy(false); }
  }
  async function terminateAssignment(assignmentId, userId) {
    if (!confirm('この所属を終了しますか？（履歴は残ります）')) return;
    try {
      await api.orgAssignmentDelete(assignmentId);
      notify('終了しました');
      await reload();
      const m = (await api.orgMembers()).members.find(x => x.user_id === userId);
      if (m) setMemberModal(m); else setMemberModal(null);
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
  }
  async function updateAssignment(assignmentId, body, userId) {
    try {
      await api.orgAssignmentUpdate(assignmentId, body);
      notify('更新しました');
      await reload();
      const m = (await api.orgMembers()).members.find(x => x.user_id === userId);
      if (m) setMemberModal(m);
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
  }

  // ─ grant CRUD ─
  async function saveGrant(form) {
    try {
      await api.orgPermissionGrantCreate(form);
      setGrantModal(null);
      notify('追加しました'); await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
  }
  async function deleteGrant(id) {
    if (!confirm('このグラントを削除しますか？')) return;
    try {
      await api.orgPermissionGrantDelete(id);
      notify('削除しました'); await reload();
    } catch (e) { notify('エラー: ' + e.message, 'error'); }
  }

  // ─ ツリー組み立て ─
  function buildTree(list) {
    const byId = new Map(list.map(u => [u.id, { ...u, children: [] }]));
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
        <span style={{ marginLeft: 12 }}>
          <button onClick={() => setUnitModal({ mode: 'create', data: { parent_id: node.id, type: node.type === 'ceo' ? 'division' : (node.type === 'division' ? 'dept' : 'team') } })}
            style={btnMini('#10b981')}>+ 子追加</button>
          {node.type !== 'ceo' && (
            <>
              <button onClick={() => setUnitModal({ mode: 'edit', data: node })} style={btnMini('#3b82f6')}>編集</button>
              <button onClick={() => deleteUnit(node)} style={btnMini('#ef4444')}>削除</button>
            </>
          )}
        </span>
        {node.children?.map(c => renderNode(c, depth + 1))}
      </div>
    );
  }
  const tree = buildTree(units);

  if (loading) return <div style={{ padding: 20 }}>読み込み中...</div>;

  return (
    <div style={{ padding: 20, maxWidth: 1100, position: 'relative' }}>
      <h2 style={{ margin: '0 0 16px', fontSize: '1.2rem' }}>組織・役職・権限管理</h2>

      {/* 移行カード */}
      <div style={cardStyle}>
        <div style={{ display: 'flex', gap: 24, fontSize: 13, alignItems: 'center', flexWrap: 'wrap' }}>
          <div>
            <div style={lblStyle}>新スキーマ</div>
            <div>org_units: <strong>{status?.newSchema?.orgUnits ?? '—'}</strong></div>
            <div>有効 assignments: <strong>{status?.newSchema?.activeAssignments ?? '—'}</strong></div>
            <div>permission_grants: <strong>{grants.length}</strong></div>
          </div>
          <div>
            <div style={lblStyle}>旧スキーマ（参考）</div>
            <div>dash_teams: {status?.legacy?.dashTeams ?? '—'}</div>
            <div>dash_team_members: {status?.legacy?.dashTeamMembers ?? '—'}</div>
          </div>
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
            <button onClick={handleMigrate} disabled={busy} style={btn('#3b82f6')}>組織データ移行</button>
            <button onClick={handleMigratePermissions} disabled={busy} style={btn('#8b5cf6')}>権限データ移行</button>
          </div>
        </div>
      </div>

      {/* タブ */}
      <div style={{ display: 'flex', gap: 4, borderBottom: '1px solid #e5e7eb', marginBottom: 16 }}>
        {[
          { key: 'chart',     label: `組織ツリー (${units.length})` },
          { key: 'positions', label: `役職 (${positions.length})` },
          { key: 'members',   label: `メンバー (${members.length})` },
          { key: 'grants',    label: `権限グラント (${grants.length})` },
        ].map(t => (
          <button key={t.key} onClick={() => setTab(t.key)} style={tabStyle(tab === t.key)}>{t.label}</button>
        ))}
      </div>

      {/* タブ内容 */}
      {tab === 'chart' && (
        <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8, padding: 16 }}>
          {tree.length === 0
            ? <div style={{ color: '#9ca3af' }}>組織がありません</div>
            : tree.map(node => renderNode(node))}
        </div>
      )}

      {tab === 'positions' && (
        <div>
          <button onClick={() => setPositionModal({ mode: 'create', data: { level: 1, sort_order: 0 } })}
            style={{ ...btn('#10b981'), marginBottom: 8 }}>+ 役職追加</button>
          <table style={tableStyle}>
            <thead><tr style={theadRow}>
              <th style={thStyle}>ID</th><th style={thStyle}>名前</th><th style={thStyle}>Level</th><th style={thStyle}></th>
            </tr></thead>
            <tbody>
              {positions.map(p => (
                <tr key={p.id} style={trStyle}>
                  <td style={{ ...tdStyle, color: '#6b7280' }}>{p.id}</td>
                  <td style={tdStyle}>{p.name}</td>
                  <td style={tdStyle}>{p.level}</td>
                  <td style={tdStyle}>
                    <button onClick={() => setPositionModal({ mode: 'edit', data: p })} style={btnMini('#3b82f6')}>編集</button>
                    <button onClick={() => deletePosition(p)} style={btnMini('#ef4444')}>削除</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'members' && (
        <div>
          <div style={{ marginBottom: 8, fontSize: 12, color: '#6b7280' }}>
            行クリックで所属詳細・編集
          </div>
          <table style={tableStyle}>
            <thead><tr style={theadRow}>
              <th style={thStyle}>名前</th><th style={thStyle}>所属（役職）★=本務</th>
            </tr></thead>
            <tbody>
              {members.map(m => (
                <tr key={m.user_id} style={{ ...trStyle, cursor: 'pointer' }} onClick={() => setMemberModal(m)}>
                  <td style={tdStyle}>{m.display_name || m.real_name || m.user_id}</td>
                  <td style={tdStyle}>
                    {m.assignments.length === 0
                      ? <span style={{ color: '#9ca3af' }}>—</span>
                      : m.assignments.map((a, i) => (
                        <span key={i} style={tagStyle(a.is_primary)}>
                          {a.org_unit_name} ({a.position_name}){a.is_primary && ' ★'}
                        </span>
                      ))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'grants' && (
        <div>
          <button onClick={() => setGrantModal({ feature_key: features[0]?.key, subject_type: 'org_unit', effect: 'allow' })}
            style={{ ...btn('#10b981'), marginBottom: 8 }}>+ 権限追加</button>
          <table style={tableStyle}>
            <thead><tr style={theadRow}>
              <th style={thStyle}>機能</th><th style={thStyle}>対象</th><th style={thStyle}>効果</th><th style={thStyle}></th>
            </tr></thead>
            <tbody>
              {grants.map(g => (
                <tr key={g.id} style={trStyle}>
                  <td style={tdStyle}>{g.feature_key}</td>
                  <td style={tdStyle}>{g.subject_type}: {g.subject_label || g.composite_json?.user_id || g.subject_id}</td>
                  <td style={tdStyle}>
                    <span style={{
                      padding: '2px 6px', borderRadius: 3, fontSize: 11,
                      background: g.effect === 'allow' ? '#dcfce7' : '#fee2e2',
                      color: g.effect === 'allow' ? '#166534' : '#991b1b',
                    }}>{g.effect}</span>
                  </td>
                  <td style={tdStyle}>
                    <button onClick={() => deleteGrant(g.id)} style={btnMini('#ef4444')}>削除</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* ─ モーダル群 ─ */}
      {unitModal && (
        <UnitModal modal={unitModal} units={units} onClose={() => setUnitModal(null)} onSave={saveUnit} />
      )}
      {positionModal && (
        <PositionModal modal={positionModal} onClose={() => setPositionModal(null)} onSave={savePosition} />
      )}
      {memberModal && (
        <MemberModal
          member={memberModal} positions={positions} units={units}
          onClose={() => setMemberModal(null)}
          onAddAssignment={() => setAssignModal({ user_id: memberModal.user_id })}
          onTerminate={(aid) => terminateAssignment(aid, memberModal.user_id)}
          onUpdate={(aid, body) => updateAssignment(aid, body, memberModal.user_id)}
        />
      )}
      {assignModal && (
        <AssignModal
          units={units} positions={positions}
          onClose={() => setAssignModal(null)} onSave={saveAssignment}
        />
      )}
      {grantModal && (
        <GrantModal
          features={features} units={units} positions={positions} members={members}
          onClose={() => setGrantModal(null)} onSave={saveGrant}
        />
      )}

      {toast && (
        <div style={{
          position: 'fixed', bottom: 20, right: 20, padding: '10px 16px',
          background: toast.type === 'error' ? '#dc2626' : '#10b981', color: 'white',
          borderRadius: 6, fontSize: 13, boxShadow: '0 4px 12px rgba(0,0,0,0.15)', zIndex: 1000,
        }}>{toast.message}</div>
      )}

      <div style={{ marginTop: 16, fontSize: 11, color: '#9ca3af' }}>
        ※ Phase 2-5 編集機能を実装。旧テーブルへの fallback は Phase 6 で実装予定。詳細: docs/permission-redesign.md
      </div>
    </div>
  );
}

// ─ サブコンポーネント ─

function UnitModal({ modal, units, onClose, onSave }) {
  const [form, setForm] = useState({
    name: modal.data?.name || '',
    type: modal.data?.type || 'dept',
    parent_id: modal.data?.parent_id ?? '',
    sort_order: modal.data?.sort_order || 0,
  });
  return (
    <Modal title={modal.mode === 'create' ? '組織ユニット追加' : '組織ユニット編集'} onClose={onClose}>
      <Field label="名前"><input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} style={inputStyle} /></Field>
      <Field label="タイプ">
        <select value={form.type} onChange={e => setForm({ ...form, type: e.target.value })} style={inputStyle}>
          <option value="division">division (事業部)</option>
          <option value="dept">dept (部署)</option>
          <option value="team">team (チーム)</option>
        </select>
      </Field>
      <Field label="親">
        <select value={form.parent_id || ''} onChange={e => setForm({ ...form, parent_id: e.target.value ? Number(e.target.value) : null })} style={inputStyle}>
          <option value="">（最上位）</option>
          {units.filter(u => u.id !== modal.data?.id).map(u => (
            <option key={u.id} value={u.id}>{u.name} ({u.type})</option>
          ))}
        </select>
      </Field>
      <Field label="並び順"><input type="number" value={form.sort_order} onChange={e => setForm({ ...form, sort_order: Number(e.target.value) })} style={inputStyle} /></Field>
      <ModalFooter onClose={onClose} onSave={() => onSave(form)} />
    </Modal>
  );
}

function PositionModal({ modal, onClose, onSave }) {
  const [form, setForm] = useState({
    name: modal.data?.name || '',
    level: modal.data?.level ?? 1,
    sort_order: modal.data?.sort_order || 0,
  });
  return (
    <Modal title={modal.mode === 'create' ? '役職追加' : '役職編集'} onClose={onClose}>
      <Field label="名前"><input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} style={inputStyle} /></Field>
      <Field label="Level (1-8)"><input type="number" value={form.level} onChange={e => setForm({ ...form, level: Number(e.target.value) })} style={inputStyle} /></Field>
      <Field label="並び順"><input type="number" value={form.sort_order} onChange={e => setForm({ ...form, sort_order: Number(e.target.value) })} style={inputStyle} /></Field>
      <ModalFooter onClose={onClose} onSave={() => onSave(form)} />
    </Modal>
  );
}

function MemberModal({ member, positions, units, onClose, onAddAssignment, onTerminate, onUpdate }) {
  return (
    <Modal title={`${member.display_name || member.user_id} の所属`} onClose={onClose} width={680}>
      <div style={{ marginBottom: 12 }}>
        <button onClick={onAddAssignment} style={btn('#10b981')}>+ 所属追加</button>
      </div>
      {member.assignments.length === 0
        ? <div style={{ color: '#9ca3af' }}>所属がありません</div>
        : (
          <table style={tableStyle}>
            <thead><tr style={theadRow}>
              <th style={thStyle}>所属</th><th style={thStyle}>役職</th><th style={thStyle}>本務</th><th style={thStyle}></th>
            </tr></thead>
            <tbody>
              {member.assignments.map((a) => (
                <tr key={a.assignment_id} style={trStyle}>
                  <td style={tdStyle}>{a.org_unit_name}</td>
                  <td style={tdStyle}>
                    <select value={a.position_id}
                      onChange={e => onUpdate(a.assignment_id, { position_id: Number(e.target.value) })}
                      style={{ ...inputStyle, padding: '2px 6px', fontSize: 12 }}>
                      {positions.map(p => <option key={p.id} value={p.id}>{p.name} (L{p.level})</option>)}
                    </select>
                  </td>
                  <td style={tdStyle}>
                    <input type="radio" name={`primary-${member.user_id}`}
                      checked={a.is_primary}
                      onChange={() => onUpdate(a.assignment_id, { is_primary: true })} />
                  </td>
                  <td style={tdStyle}>
                    <button onClick={() => onTerminate(a.assignment_id)} style={btnMini('#ef4444')}>終了</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      <ModalFooter onClose={onClose} hideSave />
    </Modal>
  );
}

function AssignModal({ units, positions, onClose, onSave }) {
  const [form, setForm] = useState({
    org_unit_id: units.find(u => u.type !== 'ceo')?.id || '',
    position_id: positions[0]?.id || '',
    is_primary: false,
  });
  return (
    <Modal title="所属追加" onClose={onClose}>
      <Field label="所属先">
        <select value={form.org_unit_id} onChange={e => setForm({ ...form, org_unit_id: Number(e.target.value) })} style={inputStyle}>
          {units.filter(u => u.type !== 'ceo').map(u => (
            <option key={u.id} value={u.id}>{u.name} ({u.type})</option>
          ))}
        </select>
      </Field>
      <Field label="役職">
        <select value={form.position_id} onChange={e => setForm({ ...form, position_id: Number(e.target.value) })} style={inputStyle}>
          {positions.map(p => <option key={p.id} value={p.id}>{p.name} (L{p.level})</option>)}
        </select>
      </Field>
      <Field label="本務（is_primary）">
        <input type="checkbox" checked={form.is_primary} onChange={e => setForm({ ...form, is_primary: e.target.checked })} />
        <span style={{ marginLeft: 6, fontSize: 12, color: '#6b7280' }}>ONにすると他の本務は解除されます</span>
      </Field>
      <ModalFooter onClose={onClose} onSave={() => onSave(form)} />
    </Modal>
  );
}

function GrantModal({ features, units, positions, members, onClose, onSave }) {
  const [form, setForm] = useState({
    feature_key: features[0]?.key || '',
    subject_type: 'org_unit',
    subject_id: '',
    effect: 'allow',
  });
  return (
    <Modal title="権限グラント追加" onClose={onClose}>
      <Field label="機能">
        <select value={form.feature_key} onChange={e => setForm({ ...form, feature_key: e.target.value })} style={inputStyle}>
          {features.map(f => <option key={f.key} value={f.key}>{f.label} ({f.key})</option>)}
        </select>
      </Field>
      <Field label="対象タイプ">
        <select value={form.subject_type} onChange={e => setForm({ ...form, subject_type: e.target.value, subject_id: '' })} style={inputStyle}>
          <option value="org_unit">org_unit (組織)</option>
          <option value="position">position (役職)</option>
          <option value="user">user (個人)</option>
        </select>
      </Field>
      <Field label="対象">
        {form.subject_type === 'org_unit' && (
          <select value={form.subject_id} onChange={e => setForm({ ...form, subject_id: Number(e.target.value) })} style={inputStyle}>
            <option value="">（選択）</option>
            {units.map(u => <option key={u.id} value={u.id}>{u.name} ({u.type})</option>)}
          </select>
        )}
        {form.subject_type === 'position' && (
          <select value={form.subject_id} onChange={e => setForm({ ...form, subject_id: Number(e.target.value) })} style={inputStyle}>
            <option value="">（選択）</option>
            {positions.map(p => <option key={p.id} value={p.id}>{p.name} (L{p.level})</option>)}
          </select>
        )}
        {form.subject_type === 'user' && (
          <select value={form.subject_id} onChange={e => setForm({ ...form, subject_id: e.target.value })} style={inputStyle}>
            <option value="">（選択）</option>
            {members.map(m => <option key={m.user_id} value={m.user_id}>{m.display_name || m.user_id}</option>)}
          </select>
        )}
      </Field>
      <Field label="効果">
        <select value={form.effect} onChange={e => setForm({ ...form, effect: e.target.value })} style={inputStyle}>
          <option value="allow">allow（許可）</option>
          <option value="deny">deny（拒否）</option>
        </select>
      </Field>
      <ModalFooter onClose={onClose} onSave={() => {
        if (!form.subject_id) { alert('対象を選択してください'); return; }
        // user の場合は composite_json で渡す
        if (form.subject_type === 'user') {
          onSave({
            feature_key: form.feature_key,
            subject_type: 'user',
            composite_json: { user_id: form.subject_id },
            effect: form.effect,
          });
        } else {
          onSave({
            feature_key: form.feature_key,
            subject_type: form.subject_type,
            subject_id: Number(form.subject_id),
            effect: form.effect,
          });
        }
      }} />
    </Modal>
  );
}

// ─ 共通モーダル/スタイル ─
function Modal({ title, onClose, width = 480, children }) {
  return (
    <div style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)',
      display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 999,
    }} onClick={onClose}>
      <div style={{
        background: 'white', borderRadius: 8, padding: 20, width, maxWidth: '90%',
        maxHeight: '85vh', overflow: 'auto',
      }} onClick={e => e.stopPropagation()}>
        <h3 style={{ margin: '0 0 16px', fontSize: '1rem' }}>{title}</h3>
        {children}
      </div>
    </div>
  );
}
function Field({ label, children }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 4 }}>{label}</div>
      {children}
    </div>
  );
}
function ModalFooter({ onClose, onSave, hideSave }) {
  return (
    <div style={{ marginTop: 16, display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
      <button onClick={onClose} style={btn('#6b7280')}>{hideSave ? '閉じる' : 'キャンセル'}</button>
      {!hideSave && <button onClick={onSave} style={btn('#3b82f6')}>保存</button>}
    </div>
  );
}

const inputStyle = { width: '100%', padding: '6px 10px', border: '1px solid #d1d5db', borderRadius: 4, fontSize: 13 };
const cardStyle = { background: '#f8fafc', border: '1px solid #e5e7eb', borderRadius: 8, padding: 12, marginBottom: 16 };
const lblStyle = { color: '#6b7280', fontSize: 11 };
const tableStyle = { width: '100%', borderCollapse: 'collapse', background: '#fff' };
const theadRow = { background: '#f9fafb', borderBottom: '1px solid #e5e7eb' };
const thStyle = { padding: 10, textAlign: 'left', fontSize: 12, fontWeight: 600, color: '#374151' };
const trStyle = { borderBottom: '1px solid #f3f4f6' };
const tdStyle = { padding: 10, fontSize: 13 };
const tagStyle = (primary) => ({
  display: 'inline-block', marginRight: 6, marginBottom: 2, padding: '2px 6px',
  borderRadius: 3, background: primary ? '#dbeafe' : '#f3f4f6',
  color: primary ? '#1e40af' : '#374151', fontSize: 11,
});
const tabStyle = (active) => ({
  padding: '8px 16px', border: 'none', background: 'none', cursor: 'pointer',
  fontSize: 13, fontWeight: active ? 700 : 400, color: active ? '#1d4ed8' : '#6b7280',
  borderBottom: active ? '2px solid #3b82f6' : '2px solid transparent',
});
const btn = (color) => ({
  padding: '8px 16px', background: color, color: 'white', border: 'none',
  borderRadius: 6, cursor: 'pointer', fontSize: 13, fontWeight: 600, whiteSpace: 'nowrap',
});
const btnMini = (color) => ({
  padding: '2px 8px', background: color, color: 'white', border: 'none',
  borderRadius: 3, cursor: 'pointer', fontSize: 11, marginLeft: 4,
});
