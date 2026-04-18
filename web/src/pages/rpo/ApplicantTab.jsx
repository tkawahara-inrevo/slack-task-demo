import { useEffect, useState, useRef } from 'react';
import { api } from '../../api/client';

const STATUSES = ['応募', '書類選考', '一次面接', '二次面接', '最終面接', '内定', '内定承諾', '不合格', '辞退'];

const STATUS_COLORS = {
  '応募':     '#6b7280',
  '書類選考': '#3b82f6',
  '一次面接': '#8b5cf6',
  '二次面接': '#f59e0b',
  '最終面接': '#ef4444',
  '内定':     '#10b981',
  '内定承諾': '#059669',
  '不合格':   '#9ca3af',
  '辞退':     '#9ca3af',
};

const ACTION_TYPES = [
  { value: 'note',         label: 'メモ' },
  { value: 'interview',    label: '面接実施' },
  { value: 'offer',        label: 'オファー' },
  { value: 'followup',     label: 'フォローアップ' },
  { value: 'rejection',    label: '不合格通知' },
  { value: 'withdrawal',   label: '辞退' },
];

export default function ApplicantTab({ client, users = [] }) {
  const [applicants, setApplicants] = useState([]);
  const [counts, setCounts]         = useState([]);
  const [loading, setLoading]       = useState(true);
  const [filterStatus, setFilterStatus] = useState('');
  const [filterSource, setFilterSource] = useState('');
  const [selected, setSelected]     = useState(null);
  const [actions, setActions]       = useState([]);
  const [actionsLoading, setActionsLoading] = useState(false);
  const [masters, setMasters]       = useState([]); // 応募媒体ピッカー用（全マスタではなく案件登録媒体）

  // 案件に登録されている媒体一覧（重複除去）
  const registeredMedia = [...new Set((client.data?.mediaStatus || []).map(m => m.name))];

  // 新規追加フォーム
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm]             = useState({ name: '', status: '応募', source: '', appliedAt: '', notes: '' });
  const [saving, setSaving]         = useState(false);

  // アクション追加
  const [actionForm, setActionForm] = useState({ type: 'note', content: '' });
  const [addingAction, setAddingAction] = useState(false);

  // CSVインポート
  const csvRef = useRef(null);
  const [importing, setImporting]   = useState(false);

  const load = async (status = filterStatus) => {
    setLoading(true);
    try {
      const params = status ? { status } : {};
      const r = await api.rpoApplicants(client.id, params);
      setApplicants(r.applicants || []);
      setCounts(r.counts || []);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
    // この案件に登録されている媒体名（重複除去）をピッカーに使う
    const names = [...new Set((client.data?.mediaStatus || []).map(m => m.name))];
    setMasters(names.map((name, i) => ({ id: i, name })));
  }, [client.id]);

  const handleFilterStatus = (s) => {
    const next = filterStatus === s ? '' : s;
    setFilterStatus(next);
    load(next);
  };

  // 応募者選択 → アクション読み込み
  const selectApplicant = async (ap) => {
    setSelected(ap);
    setActionsLoading(true);
    try {
      const r = await api.rpoApplicantActions(ap.id);
      setActions(r.actions || []);
    } catch (e) { setActions([]); }
    finally { setActionsLoading(false); }
  };

  const handleCreate = async (e) => {
    e.preventDefault();
    if (!form.name.trim()) return;
    setSaving(true);
    try {
      const r = await api.rpoCreateApplicant(client.id, {
        name:      form.name.trim(),
        status:    form.status,
        source:    form.source.trim() || null,
        appliedAt: form.appliedAt || null,
        notes:     form.notes.trim() || null,
      });
      setApplicants(prev => [r.applicant, ...prev]);
      setShowCreate(false);
      setForm({ name: '', status: '応募', source: '', appliedAt: '', notes: '' });
      // countsは再ロード
      load(filterStatus);
    } catch (err) {
      alert('作成に失敗: ' + err.message);
    } finally {
      setSaving(false);
    }
  };

  const handleStatusChange = async (ap, newStatus) => {
    try {
      const r = await api.rpoUpdateApplicant(ap.id, { status: newStatus });
      setApplicants(prev => prev.map(a => a.id === ap.id ? r.applicant : a));
      if (selected?.id === ap.id) setSelected(r.applicant);
      // アクション自動追加
      await api.rpoAddApplicantAction(ap.id, { type: 'status_change', content: `ステータス変更: ${ap.status} → ${newStatus}` });
      if (selected?.id === ap.id) {
        const r2 = await api.rpoApplicantActions(ap.id);
        setActions(r2.actions || []);
      }
      load(filterStatus);
    } catch (err) { alert('更新失敗: ' + err.message); }
  };

  const handleDelete = async (ap) => {
    if (!confirm(`${ap.name} を削除しますか？`)) return;
    try {
      await api.rpoDeleteApplicant(ap.id);
      setApplicants(prev => prev.filter(a => a.id !== ap.id));
      if (selected?.id === ap.id) setSelected(null);
      load(filterStatus);
    } catch (err) { alert('削除失敗: ' + err.message); }
  };

  const handleAddAction = async (e) => {
    e.preventDefault();
    if (!selected || !actionForm.type) return;
    setAddingAction(true);
    try {
      const r = await api.rpoAddApplicantAction(selected.id, {
        type:    actionForm.type,
        content: actionForm.content.trim() || null,
      });
      setActions(prev => [r.action, ...prev]);
      setActionForm({ type: 'note', content: '' });
    } catch (err) { alert('追加失敗: ' + err.message); }
    finally { setAddingAction(false); }
  };

  const handleCsvImport = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setImporting(true);
    try {
      const r = await api.rpoImportCsv(client.id, file);
      alert(`${r.imported}件インポートしました${r.errors?.length ? `\nエラー: ${r.errors.length}件` : ''}`);
      load(filterStatus);
    } catch (err) { alert('インポート失敗: ' + err.message); }
    finally { setImporting(false); e.target.value = ''; }
  };

  // ステータス別件数マップ
  const countMap = counts.reduce((m, c) => { m[c.status] = c.count; return m; }, {});

  // 媒体フィルター適用後の一覧
  const displayApplicants = filterSource
    ? applicants.filter(ap => ap.source === filterSource)
    : applicants;

  return (
    <div className="applicant-tab">
      {/* ステータスフィルターバー */}
      <div className="applicant-status-bar">
        <button
          className={`ap-status-chip ${filterStatus === '' ? 'active' : ''}`}
          onClick={() => handleFilterStatus('')}
        >
          全員 <span className="ap-count">{Object.values(countMap).reduce((s,v) => s+v, 0)}</span>
        </button>
        {STATUSES.map(s => (
          <button
            key={s}
            className={`ap-status-chip ${filterStatus === s ? 'active' : ''}`}
            style={filterStatus === s ? { background: STATUS_COLORS[s], color: '#fff', borderColor: STATUS_COLORS[s] } : {}}
            onClick={() => handleFilterStatus(s)}
          >
            {s} {countMap[s] ? <span className="ap-count">{countMap[s]}</span> : null}
          </button>
        ))}
      </div>

      {/* 媒体フィルター */}
      {registeredMedia.length > 0 && (
        <div className="applicant-source-bar">
          <button
            className={`ap-source-filter-chip ${filterSource === '' ? 'active' : ''}`}
            onClick={() => setFilterSource('')}
          >すべての媒体</button>
          {registeredMedia.map(name => (
            <button
              key={name}
              className={`ap-source-filter-chip ${filterSource === name ? 'active' : ''}`}
              onClick={() => setFilterSource(filterSource === name ? '' : name)}
            >
              {name}
              <span className="ap-count">
                {applicants.filter(ap => ap.source === name).length}
              </span>
            </button>
          ))}
        </div>
      )}

      {/* アクションバー */}
      <div className="applicant-actions-bar">
        <button className="btn-primary small" onClick={() => setShowCreate(true)}>＋ 応募者追加</button>
        <button
          className="btn-secondary small"
          onClick={() => csvRef.current?.click()}
          disabled={importing}
        >
          {importing ? 'インポート中...' : 'CSV一括登録'}
        </button>
        <input ref={csvRef} type="file" accept=".csv" hidden onChange={handleCsvImport} />
      </div>

      {/* 新規追加フォーム */}
      {showCreate && (
        <div className="modal-overlay" onClick={() => setShowCreate(false)}>
          <div className="modal-box" onClick={e => e.stopPropagation()}>
            <h2 className="modal-title">応募者を追加</h2>
            <form onSubmit={handleCreate} className="modal-form">
              <div className="form-group">
                <label>氏名 *</label>
                <input type="text" value={form.name} onChange={e => setForm(f => ({...f, name: e.target.value}))} required autoFocus />
              </div>
              <div className="form-group">
                <label>ステータス</label>
                <select value={form.status} onChange={e => setForm(f => ({...f, status: e.target.value}))}>
                  {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
                </select>
              </div>
              <div className="form-group">
                <label>応募媒体</label>
                <div className="ap-source-picker">
                  {masters.map(m => (
                    <button
                      key={m.id}
                      type="button"
                      className={`ap-source-chip ${form.source === m.name ? 'selected' : ''}`}
                      onClick={() => setForm(f => ({ ...f, source: f.source === m.name ? '' : m.name }))}
                    >
                      {m.name}
                    </button>
                  ))}
                </div>
                <input
                  type="text"
                  value={masters.some(m => m.name === form.source) ? '' : form.source}
                  onChange={e => setForm(f => ({ ...f, source: e.target.value }))}
                  placeholder="マスタにない場合は直接入力"
                  style={{ marginTop: '6px' }}
                />
                {form.source && <div className="ap-source-selected">選択中: <strong>{form.source}</strong></div>}
              </div>
              <div className="form-group">
                <label>応募日</label>
                <input type="date" value={form.appliedAt} onChange={e => setForm(f => ({...f, appliedAt: e.target.value}))} />
              </div>
              <div className="form-group">
                <label>備考</label>
                <textarea value={form.notes} onChange={e => setForm(f => ({...f, notes: e.target.value}))} rows={2} />
              </div>
              <div className="modal-actions">
                <button type="button" className="btn-secondary" onClick={() => setShowCreate(false)}>キャンセル</button>
                <button type="submit" className="btn-primary" disabled={saving}>{saving ? '追加中...' : '追加'}</button>
              </div>
            </form>
          </div>
        </div>
      )}

      <div className="applicant-layout">
        {/* 応募者リスト */}
        <div className="applicant-list">
          {loading ? (
            <div className="ap-loading">読み込み中...</div>
          ) : displayApplicants.length === 0 ? (
            <div className="ap-empty">応募者がいません</div>
          ) : (
            displayApplicants.map(ap => (
              <div
                key={ap.id}
                className={`ap-card ${selected?.id === ap.id ? 'selected' : ''}`}
                onClick={() => selectApplicant(ap)}
              >
                <div className="ap-card-header">
                  <span className="ap-name">{ap.name}</span>
                  <span
                    className="ap-status-badge-lg"
                    style={{ background: STATUS_COLORS[ap.status] || '#9ca3af' }}
                  >
                    {ap.status}
                  </span>
                </div>
                <div className="ap-card-meta">
                  {ap.source && <span className="ap-source">{ap.source}</span>}
                  {ap.applied_at && <span className="ap-date">{formatDate(ap.applied_at)}</span>}
                  {ap.assigned_cs_name && <span className="ap-cs">CS: {ap.assigned_cs_name}</span>}
                </div>
                {ap.latest_action_content && (
                  <div className="ap-card-latest-memo">
                    <span className="ap-card-memo-icon">💬</span>
                    <span className="ap-card-memo-text">{ap.latest_action_content}</span>
                  </div>
                )}
              </div>
            ))
          )}
        </div>

        {/* 詳細パネル */}
        {selected && (
          <div className="applicant-detail-panel">
            <div className="ap-detail-header">
              <div>
                <div className="ap-detail-name">{selected.name}</div>
                {selected.source && <div className="ap-detail-source">{selected.source}</div>}
              </div>
              <button className="ap-close-btn" onClick={() => setSelected(null)}>✕</button>
            </div>

            {/* ステータス変更 */}
            <div className="ap-detail-section">
              <label className="ap-section-label">ステータス</label>
              <div className="ap-status-selector">
                {STATUSES.map(s => (
                  <button
                    key={s}
                    className={`ap-status-btn ${selected.status === s ? 'active' : ''}`}
                    style={selected.status === s ? { background: STATUS_COLORS[s], color: '#fff', borderColor: STATUS_COLORS[s] } : {}}
                    onClick={() => handleStatusChange(selected, s)}
                  >
                    {s}
                  </button>
                ))}
              </div>
            </div>

            {/* 担当CS */}
            <div className="ap-detail-section">
              <label className="ap-section-label">担当CS</label>
              <select
                value={selected.assigned_cs_user_id || ''}
                onChange={async (e) => {
                  const uid = e.target.value || null;
                  const r = await api.rpoUpdateApplicant(selected.id, { assignedCsUserId: uid });
                  setApplicants(prev => prev.map(a => a.id === selected.id ? r.applicant : a));
                  setSelected(r.applicant);
                }}
              >
                <option value="">未設定</option>
                {users.map(u => <option key={u.id} value={u.id}>{u.display_name || u.name}</option>)}
              </select>
            </div>

            {/* 備考 */}
            {selected.notes && (
              <div className="ap-detail-section">
                <label className="ap-section-label">備考</label>
                <div className="ap-notes">{selected.notes}</div>
              </div>
            )}

            {/* アクション履歴 */}
            <div className="ap-detail-section">
              <label className="ap-section-label">アクション履歴</label>
              <form onSubmit={handleAddAction} className="ap-action-form">
                <select
                  value={actionForm.type}
                  onChange={e => setActionForm(f => ({...f, type: e.target.value}))}
                >
                  {ACTION_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
                </select>
                <input
                  type="text"
                  placeholder="コメント（任意）"
                  value={actionForm.content}
                  onChange={e => setActionForm(f => ({...f, content: e.target.value}))}
                />
                <button type="submit" className="btn-primary small" disabled={addingAction}>追加</button>
              </form>

              {actionsLoading ? (
                <div className="ap-loading">読み込み中...</div>
              ) : actions.length === 0 ? (
                <div className="ap-empty small">履歴なし</div>
              ) : (
                <div className="ap-actions-list">
                  {actions.map(a => (
                    <div key={a.id} className="ap-action-item">
                      <span className="ap-action-type">{ACTION_TYPES.find(t => t.value === a.type)?.label || a.type}</span>
                      {a.content && <span className="ap-action-content">{a.content}</span>}
                      <span className="ap-action-date">{formatDateTime(a.created_at)}</span>
                      {a.created_by_name && <span className="ap-action-by">{a.created_by_name}</span>}
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* 削除 */}
            <div className="ap-detail-footer">
              <button className="btn-danger small" onClick={() => handleDelete(selected)}>この応募者を削除</button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function formatDate(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleDateString('ja-JP', { month: 'short', day: 'numeric' });
}

function formatDateTime(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleString('ja-JP', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}
