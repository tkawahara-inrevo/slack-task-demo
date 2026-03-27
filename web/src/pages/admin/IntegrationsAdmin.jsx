import { useEffect, useState } from 'react';
import { api } from '../../api/client';

const SERVICE_TYPES = [
  { value: 'kintone', label: 'kintone' },
];

const DIRECTION_OPTIONS = [
  { value: 'both', label: '双方向' },
  { value: 'to_remote', label: 'ローカル → kintone' },
  { value: 'from_remote', label: 'kintone → ローカル' },
];

const MAPPING_DIRECTIONS = [
  { value: 'both', label: '双方向' },
  { value: 'to_remote', label: '→ リモートのみ' },
  { value: 'from_remote', label: '← ローカルのみ' },
];

function formatDate(d) {
  if (!d) return '-';
  return new Date(d).toLocaleString('ja-JP', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

export default function IntegrationsAdmin() {
  const [integrations, setIntegrations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [createForm, setCreateForm] = useState({ service_type: 'kintone', name: '', subdomain: '', appId: '', apiToken: '' });
  const [selectedId, setSelectedId] = useState(null);
  const [detail, setDetail] = useState(null);
  const [editConfig, setEditConfig] = useState(null);
  const [localFields, setLocalFields] = useState([]);
  const [remoteFields, setRemoteFields] = useState([]);
  const [mappings, setMappings] = useState([]);
  const [testResult, setTestResult] = useState(null);
  const [syncDirection, setSyncDirection] = useState('both');
  const [syncMessage, setSyncMessage] = useState('');
  const [saving, setSaving] = useState(false);

  const loadList = async () => {
    try {
      const r = await api.integrations();
      setIntegrations(r.integrations || []);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  useEffect(() => { loadList(); }, []);

  const handleCreate = async () => {
    if (!createForm.name.trim()) return;
    try {
      await api.integrationCreate({
        service_type: createForm.service_type,
        name: createForm.name,
        config: {
          subdomain: createForm.subdomain,
          appId: createForm.appId,
          apiToken: createForm.apiToken,
        },
      });
      setShowCreate(false);
      setCreateForm({ service_type: 'kintone', name: '', subdomain: '', appId: '', apiToken: '' });
      loadList();
    } catch (e) { console.error(e); }
  };

  const loadDetail = async (id) => {
    setSelectedId(id);
    setTestResult(null);
    setSyncMessage('');
    setRemoteFields([]);
    try {
      const [r, lf] = await Promise.all([
        api.integrationDetail(id),
        api.localFields(),
      ]);
      setDetail(r.integration);
      setEditConfig({ ...r.integration.config });
      setMappings(r.mappings || []);
      setLocalFields(lf.fields || []);
    } catch (e) { console.error(e); }
  };

  const handleSaveConfig = async () => {
    if (!detail) return;
    setSaving(true);
    try {
      await api.integrationUpdate(detail.id, { config: editConfig });
      setTestResult(null);
      loadDetail(detail.id);
    } catch (e) { console.error(e); }
    setSaving(false);
  };

  const handleToggleEnabled = async () => {
    if (!detail) return;
    try {
      await api.integrationUpdate(detail.id, { enabled: !detail.enabled });
      loadDetail(detail.id);
      loadList();
    } catch (e) { console.error(e); }
  };

  const handleTestConnection = async () => {
    if (!detail) return;
    setTestResult({ testing: true });
    try {
      const r = await api.integrationTest(detail.id);
      setTestResult(r);
    } catch (e) {
      setTestResult({ ok: false, error: e.message });
    }
  };

  const handleFetchRemoteFields = async () => {
    if (!detail) return;
    try {
      const r = await api.integrationRemoteFields(detail.id);
      setRemoteFields(r.fields || []);
    } catch (e) { console.error(e); }
  };

  const handleAddMapping = () => {
    setMappings([...mappings, { local_field: '', remote_field: '', direction: 'both' }]);
  };

  const handleRemoveMapping = (idx) => {
    setMappings(mappings.filter((_, i) => i !== idx));
  };

  const handleMappingChange = (idx, field, value) => {
    setMappings(mappings.map((m, i) => i === idx ? { ...m, [field]: value } : m));
  };

  const handleSaveMappings = async () => {
    if (!detail) return;
    setSaving(true);
    try {
      const valid = mappings.filter((m) => m.local_field && m.remote_field);
      const r = await api.integrationSaveMappings(detail.id, valid);
      setMappings(r.mappings || valid);
    } catch (e) { console.error(e); }
    setSaving(false);
  };

  const handleSync = async () => {
    if (!detail) return;
    setSyncMessage('同期中...');
    try {
      const r = await api.integrationSync(detail.id, syncDirection);
      setSyncMessage(r.message || '同期を開始しました');
      // 少し待ってからログを更新
      setTimeout(() => loadDetail(detail.id), 3000);
    } catch (e) {
      setSyncMessage(`エラー: ${e.message}`);
    }
  };

  const handleDelete = async (id) => {
    if (!confirm('この連携設定を削除しますか？')) return;
    try {
      await api.integrationDelete(id);
      if (selectedId === id) {
        setSelectedId(null);
        setDetail(null);
      }
      loadList();
    } catch (e) { console.error(e); }
  };

  if (loading) return <div className="loading">読み込み中...</div>;

  return (
    <div className="integrations-admin">
      <div className="admin-section-header">
        <h2>外部連携</h2>
        <button className="btn-sm btn-primary" onClick={() => setShowCreate(!showCreate)}>
          {showCreate ? '閉じる' : '＋ 連携を追加'}
        </button>
      </div>

      {showCreate && (
        <div className="integration-create-form">
          <h3>新しい連携を追加</h3>
          <div className="int-form-grid">
            <div className="form-group">
              <label>サービス</label>
              <select value={createForm.service_type} onChange={(e) => setCreateForm({ ...createForm, service_type: e.target.value })}>
                {SERVICE_TYPES.map((s) => <option key={s.value} value={s.value}>{s.label}</option>)}
              </select>
            </div>
            <div className="form-group">
              <label>連携名</label>
              <input placeholder="例: 営業部タスク管理" value={createForm.name} onChange={(e) => setCreateForm({ ...createForm, name: e.target.value })} />
            </div>
            <div className="form-group">
              <label>サブドメイン</label>
              <input placeholder="example（.cybozu.com は不要）" value={createForm.subdomain} onChange={(e) => setCreateForm({ ...createForm, subdomain: e.target.value })} />
            </div>
            <div className="form-group">
              <label>アプリID</label>
              <input placeholder="123" value={createForm.appId} onChange={(e) => setCreateForm({ ...createForm, appId: e.target.value })} />
            </div>
            <div className="form-group full-width">
              <label>APIトークン</label>
              <input type="password" placeholder="kintoneで発行したAPIトークン" value={createForm.apiToken} onChange={(e) => setCreateForm({ ...createForm, apiToken: e.target.value })} />
            </div>
          </div>
          <button className="btn-sm btn-primary" onClick={handleCreate}>作成</button>
        </div>
      )}

      {/* 連携一覧 */}
      <div className="integration-list">
        {integrations.length === 0 && !showCreate && (
          <p className="empty-text">連携設定がありません</p>
        )}
        {integrations.map((int) => (
          <div
            key={int.id}
            className={`integration-card ${selectedId === int.id ? 'selected' : ''}`}
            onClick={() => loadDetail(int.id)}
          >
            <div className="int-card-header">
              <span className="int-service-badge">{int.service_type}</span>
              <span className="int-name">{int.name}</span>
              <span className={`int-status-dot ${int.enabled ? 'active' : 'inactive'}`} />
            </div>
            <div className="int-card-meta">
              {int.last_synced_at && <span>最終同期: {formatDate(int.last_synced_at)}</span>}
              {int.last_sync_status && (
                <span className={`sync-status ${int.last_sync_status}`}>
                  {int.last_sync_status === 'success' ? '成功' : int.last_sync_status === 'error' ? 'エラー' : int.last_sync_status}
                </span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* 詳細パネル */}
      {detail && (
        <div className="integration-detail">
          <div className="int-detail-header">
            <h3>{detail.name}</h3>
            <div className="int-detail-actions">
              <button className={`btn-sm ${detail.enabled ? 'btn-outline' : 'btn-primary'}`} onClick={handleToggleEnabled}>
                {detail.enabled ? '無効化' : '有効化'}
              </button>
              <button className="btn-sm btn-danger" onClick={() => handleDelete(detail.id)}>削除</button>
            </div>
          </div>

          {/* 接続設定 */}
          <div className="int-section">
            <h4>接続設定</h4>
            {editConfig && (
              <div className="int-form-grid">
                <div className="form-group">
                  <label>サブドメイン</label>
                  <input value={editConfig.subdomain || ''} onChange={(e) => setEditConfig({ ...editConfig, subdomain: e.target.value })} />
                </div>
                <div className="form-group">
                  <label>アプリID</label>
                  <input value={editConfig.appId || ''} onChange={(e) => setEditConfig({ ...editConfig, appId: e.target.value })} />
                </div>
                <div className="form-group full-width">
                  <label>APIトークン</label>
                  <input type="password" value={editConfig.apiToken || ''} onChange={(e) => setEditConfig({ ...editConfig, apiToken: e.target.value })} />
                </div>
              </div>
            )}
            <div className="int-btn-row">
              <button className="btn-sm btn-primary" onClick={handleSaveConfig} disabled={saving}>
                {saving ? '保存中...' : '接続情報を保存'}
              </button>
              <button className="btn-sm btn-outline" onClick={handleTestConnection}>
                テスト接続
              </button>
            </div>
            {testResult && (
              <div className={`test-result ${testResult.ok ? 'success' : testResult.testing ? 'testing' : 'error'}`}>
                {testResult.testing ? 'テスト中...'
                  : testResult.ok ? `接続成功 — アプリ名: ${testResult.detail?.appName}`
                  : `接続失敗: ${testResult.error}`}
              </div>
            )}
          </div>

          {/* フィールドマッピング */}
          <div className="int-section">
            <h4>フィールドマッピング</h4>
            <p className="int-section-desc">ローカルのタスクフィールドとkintoneのフィールドコードを紐付けます。</p>
            <div className="int-btn-row">
              <button className="btn-sm btn-outline" onClick={handleFetchRemoteFields}>
                kintoneフィールドを取得
              </button>
              <button className="btn-sm btn-outline" onClick={handleAddMapping}>
                ＋ マッピング追加
              </button>
            </div>

            {remoteFields.length > 0 && (
              <div className="remote-fields-preview">
                <span className="rf-label">取得済みフィールド:</span>
                {remoteFields.map((f) => (
                  <span key={f.code} className="rf-chip" title={`type: ${f.type}`}>{f.label} ({f.code})</span>
                ))}
              </div>
            )}

            <div className="mapping-list">
              <div className="mapping-header-row">
                <span>ローカル</span>
                <span>方向</span>
                <span>kintoneフィールドコード</span>
                <span />
              </div>
              {mappings.map((m, idx) => (
                <div key={idx} className="mapping-row">
                  <select value={m.local_field} onChange={(e) => handleMappingChange(idx, 'local_field', e.target.value)}>
                    <option value="">-- 選択 --</option>
                    {localFields.map((f) => <option key={f.field} value={f.field}>{f.label}</option>)}
                  </select>
                  <select value={m.direction} onChange={(e) => handleMappingChange(idx, 'direction', e.target.value)}>
                    {MAPPING_DIRECTIONS.map((d) => <option key={d.value} value={d.value}>{d.label}</option>)}
                  </select>
                  {remoteFields.length > 0 ? (
                    <select value={m.remote_field} onChange={(e) => handleMappingChange(idx, 'remote_field', e.target.value)}>
                      <option value="">-- 選択 --</option>
                      {remoteFields.map((f) => <option key={f.code} value={f.code}>{f.label} ({f.code})</option>)}
                    </select>
                  ) : (
                    <input
                      placeholder="フィールドコード"
                      value={m.remote_field}
                      onChange={(e) => handleMappingChange(idx, 'remote_field', e.target.value)}
                    />
                  )}
                  <button className="btn-sm btn-danger" onClick={() => handleRemoveMapping(idx)}>×</button>
                </div>
              ))}
            </div>

            <button className="btn-sm btn-primary" onClick={handleSaveMappings} disabled={saving}>
              {saving ? '保存中...' : 'マッピングを保存'}
            </button>
          </div>

          {/* 同期 */}
          <div className="int-section">
            <h4>同期</h4>
            <div className="int-btn-row">
              <select value={syncDirection} onChange={(e) => setSyncDirection(e.target.value)}>
                {DIRECTION_OPTIONS.map((d) => <option key={d.value} value={d.value}>{d.label}</option>)}
              </select>
              <button
                className="btn-sm btn-primary"
                onClick={handleSync}
                disabled={!detail.enabled || mappings.length === 0}
              >
                同期実行
              </button>
            </div>
            {!detail.enabled && <p className="int-warn">連携を有効化してから同期してください。</p>}
            {syncMessage && <div className="sync-message">{syncMessage}</div>}

            {/* 同期ログ */}
            {detail && (
              <div className="sync-logs">
                <h5>同期履歴</h5>
                {(!detail._logs || detail._logs?.length === 0) && (
                  <p className="empty-text">同期履歴がありません</p>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
