import { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

const COLOR_OPTIONS = [
  { name: 'Ocean',   bg: '#3b82f6', label: 'ブルー' },
  { name: 'Emerald', bg: '#10b981', label: 'グリーン' },
  { name: 'Amber',   bg: '#f59e0b', label: 'オレンジ' },
  { name: 'Rose',    bg: '#ef4444', label: 'レッド' },
  { name: 'Violet',  bg: '#8b5cf6', label: 'パープル' },
  { name: 'Pink',    bg: '#ec4899', label: 'ピンク' },
  { name: 'Teal',    bg: '#14b8a6', label: 'ティール' },
  { name: 'Slate',   bg: '#64748b', label: 'スレート' },
];

const EMPTY_FORM = { name: '', color: 'Ocean', plan: 'guarantee', kintone: null, hrAssigneeId: '', driveFolder: null };

// kintone支払方式 → プランコードに変換
function mapPlan(shiharaiHoshiki) {
  if (!shiharaiHoshiki) return 'monthly';
  if (shiharaiHoshiki.includes('月額')) return 'monthly';
  if (shiharaiHoshiki.includes('保証') || shiharaiHoshiki.includes('成功')) return 'guarantee';
  return 'monthly';
}

export default function ClientList() {
  const navigate = useNavigate();
  const [clients, setClients]     = useState([]);
  const [loading, setLoading]     = useState(true);
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm]           = useState(EMPTY_FORM);
  const [saving, setSaving]       = useState(false);
  const [error, setError]         = useState(null);

  // アクセス権限とチーム一覧
  const [access, setAccess]           = useState(null);
  const [accessLoaded, setAccessLoaded] = useState(false);
  const [teams, setTeams]             = useState([]);
  const [filterTeamId, setFilterTeamId] = useState('');

  // チームメンバー（モーダル用・フィルター用共有）
  const [teamUsers,      setTeamUsers]      = useState([]);
  const [usersLoading,   setUsersLoading]   = useState(false);
  const [filterHrId,     setFilterHrId]     = useState('');
  const [filterName,     setFilterName]     = useState('');

  // kintoneサジェスト
  const [suggestions, setSuggestions] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const suggestRef = useRef(null);
  const debounceRef = useRef(null);

  // Driveフォルダ候補
  const [driveCandidates, setDriveCandidates] = useState([]);
  const [driveLoading, setDriveLoading] = useState(false);
  const driveDebounceRef = useRef(null);

  // 初期ロード: アクセス権限・チーム
  useEffect(() => {
    Promise.all([
      api.rpoAccess().catch(() => ({ canAccess: false })),
      api.rpoTeams().catch(() => ({ teams: [] })),
    ]).then(([acc, teamsRes]) => {
      setAccess(acc);
      setTeams(teamsRes.teams || []);
      setAccessLoaded(true);
    });
  }, []);

  // 案件取得: アクセスロード後、フルアクセスはチーム選択後のみ
  useEffect(() => {
    if (!accessLoaded) return;
    // フルアクセスでチーム未選択 → チーム選択画面を表示するだけ
    if (access?.fullAccess && !filterTeamId) {
      setLoading(false);
      return;
    }
    setLoading(true);
    const params = (filterTeamId && filterTeamId !== '__all__') ? { teamId: filterTeamId } : {};
    api.rpoClients(params)
      .then(r => setClients(r.clients || []))
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [filterTeamId, accessLoaded]);

  // チームが確定したらメンバー取得（フィルター用・モーダル共用）
  useEffect(() => {
    if (!accessLoaded) return;
    const teamId = (filterTeamId && filterTeamId !== '__all__') ? filterTeamId : access?.myTeams?.[0]?.id;
    if (!teamId) { setTeamUsers([]); return; }
    api.workloadUsers(teamId)
      .then(r => setTeamUsers((r.members || r.users || []).map(m => ({
        userId: m.user_id || m.userId,
        displayName: m.display_name || m.displayName,
      }))))
      .catch(() => setTeamUsers([]));
  }, [accessLoaded, filterTeamId]);

  // モーダル外クリックでサジェストを閉じる
  useEffect(() => {
    const close = (e) => {
      if (suggestRef.current && !suggestRef.current.contains(e.target)) {
        setSuggestions([]);
      }
    };
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, []);

  const fetchDriveCandidates = (name) => {
    clearTimeout(driveDebounceRef.current);
    if (!name || name.trim().length < 1) { setDriveCandidates([]); return; }
    driveDebounceRef.current = setTimeout(async () => {
      setDriveLoading(true);
      try {
        const r = await api.driveCandidates(name.trim());
        setDriveCandidates(r.candidates || []);
      } catch {
        setDriveCandidates([]);
      } finally {
        setDriveLoading(false);
      }
    }, 500);
  };

  const handleNameInput = (value) => {
    setForm(f => ({ ...f, name: value, kintone: null, driveFolder: null }));
    clearTimeout(debounceRef.current);
    fetchDriveCandidates(value);
    if (value.trim().length < 1) { setSuggestions([]); return; }
    debounceRef.current = setTimeout(async () => {
      setSearchLoading(true);
      try {
        const r = await api.kintoneSearch(value.trim());
        setSuggestions(r.results || []);
      } catch {
        setSuggestions([]);
      } finally {
        setSearchLoading(false);
      }
    }, 300);
  };

  const selectSuggestion = (s) => {
    const plan = mapPlan(s.data['支払方式']);
    setForm(f => ({
      ...f,
      name:        s.company_name,
      plan,
      kintone:     { recordId: s.record_id, appId: s.app_id, data: s.data },
      driveFolder: null,
    }));
    setSuggestions([]);
    fetchDriveCandidates(s.company_name);
  };

  const handleCreate = async (e) => {
    e.preventDefault();
    if (!form.name.trim()) return;
    setSaving(true);
    try {
      // kintoneから取得した情報をdataに含めて保存
      const hrUser = teamUsers.find(u => u.userId === form.hrAssigneeId);
      const initialData = {
        ...(form.kintone ? {
          kintone: form.kintone,
          projectInfo: {
            inrevoContact: form.kintone.data['担当営業_0'] || '',
            startDate:     form.kintone.data['受注日'] || '',
          },
        } : {}),
        ...(hrUser ? { hrAssigneeId: hrUser.userId, hrAssigneeName: hrUser.displayName } : {}),
        ...(form.driveFolder ? { driveFolder: form.driveFolder } : {}),
      };

      // 担当チームを自動決定: フルアクセスは選択中チーム、それ以外は自チーム
      const autoTeamId = effectiveTeamId || access?.myTeams?.[0]?.id || null;

      const r = await api.rpoCreateClient({
        name:       form.name.trim(),
        color:      form.color,
        plan:       form.plan,
        dashTeamId: autoTeamId,
        data:       initialData,
      });
      // テンプレートタスクを自動登録（エラーは無視）
      if (r.client?.id) {
        api.rpoApplyTemplates(r.client.id).catch(() => {});
      }
      setClients(prev => [r.client, ...prev]);
      setShowCreate(false);
      setForm(EMPTY_FORM);
    } catch (e) {
      alert('作成に失敗しました: ' + e.message);
    } finally {
      setSaving(false);
    }
  };

  const closeModal = () => {
    setShowCreate(false);
    setForm(EMPTY_FORM);
    setSuggestions([]);
    setDriveCandidates([]);
  };

  const colorOf = (name) => COLOR_OPTIONS.find(c => c.name === name)?.bg || 'var(--gray-500)';

  // フルアクセス＆チーム未選択 → チーム選択画面
  if (accessLoaded && access?.fullAccess && !filterTeamId) {
    return (
      <div className="rpo-page">
        <div className="rpo-team-picker-header">
          <h1 className="rpo-title">案件管理</h1>
          <p className="rpo-subtitle">チームを選択してください</p>
        </div>
        <div className="rpo-team-picker-grid">
          <button
            className="rpo-team-picker-card all"
            onClick={() => setFilterTeamId('__all__')}
          >
            <span className="rpo-team-picker-icon">📋</span>
            <span className="rpo-team-picker-name">全チーム</span>
          </button>
          {teams.map(t => (
            <button
              key={t.id}
              className="rpo-team-picker-card"
              onClick={() => setFilterTeamId(t.id)}
            >
              <span className="rpo-team-picker-icon">{t.name.charAt(0)}</span>
              <span className="rpo-team-picker-name">{t.name}</span>
            </button>
          ))}
        </div>
      </div>
    );
  }

  if (loading) return <div className="page-loading">読み込み中...</div>;

  // 「全チーム」選択時は filterTeamId を空文字として API に渡す
  const effectiveTeamId = filterTeamId === '__all__' ? '' : filterTeamId;

  return (
    <div className="rpo-page">
      <div className="rpo-header">
        <div>
          {access?.fullAccess && (
            <button className="btn-back-inline" onClick={() => { setFilterTeamId(''); setClients([]); }}>
              ← チーム選択
            </button>
          )}
          <h1 className="rpo-title">
            {effectiveTeamId
              ? teams.find(t => t.id === effectiveTeamId)?.name
              : access?.fullAccess
                ? '全チーム'
                : access?.myTeams?.length === 1
                  ? access.myTeams[0].name
                  : access?.myTeams?.map(t => t.name).join(' / ') || '案件管理'}
          </h1>
          <p className="rpo-subtitle">クライアント企業の採用案件を管理します</p>
        </div>
        <div style={{ display: 'flex', gap: '8px' }}>
          <button className="btn-secondary" onClick={() => navigate('/rpo/mytasks')}>マイタスク</button>
<button className="btn-secondary" onClick={() => navigate('/rpo/summary', { state: { dashTeamId: effectiveTeamId || null } })}>サマリー</button>
          <button className="btn-secondary" onClick={() => navigate('/rpo/workload', { state: { dashTeamId: effectiveTeamId || null } })}>工数管理</button>
          <button className="btn-primary" onClick={() => {
            setShowCreate(true);
          }}>＋ 新規案件</button>
        </div>
      </div>

      {error && <div className="error-banner">{error}</div>}

      {/* フィルターバー */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px', flexWrap: 'wrap' }}>
        <input
          type="text"
          value={filterName}
          onChange={e => setFilterName(e.target.value)}
          placeholder="案件名で検索…"
          style={{ padding: '5px 10px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.85rem', width: '200px' }}
        />
        {teamUsers.length > 0 && (
          <>
            <label style={{ fontSize: '0.8rem', color: 'var(--gray-500)', whiteSpace: 'nowrap' }}>HR担当者</label>
            <select
              value={filterHrId}
              onChange={e => setFilterHrId(e.target.value)}
              style={{ padding: '5px 10px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.85rem' }}
            >
              <option value="">全員</option>
              {teamUsers.map(u => (
                <option key={u.userId} value={u.userId}>{u.displayName}</option>
              ))}
            </select>
          </>
        )}
        {(filterName || filterHrId) && (
          <button onClick={() => { setFilterName(''); setFilterHrId(''); }} style={{ background: 'none', border: 'none', color: 'var(--gray-400)', cursor: 'pointer', fontSize: '0.8rem' }}>✕ クリア</button>
        )}
      </div>

      {/* 新規作成モーダル */}
      {showCreate && (
        <div className="modal-overlay" onClick={closeModal}>
          <div className="modal-box" onClick={e => e.stopPropagation()}>
            <h2 className="modal-title">新規案件を作成</h2>
            <form onSubmit={handleCreate} className="modal-form">

              {/* 企業名（kintoneオートコンプリート） */}
              <div className="form-group" ref={suggestRef} style={{ position: 'relative' }}>
                <label>企業名 *</label>
                <div style={{ position: 'relative' }}>
                  <input
                    type="text"
                    value={form.name}
                    onChange={e => handleNameInput(e.target.value)}
                    placeholder="企業名を入力して検索"
                    autoFocus
                    required
                    autoComplete="off"
                  />
                  {searchLoading && (
                    <span className="kintone-search-spinner">検索中...</span>
                  )}
                </div>

                {form.kintone && (
                  <div className="kintone-selected-badge">
                    kintone連携済
                    {form.kintone.data['担当営業_0'] && (
                      <span>　担当: {form.kintone.data['担当営業_0']}</span>
                    )}
                    {form.kintone.data['受注日'] && (
                      <span>　受注日: {form.kintone.data['受注日']}</span>
                    )}
                  </div>
                )}

                {suggestions.length > 0 && (
                  <div className="kintone-suggestions">
                    {suggestions.map(s => (
                      <div
                        key={`${s.app_id}-${s.record_id}`}
                        className="kintone-suggestion-item"
                        onMouseDown={e => { e.preventDefault(); selectSuggestion(s); }}
                      >
                        <span className="suggestion-company">{s.company_name}</span>
                        <span className="suggestion-meta">
                          {s.data['支払方式'] && <span>{s.data['支払方式']}</span>}
                          {s.data['担当営業_0'] && <span>担当: {s.data['担当営業_0']}</span>}
                          {s.data['受注日'] && <span>{s.data['受注日']}</span>}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Driveフォルダ候補 */}
              {(driveLoading || driveCandidates.length > 0) && (
                <div className="form-group">
                  <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span>📁</span> Driveフォルダ
                    {driveLoading && <span style={{ fontSize: '0.75rem', color: 'var(--gray-400)' }}>検索中...</span>}
                  </label>
                  {!driveLoading && driveCandidates.length === 0 && (
                    <p style={{ fontSize: '0.75rem', color: 'var(--gray-400)', margin: '4px 0 0' }}>一致するフォルダが見つかりませんでした</p>
                  )}
                  {driveCandidates.map((c, i) => (
                    <div
                      key={i}
                      className={`drive-candidate-item${form.driveFolder === c.webViewLink ? ' selected' : ''}`}
                      onClick={() => setForm(f => ({
                        ...f,
                        driveFolder: f.driveFolder === c.webViewLink ? null : c.webViewLink,
                      }))}
                    >
                      <span className="drive-candidate-check">{form.driveFolder === c.webViewLink ? '✓' : '○'}</span>
                      <span className="drive-candidate-name">{c.name}</span>
                    </div>
                  ))}
                  {form.driveFolder && (
                    <p style={{ fontSize: '0.75rem', color: '#10b981', margin: '4px 0 0' }}>✓ フォルダを選択しました</p>
                  )}
                </div>
              )}

              {/* HR担当者 */}
              <div className="form-group">
                <label>HR担当者</label>
                <select
                  value={form.hrAssigneeId}
                  onChange={e => setForm(f => ({ ...f, hrAssigneeId: e.target.value }))}
                  disabled={usersLoading}
                  style={{ width: '100%', padding: '8px 10px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.875rem' }}
                >
                  <option value="">{usersLoading ? '読み込み中…' : '未設定'}</option>
                  {teamUsers.map(u => (
                    <option key={u.userId} value={u.userId}>{u.displayName}</option>
                  ))}
                </select>
                {!usersLoading && teamUsers.length === 0 && (
                  <p style={{ fontSize: '0.75rem', color: 'var(--gray-400)', margin: '4px 0 0' }}>
                    チームにメンバーが登録されると選択できます
                  </p>
                )}
              </div>

              {/* カラー */}
              <div className="form-group">
                <label>カラー</label>
                <div className="color-picker">
                  {COLOR_OPTIONS.map(c => (
                    <button
                      key={c.name}
                      type="button"
                      className={`color-dot ${form.color === c.name ? 'selected' : ''}`}
                      style={{ background: c.bg }}
                      title={c.label}
                      onClick={() => setForm(f => ({ ...f, color: c.name }))}
                    />
                  ))}
                </div>
              </div>

              <div className="modal-actions">
                <button type="button" className="btn-secondary" onClick={closeModal}>
                  キャンセル
                </button>
                <button type="submit" className="btn-primary" disabled={saving}>
                  {saving ? '作成中...' : '作成'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* 案件一覧 */}
      {(() => {
        const nameFilter = c => !filterName || c.name.toLowerCase().includes(filterName.toLowerCase());
        const hrFilter   = c => !filterHrId || c.data?.hrAssigneeId === filterHrId;
        const active     = clients.filter(c => c.status !== 'archived' && hrFilter(c) && nameFilter(c));
        const archived   = clients.filter(c => c.status === 'archived' && hrFilter(c) && nameFilter(c));

        const archiveClient = async (e, clientId) => {
          e.stopPropagation();
          if (!window.confirm('この案件を終了済みにしますか？')) return;
          await api.rpoUpdateClient(clientId, { status: 'archived' }).catch(() => {});
          setClients(prev => prev.map(c => c.id === clientId ? { ...c, status: 'archived' } : c));
        };
        const restoreClient = async (e, clientId) => {
          e.stopPropagation();
          await api.rpoUpdateClient(clientId, { status: 'active' }).catch(() => {});
          setClients(prev => prev.map(c => c.id === clientId ? { ...c, status: 'active' } : c));
        };

        const ClientCard = ({ client, isArchived }) => (
          <div
            key={client.id}
            className={`rpo-card${isArchived ? ' archived' : ''}`}
            onClick={() => navigate(`/rpo/${client.id}`)}
            style={{ borderTop: `4px solid ${colorOf(client.color)}`, opacity: isArchived ? 0.65 : 1 }}
          >
            <div className="rpo-card-header">
              <div className="rpo-card-avatar" style={{ background: colorOf(client.color) }}>
                {client.name.charAt(0)}
              </div>
              <div style={{ flex: 1 }}>
                <div className="rpo-card-name">{client.name}</div>
                <div className="rpo-card-plan">{planLabel(client.plan)}</div>
              </div>
              {isArchived ? (
                <button
                  className="btn-secondary small"
                  style={{ fontSize: '0.72rem', padding: '2px 8px', flexShrink: 0 }}
                  onClick={e => restoreClient(e, client.id)}
                  title="進行中に戻す"
                >復帰</button>
              ) : (
                <button
                  className="rpo-card-end-btn"
                  onClick={e => archiveClient(e, client.id)}
                  title="案件を終了する"
                >終了</button>
              )}
            </div>
            <div className="rpo-card-meta">
              {isArchived ? (
                <span className="rpo-status-pill archived">終了済み</span>
              ) : (
                <span className="rpo-status-pill active">進行中</span>
              )}
              {client.data?.hrAssigneeName && (
                <span className="rpo-card-assignee">👤 {client.data.hrAssigneeName}</span>
              )}
              {client.dash_team_id && !effectiveTeamId && (
                <span className="rpo-card-team">
                  {teams.find(t => t.id === client.dash_team_id)?.name || ''}
                </span>
              )}
              <span className="rpo-card-date">{formatDate(client.updated_at)}</span>
            </div>
          </div>
        );

        return (
          <>
            {active.length === 0 && archived.length === 0 ? (
              <div className="rpo-empty">
                <p>案件がまだありません</p>
                <button className="btn-primary" onClick={() => setShowCreate(true)}>最初の案件を作成</button>
              </div>
            ) : (
              <div className="rpo-grid">
                {active.map(client => <ClientCard key={client.id} client={client} isArchived={false} />)}
              </div>
            )}
            {archived.length > 0 && (
              <ArchivedSection>
                <div className="rpo-grid">
                  {archived.map(client => <ClientCard key={client.id} client={client} isArchived={true} />)}
                </div>
              </ArchivedSection>
            )}
          </>
        );
      })()}
    </div>
  );
}

function planLabel(plan) {
  return { monthly: '月額', guarantee: '採用保証' }[plan] || plan;
}

function formatDate(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleDateString('ja-JP', { month: 'short', day: 'numeric' });
}

function ArchivedSection({ children }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ marginTop: '32px' }}>
      <button
        onClick={() => setOpen(v => !v)}
        style={{ display: 'flex', alignItems: 'center', gap: '6px', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--gray-500)', fontSize: '0.875rem', fontWeight: 600, padding: '4px 0', marginBottom: open ? '12px' : 0 }}
      >
        <span style={{ fontSize: '0.75rem' }}>{open ? '▼' : '▶'}</span>
        終了済み
      </button>
      {open && children}
    </div>
  );
}
