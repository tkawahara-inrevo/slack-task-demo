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

const EMPTY_FORM = { name: '', color: 'Ocean', plan: 'guarantee', crmDeal: null, hrAssigneeId: '', driveFolder: null };

const PHASES = [
  { key: 'dr1',   label: 'DR①',  desc: '初期対応',        color: '#8b5cf6' },
  { key: 'cr1',   label: 'CR①',  desc: '初回インタビュー', color: '#f59e0b' },
  { key: 'dr2',   label: 'DR②',  desc: '二次分析・KO',     color: '#ef4444' },
  { key: 'cs_op', label: 'CS/OP', desc: '採用活動中',       color: '#3b82f6' },
];

function mapCrmPlan(paymentMethod) {
  if (!paymentMethod) return 'guarantee';
  if (paymentMethod.includes('月額')) return 'monthly';
  return 'guarantee';
}

function calcContractAmount(deal) {
  if (!deal) return 0;
  if (deal.unit_price && deal.guarantee_count) return Number(deal.unit_price) * Number(deal.guarantee_count);
  if (deal.monthly_cost && deal.contract_months) return Number(deal.monthly_cost) * Number(deal.contract_months) + (Number(deal.initial_cost) || 0);
  return 0;
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
    setForm(f => ({ ...f, name: value, crmDeal: null, driveFolder: null }));
    clearTimeout(debounceRef.current);
    fetchDriveCandidates(value);
    if (value.trim().length < 1) { setSuggestions([]); return; }
    debounceRef.current = setTimeout(async () => {
      setSearchLoading(true);
      try {
        const r = await api.rpoCrmWonDeals(value.trim());
        setSuggestions(r.deals || []);
      } catch {
        setSuggestions([]);
      } finally {
        setSearchLoading(false);
      }
    }, 300);
  };

  const selectSuggestion = (s) => {
    setForm(f => ({
      ...f,
      name:     s.company_name,
      plan:     mapCrmPlan(s.payment_method),
      crmDeal:  s,
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
      const hrUser = teamUsers.find(u => u.userId === form.hrAssigneeId);
      const initialData = {
        ...(form.crmDeal ? {
          crmDealId: form.crmDeal.deal_id,
          projectInfo: {
            inrevoContact:  form.crmDeal.sales_person || '',
            startDate:      form.crmDeal.order_date ? String(form.crmDeal.order_date).slice(0, 10) : '',
            contractAmount: calcContractAmount(form.crmDeal),
            hiringTarget:   form.crmDeal.hiring_target || 0,
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

              {/* 企業名（CRM受注案件検索） */}
              <div className="form-group" ref={suggestRef} style={{ position: 'relative' }}>
                <label>企業名 * <span style={{ fontSize: '0.72rem', color: 'var(--gray-400)', fontWeight: 400 }}>CRMの受注案件から検索</span></label>
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

                {form.crmDeal && (
                  <div className="kintone-selected-badge">
                    CRM連携済
                    {form.crmDeal.sales_person && <span>　担当: {form.crmDeal.sales_person}</span>}
                    {form.crmDeal.order_date && <span>　受注日: {String(form.crmDeal.order_date).slice(0, 10)}</span>}
                    {form.crmDeal.already_linked && <span style={{ color: '#f59e0b' }}>　※既に案件あり</span>}
                  </div>
                )}

                {suggestions.length > 0 && (
                  <div className="kintone-suggestions">
                    {suggestions.map(s => (
                      <div
                        key={s.deal_id}
                        className="kintone-suggestion-item"
                        onMouseDown={e => { e.preventDefault(); selectSuggestion(s); }}
                      >
                        <span className="suggestion-company">
                          {s.company_name}
                          {s.already_linked && <span style={{ fontSize: '0.7rem', color: '#f59e0b', marginLeft: 6 }}>案件あり</span>}
                        </span>
                        <span className="suggestion-meta">
                          {s.payment_method && <span>{s.payment_method}</span>}
                          {s.sales_person && <span>担当: {s.sales_person}</span>}
                          {s.order_date && <span>{String(s.order_date).slice(0, 10)}</span>}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
                {!searchLoading && suggestions.length === 0 && form.name.trim().length >= 1 && !form.crmDeal && (
                  <p style={{ fontSize: '0.75rem', color: 'var(--gray-400)', margin: '4px 0 0' }}>
                    CRMに受注案件が見つかりません。企業名を直接入力して作成できます。
                  </p>
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

      {/* 案件一覧（Kanbanビュー） */}
      {(() => {
        const nameFilter  = c => !filterName || c.name.toLowerCase().includes(filterName.toLowerCase());
        const hrFilter    = c => !filterHrId || c.data?.hrAssigneeId === filterHrId;
        const unassigned  = clients.filter(c => c.status !== 'archived' && !c.dash_team_id && hrFilter(c) && nameFilter(c));
        const active      = clients.filter(c => c.status !== 'archived' && c.dash_team_id && hrFilter(c) && nameFilter(c));
        const archived    = clients.filter(c => c.status === 'archived' && hrFilter(c) && nameFilter(c));

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
        const changePhase = async (e, clientId, newPhase) => {
          e.stopPropagation();
          await api.rpoUpdateClient(clientId, { phase: newPhase }).catch(() => {});
          setClients(prev => prev.map(c => c.id === clientId ? { ...c, phase: newPhase } : c));
        };

        const checklistProgress = (client) => {
          const ph = client.phase || 'cr';
          const items = { cr: 4, st_an: 4, dr: 4, cs_op: 4 }[ph] || 4;
          const done = Object.values(client.data?.checklist?.[ph] || {}).filter(Boolean).length;
          return { done, total: items };
        };

        const KanbanCard = ({ client }) => {
          const { done, total } = checklistProgress(client);
          return (
          <div
            onClick={() => navigate(`/rpo/${client.id}`)}
            style={{ background: 'var(--surface)', borderRadius: 8, padding: '10px 12px', cursor: 'pointer', borderLeft: `4px solid ${colorOf(client.color)}`, boxShadow: '0 1px 3px rgba(0,0,0,0.08)', display: 'flex', flexDirection: 'column', gap: 6 }}
          >
            <div style={{ fontWeight: 600, fontSize: '0.84rem', color: 'var(--gray-800)', lineHeight: 1.3 }}>{client.name}</div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <div style={{ flex: 1, height: 4, background: 'var(--gray-200)', borderRadius: 2, overflow: 'hidden' }}>
                <div style={{ width: `${total > 0 ? (done / total) * 100 : 0}%`, height: '100%', background: done === total ? '#10b981' : '#f59e0b', borderRadius: 2, transition: 'width 0.3s' }} />
              </div>
              <span style={{ fontSize: '0.68rem', color: done === total ? '#10b981' : 'var(--gray-500)', fontWeight: 600, flexShrink: 0 }}>{done}/{total}</span>
            </div>
            <div style={{ fontSize: '0.72rem', color: 'var(--gray-500)' }}>{planLabel(client.plan)}</div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 4, marginTop: 2 }}>
              <span style={{ fontSize: '0.72rem', color: 'var(--gray-500)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {client.data?.hrAssigneeName ? `👤 ${client.data.hrAssigneeName.split('/')[0].trim()}` : ''}
                {client.dash_team_id && !effectiveTeamId ? ` · ${teams.find(t => t.id === client.dash_team_id)?.name || ''}` : ''}
              </span>
              <div style={{ display: 'flex', gap: 4, flexShrink: 0 }} onClick={e => e.stopPropagation()}>
                <select
                  value={client.phase || 'cr'}
                  onChange={e => changePhase(e, client.id, e.target.value)}
                  style={{ fontSize: '0.68rem', padding: '1px 4px', borderRadius: 4, border: '1px solid var(--gray-300)', background: 'var(--surface-2)', color: 'var(--gray-600)', cursor: 'pointer' }}
                >
                  {PHASES.map(p => <option key={p.key} value={p.key}>{p.label}</option>)}
                </select>
                <button
                  onClick={e => archiveClient(e, client.id)}
                  title="案件を終了する"
                  style={{ fontSize: '0.68rem', padding: '1px 6px', borderRadius: 4, border: '1px solid var(--gray-300)', background: 'none', color: 'var(--gray-400)', cursor: 'pointer' }}
                >終了</button>
              </div>
            </div>
          </div>
          );
        };

        const assignTeam = async (e, clientId, dashTeamId) => {
          e.stopPropagation();
          await api.rpoUpdateClient(clientId, { dashTeamId }).catch(() => {});
          setClients(prev => prev.map(c => c.id === clientId ? { ...c, dash_team_id: dashTeamId } : c));
        };

        if (unassigned.length === 0 && active.length === 0 && archived.length === 0) {
          return (
            <div className="rpo-empty">
              <p>案件がまだありません</p>
              <button className="btn-primary" onClick={() => setShowCreate(true)}>最初の案件を作成</button>
            </div>
          );
        }

        return (
          <>
            {/* 担当チーム未割当 */}
            {unassigned.length > 0 && (
              <div style={{ marginBottom: 20, background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 10, padding: '12px 16px' }}>
                <div style={{ fontWeight: 700, fontSize: '0.82rem', color: '#92400e', marginBottom: 10 }}>
                  ⚠ 担当チーム未割当 ({unassigned.length}件) — CRMで受注になった案件です
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {unassigned.map(client => (
                    <div key={client.id} onClick={() => navigate(`/rpo/${client.id}`)}
                      style={{ display: 'flex', alignItems: 'center', gap: 10, background: '#fff', borderRadius: 7, padding: '8px 12px', cursor: 'pointer', border: '1px solid #fde68a' }}>
                      <div style={{ width: 10, height: 10, borderRadius: '50%', background: colorOf(client.color), flexShrink: 0 }} />
                      <span style={{ fontWeight: 600, fontSize: '0.84rem', flex: 1 }}>{client.name}</span>
                      <span style={{ fontSize: '0.72rem', color: '#9ca3af' }}>{planLabel(client.plan)}</span>
                      {teams.length > 0 && (
                        <select
                          onClick={e => e.stopPropagation()}
                          onChange={e => e.target.value && assignTeam(e, client.id, e.target.value)}
                          defaultValue=""
                          style={{ fontSize: '0.72rem', padding: '2px 6px', borderRadius: 5, border: '1px solid #fde68a', background: '#fffbeb', cursor: 'pointer' }}
                        >
                          <option value="" disabled>チームを割り当て</option>
                          {teams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
                        </select>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
            {/* Kanbanボード */}
            <div style={{ display: 'flex', gap: 12, overflowX: 'auto', alignItems: 'flex-start', paddingBottom: 16 }}>
              {PHASES.map(phase => {
                const cols = active.filter(c => (c.phase || 'cr') === phase.key);
                return (
                  <div key={phase.key} style={{ minWidth: 240, flex: '0 0 240px' }}>
                    <div style={{ background: phase.color, color: '#fff', borderRadius: '8px 8px 0 0', padding: '8px 12px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                      <div>
                        <span style={{ fontWeight: 700, fontSize: '0.85rem' }}>{phase.label}</span>
                        <span style={{ fontSize: '0.72rem', opacity: 0.85, marginLeft: 6 }}>{phase.desc}</span>
                      </div>
                      <span style={{ background: 'rgba(255,255,255,0.25)', borderRadius: 12, padding: '0 8px', fontSize: '0.78rem', fontWeight: 700 }}>{cols.length}</span>
                    </div>
                    <div style={{ background: 'var(--gray-100)', borderRadius: '0 0 8px 8px', padding: 8, display: 'flex', flexDirection: 'column', gap: 8, minHeight: 100 }}>
                      {cols.map(client => <KanbanCard key={client.id} client={client} />)}
                      {cols.length === 0 && <div style={{ fontSize: '0.75rem', color: 'var(--gray-400)', textAlign: 'center', padding: '12px 0' }}>なし</div>}
                    </div>
                  </div>
                );
              })}
            </div>

            {/* 終了済み（折りたたみ） */}
            {archived.length > 0 && (
              <ArchivedSection>
                <div className="rpo-grid">
                  {archived.map(client => (
                    <div
                      key={client.id}
                      className="rpo-card archived"
                      onClick={() => navigate(`/rpo/${client.id}`)}
                      style={{ borderTop: `4px solid ${colorOf(client.color)}`, opacity: 0.65 }}
                    >
                      <div className="rpo-card-header">
                        <div className="rpo-card-avatar" style={{ background: colorOf(client.color) }}>{client.name.charAt(0)}</div>
                        <div style={{ flex: 1 }}>
                          <div className="rpo-card-name">{client.name}</div>
                          <div className="rpo-card-plan">{planLabel(client.plan)}</div>
                        </div>
                        <button className="btn-secondary small" style={{ fontSize: '0.72rem', padding: '2px 8px', flexShrink: 0 }} onClick={e => restoreClient(e, client.id)}>復帰</button>
                      </div>
                      <div className="rpo-card-meta">
                        <span className="rpo-status-pill archived">終了済み</span>
                        {client.data?.hrAssigneeName && <span className="rpo-card-assignee">👤 {client.data.hrAssigneeName}</span>}
                        <span className="rpo-card-date">{formatDate(client.updated_at)}</span>
                      </div>
                    </div>
                  ))}
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
