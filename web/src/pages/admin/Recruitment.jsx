import { useEffect, useRef, useState, useMemo } from 'react';
import { api } from '../../api/client';

const TEST_STATUS_LABEL = { pending: '未送信', scheduled: '予約済み', sent: '送信済', completed: '完了', error: 'エラー' };
const TEST_STATUS_COLOR = { pending: '#9ca3af', scheduled: '#7c3aed', sent: '#2563eb', completed: '#059669', error: '#dc2626' };
const TEST_STATUS_BG    = { pending: '#f3f4f6', scheduled: '#f5f3ff', sent: '#eff6ff', completed: '#f0fdf4', error: '#fef2f2' };

function effectiveStatus(c) {
  if (c.scheduled_at && c.status === 'pending') return 'scheduled';
  return c.status;
}

function TestStatusBadge({ c }) {
  const s = effectiveStatus(c);
  const label = s === 'scheduled'
    ? `🕐 ${new Date(c.scheduled_at).toLocaleString('ja-JP', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' })}送信予定`
    : TEST_STATUS_LABEL[s] || s;
  return <span style={{ fontSize: 11, fontWeight: 700, padding: '2px 8px', borderRadius: 99, color: TEST_STATUS_COLOR[s] || '#9ca3af', background: TEST_STATUS_BG[s] || '#f3f4f6' }}>{label}</span>;
}

export default function Recruitment() {
  const [candidates, setCandidates] = useState([]);
  const [settings, setSettings]     = useState({});
  const [loading, setLoading]       = useState(true);
  const [sendingIds, setSendingIds] = useState(new Set());
  const [showSettings, setShowSettings] = useState(false);
  const [settingsForm, setSettingsForm] = useState({});
  const [savingSettings, setSavingSettings] = useState(false);
  const [form, setForm]             = useState({ name: '', email: '' });
  const [importing, setImporting]   = useState(false);
  const [filterStatuses, setFilterStatuses] = useState(new Set(['pending','scheduled','sent','completed','error']));

  // 予約送信
  const [showSchedule, setShowSchedule]       = useState(false);
  const [scheduleChecked, setScheduleChecked] = useState(new Set());
  const [scheduleAt, setScheduleAt]           = useState('');
  const [scheduling, setScheduling]           = useState(false);
  const [schedules, setSchedules]             = useState([]);

  const load = async () => {
    setLoading(true);
    try {
      const [cRes, sRes] = await Promise.all([api.recruitmentCandidates(), api.recruitmentSettings()]);
      setCandidates(cRes.candidates || []);
      setSettings(sRes.settings || {});
      setSettingsForm(sRes.settings || {});
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, []);

  const update = (id, patch) => setCandidates(prev => prev.map(c => c.id === id ? { ...c, ...patch } : c));

  const handleAdd = async () => {
    if (!form.name.trim() || !form.email.trim()) return;
    try {
      const r = await api.recruitmentCandidateAdd({ name: form.name.trim(), email: form.email.trim() });
      setCandidates(prev => [r.candidate, ...prev]);
      setForm({ name: '', email: '' });
    } catch (e) { alert(e.message); }
  };

  const handleDelete = async (id) => {
    if (!window.confirm('この候補者を削除しますか？')) return;
    await api.recruitmentCandidateDelete(id);
    setCandidates(prev => prev.filter(c => c.id !== id));
  };

  // 一次面接通過 → 性格診断ステージへ
  const handlePassFirstInterview = async (id) => {
    if (!window.confirm('一次面接通過として、性格診断を解放しますか？')) return;
    await api.recruitmentStage(id, 'personality').catch(() => {});
    update(id, { stage: 'personality' });
  };

  // 実技テスト送付
  const handleSendTest = async (id) => {
    const c = candidates.find(x => x.id === id);
    if (!window.confirm(`${c?.name} さんに実技テストを送付しますか？`)) return;
    setSendingIds(s => new Set([...s, id]));
    try {
      await api.recruitmentSendOne(id);
      const fresh = await api.recruitmentCandidates();
      setCandidates(fresh.candidates || []);
    } catch (e) { alert('送付失敗: ' + e.message); }
    finally { setSendingIds(s => { const n = new Set(s); n.delete(id); return n; }); }
  };

  // 性格診断送付
  const handleSendPersonality = async (id) => {
    if (!settings?.personality_gas_url) { alert('設定 → 適性診断GAS URLを設定してください'); return; }
    const c = candidates.find(x => x.id === id);
    if (!window.confirm(`${c?.name} さんに性格診断を送付しますか？`)) return;
    try {
      await api.personalitySend(id);
      update(id, { personality_status: 'sent' });
    } catch (e) { alert('送付失敗: ' + e.message); }
  };

  const handlePersonalityPdf = async (id) => {
    if (!settings?.personality_gas_url) { alert('設定 → 適性診断GAS URLを設定してください'); return; }
    const [loading2, setLoading2] = [false, () => {}]; // local loading はアクションセルで管理
    try {
      const r = await api.personalityPdf(id);
      if (r.pdfUrl) window.open(r.pdfUrl, '_blank');
    } catch (e) { alert('PDF生成失敗: ' + e.message); }
  };

  const handleSaveSettings = async () => {
    setSavingSettings(true);
    try {
      await api.recruitmentSettingsSave({
        templateSpreadsheetId: settingsForm.template_spreadsheet_id,
        gasEndpointUrl: settingsForm.gas_endpoint_url,
        notifyChannelId: settingsForm.notify_channel_id,
        notifyMentionUserId: settingsForm.notify_mention_user_id,
        webhookSecret: settingsForm.webhook_secret,
        fromEmail: settingsForm.from_email,
        emailSubject: settingsForm.email_subject,
        emailBody: settingsForm.email_body,
        totalScore: settingsForm.total_score ? Number(settingsForm.total_score) : null,
        importSheetUrl: settingsForm.import_sheet_url || null,
        personalityGasUrl: settingsForm.personality_gas_url || null,
        personalitySheetUrl: settingsForm.personality_sheet_url || null,
        personalityWebhookSecret: settingsForm.personality_webhook_secret || null,
      });
      setSettings(settingsForm);
      setShowSettings(false);
      alert('保存しました');
    } catch (e) { alert(e.message); }
    finally { setSavingSettings(false); }
  };

  const handleImport = async () => {
    if (!settings.import_sheet_url) { alert('取り込み用スプレッドシートURLを設定してください'); return; }
    setImporting(true);
    try {
      const r = await api.recruitmentImportFromSheet(settings.import_sheet_url);
      setCandidates(prev => [...(r.added || []), ...prev]);
      alert(`${r.added?.length || 0}名取り込み完了`);
    } catch (e) { alert('取り込み失敗: ' + e.message); }
    finally { setImporting(false); }
  };

  // 予約送信
  const loadSchedules = async () => {
    try { const r = await api.recruitmentScheduled(); setSchedules(r.schedules || []); } catch { setSchedules([]); }
  };
  const openSchedule = () => {
    setScheduleChecked(new Set());
    const d = new Date(); d.setDate(d.getDate() + 3); d.setHours(9, 0, 0, 0);
    const pad = n => String(n).padStart(2, '0');
    setScheduleAt(`${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T09:00`);
    loadSchedules();
    setShowSchedule(true);
  };
  const handleScheduleSubmit = async () => {
    if (!scheduleChecked.size || !scheduleAt) return;
    setScheduling(true);
    try {
      await api.recruitmentScheduleCreate([...scheduleChecked], new Date(scheduleAt).toISOString());
      setCandidates(prev => prev.map(c => scheduleChecked.has(c.id) ? { ...c, scheduled_at: scheduleAt } : c));
      await loadSchedules();
      setShowSchedule(false);
    } catch (e) { alert('予約失敗: ' + e.message); }
    finally { setScheduling(false); }
  };
  const handleScheduleCancel = async (id) => {
    if (!window.confirm('予約をキャンセルしますか？')) return;
    const target = schedules.find(s => s.id === id);
    await api.recruitmentScheduleCancel(id).catch(() => {});
    setSchedules(prev => prev.filter(s => s.id !== id));
    if (target) update(target.candidate_id, { scheduled_at: null });
  };

  const totalScore = settings.total_score || 10;
  const webhookUrl = `${window.location.origin}/api/dashboard/recruitment/webhook/complete`;
  const pendingCount = candidates.filter(c => !c.spreadsheet_url && ['pending','error'].includes(c.status)).length;
  const filtered = candidates.filter(c => filterStatuses.has(effectiveStatus(c)));

  return (
    <div>
      {/* ヘッダー */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: '0 0 4px', fontSize: '1rem', fontWeight: 700 }}>採用管理（実技テスト）</h2>
          <p style={{ margin: 0, fontSize: '0.82rem', color: '#6b7280' }}>候補者にテストを送付し、採点結果を自動収集します</p>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={handleImport} disabled={importing}
            style={{ fontSize: 12, padding: '5px 12px', border: '1px solid #93c5fd', borderRadius: 6, background: '#fff', color: '#1d4ed8', fontWeight: 600, cursor: importing ? 'default' : 'pointer' }}>
            {importing ? '取り込み中…' : '📋 スプシ取り込み'}
          </button>
          <button onClick={() => { setShowSettings(v => !v); setSettingsForm(settings); }}
            style={{ fontSize: 12, padding: '5px 12px', border: '1px solid #e5e7eb', borderRadius: 6, background: '#fff', cursor: 'pointer', color: '#6b7280' }}>
            ⚙️ 設定
          </button>
          <button onClick={openSchedule} disabled={pendingCount === 0}
            style={{ fontSize: 13, padding: '6px 14px', border: '1px solid #6366f1', borderRadius: 6, background: '#fff', color: '#6366f1', fontWeight: 600, cursor: pendingCount === 0 ? 'not-allowed' : 'pointer', opacity: pendingCount === 0 ? 0.5 : 1 }}>
            🕐 予約送信
          </button>
          <button onClick={async () => {
            const targets = candidates.filter(c => !c.spreadsheet_url && ['pending','error'].includes(c.status));
            if (!targets.length) { alert('送信対象がいません'); return; }
            if (!window.confirm(`${targets.length}名に実技テストを送付しますか？`)) return;
            setSendingIds(new Set(targets.map(c => c.id)));
            try { await api.recruitmentSend(); const fresh = await api.recruitmentCandidates(); setCandidates(fresh.candidates || []); }
            finally { setSendingIds(new Set()); }
          }} disabled={sendingIds.size > 0 || pendingCount === 0}
            className="btn btn-primary" style={{ fontSize: 13 }}>
            {sendingIds.size > 0 ? '送信中…' : `送信 ${pendingCount > 0 ? `(${pendingCount}名)` : ''}`}
          </button>
        </div>
      </div>

      {/* 設定パネル */}
      {showSettings && (
        <div style={{ marginBottom: 20, border: '1px solid #e5e7eb', borderRadius: 10, padding: 20, background: '#fafafa' }}>
          <h3 style={{ margin: '0 0 14px', fontSize: '0.9rem', fontWeight: 700 }}>設定</h3>
          <div style={{ marginBottom: 14 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#9ca3af', marginBottom: 8, textTransform: 'uppercase' }}>実技テスト</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {[
                ['template_spreadsheet_id', 'テンプレートスプレッドシートID'],
                ['gas_endpoint_url', 'GAS Web App URL'],
                ['notify_channel_id', 'HR通知チャンネルID'],
                ['notify_mention_user_id', 'メンション先ユーザーID'],
                ['webhook_secret', 'Webhookシークレット'],
                ['total_score', '満点'],
                ['import_sheet_url', '取り込み用スプレッドシートURL'],
                ['from_email', '送信元メールアドレス'],
              ].map(([key, label]) => (
                <div key={key}>
                  <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>{label}</label>
                  <input value={settingsForm[key] || ''} onChange={e => setSettingsForm(f => ({ ...f, [key]: e.target.value }))}
                    type={key === 'total_score' ? 'number' : 'text'}
                    style={{ width: '100%', fontSize: 12, padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 6, boxSizing: 'border-box' }} />
                </div>
              ))}
              <div>
                <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>Webhook URL（GASに設定）</label>
                <div style={{ fontSize: 11, padding: '6px 10px', background: '#f3f4f6', borderRadius: 6, wordBreak: 'break-all', fontFamily: 'monospace' }}>{webhookUrl}</div>
              </div>
            </div>
          </div>
          <div style={{ paddingTop: 14, borderTop: '1px solid #e5e7eb', marginBottom: 14 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#9ca3af', marginBottom: 8, textTransform: 'uppercase' }}>④ 性格診断</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {[
                ['personality_gas_url', '適性診断 GAS Web App URL'],
                ['personality_sheet_url', '結果スプレッドシートURL（参照用）'],
                ['personality_webhook_secret', 'Webhookシークレット'],
              ].map(([key, label]) => (
                <div key={key}>
                  <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>{label}</label>
                  <input value={settingsForm[key] || ''} onChange={e => setSettingsForm(f => ({ ...f, [key]: e.target.value }))}
                    style={{ width: '100%', fontSize: 12, padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 6, boxSizing: 'border-box' }} />
                </div>
              ))}
              <div>
                <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>Webhook URL（GASに設定）</label>
                <div style={{ fontSize: 11, padding: '6px 10px', background: '#f3f4f6', borderRadius: 6, wordBreak: 'break-all', fontFamily: 'monospace' }}>
                  {`${window.location.origin}/api/dashboard/recruitment/webhook/personality`}
                </div>
              </div>
            </div>
          </div>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <button onClick={() => setShowSettings(false)} style={{ fontSize: 12, padding: '6px 14px', border: '1px solid #e5e7eb', borderRadius: 6, background: '#fff', cursor: 'pointer' }}>キャンセル</button>
            <button onClick={handleSaveSettings} disabled={savingSettings}
              style={{ fontSize: 12, padding: '6px 14px', border: 'none', borderRadius: 6, background: '#2563eb', color: '#fff', fontWeight: 700, cursor: 'pointer' }}>
              {savingSettings ? '保存中…' : '保存'}
            </button>
          </div>
        </div>
      )}

      {/* 予約送信モーダル */}
      {showSchedule && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', zIndex: 500, display: 'flex', alignItems: 'flex-start', justifyContent: 'flex-end' }}
          onClick={() => setShowSchedule(false)}>
          <div style={{ background: '#fff', width: 380, height: '100%', boxShadow: '-4px 0 20px rgba(0,0,0,0.15)', display: 'flex', flexDirection: 'column' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding: '16px 20px', borderBottom: '1px solid #e5e7eb', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontWeight: 700 }}>🕐 予約送信（実技テスト）</span>
              <button onClick={() => setShowSchedule(false)} style={{ background: 'none', border: 'none', fontSize: 18, cursor: 'pointer', color: '#9ca3af' }}>×</button>
            </div>
            <div style={{ padding: '16px 20px', flex: 1, overflowY: 'auto' }}>
              <div style={{ marginBottom: 14 }}>
                <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 6 }}>送信日時</label>
                <input type="datetime-local" value={scheduleAt} onChange={e => setScheduleAt(e.target.value)}
                  style={{ width: '100%', fontSize: 13, padding: '7px 10px', border: '1px solid #e5e7eb', borderRadius: 6, boxSizing: 'border-box' }} />
              </div>
              {(() => {
                const schedulable = candidates.filter(c => !c.spreadsheet_url && ['pending','error'].includes(c.status));
                return (
                  <div style={{ marginBottom: 14 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                      <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280' }}>送信対象（未送信）</label>
                      <button onClick={() => setScheduleChecked(prev => prev.size === schedulable.length ? new Set() : new Set(schedulable.map(c => c.id)))}
                        style={{ fontSize: 11, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer' }}>
                        {scheduleChecked.size === schedulable.length ? '全解除' : '全選択'}
                      </button>
                    </div>
                    {schedulable.length === 0 ? <div style={{ color: '#9ca3af', fontSize: 13 }}>未送信の候補者がいません</div> :
                      schedulable.map(c => (
                        <label key={c.id} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '7px 10px', border: '1px solid #e5e7eb', borderRadius: 6, cursor: 'pointer', marginBottom: 6, background: scheduleChecked.has(c.id) ? '#eef2ff' : '#fff' }}>
                          <input type="checkbox" checked={scheduleChecked.has(c.id)}
                            onChange={() => setScheduleChecked(prev => { const s = new Set(prev); s.has(c.id) ? s.delete(c.id) : s.add(c.id); return s; })}
                            style={{ accentColor: '#6366f1' }} />
                          <div><div style={{ fontWeight: 600, fontSize: 13 }}>{c.name}</div><div style={{ fontSize: 11, color: '#9ca3af' }}>{c.email}</div></div>
                        </label>
                      ))}
                  </div>
                );
              })()}
              {schedules.length > 0 && (
                <div>
                  <div style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', marginBottom: 6 }}>予約中</div>
                  {schedules.map(s => (
                    <div key={s.id} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 10px', background: '#f9fafb', borderRadius: 6, fontSize: 12, marginBottom: 4 }}>
                      <span style={{ flex: 1 }}>{s.candidate_name}</span>
                      <span style={{ color: '#6b7280' }}>{new Date(s.scheduled_at).toLocaleString('ja-JP', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</span>
                      <button onClick={() => handleScheduleCancel(s.id)} style={{ background: 'none', border: 'none', color: '#ef4444', cursor: 'pointer', fontSize: 11 }}>取消</button>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div style={{ padding: '14px 20px', borderTop: '1px solid #e5e7eb' }}>
              <button onClick={handleScheduleSubmit} disabled={scheduling || scheduleChecked.size === 0 || !scheduleAt}
                style={{ width: '100%', padding: 10, background: scheduleChecked.size > 0 && scheduleAt ? '#6366f1' : '#e5e7eb', color: '#fff', border: 'none', borderRadius: 8, fontWeight: 700, fontSize: 14, cursor: 'pointer' }}>
                {scheduling ? '予約中...' : `${scheduleChecked.size}名を予約送信`}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 候補者追加 */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 12, flexWrap: 'wrap' }}>
        <input value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} placeholder="氏名"
          onKeyDown={e => e.key === 'Enter' && handleAdd()}
          style={{ fontSize: 13, padding: '7px 12px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', width: 160 }} />
        <input value={form.email} onChange={e => setForm(f => ({ ...f, email: e.target.value }))} placeholder="メールアドレス"
          onKeyDown={e => e.key === 'Enter' && handleAdd()}
          style={{ fontSize: 13, padding: '7px 12px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', width: 240 }} />
        <button onClick={handleAdd} disabled={!form.name.trim() || !form.email.trim()}
          style={{ fontSize: 13, padding: '7px 12px', border: 'none', borderRadius: 8, background: '#2563eb', color: '#fff', fontWeight: 600, cursor: 'pointer' }}>
          ＋ 追加
        </button>
        <span style={{ fontSize: 12, color: '#9ca3af', alignSelf: 'center' }}>{candidates.length}名登録中</span>
      </div>

      {/* ステータスフィルター */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10, flexWrap: 'wrap' }}>
        <span style={{ fontSize: 12, color: '#6b7280', fontWeight: 600 }}>表示：</span>
        {Object.entries(TEST_STATUS_LABEL).map(([s, label]) => (
          <label key={s} style={{ display: 'flex', alignItems: 'center', gap: 4, cursor: 'pointer', fontSize: 12, padding: '3px 10px', borderRadius: 99,
            border: `1px solid ${TEST_STATUS_COLOR[s]}`, background: filterStatuses.has(s) ? TEST_STATUS_BG[s] : '#f9fafb',
            color: filterStatuses.has(s) ? TEST_STATUS_COLOR[s] : '#9ca3af', fontWeight: 600, userSelect: 'none' }}>
            <input type="checkbox" checked={filterStatuses.has(s)}
              onChange={() => setFilterStatuses(prev => { const n = new Set(prev); n.has(s) ? n.delete(s) : n.add(s); return n; })}
              style={{ accentColor: TEST_STATUS_COLOR[s], width: 12, height: 12 }} />
            {label}
          </label>
        ))}
        <span style={{ fontSize: 11, color: '#9ca3af' }}>{filtered.length}/{candidates.length}名</span>
      </div>

      {/* テーブル */}
      {loading ? (
        <div style={{ color: '#9ca3af', padding: 24 }}>読み込み中…</div>
      ) : (
        <div style={{ border: '1px solid #e5e7eb', borderRadius: 10, overflowX: 'auto' }}>
          <table style={{ width: '100%', minWidth: 700, borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: '#f8fafc' }}>
                {['氏名', 'メール', '② 実技テスト', 'スコア', '④ 性格診断', ''].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', whiteSpace: 'nowrap' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map(c => {
                const ps = c.personality_status || 'pending';
                const passedFirst = c.stage === 'personality';
                return (
                  <tr key={c.id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                    <td style={{ padding: '10px 14px', fontWeight: 600, color: '#111827' }}>{c.name}</td>
                    <td style={{ padding: '10px 14px', color: '#6b7280', fontSize: 12 }}>{c.email}</td>

                    {/* 実技テスト列 */}
                    <td style={{ padding: '10px 14px' }}>
                      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                        {sendingIds.has(c.id)
                          ? <span style={{ fontSize: 11, fontWeight: 700, color: '#d97706' }}>⏳ 送信中…</span>
                          : <TestStatusBadge c={c} />}
                        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                          {['pending','error'].includes(c.status) && !c.scheduled_at && (
                            <button onClick={() => handleSendTest(c.id)} disabled={sendingIds.has(c.id)}
                              style={{ fontSize: 11, padding: '2px 8px', borderRadius: 5, border: '1px solid #fde68a', background: '#fffbeb', color: '#d97706', fontWeight: 600, cursor: 'pointer' }}>
                              📧 送付
                            </button>
                          )}
                          {c.spreadsheet_url && (
                            <a href={c.spreadsheet_url} target="_blank" rel="noreferrer" style={{ fontSize: 11, color: '#3b82f6' }}>スプシ→</a>
                          )}
                          {c.error_message && (
                            <span style={{ fontSize: 10, color: '#dc2626' }} title={c.error_message}>⚠エラー</span>
                          )}
                        </div>
                      </div>
                    </td>

                    {/* スコア列 */}
                    <td style={{ padding: '10px 14px' }}>
                      {c.score != null ? (
                        <div>
                          <span style={{ fontWeight: 700, fontSize: 14, color: c.score >= totalScore * 0.8 ? '#059669' : c.score >= totalScore * 0.6 ? '#d97706' : '#dc2626' }}>
                            {c.score}点
                          </span>
                          {c.typing_level && <div style={{ fontSize: 11, color: '#6b7280', marginTop: 2 }}>{c.typing_level}</div>}
                        </div>
                      ) : <span style={{ color: '#d1d5db' }}>—</span>}
                    </td>

                    {/* 性格診断列 */}
                    <td style={{ padding: '10px 14px' }}>
                      {!passedFirst ? (
                        /* 一次面接通過前：解放ボタン */
                        <button onClick={() => handlePassFirstInterview(c.id)}
                          style={{ fontSize: 11, padding: '3px 10px', borderRadius: 5, border: '1px solid #e2e8f0', background: '#f8fafc', color: '#94a3b8', cursor: 'pointer' }}>
                          一次面接通過 →
                        </button>
                      ) : (
                        /* 通過後：性格診断の状態・操作 */
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          <span style={{ fontSize: 11, fontWeight: 700, padding: '1px 8px', borderRadius: 99, display: 'inline-block',
                            background: ps === 'completed' ? '#f0fdf4' : ps === 'sent' ? '#eff6ff' : '#f3f4f6',
                            color: ps === 'completed' ? '#059669' : ps === 'sent' ? '#2563eb' : '#9ca3af' }}>
                            {ps === 'completed' ? '回答済' : ps === 'sent' ? '送付済' : '未送付'}
                          </span>
                          <div style={{ display: 'flex', gap: 6 }}>
                            {ps === 'pending' && (
                              <button onClick={() => handleSendPersonality(c.id)}
                                style={{ fontSize: 11, padding: '2px 8px', borderRadius: 5, border: '1px solid #86efac', background: '#f0fdf4', color: '#15803d', fontWeight: 600, cursor: 'pointer' }}>
                                📧 送付
                              </button>
                            )}
                            {ps === 'completed' && (
                              <button onClick={() => handlePersonalityPdf(c.id)}
                                style={{ fontSize: 11, padding: '2px 8px', borderRadius: 5, border: '1px solid #86efac', background: '#f0fdf4', color: '#15803d', fontWeight: 600, cursor: 'pointer' }}>
                                📄 PDF
                              </button>
                            )}
                            {settings?.personality_sheet_url && (
                              <a href={settings.personality_sheet_url} target="_blank" rel="noreferrer" style={{ fontSize: 11, color: '#3b82f6' }}>結果→</a>
                            )}
                          </div>
                        </div>
                      )}
                    </td>

                    <td style={{ padding: '10px 14px', textAlign: 'right' }}>
                      <button onClick={() => handleDelete(c.id)}
                        style={{ fontSize: 11, color: '#9ca3af', background: 'none', border: '1px solid #e5e7eb', borderRadius: 5, padding: '3px 8px', cursor: 'pointer' }}>
                        削除
                      </button>
                    </td>
                  </tr>
                );
              })}
              {filtered.length === 0 && (
                <tr><td colSpan={6} style={{ padding: '48px', textAlign: 'center', color: '#9ca3af' }}>
                  {candidates.length === 0 ? '候補者を追加してください' : '表示対象がいません'}
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
