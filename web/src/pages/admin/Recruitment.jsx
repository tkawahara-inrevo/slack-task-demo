import { useEffect, useRef, useState, useMemo } from 'react';
import { api } from '../../api/client';

// ── 選考フロー定義 ────────────────────────────────────────────
const STAGES = [
  { key: 'casual_talk',     label: 'カジュアル面談', short: '①',  color: '#8b5cf6', bg: '#f5f3ff' },
  { key: 'skill_test',      label: '実技テスト',     short: '②',  color: '#f59e0b', bg: '#fffbeb' },
  { key: 'first_interview', label: '一次面接',       short: '③',  color: '#3b82f6', bg: '#eff6ff' },
  { key: 'personality',     label: '性格診断',       short: '④',  color: '#10b981', bg: '#f0fdf4' },
  { key: 'final_interview', label: '最終面接',       short: '⑤',  color: '#f97316', bg: '#fff7ed' },
  { key: 'offer',           label: '内定',           short: '✓',  color: '#059669', bg: '#f0fdf4' },
];
const STAGE_KEYS   = STAGES.map(s => s.key);
const STAGE_MAP    = Object.fromEntries(STAGES.map(s => [s.key, s]));
const NEXT_STAGE   = Object.fromEntries(STAGES.slice(0, -1).map((s, i) => [s.key, STAGES[i + 1].key]));

const TEST_STATUS_LABEL = { pending: '未送信', scheduled: '予約済み', sent: '送信済', completed: '完了', error: 'エラー' };
const TEST_STATUS_COLOR = { pending: '#9ca3af', scheduled: '#7c3aed', sent: '#2563eb', completed: '#059669', error: '#dc2626' };
const TEST_STATUS_BG    = { pending: '#f3f4f6', scheduled: '#f5f3ff', sent: '#eff6ff', completed: '#f0fdf4', error: '#fef2f2' };

function effectiveTestStatus(c) {
  if (c.scheduled_at && c.status === 'pending') return 'scheduled';
  return c.status;
}

// ── ステージバッジ ───────────────────────────────────────────
function StageBadge({ stageKey }) {
  const s = STAGE_MAP[stageKey] || STAGES[0];
  return (
    <span style={{ fontSize: 11, fontWeight: 700, padding: '2px 9px', borderRadius: 99,
      background: s.bg, color: s.color, border: `1px solid ${s.color}40`, whiteSpace: 'nowrap' }}>
      {s.short} {s.label}
    </span>
  );
}

// ── アクションセル ─────────────────────────────────────────
function ActionCell({ c, settings, sendingId, onStageChange, onSendTest, onSendPersonality, onPersonalityPdf, onDelete }) {
  const [advancing, setAdvancing]   = useState(false);
  const [rejecting, setRejecting]   = useState(false);
  const [pdfLoading, setPdfLoading] = useState(false);

  const stage   = c.stage || 'casual_talk';
  const isEnded = stage === 'offer' || stage === 'rejected';
  const nextKey = NEXT_STAGE[stage];
  const nextStage = nextKey ? STAGE_MAP[nextKey] : null;

  const advance = async () => {
    if (!nextKey) return;
    if (!window.confirm(`${c.name} さんを「${nextStage.label}」に進めますか？`)) return;
    setAdvancing(true);
    try { await onStageChange(c.id, nextKey); }
    finally { setAdvancing(false); }
  };

  const reject = async () => {
    if (!window.confirm(`${c.name} さんを不通過/辞退にしますか？`)) return;
    setRejecting(true);
    try { await onStageChange(c.id, 'rejected'); }
    finally { setRejecting(false); }
  };

  const pdf = async () => {
    setPdfLoading(true);
    try { await onPersonalityPdf(c.id); }
    finally { setPdfLoading(false); }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {/* ステージ固有アクション */}
      {stage === 'skill_test' && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
          <span style={{ fontSize: 11, fontWeight: 700, padding: '1px 8px', borderRadius: 99,
            background: TEST_STATUS_BG[effectiveTestStatus(c)], color: TEST_STATUS_COLOR[effectiveTestStatus(c)] }}>
            実技: {TEST_STATUS_LABEL[effectiveTestStatus(c)]}
          </span>
          {['pending','error'].includes(c.status) && (
            <button onClick={() => onSendTest(c.id)} disabled={!!sendingId}
              style={btnStyle('#f59e0b', '#fffbeb')}>
              {sendingId ? '…' : '📧 実技送付'}
            </button>
          )}
          {c.status === 'completed' && c.score != null && (
            <span style={{ fontSize: 11, fontWeight: 700, color: '#059669' }}>{c.score}点</span>
          )}
          {c.spreadsheet_url && (
            <a href={c.spreadsheet_url} target="_blank" rel="noreferrer" style={{ fontSize: 11, color: '#3b82f6' }}>スプシ→</a>
          )}
        </div>
      )}

      {stage === 'personality' && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
          <span style={{ fontSize: 11, fontWeight: 700, padding: '1px 8px', borderRadius: 99,
            background: c.personality_status === 'completed' ? '#f0fdf4' : c.personality_status === 'sent' ? '#eff6ff' : '#f3f4f6',
            color: c.personality_status === 'completed' ? '#059669' : c.personality_status === 'sent' ? '#2563eb' : '#9ca3af' }}>
            診断: {c.personality_status === 'completed' ? '回答済' : c.personality_status === 'sent' ? '送付済' : '未送付'}
          </span>
          {c.personality_status === 'pending' && (
            <button onClick={() => onSendPersonality(c.id)} style={btnStyle('#10b981', '#f0fdf4')}>
              📧 診断送付
            </button>
          )}
          {c.personality_status === 'completed' && (
            <button onClick={pdf} disabled={pdfLoading} style={btnStyle('#059669', '#f0fdf4')}>
              {pdfLoading ? '…' : '📄 PDF'}
            </button>
          )}
          {settings?.personality_sheet_url && (
            <a href={settings.personality_sheet_url} target="_blank" rel="noreferrer" style={{ fontSize: 11, color: '#3b82f6' }}>結果→</a>
          )}
        </div>
      )}

      {/* 通過・不通過ボタン */}
      {!isEnded && (
        <div style={{ display: 'flex', gap: 5 }}>
          {nextStage && (
            <button onClick={advance} disabled={advancing}
              style={{ ...btnStyle(nextStage.color, nextStage.bg), fontSize: 11 }}>
              {advancing ? '…' : `✓ ${nextStage.label}へ`}
            </button>
          )}
          <button onClick={reject} disabled={rejecting}
            style={{ fontSize: 11, padding: '3px 8px', borderRadius: 5, border: '1px solid #fca5a5', background: '#fff', color: '#dc2626', cursor: rejecting ? 'default' : 'pointer' }}>
            {rejecting ? '…' : '✗'}
          </button>
        </div>
      )}

      {isEnded && (
        <button onClick={() => onDelete(c.id)}
          style={{ fontSize: 11, color: '#9ca3af', background: 'none', border: '1px solid #e5e7eb', borderRadius: 5, padding: '2px 8px', cursor: 'pointer' }}>
          削除
        </button>
      )}
    </div>
  );
}

function btnStyle(color, bg) {
  return { fontSize: 11, padding: '3px 8px', borderRadius: 5, border: `1px solid ${color}60`, background: bg, color, fontWeight: 600, cursor: 'pointer' };
}

// ── メイン ──────────────────────────────────────────────────
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
  const [activeTab, setActiveTab]   = useState('active'); // active | ended
  const [filterStage, setFilterStage] = useState('');
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

  const updateCandidate = (id, patch) => setCandidates(prev => prev.map(c => c.id === id ? { ...c, ...patch } : c));

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

  const handleStageChange = async (id, newStage) => {
    const r = await api.recruitmentStage(id, newStage);
    updateCandidate(id, { stage: r.candidate.stage });
  };

  const handleSendTest = async (id) => {
    const c = candidates.find(x => x.id === id);
    if (!c) return;
    if (!window.confirm(`${c.name} さんに実技テストを送付しますか？`)) return;
    setSendingIds(s => new Set([...s, id]));
    try {
      await api.recruitmentSendOne(id);
      const fresh = await api.recruitmentCandidates();
      setCandidates(fresh.candidates || []);
    } catch (e) { alert('送付失敗: ' + e.message); }
    finally { setSendingIds(s => { const n = new Set(s); n.delete(id); return n; }); }
  };

  const handleSendPersonality = async (id) => {
    if (!settings?.personality_gas_url) { alert('設定 → 適性診断GAS URLを設定してください'); return; }
    const c = candidates.find(x => x.id === id);
    if (!window.confirm(`${c?.name} さんに性格診断を送付しますか？`)) return;
    try {
      await api.personalitySend(id);
      updateCandidate(id, { personality_status: 'sent' });
    } catch (e) { alert('送付失敗: ' + e.message); }
  };

  const handlePersonalityPdf = async (id) => {
    if (!settings?.personality_gas_url) { alert('設定 → 適性診断GAS URLを設定してください'); return; }
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
    if (target) updateCandidate(target.candidate_id, { scheduled_at: null });
  };

  // フィルタリング
  const displayed = useMemo(() => {
    let list = candidates.filter(c => {
      const stage = c.stage || 'casual_talk';
      if (activeTab === 'active') return stage !== 'offer' && stage !== 'rejected';
      return stage === 'offer' || stage === 'rejected';
    });
    if (filterStage) list = list.filter(c => (c.stage || 'casual_talk') === filterStage);
    return list;
  }, [candidates, activeTab, filterStage]);

  const pendingForTest = candidates.filter(c => {
    const stage = c.stage || 'casual_talk';
    return stage === 'skill_test' && !c.spreadsheet_url && ['pending','error'].includes(c.status);
  });
  const schedulableCandidates = pendingForTest;

  const totalScore = settings.total_score || 10;
  const webhookUrl = `${window.location.origin}/api/dashboard/recruitment/webhook/complete`;

  return (
    <div>
      {/* ヘッダー */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16, flexWrap: 'wrap', gap: 8 }}>
        <div>
          <h2 style={{ margin: '0 0 4px', fontSize: '1rem', fontWeight: 700 }}>採用管理（自社選考）</h2>
          <p style={{ margin: 0, fontSize: '0.82rem', color: '#6b7280' }}>①カジュアル面談 → ②実技テスト → ③一次面接 → ④性格診断 → ⑤最終面接</p>
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
        </div>
      </div>

      {/* 設定パネル */}
      {showSettings && (
        <div style={{ marginBottom: 20, border: '1px solid #e5e7eb', borderRadius: 10, padding: 20, background: '#fafafa' }}>
          <h3 style={{ margin: '0 0 16px', fontSize: '0.9rem', fontWeight: 700 }}>設定</h3>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#9ca3af', marginBottom: 8, textTransform: 'uppercase' }}>実技テスト設定</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {[
                ['template_spreadsheet_id', 'テンプレートスプレッドシートID', ''],
                ['gas_endpoint_url', 'GAS Web App URL', ''],
                ['notify_channel_id', 'HR通知チャンネルID', ''],
                ['notify_mention_user_id', 'メンション先ユーザーID', ''],
                ['webhook_secret', 'Webhookシークレット', ''],
                ['total_score', '満点', '10'],
                ['import_sheet_url', '取り込み用スプレッドシートURL', ''],
                ['from_email', '送信元メールアドレス', ''],
              ].map(([key, label, placeholder]) => (
                <div key={key}>
                  <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>{label}</label>
                  <input value={settingsForm[key] || ''} onChange={e => setSettingsForm(f => ({ ...f, [key]: e.target.value }))}
                    placeholder={placeholder} type={key === 'total_score' ? 'number' : 'text'}
                    style={{ width: '100%', fontSize: 12, padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 6, outline: 'none', boxSizing: 'border-box' }} />
                </div>
              ))}
              <div>
                <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>Webhook URL（GASに設定）</label>
                <div style={{ fontSize: 11, padding: '6px 10px', background: '#f3f4f6', borderRadius: 6, color: '#374151', wordBreak: 'break-all', fontFamily: 'monospace' }}>{webhookUrl}</div>
              </div>
            </div>
          </div>
          <div style={{ marginBottom: 16, paddingTop: 12, borderTop: '1px solid #e5e7eb' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#9ca3af', marginBottom: 8, textTransform: 'uppercase' }}>④ 性格診断設定</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
              {[
                ['personality_gas_url', '適性診断 GAS Web App URL', ''],
                ['personality_sheet_url', '結果スプレッドシートURL', ''],
                ['personality_webhook_secret', 'Webhookシークレット（実技テストと同じでもOK）', ''],
              ].map(([key, label, placeholder]) => (
                <div key={key}>
                  <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>{label}</label>
                  <input value={settingsForm[key] || ''} onChange={e => setSettingsForm(f => ({ ...f, [key]: e.target.value }))}
                    placeholder={placeholder}
                    style={{ width: '100%', fontSize: 12, padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 6, outline: 'none', boxSizing: 'border-box' }} />
                </div>
              ))}
              <div>
                <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>Webhook URL（GASに設定）</label>
                <div style={{ fontSize: 11, padding: '6px 10px', background: '#f3f4f6', borderRadius: 6, color: '#374151', wordBreak: 'break-all', fontFamily: 'monospace' }}>
                  {`${window.location.origin}/api/dashboard/recruitment/webhook/personality`}
                </div>
              </div>
            </div>
          </div>
          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <button onClick={() => setShowSettings(false)} style={{ fontSize: 12, padding: '6px 14px', border: '1px solid #e5e7eb', borderRadius: 6, background: '#fff', cursor: 'pointer' }}>キャンセル</button>
            <button onClick={handleSaveSettings} disabled={savingSettings}
              style={{ fontSize: 12, padding: '6px 14px', border: 'none', borderRadius: 6, background: '#2563eb', color: '#fff', fontWeight: 700, cursor: savingSettings ? 'default' : 'pointer' }}>
              {savingSettings ? '保存中…' : '保存'}
            </button>
          </div>
        </div>
      )}

      {/* 候補者追加フォーム */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16, flexWrap: 'wrap' }}>
        <input value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} placeholder="氏名"
          onKeyDown={e => e.key === 'Enter' && handleAdd()}
          style={{ fontSize: 13, padding: '7px 12px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', width: 160 }} />
        <input value={form.email} onChange={e => setForm(f => ({ ...f, email: e.target.value }))} placeholder="メールアドレス"
          onKeyDown={e => e.key === 'Enter' && handleAdd()}
          style={{ fontSize: 13, padding: '7px 12px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', width: 240 }} />
        <button onClick={handleAdd} disabled={!form.name.trim() || !form.email.trim()}
          style={{ fontSize: 13, padding: '7px 14px', border: 'none', borderRadius: 8, background: '#2563eb', color: '#fff', fontWeight: 600, cursor: 'pointer' }}>
          ＋ 追加
        </button>
        <span style={{ fontSize: 12, color: '#9ca3af', alignSelf: 'center' }}>{candidates.length}名登録</span>
      </div>

      {/* タブ + フィルター */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
        <div style={{ display: 'flex', background: '#f1f5f9', borderRadius: 8, padding: 3, gap: 2 }}>
          {[['active','選考中'], ['ended','完了']].map(([v, l]) => (
            <button key={v} onClick={() => { setActiveTab(v); setFilterStage(''); }}
              style={{ padding: '4px 14px', borderRadius: 6, border: 'none', fontSize: 12, fontWeight: 700, cursor: 'pointer',
                background: activeTab === v ? '#fff' : 'transparent',
                color: activeTab === v ? '#2563eb' : '#6b7280',
                boxShadow: activeTab === v ? '0 1px 3px rgba(0,0,0,0.1)' : 'none' }}>
              {l}
            </button>
          ))}
        </div>

        {/* ステージフィルター */}
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
          <button onClick={() => setFilterStage('')}
            style={{ fontSize: 11, padding: '2px 9px', borderRadius: 99, border: '1px solid #e2e8f0', cursor: 'pointer',
              background: filterStage === '' ? '#2563eb' : '#fff', color: filterStage === '' ? '#fff' : '#6b7280', fontWeight: 600 }}>
            全て
          </button>
          {STAGES.filter(s => activeTab === 'active' ? !['offer'].includes(s.key) : ['offer','rejected'].includes(s.key))
            .map(s => (
              <button key={s.key} onClick={() => setFilterStage(s.key === filterStage ? '' : s.key)}
                style={{ fontSize: 11, padding: '2px 9px', borderRadius: 99, border: `1px solid ${s.color}50`, cursor: 'pointer',
                  background: filterStage === s.key ? s.color : s.bg, color: filterStage === s.key ? '#fff' : s.color, fontWeight: 600 }}>
                {s.short} {s.label}
              </button>
            ))}
        </div>

        {/* 実技テスト一括 */}
        {activeTab === 'active' && pendingForTest.length > 0 && (
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
            <button onClick={openSchedule}
              style={{ fontSize: 12, padding: '5px 12px', border: '1px solid #6366f1', borderRadius: 6, background: '#fff', color: '#6366f1', fontWeight: 600, cursor: 'pointer' }}>
              🕐 予約送信
            </button>
            <button onClick={async () => {
              if (!window.confirm(`${pendingForTest.length}名に実技テストを送付しますか？`)) return;
              setSendingIds(new Set(pendingForTest.map(c => c.id)));
              try {
                await api.recruitmentSend();
                const fresh = await api.recruitmentCandidates();
                setCandidates(fresh.candidates || []);
              } finally { setSendingIds(new Set()); }
            }} style={{ fontSize: 12, padding: '5px 12px', border: 'none', borderRadius: 6, background: '#f59e0b', color: '#fff', fontWeight: 700, cursor: 'pointer' }}>
              📧 実技一括送付 ({pendingForTest.length}名)
            </button>
          </div>
        )}
      </div>

      {/* 予約送信モーダル */}
      {showSchedule && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', zIndex: 500, display: 'flex', alignItems: 'flex-start', justifyContent: 'flex-end' }}
          onClick={() => setShowSchedule(false)}>
          <div style={{ background: '#fff', width: 380, height: '100%', boxShadow: '-4px 0 20px rgba(0,0,0,0.15)', display: 'flex', flexDirection: 'column', overflowY: 'auto' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding: '16px 20px', borderBottom: '1px solid #e5e7eb', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontWeight: 700 }}>🕐 予約送信（実技テスト）</span>
              <button onClick={() => setShowSchedule(false)} style={{ background: 'none', border: 'none', fontSize: 18, cursor: 'pointer', color: '#9ca3af' }}>×</button>
            </div>
            <div style={{ padding: '16px 20px', flex: 1 }}>
              <div style={{ marginBottom: 14 }}>
                <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 6 }}>送信日時</label>
                <input type="datetime-local" value={scheduleAt} onChange={e => setScheduleAt(e.target.value)}
                  style={{ width: '100%', fontSize: 13, padding: '7px 10px', border: '1px solid #e5e7eb', borderRadius: 6, boxSizing: 'border-box' }} />
              </div>
              <div style={{ marginBottom: 14 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                  <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280' }}>送信対象（②実技テスト・未送信）</label>
                  <button onClick={() => setScheduleChecked(prev => prev.size === schedulableCandidates.length ? new Set() : new Set(schedulableCandidates.map(c => c.id)))}
                    style={{ fontSize: 11, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer' }}>
                    {scheduleChecked.size === schedulableCandidates.length ? '全解除' : '全選択'}
                  </button>
                </div>
                {schedulableCandidates.map(c => (
                  <label key={c.id} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '7px 10px', border: '1px solid #e5e7eb', borderRadius: 6, cursor: 'pointer', marginBottom: 6,
                    background: scheduleChecked.has(c.id) ? '#eef2ff' : '#fff' }}>
                    <input type="checkbox" checked={scheduleChecked.has(c.id)}
                      onChange={() => setScheduleChecked(prev => { const s = new Set(prev); s.has(c.id) ? s.delete(c.id) : s.add(c.id); return s; })}
                      style={{ accentColor: '#6366f1' }} />
                    <div>
                      <div style={{ fontWeight: 600, fontSize: 13 }}>{c.name}</div>
                      <div style={{ fontSize: 11, color: '#9ca3af' }}>{c.email}</div>
                    </div>
                  </label>
                ))}
              </div>
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

      {/* 候補者テーブル */}
      {loading ? (
        <div style={{ color: '#9ca3af', padding: 24 }}>読み込み中…</div>
      ) : displayed.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '48px 0', color: '#9ca3af' }}>
          <div style={{ fontSize: '2rem', marginBottom: 8 }}>{activeTab === 'active' ? '🎯' : '✅'}</div>
          <div>{activeTab === 'active' ? '選考中の候補者がいません' : '完了した候補者がいません'}</div>
        </div>
      ) : (
        <div style={{ border: '1px solid #e5e7eb', borderRadius: 10, overflowX: 'auto' }}>
          <table style={{ width: '100%', minWidth: 700, borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: '#f8fafc' }}>
                {['氏名', 'メール', '現在のステージ', 'テスト結果', 'アクション'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', whiteSpace: 'nowrap' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {displayed.map(c => (
                <tr key={c.id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                  <td style={{ padding: '12px 14px', fontWeight: 600, color: '#111827' }}>{c.name}</td>
                  <td style={{ padding: '12px 14px', color: '#6b7280', fontSize: 12 }}>{c.email}</td>
                  <td style={{ padding: '12px 14px' }}>
                    <StageBadge stageKey={c.stage || 'casual_talk'} />
                  </td>
                  <td style={{ padding: '12px 14px' }}>
                    {c.score != null ? (
                      <div>
                        <span style={{ fontWeight: 700, color: c.score >= totalScore * 0.8 ? '#059669' : c.score >= totalScore * 0.6 ? '#d97706' : '#dc2626' }}>
                          {c.score}点
                        </span>
                        {c.typing_level && <span style={{ fontSize: 11, color: '#6b7280', marginLeft: 6 }}>{c.typing_level}</span>}
                      </div>
                    ) : <span style={{ color: '#d1d5db', fontSize: 12 }}>—</span>}
                  </td>
                  <td style={{ padding: '12px 14px' }}>
                    <ActionCell
                      c={c} settings={settings}
                      sendingId={sendingIds.has(c.id) ? c.id : null}
                      onStageChange={handleStageChange}
                      onSendTest={handleSendTest}
                      onSendPersonality={handleSendPersonality}
                      onPersonalityPdf={handlePersonalityPdf}
                      onDelete={handleDelete}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
