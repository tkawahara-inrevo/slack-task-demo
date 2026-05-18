import { useEffect, useState } from 'react';
import { api } from '../../api/client';

const STATUS_LABEL = { pending: '未送信', sent: '送信済', completed: '完了', error: 'エラー' };
const STATUS_COLOR = { pending: '#9ca3af', sent: '#2563eb', completed: '#059669', error: '#dc2626' };
const STATUS_BG   = { pending: '#f3f4f6', sent: '#eff6ff', completed: '#f0fdf4', error: '#fef2f2' };

function StatusBadge({ status }) {
  return (
    <span style={{ fontSize: 11, fontWeight: 700, padding: '2px 8px', borderRadius: 99,
      color: STATUS_COLOR[status] || '#9ca3af', background: STATUS_BG[status] || '#f3f4f6' }}>
      {STATUS_LABEL[status] || status}
    </span>
  );
}


export default function Recruitment() {
  const [candidates, setCandidates]     = useState([]);
  const [settings, setSettings]         = useState({});
  const [loading, setLoading]           = useState(true);
  const [sendingIds, setSendingIds]     = useState(new Set());
  const [showSettings, setShowSettings] = useState(false);
  const [form, setForm]                 = useState({ name: '', email: '' });
  const [settingsForm, setSettingsForm] = useState({});
  const [savingSettings, setSavingSettings] = useState(false);
  const [importing, setImporting]       = useState(false);

  // 予約送信
  const [showSchedule, setShowSchedule]       = useState(false);
  const [scheduleChecked, setScheduleChecked] = useState(new Set());
  const [scheduleAt, setScheduleAt]           = useState('');
  const [scheduling, setScheduling]           = useState(false);
  const [schedules, setSchedules]             = useState([]);
  const [showScheduleList, setShowScheduleList] = useState(false);

  const loadSchedules = async () => {
    try { const r = await api.recruitmentScheduled(); setSchedules(r.schedules || []); }
    catch { setSchedules([]); }
  };

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

  const handleSend = async () => {
    const targets = candidates.filter(c => c.name && c.email && !c.spreadsheet_url && ['pending','error'].includes(c.status));
    if (targets.length === 0) { alert('送信対象の候補者がいません'); return; }
    if (!window.confirm(`${targets.length}名にテストを送信しますか？\n\n送信はサーバー側で処理されます。タブを離れても問題ありません。`)) return;

    setSendingIds(new Set(targets.map(c => c.id)));
    try {
      // サーバー側で一括処理 → タブを離れても止まらない
      const r = await api.recruitmentSend();
      const succeeded = r.results?.filter(x => x.ok).length ?? 0;
      const failed    = r.results?.filter(x => !x.ok).length ?? 0;
      // 完了後にリスト再取得
      const fresh = await api.recruitmentCandidates();
      setCandidates(fresh.candidates || []);
      if (failed > 0) alert(`送信完了：${succeeded}名成功 / ${failed}名失敗`);
    } catch (e) {
      alert('送信に失敗しました: ' + e.message);
    } finally {
      setSendingIds(new Set());
    }
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
      });
      setSettings(settingsForm);
      setShowSettings(false);
      alert('設定を保存しました');
    } catch (e) { alert(e.message); }
    finally { setSavingSettings(false); }
  };

  // スプレッドシートから取り込み（設定の import_sheet_url を使用）
  const handleImportFromSheet = async () => {
    const url = settings.import_sheet_url;
    if (!url) { alert('設定 → 取り込み用スプレッドシートURL を設定してください'); return; }
    setImporting(true);
    try {
      const r = await api.recruitmentImportFromSheet(url);
      setCandidates(prev => [...(r.added || []), ...prev]);
      alert(`${r.added?.length || 0}名取り込み完了（${r.skipped || 0}名スキップ・重複）`);
    } catch (e) { alert('取り込みに失敗しました: ' + e.message); }
    finally { setImporting(false); }
  };

  const handleScheduleOpen = () => {
    setScheduleChecked(new Set());
    const now = new Date(); now.setMinutes(now.getMinutes() + 30); now.setSeconds(0, 0);
    setScheduleAt(now.toISOString().slice(0, 16));
    loadSchedules();
    setShowSchedule(true);
  };

  const handleScheduleSubmit = async () => {
    if (!scheduleChecked.size || !scheduleAt) return;
    setScheduling(true);
    try {
      await api.recruitmentScheduleCreate([...scheduleChecked], new Date(scheduleAt).toISOString());
      await loadSchedules();
      setShowSchedule(false);
      alert(`${scheduleChecked.size}名の予約送信を設定しました`);
    } catch (e) { alert('予約に失敗しました: ' + e.message); }
    finally { setScheduling(false); }
  };

  const handleScheduleCancel = async (id) => {
    if (!window.confirm('この予約をキャンセルしますか？')) return;
    await api.recruitmentScheduleCancel(id).catch(() => {});
    setSchedules(prev => prev.filter(s => s.id !== id));
  };

  const isSending   = sendingIds.size > 0;
  const pendingCount = candidates.filter(c => !c.spreadsheet_url && ['pending','error'].includes(c.status)).length;
  const schedulableCandidates = candidates.filter(c => !c.spreadsheet_url && ['pending','error'].includes(c.status));
  const webhookUrl   = `${window.location.origin}/api/dashboard/recruitment/webhook/complete`;
  const totalScore   = settings.total_score || 10;

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: '0 0 4px', fontSize: '1rem', fontWeight: 700 }}>採用管理（実技テスト）</h2>
          <p style={{ margin: 0, fontSize: '0.82rem', color: '#6b7280' }}>候補者にテストを送付し、採点結果を自動収集します</p>
        </div>
        <div style={{ display: 'flex', gap: 8 }}>
          <button onClick={() => { setShowSettings(v => !v); setSettingsForm(settings); }}
            style={{ fontSize: 12, padding: '5px 12px', border: '1px solid #e5e7eb', borderRadius: 6, background: '#fff', cursor: 'pointer', color: '#6b7280' }}>
            ⚙️ 設定
          </button>
          <button onClick={handleScheduleOpen} disabled={isSending || pendingCount === 0}
            style={{ fontSize: 13, padding: '6px 14px', border: '1px solid #6366f1', borderRadius: 6, background: '#fff', color: '#6366f1', fontWeight: 600, cursor: pendingCount === 0 ? 'not-allowed' : 'pointer', opacity: pendingCount === 0 ? 0.5 : 1 }}>
            🕐 予約送信
          </button>
          <button onClick={handleSend} disabled={isSending || pendingCount === 0}
            className="btn btn-primary" style={{ fontSize: 13 }}>
            {isSending ? '送信中… (タブを離れても継続)' : `送信 ${pendingCount > 0 ? `(${pendingCount}名)` : ''}`}
          </button>
        </div>
      </div>

      {/* 予約送信モーダル */}
      {showSchedule && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', zIndex: 500, display: 'flex', alignItems: 'flex-start', justifyContent: 'flex-end' }}
          onClick={() => setShowSchedule(false)}>
          <div style={{ background: '#fff', width: 400, height: '100%', boxShadow: '-4px 0 20px rgba(0,0,0,0.15)', display: 'flex', flexDirection: 'column', overflowY: 'auto' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding: '16px 20px', borderBottom: '1px solid #e5e7eb', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontWeight: 700, fontSize: '0.95rem' }}>🕐 予約送信</span>
              <button onClick={() => setShowSchedule(false)} style={{ background: 'none', border: 'none', fontSize: 18, cursor: 'pointer', color: '#9ca3af' }}>×</button>
            </div>

            <div style={{ padding: '16px 20px', flex: 1 }}>
              {/* 送信日時 */}
              <div style={{ marginBottom: 16 }}>
                <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 6 }}>送信日時</label>
                <input type="datetime-local" value={scheduleAt} onChange={e => setScheduleAt(e.target.value)}
                  style={{ width: '100%', fontSize: 13, padding: '7px 10px', border: '1px solid #e5e7eb', borderRadius: 6, boxSizing: 'border-box' }} />
              </div>

              {/* 候補者チェックリスト */}
              <div style={{ marginBottom: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                  <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280' }}>送信対象（未送信者）</label>
                  <button onClick={() => {
                    if (scheduleChecked.size === schedulableCandidates.length) setScheduleChecked(new Set());
                    else setScheduleChecked(new Set(schedulableCandidates.map(c => c.id)));
                  }} style={{ fontSize: 11, color: '#6366f1', background: 'none', border: 'none', cursor: 'pointer' }}>
                    {scheduleChecked.size === schedulableCandidates.length ? '全解除' : '全選択'}
                  </button>
                </div>
                {schedulableCandidates.length === 0 ? (
                  <div style={{ color: '#9ca3af', fontSize: 13, padding: '12px 0' }}>未送信の候補者がいません</div>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                    {schedulableCandidates.map(c => (
                      <label key={c.id} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '8px 10px', border: '1px solid #e5e7eb', borderRadius: 6, cursor: 'pointer', background: scheduleChecked.has(c.id) ? '#eef2ff' : '#fff' }}>
                        <input type="checkbox" checked={scheduleChecked.has(c.id)}
                          onChange={() => setScheduleChecked(prev => {
                            const s = new Set(prev);
                            s.has(c.id) ? s.delete(c.id) : s.add(c.id);
                            return s;
                          })}
                          style={{ accentColor: '#6366f1' }} />
                        <div>
                          <div style={{ fontWeight: 600, fontSize: 13 }}>{c.name}</div>
                          <div style={{ fontSize: 11, color: '#9ca3af' }}>{c.email}</div>
                        </div>
                      </label>
                    ))}
                  </div>
                )}
              </div>

              {/* 予約一覧 */}
              {schedules.length > 0 && (
                <div>
                  <div style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', marginBottom: 8, display: 'flex', justifyContent: 'space-between' }}>
                    <span>予約中 ({schedules.length}件)</span>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                    {schedules.map(s => (
                      <div key={s.id} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 10px', background: '#f9fafb', borderRadius: 6, fontSize: 12 }}>
                        <span style={{ flex: 1 }}>{s.candidate_name}</span>
                        <span style={{ color: '#6b7280' }}>{new Date(s.scheduled_at).toLocaleString('ja-JP', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</span>
                        <button onClick={() => handleScheduleCancel(s.id)}
                          style={{ background: 'none', border: 'none', color: '#ef4444', cursor: 'pointer', fontSize: 11, padding: '1px 6px' }}>取消</button>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            <div style={{ padding: '14px 20px', borderTop: '1px solid #e5e7eb' }}>
              <button onClick={handleScheduleSubmit}
                disabled={scheduling || scheduleChecked.size === 0 || !scheduleAt}
                style={{ width: '100%', padding: '10px', background: scheduleChecked.size > 0 && scheduleAt ? '#6366f1' : '#e5e7eb', color: '#fff', border: 'none', borderRadius: 8, fontWeight: 700, fontSize: 14, cursor: scheduleChecked.size > 0 && scheduleAt ? 'pointer' : 'not-allowed' }}>
                {scheduling ? '予約中...' : `${scheduleChecked.size}名を予約送信`}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* 設定パネル */}
      {showSettings && (
        <div style={{ marginBottom: 20, border: '1px solid #e5e7eb', borderRadius: 10, padding: 20, background: '#fafafa' }}>
          <h3 style={{ margin: '0 0 16px', fontSize: '0.9rem', fontWeight: 700 }}>設定</h3>

          {/* 基本設定 */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#9ca3af', marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.05em' }}>基本設定</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
              {[
                ['template_spreadsheet_id', 'テンプレートスプレッドシートID', '1Jq-6I_276W-e6J91X544wY6d9kIi6JnyPh6I8UKR95k'],
                ['gas_endpoint_url', 'GAS Web App URL', 'https://script.google.com/macros/s/...'],
                ['notify_channel_id', 'HR通知チャンネルID', 'C01234ABCDE'],
                ['notify_mention_user_id', 'メンション先ユーザーID（複数はカンマ区切り）', 'U01234ABCDE,U09876ZYXWV'],
                ['webhook_secret', 'Webhookシークレット', '任意のランダム文字列'],
                ['total_score', '満点（配点）', '10'],
                ['import_sheet_url', '取り込み用スプレッドシートURL', 'https://docs.google.com/spreadsheets/d/...'],
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
                <div style={{ fontSize: 11, padding: '6px 10px', background: '#f3f4f6', borderRadius: 6, color: '#374151', wordBreak: 'break-all', fontFamily: 'monospace' }}>
                  {webhookUrl}
                </div>
              </div>
            </div>
          </div>

          {/* メール設定 */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#9ca3af', marginBottom: 8, textTransform: 'uppercase', letterSpacing: '0.05em' }}>メール設定</div>
            <div style={{ marginBottom: 10 }}>
              <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>
                送信元メールアドレス（人事メアド）
              </label>
              <input value={settingsForm.from_email || ''} onChange={e => setSettingsForm(f => ({ ...f, from_email: e.target.value }))}
                placeholder="hr@inrevo.jp"
                style={{ width: '100%', fontSize: 12, padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 6, outline: 'none', boxSizing: 'border-box' }} />
              <div style={{ fontSize: 11, color: '#9ca3af', marginTop: 3 }}>GmailApp で差出人として使用（Gmailのエイリアス登録が必要）</div>
            </div>
            <div style={{ marginBottom: 10 }}>
              <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>
                件名テンプレート
              </label>
              <input value={settingsForm.email_subject || ''} onChange={e => setSettingsForm(f => ({ ...f, email_subject: e.target.value }))}
                placeholder="【inrevo】実技テストのご案内"
                style={{ width: '100%', fontSize: 12, padding: '6px 10px', border: '1px solid #e5e7eb', borderRadius: 6, outline: 'none', boxSizing: 'border-box' }} />
            </div>
            <div>
              <label style={{ fontSize: 12, fontWeight: 600, color: '#6b7280', display: 'block', marginBottom: 4 }}>
                本文テンプレート
                <span style={{ fontSize: 10, color: '#9ca3af', fontWeight: 400, marginLeft: 8 }}>
                  使用可能な変数: {'{name}'} {'{url}'}
                </span>
              </label>
              <textarea value={settingsForm.email_body || ''} onChange={e => setSettingsForm(f => ({ ...f, email_body: e.target.value }))}
                rows={8} placeholder={`{name} 様\n\nこの度は選考にお進みいただきありがとうございます。\n実技テストのURLをお送りします。\n\n{url}\n\nよろしくお願いいたします。`}
                style={{ width: '100%', fontSize: 12, padding: '8px 10px', border: '1px solid #e5e7eb', borderRadius: 6, outline: 'none', boxSizing: 'border-box', resize: 'vertical', lineHeight: 1.7, fontFamily: 'inherit' }} />
            </div>
          </div>

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 4 }}>
            <button onClick={() => setShowSettings(false)} className="btn btn-secondary" style={{ fontSize: 12 }}>キャンセル</button>
            <button onClick={handleSaveSettings} disabled={savingSettings} className="btn btn-primary" style={{ fontSize: 12 }}>
              {savingSettings ? '保存中…' : '保存'}
            </button>
          </div>
        </div>
      )}


      {/* 候補者追加フォーム */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 16, alignItems: 'center', flexWrap: 'wrap' }}>
        <input value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
          placeholder="氏名"
          style={{ fontSize: 13, padding: '7px 12px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', width: 180 }} />
        <input value={form.email} onChange={e => setForm(f => ({ ...f, email: e.target.value }))}
          placeholder="メールアドレス" type="email"
          onKeyDown={e => e.key === 'Enter' && handleAdd()}
          style={{ fontSize: 13, padding: '7px 12px', border: '1px solid #e5e7eb', borderRadius: 8, outline: 'none', width: 240 }} />
        <button onClick={handleAdd} disabled={!form.name.trim() || !form.email.trim()}
          className="btn btn-secondary" style={{ fontSize: 13 }}>
          ＋ 1名追加
        </button>
        <button onClick={handleImportFromSheet} disabled={importing}
          style={{ fontSize: 12, padding: '7px 14px', border: '1px solid #93c5fd', borderRadius: 8, background: '#fff', cursor: importing ? 'default' : 'pointer', color: '#1d4ed8', fontWeight: 600, opacity: importing ? 0.6 : 1 }}>
          {importing ? '取り込み中…' : '📋 スプシから取り込む'}
        </button>
        <span style={{ fontSize: 12, color: '#9ca3af', marginLeft: 4 }}>{candidates.length}名登録中</span>
      </div>

      {/* 候補者テーブル */}
      {loading ? (
        <div style={{ color: '#9ca3af', padding: 24 }}>読み込み中…</div>
      ) : candidates.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '48px 0', color: '#9ca3af' }}>
          <div style={{ fontSize: '2rem', marginBottom: 8 }}>📋</div>
          <div>候補者を追加してください</div>
        </div>
      ) : (
        <div style={{ border: '1px solid #e5e7eb', borderRadius: 10, overflowX: 'auto' }}>
          <table style={{ width: '100%', minWidth: 640, borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: '#f8fafc' }}>
                {['氏名', 'メール', '回答', 'ステータス', 'スコア', 'タイピング', ''].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', whiteSpace: 'nowrap' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {candidates.map(c => (
                <tr key={c.id} style={{ borderBottom: '1px solid #f3f4f6' }}>
                  <td style={{ padding: '10px 14px', fontWeight: 600, color: '#111827', minWidth: 100 }}>{c.name}</td>
                  <td style={{ padding: '10px 14px', color: '#6b7280' }}>{c.email}</td>
                  <td style={{ padding: '10px 14px' }}>
                    {c.spreadsheet_url
                      ? <a href={c.spreadsheet_url} target="_blank" rel="noreferrer" style={{ color: '#2563eb', fontSize: 12 }}>開く →</a>
                      : <span style={{ color: '#d1d5db', fontSize: 12 }}>未送信</span>}
                  </td>
                  <td style={{ padding: '10px 14px' }}>
                    {sendingIds.has(c.id)
                      ? <span style={{ fontSize: 11, fontWeight: 700, color: '#d97706' }}>⏳ 送信中…</span>
                      : <StatusBadge status={c.status} />}
                    {c.error_message && (
                      <div style={{ fontSize: 10, color: '#dc2626', marginTop: 2, maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                        title={c.error_message}>{c.error_message}</div>
                    )}
                  </td>
                  <td style={{ padding: '10px 14px' }}>
                    {c.score != null ? (
                      <span style={{ fontWeight: 700, color: c.score >= totalScore * 0.8 ? '#059669' : c.score >= totalScore * 0.6 ? '#d97706' : '#dc2626' }}>
                        {c.score}点
                      </span>
                    ) : <span style={{ color: '#d1d5db' }}>—</span>}
                  </td>
                  <td style={{ padding: '10px 14px' }}>
                    {c.typing_level ? (
                      <span style={{ fontWeight: 700, fontSize: 13,
                        color: ['Professor','Comet','Ninja','Thunder','Fast','Good!'].includes(c.typing_level) ? '#7c3aed'
                             : ['S','A+','A','A-','B+','B'].includes(c.typing_level) ? '#2563eb'
                             : '#6b7280' }}>
                        {c.typing_level}
                      </span>
                    ) : <span style={{ color: '#d1d5db' }}>—</span>}
                  </td>
                  <td style={{ padding: '10px 14px', textAlign: 'right' }}>
                    <button onClick={() => handleDelete(c.id)}
                      style={{ fontSize: 11, color: '#9ca3af', background: 'none', border: '1px solid #e5e7eb', borderRadius: 5, padding: '3px 8px', cursor: 'pointer' }}>
                      削除
                    </button>
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
