import { useEffect, useState, useMemo, useCallback } from 'react';
import { api } from '../../api/client';
import {
  Search, AlertTriangle, ChevronDown, ChevronUp, Clock,
  FileText, User, Briefcase, Link as LinkIcon, Edit2,
  Sparkles, ExternalLink, Bot, CheckCircle, Settings, RefreshCw,
  Plus, Trash2, Calendar, ArrowLeft, ArrowRight, X, Database, Tag,
} from 'lucide-react';

// ── ユーティリティ ──────────────────────────────────────────
const PRIORITY_COLORS = {
  '高': { bg: '#fef2f2', color: '#dc2626', border: '#fca5a5' },
  '中': { bg: '#fffbeb', color: '#d97706', border: '#fde68a' },
  '低': { bg: '#f0fdf4', color: '#16a34a', border: '#86efac' },
};

const isClosed = (c) => {
  if (!c) return false;
  if (c.closed_date) return true;
  const r = `${c.status_phase || ''} ${c.final_result || ''} ${c.result || ''}`;
  return /close|完了|クローズ|法務移行|返金クローズ|対象外了承/i.test(r);
};
const isStale = (ts) => ts && (Date.now() - new Date(ts).getTime()) > 48 * 60 * 60 * 1000;
const fmtDate = (d) => d ? String(d).slice(0, 10) : '';
const today = () => new Date().toISOString().slice(0, 10);

const COL_DEFS = [
  { key: 'case_name',     label: '案件名（企業名）', w: 280 },
  { key: 'na_date',       label: 'NA日',            w: 100 },
  { key: 'priority',      label: '優先度',           w: 80 },
  { key: 'chief',         label: '担当',             w: 90 },
  { key: 'status_phase',  label: 'ステータス',       w: 130 },
  { key: 'ball',          label: 'ボール',           w: 110 },
  { key: 'issue_details', label: '問題箇所',         w: 260 },
  { key: 'updated_at',    label: '最終更新',         w: 100 },
  { key: 'final_result',  label: '結果',             w: 130 },
  { key: 'closed_date',   label: 'クローズ日',       w: 100 },
];

const RECORD_TYPES = ['メール受信','メール送信','社内MTG','社外MTG','電話','チャット','ドラフト作成','レビュー','完了'];

// ── 履歴/議事録リストフォーム ───────────────────────────────
function RecordListForm({ caseId, fieldName, title, IconCmp, records = [], onAdd, onDelete }) {
  const [sortDesc, setSortDesc] = useState(true);
  const [date, setDate] = useState(today());
  const [type, setType] = useState('');
  const [content, setContent] = useState('');
  const listId = `${caseId}-${fieldName}-types`;

  const sorted = useMemo(() =>
    [...records].sort((a, b) => sortDesc ? (a.date < b.date ? 1 : -1) : (a.date < b.date ? -1 : 1)),
    [records, sortDesc]
  );

  const submit = (e) => {
    e.preventDefault();
    if (!content.trim()) return;
    onAdd({ id: `r-${Date.now()}`, date, type, content });
    setContent(''); setType('');
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 480, background: '#fff', border: '1px solid #e5e7eb', borderRadius: 12, overflow: 'hidden', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
      <div style={{ padding: '10px 14px', borderBottom: '1px solid #f3f4f6', background: '#f9fafb', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h3 style={{ margin: 0, fontWeight: 700, fontSize: 14, color: '#1f2937', display: 'flex', alignItems: 'center', gap: 6 }}>
          <IconCmp size={16} color="#2563eb" /> {title}
          <span style={{ fontSize: 11, color: '#6b7280', fontWeight: 400, marginLeft: 4 }}>({records.length})</span>
        </h3>
        <button onClick={() => setSortDesc(d => !d)} style={{ background: 'none', border: 'none', color: '#6b7280', cursor: 'pointer', fontSize: 11, display: 'flex', alignItems: 'center', gap: 2 }}>
          {sortDesc ? <ChevronDown size={14}/> : <ChevronUp size={14}/>} 日付順
        </button>
      </div>
      <div style={{ flex: 1, overflowY: 'auto', padding: 12, display: 'flex', flexDirection: 'column', gap: 8, background: '#fafafa' }}>
        {sorted.length === 0 ? (
          <div style={{ textAlign: 'center', padding: 24, color: '#9ca3af', fontSize: 13 }}>記録はありません</div>
        ) : sorted.map(r => (
          <div key={r.id} style={{ background: '#fff', padding: '10px 12px', borderRadius: 8, border: '1px solid #e5e7eb', boxShadow: '0 1px 2px rgba(0,0,0,0.03)' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
              <span style={{ fontSize: 11, color: '#6b7280', display: 'flex', alignItems: 'center', gap: 3 }}>
                <Calendar size={11}/> {r.date}
              </span>
              {r.type && <span style={{ fontSize: 10, background: '#eff6ff', color: '#1d4ed8', border: '1px solid #bfdbfe', borderRadius: 4, padding: '1px 6px', fontWeight: 700 }}>{r.type}</span>}
              <button onClick={() => onDelete(r.id)} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: '#d1d5db', cursor: 'pointer', padding: 2 }} title="削除">
                <Trash2 size={13}/>
              </button>
            </div>
            <p style={{ margin: 0, fontSize: 13, color: '#1f2937', whiteSpace: 'pre-wrap', lineHeight: 1.5 }}>{r.content}</p>
          </div>
        ))}
      </div>
      <form onSubmit={submit} style={{ padding: 10, borderTop: '1px solid #e5e7eb', background: '#fff', display: 'flex', flexDirection: 'column', gap: 6 }}>
        <div style={{ display: 'flex', gap: 6 }}>
          <input type="date" value={date} onChange={e => setDate(e.target.value)} required
            style={{ fontSize: 12, padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 6, width: 130 }} />
          <input type="text" value={type} onChange={e => setType(e.target.value)} placeholder="種別（任意）" list={listId}
            style={{ fontSize: 12, padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 6, flex: 1 }} />
          <datalist id={listId}>
            {RECORD_TYPES.map(t => <option key={t} value={t} />)}
          </datalist>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          <textarea value={content} onChange={e => setContent(e.target.value)} placeholder="内容を入力..." required
            style={{ flex: 1, fontSize: 13, padding: '6px 8px', border: '1px solid #d1d5db', borderRadius: 6, resize: 'vertical', minHeight: 40, height: 40 }} />
          <button type="submit" style={{ background: '#2563eb', color: '#fff', border: 'none', borderRadius: 6, padding: '0 14px', fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
            <Plus size={16}/>
          </button>
        </div>
      </form>
    </div>
  );
}

// ── 案件詳細 ─────────────────────────────────────────────
function CaseDetail({ c, onChange, onDelete, onBack }) {
  const [local, setLocal] = useState(c);
  const [saving, setSaving] = useState(false);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiResult, setAiResult] = useState(null);

  useEffect(() => { setLocal(c); setAiResult(null); }, [c.id]);

  const persist = useCallback(async (patch) => {
    setSaving(true);
    try {
      await api.legalUpdate(c.id, patch);
      onChange({ ...c, ...patch, updated_at: new Date().toISOString() });
    } catch { setLocal(c); }
    finally { setSaving(false); }
  }, [c, onChange]);

  // 楽観的更新: ローカル即反映 + サーバー保存（onBlur で確定）
  const updateLocal = (field, value) => setLocal(l => ({ ...l, [field]: value }));
  const commit = (field, value) => persist({ [field]: value });

  const addRecord = async (field, record) => {
    const next = [...(local[field] || []), record];
    setLocal(l => ({ ...l, [field]: next }));
    await persist({ [field]: next });
  };
  const delRecord = async (field, id) => {
    if (!window.confirm('削除しますか？')) return;
    const next = (local[field] || []).filter(r => r.id !== id);
    setLocal(l => ({ ...l, [field]: next }));
    await persist({ [field]: next });
  };

  const generateAi = async () => {
    setAiLoading(true); setAiResult(null);
    try {
      const r = await api.legalAi(c.id);
      setAiResult(r);
    } catch (e) {
      setAiResult({ summary: 'エラー: ' + (e.message || '生成失敗'), next_action: '時間をおいて再度お試しください。' });
    } finally {
      setAiLoading(false);
    }
  };

  const pri = PRIORITY_COLORS[local.priority] || PRIORITY_COLORS['中'];
  const metaBox = { background: '#f9fafb', padding: '10px 12px', borderRadius: 8, border: '1px solid #f3f4f6' };
  const metaLabel = { fontSize: 11, fontWeight: 700, color: '#6b7280', display: 'flex', alignItems: 'center', gap: 4, marginBottom: 4 };
  const baseInput = { width: '100%', padding: '8px 10px', border: '1px solid #d1d5db', borderRadius: 8, fontSize: 13, background: '#fff', boxShadow: '0 1px 2px rgba(0,0,0,0.02)' };
  const ta = (field, h = 80) => (
    <textarea value={local[field] || ''} onChange={e => updateLocal(field, e.target.value)} onBlur={e => commit(field, e.target.value)}
      style={{ ...baseInput, height: h, resize: 'vertical', fontFamily: 'inherit' }} />
  );
  const inp = (field, opts = {}) => (
    <input {...opts} value={local[field] || ''} onChange={e => updateLocal(field, e.target.value)} onBlur={e => commit(field, e.target.value)}
      style={{ ...baseInput, ...opts.style }} />
  );

  return (
    <div style={{ display: 'flex', flex: 1, overflow: 'hidden', background: '#f9fafb' }}>
      {/* 左: フォーム */}
      <div style={{ flex: '3 1 0', overflowY: 'auto', padding: '20px 28px', background: '#fff', borderRight: '1px solid #e5e7eb' }}>
        {/* ヘッダー */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20, paddingBottom: 16, borderBottom: '1px solid #f3f4f6' }}>
          <button onClick={onBack} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#6b7280', padding: 6, borderRadius: 999, display: 'flex' }}>
            <ArrowLeft size={20}/>
          </button>
          <FileText size={24} color="#2563eb"/>
          <input value={local.case_name || ''} onChange={e => updateLocal('case_name', e.target.value)} onBlur={e => commit('case_name', e.target.value)}
            placeholder="案件名" style={{ flex: 1, fontSize: 22, fontWeight: 800, border: 'none', background: 'transparent', color: '#111827', padding: 4 }} />
          {saving && <span style={{ fontSize: 11, color: '#9ca3af' }}>保存中…</span>}
          <button onClick={() => { if (window.confirm('完全に削除しますか？取り消せません。')) onDelete(c.id); }}
            style={{ color: '#dc2626', border: '1px solid #fca5a5', borderRadius: 6, padding: '6px 14px', background: '#fff', fontWeight: 700, fontSize: 12, cursor: 'pointer' }}>
            削除
          </button>
        </div>

        {/* メタ4列 */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 20 }}>
          <div style={metaBox}>
            <label style={metaLabel}><Calendar size={12}/> NA日</label>
            <input type="date" value={fmtDate(local.na_date)} onChange={e => commit('na_date', e.target.value)}
              style={{ width: '100%', border: 'none', background: 'transparent', fontSize: 13, fontWeight: 700, padding: 0 }} />
          </div>
          <div style={metaBox}>
            <label style={metaLabel}><AlertTriangle size={12}/> 優先度</label>
            <select value={local.priority || '中'} onChange={e => commit('priority', e.target.value)}
              style={{ width: '100%', border: 'none', background: 'transparent', fontSize: 13, fontWeight: 700, padding: 0, color: pri.color, cursor: 'pointer' }}>
              {['高','中','低'].map(p => <option key={p}>{p}</option>)}
            </select>
          </div>
          <div style={metaBox}>
            <label style={metaLabel}><User size={12}/> 担当</label>
            <input value={local.chief || ''} onChange={e => updateLocal('chief', e.target.value)} onBlur={e => commit('chief', e.target.value)}
              style={{ width: '100%', border: 'none', background: 'transparent', fontSize: 13, fontWeight: 700, padding: 0 }} />
          </div>
          <div style={metaBox}>
            <label style={metaLabel}><Briefcase size={12}/> ボール</label>
            <input value={local.ball || ''} onChange={e => updateLocal('ball', e.target.value)} onBlur={e => commit('ball', e.target.value)}
              style={{ width: '100%', border: 'none', background: 'transparent', fontSize: 13, fontWeight: 700, padding: 0 }} />
          </div>
        </div>

        {/* 各種フィールド */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <Tag size={14} color="#2563eb"/> ステータス（フェーズ）
              </label>
              {inp('status_phase', { placeholder: '見解送付済み / MTGセット / 法務移行 …' })}
            </div>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'block' }}>NA処理台帳番号</label>
              {inp('na_ledger', { placeholder: 'NA-2606-01' })}
            </div>
          </div>

          <div>
            <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
              <Edit2 size={14} color="#2563eb"/> 現状（フリーメモ）
            </label>
            {ta('current_state', 100)}
          </div>

          <div>
            <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
              <Edit2 size={14} color="#2563eb"/> 問題箇所（1行サマリ）
            </label>
            {inp('issue_summary', { placeholder: '問題の核心を1行で' })}
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <FileText size={14} color="#d97706"/> 契約書該当箇所
              </label>
              {ta('contract_details', 120)}
            </div>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <AlertTriangle size={14} color="#dc2626"/> 問題該当箇所詳細
              </label>
              {ta('issue_details', 120)}
            </div>
          </div>

          <div>
            <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'block' }}>方向性・対応方針</label>
            {ta('direction', 80)}
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <LinkIcon size={14} color="#9ca3af"/> 契約書URL
              </label>
              <div style={{ display: 'flex', gap: 6 }}>
                {inp('contract_url', { type: 'url', placeholder: 'https://drive.google.com/...', style: { flex: 1 } })}
                {local.contract_url && <a href={local.contract_url} target="_blank" rel="noopener noreferrer"
                  style={{ padding: '0 12px', background: '#f3f4f6', border: '1px solid #e5e7eb', borderRadius: 8, display: 'flex', alignItems: 'center', color: '#6b7280' }}>
                  <ExternalLink size={14}/>
                </a>}
              </div>
            </div>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <LinkIcon size={14} color="#9ca3af"/> 処理台帳URL
              </label>
              <div style={{ display: 'flex', gap: 6 }}>
                {inp('ledger_url', { type: 'url', placeholder: 'https://drive.google.com/...', style: { flex: 1 } })}
                {local.ledger_url && local.ledger_url.startsWith('http') && <a href={local.ledger_url} target="_blank" rel="noopener noreferrer"
                  style={{ padding: '0 12px', background: '#f3f4f6', border: '1px solid #e5e7eb', borderRadius: 8, display: 'flex', alignItems: 'center', color: '#6b7280' }}>
                  <ExternalLink size={14}/>
                </a>}
              </div>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <LinkIcon size={14} color="#9ca3af"/> スレッドURL
              </label>
              <div style={{ display: 'flex', gap: 6 }}>
                {inp('thread_url', { type: 'url', placeholder: 'https://', style: { flex: 1 } })}
                {local.thread_url && <a href={local.thread_url} target="_blank" rel="noopener noreferrer"
                  style={{ padding: '0 12px', background: '#f3f4f6', border: '1px solid #e5e7eb', borderRadius: 8, display: 'flex', alignItems: 'center', color: '#6b7280' }}>
                  <ExternalLink size={14}/>
                </a>}
              </div>
            </div>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <LinkIcon size={14} color="#9ca3af"/> メール履歴URL
              </label>
              <div style={{ display: 'flex', gap: 6 }}>
                {inp('email_slack_url', { type: 'url', placeholder: 'Slack/Doc URL', style: { flex: 1 } })}
                {local.email_slack_url && <a href={local.email_slack_url} target="_blank" rel="noopener noreferrer"
                  style={{ padding: '0 12px', background: '#f3f4f6', border: '1px solid #e5e7eb', borderRadius: 8, display: 'flex', alignItems: 'center', color: '#6b7280' }}>
                  <ExternalLink size={14}/>
                </a>}
              </div>
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <LinkIcon size={14} color="#9ca3af"/> 議事録URL
              </label>
              <div style={{ display: 'flex', gap: 6 }}>
                {inp('minutes_url', { type: 'url', placeholder: 'Google Docs URL', style: { flex: 1 } })}
                {local.minutes_url && <a href={local.minutes_url} target="_blank" rel="noopener noreferrer"
                  style={{ padding: '0 12px', background: '#f3f4f6', border: '1px solid #e5e7eb', borderRadius: 8, display: 'flex', alignItems: 'center', color: '#6b7280' }}>
                  <ExternalLink size={14}/>
                </a>}
              </div>
            </div>
            <div>
              <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
                <Calendar size={14} color="#9ca3af"/> クローズ／法務移行日
              </label>
              {inp('closed_date', { type: 'date' })}
            </div>
          </div>

          <div>
            <label style={{ fontSize: 13, fontWeight: 700, color: '#374151', marginBottom: 6, display: 'flex', alignItems: 'center', gap: 4 }}>
              <CheckCircle size={14} color="#1e40af"/> 結果（最終結論）
            </label>
            <input value={local.final_result || ''} onChange={e => updateLocal('final_result', e.target.value)} onBlur={e => commit('final_result', e.target.value)}
              style={{ ...baseInput, background: '#eff6ff', color: '#1e40af', fontWeight: 700 }} />
          </div>
        </div>

        {/* AI セクション */}
        <div style={{ marginTop: 28, background: 'linear-gradient(135deg, #eef2ff, #faf5ff)', padding: 18, borderRadius: 14, border: '1px solid #c7d2fe' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <h3 style={{ margin: 0, fontWeight: 800, color: '#312e81', display: 'flex', alignItems: 'center', gap: 6, fontSize: 14 }}>
              <Sparkles size={18} color="#6366f1"/> AI アシスタント（Claude Haiku 4.5）
            </h3>
            <button onClick={generateAi} disabled={aiLoading}
              style={{ background: aiLoading ? '#a5b4fc' : '#4f46e5', color: '#fff', border: 'none', borderRadius: 8, padding: '8px 14px', fontWeight: 700, fontSize: 13, cursor: aiLoading ? 'not-allowed' : 'pointer', display: 'flex', alignItems: 'center', gap: 6 }}>
              {aiLoading ? <RefreshCw size={14} className="spin"/> : <Bot size={14}/>}
              {aiLoading ? '推論中…' : '現状を要約・次を提案'}
            </button>
          </div>
          {aiResult && (
            <div style={{ background: '#fff', padding: 14, borderRadius: 10, border: '1px solid #e0e7ff', display: 'flex', flexDirection: 'column', gap: 12 }}>
              <div>
                <span style={{ fontSize: 10, fontWeight: 700, color: '#4f46e5', textTransform: 'uppercase', letterSpacing: 1 }}>これまでの経緯要約</span>
                <p style={{ margin: '4px 0 0', fontSize: 13, color: '#1f2937', whiteSpace: 'pre-wrap', lineHeight: 1.6 }}>{aiResult.summary}</p>
              </div>
              <div style={{ borderTop: '1px solid #e0e7ff', paddingTop: 10 }}>
                <span style={{ fontSize: 10, fontWeight: 700, color: '#a855f7', textTransform: 'uppercase', letterSpacing: 1 }}>推奨される次のアクション</span>
                <p style={{ margin: '4px 0 0', fontSize: 13, fontWeight: 700, color: '#111827', whiteSpace: 'pre-wrap', lineHeight: 1.6 }}>{aiResult.next_action}</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* 右: 履歴・議事録 */}
      <div style={{ flex: '2 1 0', overflowY: 'auto', padding: 16, display: 'flex', flexDirection: 'column', gap: 16, background: '#f3f4f6' }}>
        <RecordListForm caseId={c.id} fieldName="history" title="対応履歴" IconCmp={Clock}
          records={local.history || []} onAdd={r => addRecord('history', r)} onDelete={id => delRecord('history', id)} />
        <RecordListForm caseId={c.id} fieldName="minutes" title="議事録履歴" IconCmp={FileText}
          records={local.minutes || []} onAdd={r => addRecord('minutes', r)} onDelete={id => delRecord('minutes', id)} />
      </div>
    </div>
  );
}

// ── メイン ─────────────────────────────────────────────
export default function LegalHub() {
  const [cases, setCases] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [tab, setTab] = useState('active'); // active | all | sheet
  const [query, setQuery] = useState('');
  const [staleOnly, setStaleOnly] = useState(false);
  const [sortKey, setSortKey] = useState('updated_at');
  const [sortDir, setSortDir] = useState('desc');
  const [showColumnSettings, setShowColumnSettings] = useState(false);

  // 列カスタマイズ（localStorage 永続化）
  const [columns, setColumns] = useState(() => {
    try {
      const saved = localStorage.getItem('legal_hub_columns');
      if (saved) {
        const parsed = JSON.parse(saved);
        // 新しいカラム定義とマージ（未保存のものを追加）
        const savedKeys = new Set(parsed.map(c => c.key));
        const merged = parsed.filter(c => COL_DEFS.find(d => d.key === c.key));
        for (const def of COL_DEFS) {
          if (!savedKeys.has(def.key)) merged.push({ ...def, visible: true });
          else Object.assign(merged.find(c => c.key === def.key), { label: def.label, w: def.w });
        }
        return merged;
      }
    } catch {}
    return COL_DEFS.map(c => ({ ...c, visible: true }));
  });

  const saveColumns = (next) => {
    setColumns(next);
    localStorage.setItem('legal_hub_columns', JSON.stringify(next));
  };
  const toggleColumn = (key) => saveColumns(columns.map(c => c.key === key ? { ...c, visible: !c.visible } : c));
  const moveColumn = (idx, dir) => {
    if (dir === 'left' && idx === 0) return;
    if (dir === 'right' && idx === columns.length - 1) return;
    const next = [...columns];
    const swap = dir === 'left' ? idx - 1 : idx + 1;
    [next[idx], next[swap]] = [next[swap], next[idx]];
    saveColumns(next);
  };

  const load = async () => {
    setLoading(true);
    try {
      const r = await api.legalCases();
      setCases(r.cases || []);
    } catch { setCases([]); }
    finally { setLoading(false); }
  };
  useEffect(() => { load(); }, []);

  const handleCreate = async () => {
    try {
      const r = await api.legalCreate();
      setCases(prev => [r.case, ...prev]);
      setSelected(r.case);
      setTab('detail');
    } catch { alert('作成に失敗しました'); }
  };
  const handleDelete = async (id) => {
    await api.legalDelete(id).catch(() => {});
    setCases(prev => prev.filter(c => c.id !== id));
    setSelected(null);
    setTab(isClosed(selected) ? 'all' : 'active');
  };
  const handleChange = (u) => {
    setCases(prev => prev.map(c => c.id === u.id ? { ...c, ...u } : c));
    setSelected(prev => prev?.id === u.id ? { ...prev, ...u } : prev);
  };

  const staleCount = cases.filter(c => !isClosed(c) && isStale(c.updated_at)).length;

  const displayed = useMemo(() => {
    let list = [...cases];
    if (tab === 'active') list = list.filter(c => !isClosed(c));
    if (staleOnly) list = list.filter(c => !isClosed(c) && isStale(c.updated_at));
    if (query) {
      const q = query.toLowerCase();
      list = list.filter(c => {
        const hay = `${c.case_name || ''} ${c.chief || ''} ${c.issue_summary || ''} ${c.issue_details || ''} ${c.status_phase || ''} ${c.ball || ''} ${c.final_result || ''}`.toLowerCase();
        return hay.includes(q);
      });
    }
    list.sort((a, b) => {
      const va = a[sortKey] || '', vb = b[sortKey] || '';
      return sortDir === 'asc' ? (va < vb ? -1 : 1) : (va < vb ? 1 : -1);
    });
    return list;
  }, [cases, tab, staleOnly, query, sortKey, sortDir]);

  const visibleCols = columns.filter(c => c.visible);
  const requestSort = (k) => {
    if (sortKey === k) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortKey(k); setSortDir('desc'); }
  };

  const copyJson = async () => {
    const json = JSON.stringify(cases, null, 2);
    try {
      await navigator.clipboard.writeText(json);
      alert(`${cases.length}件をJSON形式でクリップボードにコピーしました`);
    } catch {
      const ta = document.createElement('textarea');
      ta.value = json; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove();
      alert('クリップボードにコピーしました');
    }
  };

  if (loading) return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 60, color: '#6b7280' }}>
      <RefreshCw className="spin" size={28} color="#2563eb"/>
    </div>
  );

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: '#f9fafb' }}>
      {/* ヘッダー */}
      <header style={{ background: '#fff', borderBottom: '1px solid #e5e7eb', padding: '14px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ background: '#2563eb', padding: 8, borderRadius: 10, display: 'flex' }}>
            <Bot size={22} color="#fff"/>
          </div>
          <div>
            <h1 style={{ margin: 0, fontSize: 18, fontWeight: 800, color: '#111827', letterSpacing: '-0.02em' }}>Legal Assist Hub</h1>
            <p style={{ margin: 0, fontSize: 10, color: '#6b7280', fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1 }}>法務部 案件進捗管理</p>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {staleCount > 0 && tab !== 'detail' && tab !== 'sheet' && (
            <button onClick={() => setStaleOnly(s => !s)}
              className={staleOnly ? '' : 'pulse'}
              style={{ display: 'flex', alignItems: 'center', gap: 4, padding: '6px 14px', borderRadius: 99, fontSize: 12, fontWeight: 700, border: '1px solid',
                background: staleOnly ? '#f59e0b' : '#fffbeb',
                color: staleOnly ? '#fff' : '#92400e',
                borderColor: staleOnly ? '#d97706' : '#fde68a',
                cursor: 'pointer', boxShadow: staleOnly ? '0 3px 6px rgba(245,158,11,0.3)' : 'none' }}>
              <AlertTriangle size={14}/>
              {staleOnly ? '未更新のみ' : `2日以上未更新: ${staleCount}件`}
              {staleOnly && <X size={12}/>}
            </button>
          )}

          <nav style={{ display: 'flex', background: '#f3f4f6', padding: 3, borderRadius: 10, gap: 2 }}>
            {[
              ['active', '対応中'],
              ['all', '全件'],
              ['sheet', 'データ書出'],
            ].map(([v, l]) => (
              <button key={v} onClick={() => { setTab(v); setSelected(null); setStaleOnly(false); }}
                style={{ padding: '6px 14px', borderRadius: 7, border: 'none', fontSize: 12, fontWeight: 700, cursor: 'pointer',
                  background: (tab === v || (tab === 'detail' && v === (isClosed(selected) ? 'all' : 'active'))) ? '#fff' : 'transparent',
                  color: (tab === v || (tab === 'detail' && v === (isClosed(selected) ? 'all' : 'active'))) ? '#1d4ed8' : '#6b7280',
                  boxShadow: tab === v ? '0 1px 2px rgba(0,0,0,0.06)' : 'none' }}>
                {l}
              </button>
            ))}
          </nav>
        </div>
      </header>

      {/* メイン */}
      {tab === 'detail' && selected ? (
        <CaseDetail c={selected} onChange={handleChange} onDelete={handleDelete}
          onBack={() => { setSelected(null); setTab(isClosed(selected) ? 'all' : 'active'); }} />
      ) : tab === 'sheet' ? (
        <div style={{ flex: 1, padding: 32, overflowY: 'auto' }}>
          <div style={{ maxWidth: 800, margin: '0 auto' }}>
            <h2 style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 22, fontWeight: 800, color: '#111827', borderBottom: '1px solid #e5e7eb', paddingBottom: 14 }}>
              <Database size={22} color="#2563eb"/> データエクスポート
            </h2>
            <div style={{ background: '#f0f9ff', padding: 20, borderRadius: 12, border: '1px solid #bae6fd', marginTop: 20 }}>
              <h3 style={{ margin: '0 0 10px', fontWeight: 700, color: '#075985' }}>JSON エクスポート</h3>
              <p style={{ margin: '0 0 16px', fontSize: 13, color: '#0c4a6e' }}>
                現在保存されている全{cases.length}件の案件データを JSON 形式でクリップボードにコピーします。バックアップやスプシ移行にどうぞ。
              </p>
              <button onClick={copyJson}
                style={{ background: '#0284c7', color: '#fff', border: 'none', borderRadius: 8, padding: '10px 18px', fontWeight: 700, cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: 13 }}>
                <FileText size={16}/> JSONをコピー
              </button>
            </div>
          </div>
        </div>
      ) : (
        /* リスト */
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', padding: 20, gap: 14 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
            <div style={{ position: 'relative', flex: 1, maxWidth: 420 }}>
              <Search size={16} color="#9ca3af" style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)' }}/>
              <input value={query} onChange={e => setQuery(e.target.value)}
                placeholder="案件名・担当・問題箇所・ステータスで検索…"
                style={{ width: '100%', padding: '9px 14px 9px 38px', border: '1px solid #d1d5db', borderRadius: 10, fontSize: 13, background: '#fff' }} />
            </div>
            <div style={{ display: 'flex', gap: 8, position: 'relative' }}>
              <button onClick={() => setShowColumnSettings(s => !s)}
                style={{ background: '#fff', border: '1px solid #d1d5db', color: '#374151', padding: '8px 14px', borderRadius: 10, fontSize: 12, fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6, boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                <Settings size={14}/> 表示列設定
              </button>
              {showColumnSettings && (
                <div style={{ position: 'absolute', right: 90, top: '110%', width: 280, background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, boxShadow: '0 10px 25px rgba(0,0,0,0.10)', zIndex: 30, padding: 8 }}>
                  <div style={{ fontSize: 11, fontWeight: 700, color: '#6b7280', padding: '4px 8px', borderBottom: '1px solid #f3f4f6', marginBottom: 4 }}>列の表示／順序</div>
                  <div style={{ maxHeight: 320, overflowY: 'auto' }}>
                    {columns.map((col, idx) => (
                      <div key={col.key} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 8px', borderRadius: 6 }}
                        onMouseEnter={e => e.currentTarget.style.background = '#f9fafb'}
                        onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                        <label style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 1, fontSize: 13, cursor: 'pointer', color: '#374151' }}>
                          <input type="checkbox" checked={col.visible} onChange={() => toggleColumn(col.key)} />
                          {col.label}
                        </label>
                        <div style={{ display: 'flex', gap: 2 }}>
                          <button onClick={() => moveColumn(idx, 'left')} disabled={idx === 0}
                            style={{ background: 'none', border: 'none', cursor: idx === 0 ? 'not-allowed' : 'pointer', color: idx === 0 ? '#d1d5db' : '#6b7280', padding: 2 }}>
                            <ArrowLeft size={12}/>
                          </button>
                          <button onClick={() => moveColumn(idx, 'right')} disabled={idx === columns.length - 1}
                            style={{ background: 'none', border: 'none', cursor: idx === columns.length - 1 ? 'not-allowed' : 'pointer', color: idx === columns.length - 1 ? '#d1d5db' : '#6b7280', padding: 2 }}>
                            <ArrowRight size={12}/>
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              <button onClick={handleCreate}
                style={{ background: '#2563eb', color: '#fff', border: 'none', padding: '8px 16px', borderRadius: 10, fontSize: 12, fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6, boxShadow: '0 1px 2px rgba(37,99,235,0.3)' }}>
                <Plus size={14}/> 新規案件
              </button>
            </div>
          </div>

          <div style={{ background: '#fff', borderRadius: 12, border: '1px solid #e5e7eb', boxShadow: '0 1px 2px rgba(0,0,0,0.04)', flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
            <div style={{ overflow: 'auto', flex: 1 }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: '100%' }}>
                <thead style={{ background: '#f9fafb', position: 'sticky', top: 0, zIndex: 5, boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                  <tr>
                    {visibleCols.map(col => (
                      <th key={col.key} onClick={() => requestSort(col.key)}
                        style={{ padding: '12px 14px', fontSize: 11, fontWeight: 700, color: '#4b5563', textTransform: 'uppercase', textAlign: 'left', borderBottom: '1px solid #e5e7eb', cursor: 'pointer', width: col.w, whiteSpace: 'nowrap' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                          {col.label}
                          {sortKey === col.key && (sortDir === 'asc' ? <ChevronUp size={12} color="#2563eb"/> : <ChevronDown size={12} color="#2563eb"/>)}
                        </div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {displayed.map(item => {
                    const stale = !isClosed(item) && isStale(item.updated_at);
                    return (
                      <tr key={item.id} onClick={() => { setSelected(item); setTab('detail'); }}
                        style={{ borderBottom: '1px solid #f3f4f6', cursor: 'pointer', background: stale ? '#fffbeb' : '#fff' }}
                        onMouseEnter={e => e.currentTarget.style.background = stale ? '#fef3c7' : '#eff6ff'}
                        onMouseLeave={e => e.currentTarget.style.background = stale ? '#fffbeb' : '#fff'}>
                        {visibleCols.map(col => {
                          let val = item[col.key];
                          if (col.key === 'na_date' || col.key === 'closed_date' || col.key === 'updated_at') val = fmtDate(val);
                          if (col.key === 'priority') {
                            const p = PRIORITY_COLORS[val] || PRIORITY_COLORS['中'];
                            return <td key={col.key} style={{ padding: '12px 14px' }}>
                              <span style={{ padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 700, border: `1px solid ${p.border}`, background: p.bg, color: p.color }}>{val}</span>
                            </td>;
                          }
                          if (col.key === 'final_result' || col.key === 'status_phase') {
                            const closed = isClosed(item);
                            return <td key={col.key} style={{ padding: '12px 14px' }}>
                              <span style={{ padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 700, background: closed ? '#f3f4f6' : '#dbeafe', color: closed ? '#4b5563' : '#1e40af' }}>
                                {val || '-'}
                              </span>
                            </td>;
                          }
                          if (col.key === 'case_name') {
                            return <td key={col.key} style={{ padding: '12px 14px', fontWeight: 700, color: '#111827', display: 'flex', alignItems: 'center', gap: 6 }}>
                              {stale && <span title="2日以上未更新"><AlertTriangle size={13} color="#f59e0b"/></span>}
                              {val}
                            </td>;
                          }
                          return <td key={col.key} style={{ padding: '12px 14px', fontSize: 13, color: '#374151', maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={val}>{val}</td>;
                        })}
                      </tr>
                    );
                  })}
                  {displayed.length === 0 && (
                    <tr><td colSpan={visibleCols.length} style={{ padding: 40, textAlign: 'center', color: '#9ca3af', fontSize: 13 }}>
                      該当する案件がありません
                    </td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div style={{ fontSize: 11, color: '#6b7280', textAlign: 'right' }}>全{cases.length}件 / 表示{displayed.length}件</div>
        </div>
      )}

      {/* スピンアニメーション */}
      <style>{`
        .spin { animation: spin 1s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .pulse { animation: pulse 2s ease-in-out infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.7; } }
      `}</style>
    </div>
  );
}
