import { useEffect, useState, useMemo, useCallback } from 'react';
import { api } from '../../api/client';

const PRIORITY_COLOR = { 高: { bg: '#fef2f2', color: '#dc2626', border: '#fca5a5' }, 中: { bg: '#fffbeb', color: '#d97706', border: '#fde68a' }, 低: { bg: '#f0fdf4', color: '#16a34a', border: '#86efac' } };
const isClosed = (c) => {
  if (!c) return false;
  if (c.closed_date) return true;
  const r = `${c.status_phase || ''} ${c.final_result || ''} ${c.result || ''}`;
  return /close|完了|クローズ|法務移行|返金クローズ|対象外了承/i.test(r);
};
const isStale  = (ts) => ts && (Date.now() - new Date(ts).getTime()) > 48 * 60 * 60 * 1000;
const fmtDate  = (d) => d ? String(d).slice(0, 10) : '';
const today    = () => new Date().toISOString().slice(0, 10);

// ── 対応履歴/議事録フォーム ────────────────────────────────
function RecordList({ records = [], onAdd, onDelete, title }) {
  const [date, setDate] = useState(today());
  const [type, setType] = useState('');
  const [content, setContent] = useState('');
  const [desc, setDesc] = useState(true);

  const sorted = useMemo(() =>
    [...records].sort((a, b) => desc ? (a.date < b.date ? 1 : -1) : (a.date < b.date ? -1 : 1)),
    [records, desc]
  );

  const submit = (e) => {
    e.preventDefault();
    if (!content.trim()) return;
    onAdd({ id: `r-${Date.now()}`, date, type, content });
    setContent(''); setType('');
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 420, background: 'var(--surface)', border: '1px solid var(--gray-200)', borderRadius: 10, overflow: 'hidden' }}>
      <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--gray-100)', background: 'var(--surface-2)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontWeight: 700, fontSize: '0.85rem', color: 'var(--gray-700)' }}>{title} <span style={{ fontSize: '0.72rem', color: 'var(--gray-400)', fontWeight: 400 }}>({records.length})</span></span>
        <button onClick={() => setDesc(d => !d)} style={{ fontSize: '0.72rem', color: 'var(--gray-500)', background: 'none', border: 'none', cursor: 'pointer' }}>
          {desc ? '▼ 新しい順' : '▲ 古い順'}
        </button>
      </div>
      <div style={{ flex: 1, overflowY: 'auto', padding: 12, display: 'flex', flexDirection: 'column', gap: 8 }}>
        {sorted.length === 0 && <div style={{ textAlign: 'center', padding: '24px 0', color: 'var(--gray-400)', fontSize: '0.82rem' }}>記録はありません</div>}
        {sorted.map(r => (
          <div key={r.id} style={{ background: 'var(--surface-2)', borderRadius: 8, padding: '8px 10px', border: '1px solid var(--gray-200)', position: 'relative' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <span style={{ fontSize: '0.72rem', color: 'var(--gray-500)' }}>📅 {r.date}</span>
              {r.type && <span style={{ fontSize: '0.68rem', background: '#eff6ff', color: '#1d4ed8', border: '1px solid #bfdbfe', borderRadius: 4, padding: '0 6px', fontWeight: 700 }}>{r.type}</span>}
              <button onClick={() => onDelete(r.id)} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: 'var(--gray-300)', cursor: 'pointer', fontSize: 12, padding: 2 }} title="削除">×</button>
            </div>
            <p style={{ fontSize: '0.82rem', color: 'var(--gray-700)', margin: 0, whiteSpace: 'pre-wrap', lineHeight: 1.5 }}>{r.content}</p>
          </div>
        ))}
      </div>
      <div style={{ padding: '10px 12px', borderTop: '1px solid var(--gray-200)', background: 'var(--surface)' }}>
        <form onSubmit={submit} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          <div style={{ display: 'flex', gap: 6 }}>
            <input type="date" value={date} onChange={e => setDate(e.target.value)} required
              style={{ fontSize: '0.75rem', padding: '4px 8px', border: '1px solid var(--gray-300)', borderRadius: 6, width: 120 }} />
            <input type="text" value={type} onChange={e => setType(e.target.value)} placeholder="種別" list="record-types"
              style={{ fontSize: '0.75rem', padding: '4px 8px', border: '1px solid var(--gray-300)', borderRadius: 6, flex: 1 }} />
            <datalist id="record-types">
              {['メール受信','メール送信','社内MTG','社外MTG','電話','チャット','ドラフト作成','レビュー','完了'].map(t => <option key={t} value={t} />)}
            </datalist>
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            <textarea value={content} onChange={e => setContent(e.target.value)} placeholder="内容..." required
              style={{ flex: 1, fontSize: '0.82rem', padding: '6px 8px', border: '1px solid var(--gray-300)', borderRadius: 6, resize: 'none', height: 36, minHeight: 36 }} />
            <button type="submit" style={{ background: '#2563eb', color: '#fff', border: 'none', borderRadius: 6, padding: '0 12px', fontWeight: 700, cursor: 'pointer', fontSize: 16 }}>＋</button>
          </div>
        </form>
      </div>
    </div>
  );
}

// ── 案件詳細パネル ──────────────────────────────────────────
function CaseDetail({ c, onChange, onDelete, onBack }) {
  const [saving, setSaving] = useState(false);
  const [local, setLocal] = useState(c);

  useEffect(() => { setLocal(c); }, [c.id]);

  const save = useCallback(async (patch) => {
    const updated = { ...local, ...patch };
    setLocal(updated);
    setSaving(true);
    try { await api.legalUpdate(c.id, patch); }
    catch { setLocal(local); }
    finally { setSaving(false); }
    onChange({ ...c, ...patch, updated_at: new Date().toISOString() });
  }, [local, c]);

  const addRecord = async (field, record) => {
    const list = [...(local[field] || []), record];
    await save({ [field]: list });
  };
  const delRecord = async (field, id) => {
    if (!window.confirm('削除しますか？')) return;
    const list = (local[field] || []).filter(r => r.id !== id);
    await save({ [field]: list });
  };

  const F = ({ label, children }) => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <label style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--gray-500)' }}>{label}</label>
      {children}
    </div>
  );
  const inp = (field, opts = {}) => (
    <input {...opts} value={local[field] || ''} onChange={e => setLocal(l => ({ ...l, [field]: e.target.value }))}
      onBlur={e => save({ [field]: e.target.value })}
      style={{ padding: '6px 8px', border: '1px solid var(--gray-200)', borderRadius: 6, fontSize: '0.85rem', background: 'var(--surface)', color: 'var(--gray-800)', ...opts.style }} />
  );
  const ta = (field, rows = 3) => (
    <textarea value={local[field] || ''} onChange={e => setLocal(l => ({ ...l, [field]: e.target.value }))}
      onBlur={e => save({ [field]: e.target.value })} rows={rows}
      style={{ padding: '6px 8px', border: '1px solid var(--gray-200)', borderRadius: 6, fontSize: '0.85rem', resize: 'vertical', background: 'var(--surface)', color: 'var(--gray-800)' }} />
  );

  const pri = PRIORITY_COLOR[local.priority] || PRIORITY_COLOR['中'];

  return (
    <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
      {/* 左: 詳細フォーム */}
      <div style={{ flex: '3 1 0', overflowY: 'auto', padding: '20px 24px', borderRight: '1px solid var(--gray-200)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 20, paddingBottom: 16, borderBottom: '1px solid var(--gray-100)' }}>
          <button onClick={onBack} style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: '1.1rem', color: 'var(--gray-500)', padding: 4 }}>←</button>
          <input value={local.case_name || ''} onChange={e => setLocal(l => ({ ...l, case_name: e.target.value }))}
            onBlur={e => save({ case_name: e.target.value })}
            style={{ flex: 1, fontSize: '1.15rem', fontWeight: 700, border: 'none', borderBottom: '2px solid transparent', background: 'transparent', color: 'var(--gray-900)', padding: '2px 0' }}
            onFocus={e => e.target.style.borderBottomColor = '#2563eb'}
          />
          <button onClick={() => { if (window.confirm('削除しますか？この操作は取り消せません。')) onDelete(c.id); }}
            style={{ fontSize: '0.78rem', color: '#dc2626', border: '1px solid #fca5a5', borderRadius: 6, padding: '4px 10px', background: 'none', cursor: 'pointer' }}>削除</button>
          {saving && <span style={{ fontSize: '0.72rem', color: 'var(--gray-400)' }}>保存中...</span>}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12, marginBottom: 20 }}>
          <F label="📅 NA日">{inp('na_date', { type: 'date' })}</F>
          <F label="⚡ 優先度">
            <select value={local.priority || '中'} onChange={e => save({ priority: e.target.value })}
              style={{ padding: '6px 8px', border: `1px solid ${pri.border}`, borderRadius: 6, fontSize: '0.85rem', background: pri.bg, color: pri.color, fontWeight: 700 }}>
              {['高','中','低'].map(p => <option key={p}>{p}</option>)}
            </select>
          </F>
          <F label="👤 担当">{inp('chief', { placeholder: '担当者名' })}</F>
          <F label="🏐 ボール">{inp('ball', { placeholder: '誰が持っているか' })}</F>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <F label="📍 ステータス（フェーズ）">{inp('status_phase', { placeholder: '見解送付済み / MTGセット / 法務移行 etc' })}</F>
            <F label="NA処理台帳番号">{inp('na_ledger', { placeholder: 'NA-2606-01' })}</F>
          </div>
          <F label="現状（フリーメモ）">{ta('current_state', 4)}</F>
          <F label="問題箇所（1行サマリ）">{inp('issue_summary', { placeholder: '問題の核心を1行で' })}</F>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <F label="契約書該当箇所">{ta('contract_details')}</F>
            <F label="問題該当箇所詳細">{ta('issue_details')}</F>
          </div>
          <F label="方向性・対応方針">{ta('direction', 2)}</F>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <F label="📎 契約書URL">{inp('contract_url', { type: 'url', placeholder: 'https://drive.google.com/...' })}</F>
            <F label="📋 処理台帳URL">{inp('ledger_url', { type: 'url', placeholder: 'https://drive.google.com/...' })}</F>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <F label="💬 スレッドURL">
              <div style={{ display: 'flex', gap: 6 }}>
                {inp('thread_url', { type: 'url', placeholder: 'https://', style: { flex: 1 } })}
                {local.thread_url && <a href={local.thread_url} target="_blank" rel="noopener noreferrer"
                  style={{ padding: '6px 10px', background: 'var(--surface-2)', border: '1px solid var(--gray-200)', borderRadius: 6, fontSize: '0.82rem', color: 'var(--gray-600)', textDecoration: 'none' }}>開く</a>}
              </div>
            </F>
            <F label="✉️ メール履歴URL">{inp('email_slack_url', { type: 'url', placeholder: 'Slack/Doc URL' })}</F>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
            <F label="📝 議事録URL">{inp('minutes_url', { type: 'url', placeholder: 'Google Docs URL' })}</F>
            <F label="📅 クローズ／法務移行日">{inp('closed_date', { type: 'date' })}</F>
          </div>
          <F label="🏁 結果（最終結論）">
            <input value={local.final_result || ''} onChange={e => setLocal(l => ({ ...l, final_result: e.target.value }))}
              onBlur={e => save({ final_result: e.target.value })}
              style={{ padding: '6px 8px', border: '1px solid #bfdbfe', borderRadius: 6, fontSize: '0.85rem', background: '#eff6ff', color: '#1e40af', fontWeight: 700 }} />
          </F>
        </div>
      </div>

      {/* 右: 履歴・議事録 */}
      <div style={{ flex: '2 1 0', overflowY: 'auto', padding: '20px 16px', display: 'flex', flexDirection: 'column', gap: 16, background: 'var(--gray-50)' }}>
        <RecordList title="対応履歴" records={local.history || []}
          onAdd={r => addRecord('history', r)} onDelete={id => delRecord('history', id)} />
        <RecordList title="議事録履歴" records={local.minutes || []}
          onAdd={r => addRecord('minutes', r)} onDelete={id => delRecord('minutes', id)} />
      </div>
    </div>
  );
}

// ── メインページ ────────────────────────────────────────────
export default function LegalHub() {
  const [cases, setCases]       = useState([]);
  const [loading, setLoading]   = useState(true);
  const [selected, setSelected] = useState(null);
  const [tab, setTab]           = useState('active'); // active | all
  const [query, setQuery]       = useState('');
  const [staleOnly, setStaleOnly] = useState(false);
  const [sortKey, setSortKey]   = useState('updated_at');
  const [sortDir, setSortDir]   = useState('desc');

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
    } catch { alert('作成に失敗しました'); }
  };

  const handleDelete = async (id) => {
    await api.legalDelete(id).catch(() => {});
    setCases(prev => prev.filter(c => c.id !== id));
    setSelected(null);
  };

  const handleChange = (updated) => {
    setCases(prev => prev.map(c => c.id === updated.id ? updated : c));
    setSelected(updated);
  };

  const staleCount = cases.filter(c => !isClosed(c) && isStale(c.updated_at)).length;

  const displayed = useMemo(() => {
    let list = [...cases];
    if (tab === 'active') list = list.filter(c => !isClosed(c));
    if (staleOnly) list = list.filter(c => !isClosed(c) && isStale(c.updated_at));
    if (query) {
      const q = query.toLowerCase();
      list = list.filter(c => {
        const hay = `${c.case_name || ''} ${c.chief || ''} ${c.issue_summary || ''} ${c.issue_details || ''} ${c.status_phase || ''} ${c.ball || ''}`.toLowerCase();
        return hay.includes(q);
      });
    }
    list.sort((a, b) => {
      const va = a[sortKey] || '', vb = b[sortKey] || '';
      return sortDir === 'asc' ? (va < vb ? -1 : 1) : (va < vb ? 1 : -1);
    });
    return list;
  }, [cases, tab, staleOnly, query, sortKey, sortDir]);

  const sortToggle = (key) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDir('desc'); }
  };
  const SortIcon = ({ k }) => sortKey !== k ? null : <span style={{ fontSize: 10, marginLeft: 2 }}>{sortDir === 'asc' ? '▲' : '▼'}</span>;

  const COLS = [
    { key: 'case_name',     label: '案件名（企業名）', w: '22%' },
    { key: 'na_date',       label: 'NA日',            w: 90 },
    { key: 'priority',      label: '優先度',           w: 72 },
    { key: 'chief',         label: '担当',             w: 80 },
    { key: 'status_phase',  label: 'ステータス',       w: 130 },
    { key: 'ball',          label: 'ボール',           w: 100 },
    { key: 'issue_details', label: '問題箇所',         w: '24%' },
    { key: 'updated_at',    label: '最終更新',         w: 90 },
    { key: 'final_result',  label: '結果',             w: 120 },
    { key: 'closed_date',   label: 'クローズ日',       w: 100 },
  ];

  if (loading) return <div style={{ padding: 40, textAlign: 'center', color: 'var(--gray-400)' }}>読み込み中...</div>;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: 'var(--gray-50)' }}>
      {/* ヘッダー */}
      <div style={{ background: 'var(--surface)', borderBottom: '1px solid var(--gray-200)', padding: '12px 20px', display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
        <span style={{ fontWeight: 700, fontSize: '1rem', color: 'var(--gray-800)' }}>法務 案件進捗管理</span>

        {/* タブ */}
        <div style={{ display: 'flex', background: 'var(--gray-100)', borderRadius: 8, padding: 3, gap: 2 }}>
          {[['active','対応中'],['all','全件']].map(([v,l]) => (
            <button key={v} onClick={() => { setTab(v); setSelected(null); setStaleOnly(false); }}
              style={{ padding: '4px 12px', borderRadius: 6, border: 'none', fontSize: '0.8rem', fontWeight: 700, cursor: 'pointer',
                background: tab === v ? 'var(--surface)' : 'transparent',
                color: tab === v ? '#2563eb' : 'var(--gray-500)',
                boxShadow: tab === v ? '0 1px 3px rgba(0,0,0,0.08)' : 'none' }}>
              {l}
            </button>
          ))}
        </div>

        {/* 未更新アラート */}
        {staleCount > 0 && (
          <button onClick={() => setStaleOnly(s => !s)}
            style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '4px 12px', borderRadius: 99, fontSize: '0.78rem', fontWeight: 700, border: '1px solid #fde68a', cursor: 'pointer',
              background: staleOnly ? '#f59e0b' : '#fffbeb', color: staleOnly ? '#fff' : '#92400e' }}>
            ⚠ {staleOnly ? '未更新のみ表示中' : `2日以上未更新: ${staleCount}件`}
          </button>
        )}

        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <input value={query} onChange={e => setQuery(e.target.value)} placeholder="案件名・担当・問題箇所で検索..."
            style={{ padding: '6px 12px', border: '1px solid var(--gray-300)', borderRadius: 8, fontSize: '0.82rem', width: 260, background: 'var(--surface)' }} />
          <button onClick={handleCreate} style={{ padding: '6px 14px', background: '#2563eb', color: '#fff', border: 'none', borderRadius: 8, fontSize: '0.82rem', fontWeight: 700, cursor: 'pointer' }}>
            ＋ 新規案件
          </button>
        </div>
      </div>

      {/* コンテンツ */}
      {selected ? (
        <CaseDetail c={selected} onChange={handleChange} onDelete={handleDelete} onBack={() => setSelected(null)} />
      ) : (
        <div style={{ flex: 1, overflow: 'auto', padding: '16px 20px' }}>
          <div style={{ background: 'var(--surface)', borderRadius: 10, border: '1px solid var(--gray-200)', overflow: 'hidden' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.82rem' }}>
              <thead>
                <tr style={{ background: 'var(--gray-50)', borderBottom: '1px solid var(--gray-200)' }}>
                  {COLS.map(col => (
                    <th key={col.key} onClick={() => sortToggle(col.key)}
                      style={{ padding: '10px 12px', textAlign: 'left', fontWeight: 700, fontSize: '0.75rem', color: 'var(--gray-600)', cursor: 'pointer', whiteSpace: 'nowrap', width: col.w, userSelect: 'none' }}>
                      {col.label}<SortIcon k={col.key} />
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {displayed.map(c => {
                  const pri = PRIORITY_COLOR[c.priority] || PRIORITY_COLOR['中'];
                  const stale = !isClosed(c) && isStale(c.updated_at);
                  const closed = isClosed(c);
                  return (
                    <tr key={c.id} onClick={() => setSelected(c)}
                      style={{ borderBottom: '1px solid var(--gray-100)', cursor: 'pointer', opacity: closed ? 0.6 : 1,
                        background: stale ? '#fffbeb' : 'var(--surface)' }}
                      onMouseEnter={e => e.currentTarget.style.background = '#eff6ff'}
                      onMouseLeave={e => e.currentTarget.style.background = stale ? '#fffbeb' : 'var(--surface)'}>
                      <td style={{ padding: '10px 12px', fontWeight: 600, color: 'var(--gray-900)', maxWidth: 0 }}>
                        <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {stale && <span style={{ fontSize: 10, marginRight: 4 }}>⚠</span>}
                          {c.case_name}
                        </div>
                      </td>
                      <td style={{ padding: '10px 12px', color: 'var(--gray-600)', whiteSpace: 'nowrap' }}>{fmtDate(c.na_date)}</td>
                      <td style={{ padding: '10px 12px' }}>
                        <span style={{ fontSize: '0.72rem', fontWeight: 700, padding: '2px 8px', borderRadius: 4, border: `1px solid ${pri.border}`, background: pri.bg, color: pri.color }}>{c.priority}</span>
                      </td>
                      <td style={{ padding: '10px 12px', color: 'var(--gray-700)' }}>{c.chief}</td>
                      <td style={{ padding: '10px 12px', color: 'var(--gray-700)', maxWidth: 0 }}>
                        <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{c.ball}</div>
                      </td>
                      <td style={{ padding: '10px 12px', color: 'var(--gray-600)', maxWidth: 0 }}>
                        <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{c.issue_summary}</div>
                      </td>
                      <td style={{ padding: '10px 12px', color: stale ? '#d97706' : 'var(--gray-500)', fontWeight: stale ? 700 : 400, whiteSpace: 'nowrap', fontSize: '0.75rem' }}>
                        {c.updated_at ? new Date(c.updated_at).toLocaleDateString('ja-JP', { month: 'numeric', day: 'numeric' }) : '—'}
                      </td>
                      <td style={{ padding: '10px 12px' }}>
                        <span style={{ fontSize: '0.72rem', fontWeight: 700, padding: '2px 8px', borderRadius: 4,
                          background: closed ? 'var(--gray-100)' : '#eff6ff',
                          color: closed ? 'var(--gray-500)' : '#1d4ed8' }}>{c.result}</span>
                      </td>
                    </tr>
                  );
                })}
                {displayed.length === 0 && (
                  <tr><td colSpan={8} style={{ padding: '32px', textAlign: 'center', color: 'var(--gray-400)' }}>該当する案件がありません</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
