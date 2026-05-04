import { useEffect, useRef, useState } from 'react';
import { useNavigate, useParams, Link } from 'react-router-dom';
import { api } from '../../api/client';

const YOMI_ORDER = ['アポ化前','アポ化済商談前','E 5％','D 15％','C 30％','B 50％','A 70％','S 90％','受注','失注'];
const YOMI_COLOR = {
  'アポ化前':'#9ca3af','アポ化済商談前':'#6b7280',
  'E 5％':'#d1d5db','D 15％':'#94a3b8',
  'C 30％':'#93c5fd','B 50％':'#60a5fa',
  'A 70％':'#3b82f6','S 90％':'#1d4ed8',
  '受注':'#10b981','失注':'#ef4444',
};
const INDUSTRIES = ['漁業','金融業・保険業','農業、林業','建設業','製造業','情報通信業','運輸業、郵便業','卸売業・小売業','不動産業、物品賃貸業','学術研究、専門・技術サービス業','宿泊業、飲食サービス業','生活関連サービス業、娯楽業','教育、学習支援業','医療、福祉','複合サービス事業','サービス業（他に分類されないもの）','公務','分類不能の産業'];
const PREFECTURES = ['北海道','青森県','岩手県','宮城県','秋田県','山形県','福島県','茨城県','栃木県','群馬県','埼玉県','千葉県','東京都','神奈川県','新潟県','富山県','石川県','福井県','山梨県','長野県','岐阜県','静岡県','愛知県','三重県','滋賀県','京都府','大阪府','兵庫県','奈良県','和歌山県','鳥取県','島根県','岡山県','広島県','山口県','徳島県','香川県','愛媛県','高知県','福岡県','佐賀県','長崎県','熊本県','大分県','宮崎県','鹿児島県','沖縄県'];
const EMP_COUNTS = ['1-10','11-50','51-100','101-300','301-500','501-1000','1000-'];

// ─── 共通スタイル ─────────────────────────────────────────
const S = {
  label: { display:'block', fontSize:'0.78rem', fontWeight:700, color:'#374151', marginBottom:5 },
  input: {
    width:'100%', boxSizing:'border-box', padding:'9px 12px',
    border:'1.5px solid #e5e7eb', borderRadius:8, fontSize:'0.88rem',
    outline:'none', background:'#fff', color:'#111827', transition:'border-color 0.15s',
  },
  select: {
    width:'100%', boxSizing:'border-box', padding:'9px 12px',
    border:'1.5px solid #e5e7eb', borderRadius:8, fontSize:'0.88rem',
    background:'#fff', color:'#111827', cursor:'pointer',
  },
  sectionTitle: { fontSize:'0.88rem', fontWeight:800, color:'#111827', marginBottom:2 },
  sectionSub: { fontSize:'0.75rem', color:'#9ca3af', marginBottom:14 },
  row2: { display:'grid', gridTemplateColumns:'1fr 1fr', gap:'12px 16px' },
};

function CustomFieldInput({ field, value, onChange }) {
  const { field_type, options = [] } = field;
  if (field_type === 'select') return (
    <select value={value||''} onChange={e => onChange(e.target.value)} style={S.select}>
      <option value="">—</option>
      {options.map(o => <option key={o} value={o}>{o}</option>)}
    </select>
  );
  if (field_type === 'textarea') return (
    <textarea value={value||''} onChange={e => onChange(e.target.value)}
      rows={2} style={{ ...S.input, resize:'vertical' }}
      onFocus={e=>e.target.style.borderColor='#6366f1'} onBlur={e=>e.target.style.borderColor='#e5e7eb'} />
  );
  if (field_type === 'checkbox') return (
    <label style={{ display:'flex', alignItems:'center', gap:8, cursor:'pointer', marginTop:4 }}>
      <input type="checkbox" checked={!!value} onChange={e => onChange(e.target.checked)} />
      <span style={{ fontSize:'0.85rem', color:'#374151' }}>有効</span>
    </label>
  );
  return (
    <InputF type={field_type === 'number' ? 'number' : field_type === 'date' ? 'date' : 'text'}
      value={value||''} onChange={e => onChange(e.target.value)} />
  );
}

function Field({ label, required, children }) {
  return (
    <div>
      <label style={S.label}>{label}{required && <span style={{ color:'#ef4444', marginLeft:3 }}>*</span>}</label>
      {children}
    </div>
  );
}

function InputF({ value, onChange, placeholder, type='text', onFocus, onBlur }) {
  return (
    <input type={type} value={value||''} onChange={onChange} placeholder={placeholder}
      style={S.input}
      onFocus={e=>{ e.target.style.borderColor='#6366f1'; onFocus?.(); }}
      onBlur={e=>{ e.target.style.borderColor='#e5e7eb'; onBlur?.(); }}
    />
  );
}

function SelectF({ value, onChange, options, placeholder='選択してください' }) {
  return (
    <select value={value||''} onChange={onChange} style={S.select}>
      <option value="">{placeholder}</option>
      {options.map(o => <option key={o} value={o}>{o}</option>)}
    </select>
  );
}

function SectionHeader({ title, sub }) {
  return (
    <div style={{ marginBottom:16, paddingBottom:10, borderBottom:'1px solid #f3f4f6' }}>
      <div style={S.sectionTitle}>{title}</div>
      {sub && <div style={S.sectionSub}>{sub}</div>}
    </div>
  );
}

function YomiBadge({ yomi }) {
  const color = YOMI_COLOR[yomi] || '#9ca3af';
  return <span style={{ fontSize:'0.78rem', fontWeight:700, padding:'3px 10px', borderRadius:999, background:color+'22', color, border:`1px solid ${color}44` }}>{yomi}</span>;
}

function fmt(n) {
  if (!n) return null;
  return `¥${Number(n).toLocaleString()}`;
}

function formatUpdatedAt(dateStr, userId) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  const y = d.getFullYear(), mo = d.getMonth()+1, dd = d.getDate();
  const hh = String(d.getHours()).padStart(2,'0'), mm = String(d.getMinutes()).padStart(2,'0');
  return `${y}/${mo}/${dd} ${hh}:${mm}${userId ? ` by ${userId}` : ''}`;
}

// ─── DealCard ─────────────────────────────────────────────
// ── サブテーブル汎用エディタ ──────────────────────────────────────
function SubTableEditor({ dealId, path, columns }) {
  const [rows, setRows] = useState(null);
  const [newRow, setNewRow] = useState({});
  const [adding, setAdding] = useState(false);

  useEffect(() => {
    fetch(`/api/crm/deals/${dealId}/${path}`, { credentials:'include' })
      .then(r=>r.json()).then(d=>setRows(d.rows||[])).catch(()=>setRows([]));
  }, [dealId, path]);

  const handleAdd = async () => {
    setAdding(true);
    try {
      const res = await fetch(`/api/crm/deals/${dealId}/${path}`, {
        method:'POST', credentials:'include',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify(newRow),
      });
      const d = await res.json();
      setRows(prev => [...(prev||[]), d.row]);
      setNewRow({});
    } catch { alert('追加に失敗しました'); }
    finally { setAdding(false); }
  };

  const handleDelete = async (rowId) => {
    await fetch(`/api/crm/${path}/${rowId}`, { method:'DELETE', credentials:'include' });
    setRows(prev => prev.filter(r=>r.id!==rowId));
  };

  if (!rows) return <div style={{color:'#9ca3af',fontSize:12,marginBottom:16}}>読み込み中…</div>;

  return (
    <div style={{ marginBottom:20, border:'1px solid #f3f4f6', borderRadius:8, overflow:'hidden' }}>
      {rows.length > 0 && (
        <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.8rem' }}>
          <thead>
            <tr style={{ background:'#f9fafb' }}>
              {columns.map(c=><th key={c.key} style={{ padding:'5px 10px', textAlign:'left', color:'#6b7280', fontWeight:600, borderBottom:'1px solid #f3f4f6' }}>{c.label}</th>)}
              <th style={{ width:32 }} />
            </tr>
          </thead>
          <tbody>
            {rows.map(row=>(
              <tr key={row.id} style={{ borderBottom:'1px solid #f9fafb' }}>
                {columns.map(c=><td key={c.key} style={{ padding:'5px 10px', color:'#374151' }}>{row[c.key]??'—'}</td>)}
                <td style={{ padding:'5px 8px' }}>
                  <button onClick={()=>handleDelete(row.id)} style={{ background:'none',border:'none',cursor:'pointer',color:'#d1d5db',fontSize:13 }}>×</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {/* 新規追加行 */}
      <div style={{ display:'flex', gap:6, padding:'8px', background:'#fafafa', flexWrap:'wrap', alignItems:'flex-end' }}>
        {columns.map(c=>(
          <div key={c.key} style={{ display:'flex', flexDirection:'column', gap:2 }}>
            <span style={{ fontSize:'0.68rem', color:'#9ca3af' }}>{c.label}</span>
            <input type={c.type||'text'} value={newRow[c.key]||''} onChange={e=>setNewRow(p=>({...p,[c.key]:e.target.value}))}
              style={{ fontSize:12, padding:'4px 8px', border:'1px solid #e5e7eb', borderRadius:6, width:c.type==='text'?110:80, outline:'none' }} />
          </div>
        ))}
        <button onClick={handleAdd} disabled={adding}
          style={{ fontSize:12, padding:'5px 12px', border:'none', borderRadius:6, background:'#1e293b', color:'#fff', cursor:'pointer', marginTop:14 }}>
          {adding?'追加中…':'＋追加'}
        </button>
      </div>
    </div>
  );
}

// ─── DealActivitySection ──────────────────────────────────
function DealActivitySection({ deal, activitySettings }) {
  const [activities, setActivities] = useState(null);
  const [form, setForm] = useState({ activityType: '', result: '', content: '' });
  const [adding, setAdding] = useState(false);

  const { activityTypes = ['架電','商談','メール','受電','その他'], resultOptions = ['アポ獲得','有効会話','不通','折り返し','NG','その他'] } = activitySettings || {};

  useEffect(() => {
    api.crmActivities(deal.id).then(d => setActivities(d.activities || [])).catch(() => setActivities([]));
  }, [deal.id]);

  const handleAdd = async () => {
    if (!form.activityType) return;
    setAdding(true);
    try {
      const d = await api.crmAddActivity(deal.id, {
        activityType: form.activityType,
        result: form.result || null,
        content: form.content || null,
        yomiAtTime: deal.yomi,
      });
      setActivities(prev => [d.activity, ...(prev || [])]);
      setForm({ activityType: '', result: '', content: '' });
    } catch { alert('追加に失敗しました'); }
    finally { setAdding(false); }
  };

  const handleDelete = async (actId) => {
    await api.crmDeleteActivity(deal.id, actId).catch(() => {});
    setActivities(prev => (prev || []).filter(a => a.id !== actId));
  };

  const fmtDate = (d) => {
    if (!d) return '';
    const dt = new Date(d);
    return `${dt.getMonth()+1}/${dt.getDate()} ${String(dt.getHours()).padStart(2,'0')}:${String(dt.getMinutes()).padStart(2,'0')}`;
  };

  return (
    <div style={{ borderTop: '1px solid #f3f4f6', padding: '14px 18px', background: '#fafafa' }}>
      <div style={{ fontWeight: 700, fontSize: '0.82rem', color: '#374151', marginBottom: 12 }}>アクティビティ記録</div>

      {/* 追加フォーム */}
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'flex-end', marginBottom: 14,
        padding: '10px 12px', background: '#fff', borderRadius: 8, border: '1px solid #e5e7eb' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          <span style={{ fontSize: '0.7rem', color: '#9ca3af' }}>アクション種別</span>
          <select value={form.activityType} onChange={e => setForm(p => ({...p, activityType: e.target.value}))}
            style={{ padding: '5px 10px', border: '1.5px solid #e5e7eb', borderRadius: 6, fontSize: '0.82rem', background: '#fff', color: '#111827', cursor: 'pointer' }}>
            <option value="">選択</option>
            {activityTypes.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          <span style={{ fontSize: '0.7rem', color: '#9ca3af' }}>結果</span>
          <select value={form.result} onChange={e => setForm(p => ({...p, result: e.target.value}))}
            style={{ padding: '5px 10px', border: '1.5px solid #e5e7eb', borderRadius: 6, fontSize: '0.82rem', background: '#fff', color: '#111827', cursor: 'pointer' }}>
            <option value="">-</option>
            {resultOptions.map(r => <option key={r} value={r}>{r}</option>)}
          </select>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3, flex: 1, minWidth: 140 }}>
          <span style={{ fontSize: '0.7rem', color: '#9ca3af' }}>メモ</span>
          <input value={form.content} onChange={e => setForm(p => ({...p, content: e.target.value}))}
            placeholder="詳細メモ（任意）"
            style={{ padding: '5px 10px', border: '1.5px solid #e5e7eb', borderRadius: 6, fontSize: '0.82rem', outline: 'none', background: '#fff', color: '#111827' }}
            onFocus={e => e.target.style.borderColor='#6366f1'}
            onBlur={e => e.target.style.borderColor='#e5e7eb'}
            onKeyDown={e => { if (e.key === 'Enter') handleAdd(); }}
          />
        </div>
        <button onClick={handleAdd} disabled={adding || !form.activityType}
          style={{ padding: '5px 14px', border: 'none', borderRadius: 6, background: form.activityType ? '#1e293b' : '#e5e7eb',
            color: form.activityType ? '#fff' : '#9ca3af', fontSize: '0.82rem', cursor: form.activityType ? 'pointer' : 'default', fontWeight: 600, whiteSpace: 'nowrap' }}>
          {adding ? '追加中…' : '＋ 記録'}
        </button>
      </div>

      {/* ログ */}
      {activities === null ? (
        <div style={{ color: '#9ca3af', fontSize: '0.78rem' }}>読み込み中…</div>
      ) : activities.length === 0 ? (
        <div style={{ color: '#9ca3af', fontSize: '0.78rem' }}>記録がありません</div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {activities.map(a => (
            <div key={a.id} style={{ display: 'flex', alignItems: 'flex-start', gap: 8, padding: '7px 10px',
              background: '#fff', borderRadius: 7, border: '1px solid #f3f4f6' }}>
              <span style={{ fontSize: '0.72rem', color: '#6b7280', flexShrink: 0, marginTop: 1 }}>{fmtDate(a.created_at)}</span>
              <span style={{ fontSize: '0.78rem', fontWeight: 700, color: '#1e293b', flexShrink: 0,
                background: '#f1f5f9', borderRadius: 4, padding: '1px 7px' }}>{a.activity_type}</span>
              {a.result && (
                <span style={{ fontSize: '0.75rem', fontWeight: 600, color: '#059669',
                  background: '#f0fdf4', borderRadius: 4, padding: '1px 7px', flexShrink: 0 }}>{a.result}</span>
              )}
              {a.yomi_at_time && (
                <span style={{ fontSize: '0.72rem', color: '#9ca3af', flexShrink: 0 }}>[{a.yomi_at_time}]</span>
              )}
              {a.content && (
                <span style={{ fontSize: '0.78rem', color: '#374151', flex: 1 }}>{a.content}</span>
              )}
              <button onClick={() => handleDelete(a.id)}
                style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#d1d5db', fontSize: 12, flexShrink: 0, padding: '0 2px', lineHeight: 1 }}>×</button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function DealCard({ deal, meta, members, onUpdate, onDelete, activitySettings, customFields = [], fieldOptions = {} }) {
  const [editing, setEditing] = useState(false);
  const [showActivities, setShowActivities] = useState(false);
  const [form, setForm] = useState({ ...deal, ...deal.data });
  const [saving, setSaving] = useState(false);
  const [savedAt, setSavedAt] = useState(null);
  const autoSaveTimer = useRef(null);

  const set = (key, val) => setForm(p => ({ ...p, [key]: val }));

  const save = async (f = form) => {
    setSaving(true);
    try {
      const bantData = { budget:f.budget, authority:f.authority, needs:f.needs, timeframe:f.timeframe, bantMemo:f.bantMemo };
      const r = await api.crmUpdateDeal(deal.id, { ...f, data: bantData });
      onUpdate(r.deal);
      setSavedAt(new Date().toLocaleTimeString('ja-JP'));
    } catch { alert('保存に失敗しました'); }
    finally { setSaving(false); }
  };

  const scheduleAutoSave = (f) => {
    clearTimeout(autoSaveTimer.current);
    autoSaveTimer.current = setTimeout(() => save(f), 1500);
  };

  const setAuto = (key, val) => {
    const next = { ...form, [key]: val };
    setForm(next);
    scheduleAutoSave(next);
  };

  const salesUsers = meta?.salesUsers || [];
  const rpoId = deal.data?.rpo_client_id;

  // 担当者セレクト（DBの実際の名前から選択）
  const SalesSelect = ({ fieldKey, label }) => (
    <Field label={label}>
      <select value={form[fieldKey]||form[fieldKey.replace('UserId','_user_id').replace('sales','sales').replace('na','na')]||''}
        onChange={e=>setAuto(fieldKey, e.target.value)} style={S.select}>
        <option value="">選択してください</option>
        {salesUsers.map(n=><option key={n} value={n}>{n}</option>)}
      </select>
    </Field>
  );

  const card = (
    <div style={{ border:'1px solid #e5e7eb', borderRadius:12, background:'#fff', marginBottom:12, overflow:'hidden', boxShadow:'0 1px 4px rgba(0,0,0,0.05)' }}>
      {/* カードヘッダー */}
      <div style={{ padding:'14px 18px' }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', gap:8 }}>
          <div style={{ display:'flex', alignItems:'center', gap:10, flexWrap:'wrap', flex:1 }}>
            <span style={{ fontWeight:700, fontSize:'0.95rem', color:'#111827' }}>{deal.name}</span>
            <YomiBadge yomi={deal.yomi} />
            {rpoId && (
              <Link to={`/rpo/${rpoId}`} onClick={e=>e.stopPropagation()}
                style={{ fontSize:'0.75rem', padding:'2px 8px', borderRadius:4, background:'#ede9fe', color:'#6d28d9', textDecoration:'none', fontWeight:600 }}>
                → 案件管理
              </Link>
            )}
          </div>
          <div style={{ display:'flex', gap:6, flexShrink:0 }}>
            <button style={{ padding:'5px 14px', border:'1.5px solid #e5e7eb', borderRadius:8, background:'#fff', color:'#374151', fontSize:'0.82rem', fontWeight:600, cursor:'pointer' }}
              onClick={() => { setForm({...deal,...deal.data}); setSavedAt(null); setEditing(v=>!v); }}>
              {editing ? 'キャンセル' : '編集'}
            </button>
            <button style={{ padding:'5px 10px', border:'1.5px solid #fca5a5', borderRadius:8, background:'#fff', color:'#ef4444', fontSize:'0.82rem', fontWeight:600, cursor:'pointer' }}
              onClick={() => { if(window.confirm('この商談を削除しますか？')) onDelete(deal.id); }}>
              削除
            </button>
          </div>
        </div>

        {!editing && (
          <div style={{ marginTop:12 }}>
            <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:'4px 24px', fontSize:'0.83rem' }}>
              {[
                ['契約形態', deal.contract_type],
                ['支払方式', deal.payment_type],
                ['担当営業', deal.sales_user_id],
                ['NA担当者', deal.na_user_id],
                ['初期費用', fmt(deal.initial_fee)],
                ['月額費用', fmt(deal.monthly_fee)],
                ['契約月数', deal.contract_months ? `${deal.contract_months}ヶ月` : null],
                ['採用目標', deal.hiring_target ? `${deal.hiring_target}人` : null],
              ].filter(([,v])=>v).map(([k,v])=>(
                <div key={k} style={{ display:'flex', gap:8, alignItems:'baseline' }}>
                  <span style={{ color:'#9ca3af', fontSize:'0.78rem', flexShrink:0 }}>{k}</span>
                  <span style={{ color:'#374151', fontWeight:500 }}>{v}</span>
                </div>
              ))}
            </div>
            {deal.memo && (
              <div style={{ marginTop:10, padding:'10px 14px', background:'#f9fafb', borderRadius:8, fontSize:'0.83rem', color:'#374151', whiteSpace:'pre-wrap', wordBreak:'break-word', lineHeight:1.7, borderLeft:'3px solid #e5e7eb' }}>
                {deal.memo}
              </div>
            )}
          </div>
        )}
      </div>

      {/* アクティビティ toggle */}
      <div style={{ borderTop:'1px solid #f3f4f6', padding:'6px 18px', background:'#f9fafb' }}>
        <button onClick={() => setShowActivities(v => !v)}
          style={{ background:'none', border:'none', cursor:'pointer', fontSize:'0.78rem', color:'#6b7280', fontWeight:600, padding:'2px 0', display:'flex', alignItems:'center', gap:4 }}>
          <span style={{ transform: showActivities ? 'rotate(90deg)' : 'rotate(0deg)', display:'inline-block', transition:'transform 0.15s' }}>▶</span>
          アクティビティ
        </button>
      </div>
      {showActivities && <DealActivitySection deal={deal} activitySettings={activitySettings} />}

    </div>
  );

  // ─── 編集モーダル（カード外に表示）───────────────
  const editModal = editing && (
    <div className="modal-overlay" onClick={()=>{ clearTimeout(autoSaveTimer.current); setEditing(false); }}>
      <div className="modal-content" onClick={e=>e.stopPropagation()} style={{ maxWidth:820, width:'90vw', padding:0, overflow:'hidden', display:'flex', flexDirection:'column', maxHeight:'88vh' }}>
        {/* モーダルヘッダー */}
        <div style={{ padding:'14px 24px', borderBottom:'1px solid #f3f4f6', display:'flex', justifyContent:'space-between', alignItems:'center', flexShrink:0 }}>
          <div>
            <div style={{ fontSize:'0.75rem', color:'#9ca3af', marginBottom:2 }}>商談を編集</div>
            <div style={{ fontWeight:800, fontSize:'1rem', color:'#111827' }}>{deal.name}</div>
          </div>
          <div style={{ display:'flex', alignItems:'center', gap:12 }}>
            {savedAt && (
              <span style={{ fontSize:'0.75rem', color:'#10b981', fontWeight:600, display:'flex', alignItems:'center', gap:5 }}>
                <span style={{ width:7, height:7, borderRadius:'50%', background:'#10b981', display:'inline-block' }} />
                {savedAt} に保存済み
              </span>
            )}
            <button onClick={()=>{ clearTimeout(autoSaveTimer.current); setEditing(false); }}
              style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:18, lineHeight:1, padding:4 }}>×</button>
          </div>
        </div>

        <div style={{ flex:1, overflowY:'auto', padding:'20px 24px' }}>
          <SectionHeader title="基本情報" sub="商談の概要と担当者" />
          <div style={{ marginBottom:16 }}>
            <Field label="商談名" required>
              <InputF value={form.name} onChange={e=>setAuto('name',e.target.value)} />
            </Field>
          </div>
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:'12px 16px', marginBottom:20 }}>
            <Field label="ヨミ" required>
              <SelectF value={form.yomi} onChange={e=>setAuto('yomi',e.target.value)} options={YOMI_ORDER} />
            </Field>
            <Field label="契約形態">
              <SelectF value={form.contractType||form.contract_type} onChange={e=>setAuto('contractType',e.target.value)} options={fieldOptions.contract_type || meta?.contractTypes || []} />
            </Field>
            <Field label="支払方式">
              <SelectF value={form.paymentType||form.payment_type} onChange={e=>setAuto('paymentType',e.target.value)} options={fieldOptions.payment_type || meta?.paymentTypes || []} />
            </Field>
            <Field label="担当営業">
              <select value={form.salesUserId||form.sales_user_id||''} onChange={e=>setAuto('salesUserId',e.target.value)} style={S.select}>
                <option value="">選択してください</option>
                {salesUsers.map(n=><option key={n} value={n}>{n}</option>)}
              </select>
            </Field>
            <Field label="NA担当者">
              <select value={form.naUserId||form.na_user_id||''} onChange={e=>setAuto('naUserId',e.target.value)} style={S.select}>
                <option value="">選択してください</option>
                {salesUsers.map(n=><option key={n} value={n}>{n}</option>)}
              </select>
            </Field>
            <Field label="雇用形態">
              <SelectF value={form.employmentType||form.employment_type} onChange={e=>setAuto('employmentType',e.target.value)} options={fieldOptions.employment_type || ['新卒','中途','業務委託','アルバイト/インターン']} />
            </Field>
          </div>

          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:'12px 16px', marginBottom:20 }}>
            <Field label="流入日（案件）">
              <InputF type="date" value={form.inflowDate||form.inflow_date||''} onChange={e=>setAuto('inflowDate',e.target.value)} />
            </Field>
            <Field label="流入経路（案件）">
              {fieldOptions.inflow_source?.length > 0
                ? <SelectF value={form.inflowSource||form.inflow_source||''} onChange={e=>setAuto('inflowSource',e.target.value)} options={fieldOptions.inflow_source} />
                : <InputF value={form.inflowSource||form.inflow_source||''} onChange={e=>setAuto('inflowSource',e.target.value)} placeholder="例: 問い合わせ" />}
            </Field>
            <Field label="初回商談日">
              <InputF type="date" value={form.firstMeetingDate||form.first_meeting_date||''} onChange={e=>setAuto('firstMeetingDate',e.target.value)} />
            </Field>
          </div>

          <SectionHeader title="費用・条件" sub="契約金額と採用条件" />
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:'12px 16px', marginBottom:20 }}>
            <Field label="初期費用 (円)">
              <InputF type="number" value={form.initialFee??form.initial_fee} onChange={e=>setAuto('initialFee',e.target.value)} placeholder="例: 500000" />
            </Field>
            <Field label="月額費用 (円)">
              <InputF type="number" value={form.monthlyFee??form.monthly_fee} onChange={e=>setAuto('monthlyFee',e.target.value)} placeholder="例: 300000" />
            </Field>
            <Field label="採用目標人数">
              <InputF type="number" value={form.hiringTarget??form.hiring_target} onChange={e=>setAuto('hiringTarget',e.target.value)} />
            </Field>
            <Field label="契約月数">
              <InputF type="number" value={form.contractMonths??form.contract_months} onChange={e=>setAuto('contractMonths',e.target.value)} />
            </Field>
            <Field label="失注理由">
              <SelectF value={form.lostReason||form.lost_reason} onChange={e=>setAuto('lostReason',e.target.value)} options={meta?.lostReasons||[]} />
            </Field>
          </div>

          <SectionHeader title="BANT" sub="予算・決裁権・ニーズ・導入時期の確認状況" />
          <div style={{ background:'#fafafa', borderRadius:10, padding:'14px 16px', marginBottom:20, border:'1px solid #f3f4f6' }}>
            <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:'8px 16px', marginBottom:12 }}>
              {[['budget','Budget（予算）'],['authority','Authority（決裁権）'],['needs','Needs（ニーズ）'],['timeframe','Timeframe（導入時期）']].map(([k,l])=>(
                <label key={k} style={{ display:'flex', alignItems:'center', gap:8, cursor:'pointer', fontSize:'0.85rem', color:'#374151', fontWeight:500 }}>
                  <input type="checkbox" checked={!!form[k]} onChange={e=>setAuto(k,e.target.checked)}
                    style={{ width:16, height:16, accentColor:'#6366f1', cursor:'pointer' }} />
                  {l}確認済
                </label>
              ))}
            </div>
            <Field label="BANT メモ">
              <textarea value={form.bantMemo||''} onChange={e=>setAuto('bantMemo',e.target.value)}
                rows={2} style={{ ...S.input, resize:'vertical' }}
                onFocus={e=>e.target.style.borderColor='#6366f1'} onBlur={e=>e.target.style.borderColor='#e5e7eb'} />
            </Field>
          </div>

          <SectionHeader title="RPO費用" sub="RPO費用サブテーブル" />
          <SubTableEditor
            dealId={deal.id} path="rpo-costs"
            columns={[
              { key:'initial_cost', label:'初期費用', type:'number' },
              { key:'monthly_cost', label:'月額費用', type:'number' },
              { key:'months', label:'利用月数', type:'number' },
              { key:'total_cost', label:'合計費用', type:'number' },
            ]}
          />

          <SectionHeader title="採用費用" sub="採用費用サブテーブル" />
          <SubTableEditor
            dealId={deal.id} path="hiring-costs"
            columns={[
              { key:'hire_count', label:'採用人数', type:'number' },
              { key:'unit_price', label:'採用単価', type:'number' },
              { key:'media_cost', label:'媒体費用', type:'number' },
              { key:'rpo_cost', label:'RPO費用', type:'number' },
              { key:'total_cost', label:'合計費用', type:'number' },
            ]}
          />

          <SectionHeader title="人件費" sub="人件費サブテーブル" />
          <SubTableEditor
            dealId={deal.id} path="labor-costs"
            columns={[
              { key:'labor_cost', label:'人件費/月', type:'number' },
              { key:'months', label:'利用月数', type:'number' },
              { key:'total_cost', label:'合計人件費', type:'number' },
            ]}
          />

          <SectionHeader title="応募予測" sub="媒体ごとの応募予測" />
          <SubTableEditor
            dealId={deal.id} path="app-forecasts"
            columns={[
              { key:'position_name', label:'採用ポジション', type:'text' },
              { key:'media_name', label:'媒体', type:'text' },
              { key:'scout_count', label:'スカウト数', type:'number' },
              { key:'application_count', label:'応募数', type:'number' },
              { key:'offer_count', label:'内定数', type:'number' },
            ]}
          />

          <SectionHeader title="メモ" sub="商談に関する詳細メモ・ヒアリング内容など" />
          <textarea value={form.memo||''} onChange={e=>setAuto('memo',e.target.value)}
            rows={8} style={{ ...S.input, resize:'vertical', lineHeight:1.7 }}
            onFocus={e=>e.target.style.borderColor='#6366f1'} onBlur={e=>e.target.style.borderColor='#e5e7eb'} />

          {/* 案件カスタムフィールド */}
          {customFields.length > 0 && (
            <div style={{ marginTop:18, paddingTop:14, borderTop:'1px dashed #e5e7eb' }}>
              <div style={{ fontSize:'0.78rem', fontWeight:700, color:'#94a3b8', marginBottom:12 }}>カスタムフィールド</div>
              <div style={S.row2}>
                {customFields.map(f => (
                  <Field key={f.field_key} label={f.field_label}>
                    <CustomFieldInput field={f}
                      value={(form.data||{})[f.field_key]}
                      onChange={v => setAuto('data', { ...(form.data||{}), [f.field_key]: v })} />
                  </Field>
                ))}
              </div>
            </div>
          )}
        </div>

        <div style={{ padding:'12px 24px', borderTop:'1px solid #f3f4f6', display:'flex', justifyContent:'space-between', alignItems:'center', flexShrink:0 }}>
          <span style={{ fontSize:'0.75rem', color:'#9ca3af' }}>
            {deal.updated_at ? `最終更新: ${formatUpdatedAt(deal.updated_at)}` : ''}
          </span>
          <div style={{ display:'flex', gap:8 }}>
            <button style={{ padding:'8px 20px', border:'1.5px solid #e5e7eb', borderRadius:8, background:'#fff', color:'#6b7280', fontSize:'0.85rem', fontWeight:600, cursor:'pointer' }}
              onClick={()=>{ clearTimeout(autoSaveTimer.current); setEditing(false); }}>キャンセル</button>
            <button style={{ padding:'8px 22px', border:'none', borderRadius:8, background:'#1e293b', color:'#fff', fontSize:'0.85rem', fontWeight:700, cursor:'pointer' }}
              onClick={()=>save()} disabled={saving}>
              {saving ? '保存中...' : '✓ 保存して閉じる'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );

  return <>{card}{editModal}</>;
}

// ─── CustomerDetail ──────────────────────────────────────
export default function CustomerDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [customer, setCustomer] = useState(null);
  const [deals, setDeals] = useState([]);
  const [meta, setMeta] = useState(null);
  const [members, setMembers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [contacts, setContacts] = useState([]);
  const [activitySettings, setActivitySettings] = useState(null);
  const [editingContact, setEditingContact] = useState(null); // null | 'new' | contact object
  const [contactForm, setContactForm] = useState({});
  const [contactSaving, setContactSaving] = useState(false);
  const [editingCustomer, setEditingCustomer] = useState(false);
  const [custForm, setCustForm] = useState({});
  const [custSaving, setCustSaving] = useState(false);
  const [custSavedAt, setCustSavedAt] = useState(null);
  const [showDealModal, setShowDealModal] = useState(false);
  const [dealForm, setDealForm] = useState({ name:'', yomi:'アポ化前' });
  const [saving, setSaving] = useState(false);
  const [customFields, setCustomFields] = useState({ customer:[], deal:[] });
  const [fieldOptions, setFieldOptions] = useState({});

  useEffect(() => {
    api.crmActivitySettings().then(setActivitySettings).catch(() => {});
    api.crmCustomFields('customer').then(r => setCustomFields(p => ({ ...p, customer: r.fields||[] }))).catch(()=>{});
    api.crmCustomFields('deal').then(r => setCustomFields(p => ({ ...p, deal: r.fields||[] }))).catch(()=>{});
    api.crmFieldOptions().then(r => setFieldOptions(r.options || {})).catch(() => {});

    Promise.all([
      api.crmGetCustomer(id),
      api.crmCustomers(''),
      api.workloadTeams().catch(()=>({ teams:[] })),
      fetch(`/api/crm/customers/${id}/contacts`, { credentials:'include' }).then(r=>r.json()).catch(()=>({ contacts:[] })),
    ]).then(([r, mr, tr, cr]) => {
      setCustomer(r.customer);
      setCustForm(r.customer);
      setDeals(r.deals || []);
      setContacts(cr.contacts || []);
      if (mr.meta) setMeta(mr.meta);
      const teams = tr.teams || [];
      const leaf = teams.find(t=>t.parent_id) || teams[0];
      if (leaf) api.workloadUsers(leaf.id).then(ur => setMembers(ur.members||[])).catch(()=>{});
    }).catch(()=>{}).finally(()=>setLoading(false));
  }, [id]);

  const handleSaveContact = async () => {
    setContactSaving(true);
    try {
      if (editingContact === 'new') {
        const res = await fetch(`/api/crm/customers/${id}/contacts`, {
          method: 'POST', credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(contactForm),
        });
        const data = await res.json();
        setContacts(prev => [...prev, data.contact]);
      } else {
        const res = await fetch(`/api/crm/contacts/${editingContact.id}`, {
          method: 'PATCH', credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(contactForm),
        });
        const data = await res.json();
        setContacts(prev => prev.map(c => c.id === data.contact.id ? data.contact : c));
      }
      setEditingContact(null);
    } catch { alert('保存に失敗しました'); }
    finally { setContactSaving(false); }
  };

  const handleDeleteContact = async (contactId) => {
    if (!window.confirm('この担当者を削除しますか？')) return;
    await fetch(`/api/crm/contacts/${contactId}`, { method: 'DELETE', credentials: 'include' });
    setContacts(prev => prev.filter(c => c.id !== contactId));
  };

  const handleSaveCustomer = async () => {
    setCustSaving(true);
    try {
      const r = await api.crmUpdateCustomer(id, {
        name: custForm.name, industry: custForm.industry, prefecture: custForm.prefecture,
        employeeCount: custForm.employee_count, website: custForm.website, memo: custForm.memo,
        inflowDate: custForm.inflow_date || null,
        inflowSource: custForm.inflow_source || null,
        nameShort: custForm.name_short || null,
        competitors: custForm.competitors || [],
        businessDescription: custForm.business_description || null,
        postalCode: custForm.postal_code || null,
        address: custForm.address || null,
        serviceLpUrl1: custForm.service_lp_url1 || null,
        serviceLpUrl2: custForm.service_lp_url2 || null,
        data: custForm.data || {},
      });
      setCustomer(r.customer);
      setCustSavedAt(new Date().toLocaleTimeString('ja-JP'));
    } catch { alert('保存に失敗しました'); }
    finally { setCustSaving(false); }
  };

  const handleCreateDeal = async () => {
    if (!dealForm.name.trim()) return;
    setSaving(true);
    try {
      const r = await api.crmCreateDeal({ ...dealForm, customerId: id });
      setDeals(prev => [r.deal, ...prev]);
      setShowDealModal(false);
      setDealForm({ name:'', yomi:'アポ化前' });
    } catch { alert('作成に失敗しました'); }
    finally { setSaving(false); }
  };

  const handleUpdateDeal = (updated) => setDeals(prev => prev.map(d => d.id===updated.id ? {...updated, data: updated.data||{}} : d));
  const handleDeleteDeal = async (dealId) => {
    await api.crmDeleteDeal(dealId).catch(()=>{});
    setDeals(prev => prev.filter(d => d.id!==dealId));
  };

  if (loading) return <div className="page-loading">読み込み中...</div>;
  if (!customer) return <div className="page-loading">顧客が見つかりません</div>;

  return (
    <div className="rpo-page">
      {/* 顧客ヘッダー */}
      <div style={{ marginBottom:24 }}>
        <button className="btn-back-inline" onClick={()=>navigate('/crm?tab=customers')} style={{ marginBottom:8 }}>← 顧客一覧</button>
        {!editingCustomer && (
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start' }}>
            <div>
              <h1 style={{ margin:'0 0 6px', fontSize:'1.6rem', fontWeight:800, color:'#111827', letterSpacing:'-0.02em' }}>{customer.name}</h1>
              <div style={{ display:'flex', gap:10, flexWrap:'wrap', alignItems:'center', fontSize:'0.83rem', color:'#6b7280' }}>
                {customer.industry && <span style={{ background:'#f1f5f9', padding:'2px 8px', borderRadius:99, fontWeight:500 }}>{customer.industry}</span>}
                {customer.prefecture && <span>{customer.prefecture}</span>}
                {customer.employee_count && <span>{customer.employee_count}名</span>}
                {customer.inflow_source && <span style={{ background:'#eff6ff', color:'#2563eb', padding:'2px 8px', borderRadius:99, fontWeight:500 }}>{customer.inflow_source}</span>}
                {customer.inflow_date && <span>流入: {customer.inflow_date}</span>}
                {customer.website && <a href={customer.website} target="_blank" rel="noreferrer" style={{ color:'#6366f1' }}>{customer.website}</a>}
              </div>
              {customer.business_description && <p style={{ margin:'6px 0 0', fontSize:'0.83rem', color:'#374151', lineHeight:1.6 }}>{customer.business_description}</p>}
              {customer.memo && <p style={{ margin:'4px 0 0', fontSize:'0.83rem', color:'#6b7280', lineHeight:1.6 }}>{customer.memo}</p>}
              {Array.isArray(customer.competitors) && customer.competitors.length > 0 && (
                <div style={{ marginTop:4, display:'flex', gap:4, flexWrap:'wrap' }}>
                  <span style={{ fontSize:'0.75rem', color:'#9ca3af' }}>競合:</span>
                  {customer.competitors.map(c => <span key={c} style={{ fontSize:'0.75rem', background:'#fef3c7', color:'#d97706', padding:'1px 7px', borderRadius:99 }}>{c}</span>)}
                </div>
              )}
            </div>
            <button style={{ padding:'7px 16px', border:'1.5px solid #e5e7eb', borderRadius:8, background:'#fff', color:'#374151', fontSize:'0.83rem', fontWeight:600, cursor:'pointer', flexShrink:0 }}
              onClick={() => { setCustForm(customer); setCustSavedAt(null); setEditingCustomer(true); }}>
              編集
            </button>
          </div>
        )}
      </div>

      {/* 担当者情報 */}
      <div style={{ marginBottom:24, border:'1px solid #e5e7eb', borderRadius:10, overflow:'hidden' }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', padding:'10px 16px', background:'#f8fafc', borderBottom:'1px solid #e5e7eb' }}>
          <span style={{ fontWeight:700, fontSize:'0.9rem', color:'#374151' }}>担当者情報 <span style={{ fontSize:'0.8rem', color:'#9ca3af', fontWeight:400 }}>{contacts.length}名</span></span>
          <button style={{ fontSize:12, padding:'4px 12px', border:'1px solid #e5e7eb', borderRadius:6, background:'#fff', cursor:'pointer' }}
            onClick={() => { setContactForm({}); setEditingContact('new'); }}>＋ 追加</button>
        </div>
        {contacts.length === 0 ? (
          <div style={{ padding:'16px', textAlign:'center', color:'#9ca3af', fontSize:'0.83rem' }}>担当者情報がありません</div>
        ) : (
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.83rem' }}>
            <thead>
              <tr style={{ background:'#f9fafb' }}>
                {['氏名','役職','部署','メール','電話',''].map(h => (
                  <th key={h} style={{ padding:'7px 12px', textAlign:'left', fontWeight:600, color:'#6b7280', borderBottom:'1px solid #f3f4f6', whiteSpace:'nowrap' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {contacts.map(c => (
                <tr key={c.id} style={{ borderBottom:'1px solid #f9fafb' }}>
                  <td style={{ padding:'8px 12px', fontWeight:600 }}>
                    {c.last_name}{c.first_name}
                    {c.furigana && <div style={{ fontSize:'0.72rem', color:'#9ca3af' }}>{c.furigana}</div>}
                    {c.sales_prohibited && <span style={{ fontSize:'0.7rem', background:'#fee2e2', color:'#ef4444', padding:'0 5px', borderRadius:99, marginLeft:4 }}>営業禁止</span>}
                  </td>
                  <td style={{ padding:'8px 12px', color:'#6b7280' }}>{c.position_title || '—'}</td>
                  <td style={{ padding:'8px 12px', color:'#6b7280' }}>{c.department || '—'}</td>
                  <td style={{ padding:'8px 12px' }}>{c.email ? <a href={`mailto:${c.email}`} style={{ color:'#6366f1' }}>{c.email}</a> : '—'}</td>
                  <td style={{ padding:'8px 12px', color:'#6b7280' }}>{c.phone || '—'}</td>
                  <td style={{ padding:'8px 12px', textAlign:'right' }}>
                    <button onClick={() => { setContactForm(c); setEditingContact(c); }}
                      style={{ fontSize:11, color:'#6b7280', background:'none', border:'1px solid #e5e7eb', borderRadius:5, padding:'2px 8px', cursor:'pointer', marginRight:4 }}>編集</button>
                    <button onClick={() => handleDeleteContact(c.id)}
                      style={{ fontSize:11, color:'#ef4444', background:'none', border:'1px solid #fee2e2', borderRadius:5, padding:'2px 8px', cursor:'pointer' }}>削除</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* 担当者編集モーダル */}
      {editingContact && (
        <div className="modal-overlay" onClick={() => setEditingContact(null)}>
          <div className="modal-content" onClick={e=>e.stopPropagation()} style={{ maxWidth:480, padding:0, overflow:'hidden' }}>
            <div style={{ padding:'16px 24px', borderBottom:'1px solid #f3f4f6' }}>
              <div style={{ fontSize:'0.75rem', color:'#9ca3af', marginBottom:2 }}>担当者情報</div>
              <div style={{ fontWeight:800, fontSize:'1rem' }}>{editingContact === 'new' ? '担当者を追加' : '担当者を編集'}</div>
            </div>
            <div style={{ padding:'20px 24px', display:'flex', flexDirection:'column', gap:12 }}>
              <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10 }}>
                <Field label="姓"><InputF value={contactForm.lastName || contactForm.last_name || ''} onChange={e=>setContactForm(p=>({...p,lastName:e.target.value}))} /></Field>
                <Field label="名"><InputF value={contactForm.firstName || contactForm.first_name || ''} onChange={e=>setContactForm(p=>({...p,firstName:e.target.value}))} /></Field>
                <Field label="ふりがな"><InputF value={contactForm.furigana||''} onChange={e=>setContactForm(p=>({...p,furigana:e.target.value}))} /></Field>
                <Field label="役職"><InputF value={contactForm.positionTitle || contactForm.position_title || ''} onChange={e=>setContactForm(p=>({...p,positionTitle:e.target.value}))} /></Field>
                <Field label="部署"><InputF value={contactForm.department||''} onChange={e=>setContactForm(p=>({...p,department:e.target.value}))} /></Field>
                <Field label="メール"><InputF type="email" value={contactForm.email||''} onChange={e=>setContactForm(p=>({...p,email:e.target.value}))} /></Field>
                <Field label="電話"><InputF value={contactForm.phone||''} onChange={e=>setContactForm(p=>({...p,phone:e.target.value}))} /></Field>
              </div>
              <Field label="備考"><InputF value={contactForm.memo||''} onChange={e=>setContactForm(p=>({...p,memo:e.target.value}))} /></Field>
              <label style={{ display:'flex', alignItems:'center', gap:8, cursor:'pointer', fontSize:'0.83rem' }}>
                <input type="checkbox" checked={!!(contactForm.salesProhibited ?? contactForm.sales_prohibited)}
                  onChange={e=>setContactForm(p=>({...p,salesProhibited:e.target.checked}))} />
                営業禁止
              </label>
            </div>
            <div style={{ padding:'12px 24px', borderTop:'1px solid #f3f4f6', display:'flex', justifyContent:'flex-end', gap:8 }}>
              <button style={{ padding:'7px 18px', border:'1.5px solid #e5e7eb', borderRadius:8, background:'#fff', color:'#6b7280', fontSize:'0.83rem', cursor:'pointer' }}
                onClick={() => setEditingContact(null)}>キャンセル</button>
              <button style={{ padding:'7px 20px', border:'none', borderRadius:8, background:'#1e293b', color:'#fff', fontSize:'0.83rem', fontWeight:700, cursor:'pointer' }}
                onClick={handleSaveContact} disabled={contactSaving}>{contactSaving ? '保存中...' : '保存'}</button>
            </div>
          </div>
        </div>
      )}

      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:14 }}>
        <h2 style={{ margin:0, fontSize:'1rem', fontWeight:700, color:'#374151' }}>
          案件 <span style={{ fontSize:'0.85rem', color:'#9ca3af', fontWeight:400 }}>{deals.length}件</span>
        </h2>
        <button style={{ padding:'7px 16px', border:'none', borderRadius:8, background:'#1e293b', color:'#fff', fontSize:'0.83rem', fontWeight:700, cursor:'pointer' }}
          onClick={() => setShowDealModal(true)}>
          ＋ 案件を追加
        </button>
      </div>

      {deals.length === 0
        ? <p style={{ color:'#9ca3af', textAlign:'center', padding:32 }}>商談がありません</p>
        : deals.map(deal => (
          <DealCard key={deal.id} deal={{...deal, data: deal.data||{}}} meta={meta} members={members}
            onUpdate={handleUpdateDeal} onDelete={handleDeleteDeal} activitySettings={activitySettings}
            customFields={customFields.deal} fieldOptions={fieldOptions} />
        ))
      }

      {/* 顧客編集モーダル */}
      {editingCustomer && (
        <div className="modal-overlay" onClick={() => setEditingCustomer(false)}>
          <div className="modal-content" onClick={e=>e.stopPropagation()} style={{ maxWidth:580, padding:0, overflow:'hidden', display:'flex', flexDirection:'column', maxHeight:'80vh' }}>
            {/* モーダルヘッダー */}
            <div style={{ padding:'16px 24px', borderBottom:'1px solid #f3f4f6', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
              <div>
                <div style={{ fontSize:'0.75rem', color:'#9ca3af', marginBottom:2 }}>顧客を編集</div>
                <div style={{ fontWeight:800, fontSize:'1rem', color:'#111827' }}>{customer.name}</div>
              </div>
              <div style={{ display:'flex', alignItems:'center', gap:12 }}>
                {custSavedAt && (
                  <span style={{ fontSize:'0.75rem', color:'#10b981', fontWeight:600, display:'flex', alignItems:'center', gap:5 }}>
                    <span style={{ width:7, height:7, borderRadius:'50%', background:'#10b981', display:'inline-block' }} />
                    {custSavedAt} に保存済み
                  </span>
                )}
                <button onClick={() => setEditingCustomer(false)} style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:18, lineHeight:1, padding:4 }}>×</button>
              </div>
            </div>

            <div style={{ flex:1, overflowY:'auto', padding:'20px 24px' }}>
              <SectionHeader title="基本情報" sub="会社の基本情報" />
              <div style={{ marginBottom:14 }}>
                <Field label="会社名" required>
                  <InputF value={custForm.name} onChange={e=>setCustForm(p=>({...p,name:e.target.value}))} />
                </Field>
              </div>
              <div style={{ ...S.row2, marginBottom:14 }}>
                <Field label="会社名（株式会社抜き）"><InputF value={custForm.name_short||''} onChange={e=>setCustForm(p=>({...p,name_short:e.target.value}))} /></Field>
                <Field label="業界"><SelectF value={custForm.industry} onChange={e=>setCustForm(p=>({...p,industry:e.target.value}))} options={fieldOptions.industry || INDUSTRIES} /></Field>
                <Field label="都道府県"><SelectF value={custForm.prefecture} onChange={e=>setCustForm(p=>({...p,prefecture:e.target.value}))} options={PREFECTURES} /></Field>
                <Field label="従業員数"><SelectF value={custForm.employee_count} onChange={e=>setCustForm(p=>({...p,employee_count:e.target.value}))} options={EMP_COUNTS} /></Field>
              </div>
              <div style={{ ...S.row2, marginBottom:14 }}>
                <Field label="流入日"><InputF type="date" value={custForm.inflow_date||''} onChange={e=>setCustForm(p=>({...p,inflow_date:e.target.value}))} /></Field>
                <Field label="流入経路">
                  {fieldOptions.inflow_source?.length > 0
                    ? <SelectF value={custForm.inflow_source||''} onChange={e=>setCustForm(p=>({...p,inflow_source:e.target.value}))} options={fieldOptions.inflow_source} />
                    : <InputF value={custForm.inflow_source||''} onChange={e=>setCustForm(p=>({...p,inflow_source:e.target.value}))} placeholder="例: 問い合わせ・紹介" />}
                </Field>
                <Field label="Webサイト"><InputF value={custForm.website||''} onChange={e=>setCustForm(p=>({...p,website:e.target.value}))} placeholder="https://" /></Field>
                <Field label="サービスLP URL①"><InputF value={custForm.service_lp_url1||''} onChange={e=>setCustForm(p=>({...p,service_lp_url1:e.target.value}))} placeholder="https://" /></Field>
              </div>
              <div style={{ ...S.row2, marginBottom:14 }}>
                <Field label="郵便番号"><InputF value={custForm.postal_code||''} onChange={e=>setCustForm(p=>({...p,postal_code:e.target.value}))} placeholder="000-0000" /></Field>
                <Field label="住所"><InputF value={custForm.address||''} onChange={e=>setCustForm(p=>({...p,address:e.target.value}))} /></Field>
                <Field label="サービスLP URL②"><InputF value={custForm.service_lp_url2||''} onChange={e=>setCustForm(p=>({...p,service_lp_url2:e.target.value}))} placeholder="https://" /></Field>
              </div>
              <div style={{ marginBottom:14 }}>
                <div style={{ fontSize:'0.75rem', fontWeight:600, color:'#374151', marginBottom:4 }}>競合</div>
                <div style={{ display:'flex', gap:8 }}>
                  {['研修','SaaS','採用'].map(c => (
                    <label key={c} style={{ display:'flex', alignItems:'center', gap:5, cursor:'pointer', fontSize:'0.83rem' }}>
                      <input type="checkbox"
                        checked={(custForm.competitors||[]).includes(c)}
                        onChange={e => setCustForm(p => ({
                          ...p,
                          competitors: e.target.checked
                            ? [...(p.competitors||[]), c]
                            : (p.competitors||[]).filter(x=>x!==c)
                        }))}
                      /> {c}
                    </label>
                  ))}
                </div>
              </div>
              <div style={{ marginBottom:14 }}>
                <Field label="事業内容">
                  <textarea value={custForm.business_description||''} onChange={e=>setCustForm(p=>({...p,business_description:e.target.value}))}
                    rows={2} style={{ ...S.input, resize:'vertical' }} />
                </Field>
              </div>
              <Field label="メモ">
                <textarea value={custForm.memo||''} onChange={e=>setCustForm(p=>({...p,memo:e.target.value}))}
                  rows={3} style={{ ...S.input, resize:'vertical' }}
                  onFocus={e=>e.target.style.borderColor='#6366f1'}
                  onBlur={e=>e.target.style.borderColor='#e5e7eb'}
                />
              </Field>

              {/* カスタムフィールド */}
              {customFields.customer.length > 0 && (
                <div style={{ marginTop:18, paddingTop:14, borderTop:'1px dashed #e5e7eb' }}>
                  <div style={{ fontSize:'0.78rem', fontWeight:700, color:'#94a3b8', marginBottom:12 }}>カスタムフィールド</div>
                  <div style={S.row2}>
                    {customFields.customer.map(f => (
                      <Field key={f.field_key} label={f.field_label}>
                        <CustomFieldInput field={f}
                          value={(custForm.data||{})[f.field_key]}
                          onChange={v => setCustForm(p => ({ ...p, data: { ...(p.data||{}), [f.field_key]: v } }))} />
                      </Field>
                    ))}
                  </div>
                </div>
              )}
            </div>

            <div style={{ padding:'12px 24px', borderTop:'1px solid #f3f4f6', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
              <span style={{ fontSize:'0.75rem', color:'#9ca3af' }}>
                {customer.updated_at ? `最終更新: ${formatUpdatedAt(customer.updated_at)}` : ''}
              </span>
              <div style={{ display:'flex', gap:8 }}>
                <button style={{ padding:'8px 20px', border:'1.5px solid #e5e7eb', borderRadius:8, background:'#fff', color:'#6b7280', fontSize:'0.85rem', fontWeight:600, cursor:'pointer' }}
                  onClick={() => setEditingCustomer(false)}>キャンセル</button>
                <button style={{ padding:'8px 22px', border:'none', borderRadius:8, background:'#1e293b', color:'#fff', fontSize:'0.85rem', fontWeight:700, cursor:'pointer' }}
                  onClick={handleSaveCustomer} disabled={custSaving}>
                  {custSaving ? '保存中...' : '✓ 保存して閉じる'}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* 商談追加モーダル */}
      {showDealModal && (
        <div className="modal-overlay" onClick={()=>setShowDealModal(false)}>
          <div className="modal-content" onClick={e=>e.stopPropagation()} style={{ maxWidth:440, padding:0, overflow:'hidden' }}>
            <div style={{ padding:'16px 24px', borderBottom:'1px solid #f3f4f6' }}>
              <div style={{ fontSize:'0.75rem', color:'#9ca3af', marginBottom:2 }}>商談を追加</div>
              <div style={{ fontWeight:800, fontSize:'1rem', color:'#111827' }}>{customer.name}</div>
            </div>
            <div style={{ padding:'20px 24px' }}>
              <div style={{ marginBottom:14 }}>
                <Field label="商談名" required>
                  <InputF value={dealForm.name} onChange={e=>setDealForm(p=>({...p,name:e.target.value}))} placeholder="例: 2024年4月 月額フルコミット" />
                </Field>
              </div>
              <Field label="初期ヨミ">
                <SelectF value={dealForm.yomi} onChange={e=>setDealForm(p=>({...p,yomi:e.target.value}))} options={YOMI_ORDER} />
              </Field>
            </div>
            <div style={{ padding:'12px 24px', borderTop:'1px solid #f3f4f6', display:'flex', justifyContent:'flex-end', gap:8 }}>
              <button style={{ padding:'8px 20px', border:'1.5px solid #e5e7eb', borderRadius:8, background:'#fff', color:'#6b7280', fontSize:'0.85rem', fontWeight:600, cursor:'pointer' }}
                onClick={()=>setShowDealModal(false)}>キャンセル</button>
              <button style={{ padding:'8px 22px', border:'none', borderRadius:8, background:'#1e293b', color:'#fff', fontSize:'0.85rem', fontWeight:700, cursor:'pointer' }}
                onClick={handleCreateDeal} disabled={!dealForm.name.trim()||saving}>
                {saving?'作成中...':'✓ 作成して閉じる'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
