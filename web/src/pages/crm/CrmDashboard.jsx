import { useCallback, useEffect, useState } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { api } from '../../api/client';

// ── 定数 ────────────────────────────────────────────────
const TARGET_REPS = ['山本 夏乃', '板金 慎太郎', '添田 剛', '萩原 隼人', '藤原 一矢', '野村 尭弘'];

const YOMI_COLOR = {
  'アポ化前':'#cbd5e1','アポ化済商談前':'#94a3b8',
  'E 5％':'#c7d2fe','D 15％':'#a5b4fc',
  'C 30％':'#93c5fd','B 50％':'#60a5fa',
  'A 70％':'#3b82f6','S 90％':'#1d4ed8',
};
const REP_COLORS = ['#6366f1','#0ea5e9','#10b981','#f59e0b','#ef4444','#8b5cf6'];

// ── ユーティリティ ─────────────────────────────────────
const fmtM = n => { if (!n) return '¥0'; const m = Number(n); if (m >= 1e8) return `¥${(m/1e8).toFixed(1)}億`; if (m >= 1e4) return `¥${Math.round(m/1e4).toLocaleString()}万`; return `¥${m.toLocaleString()}`; };
const fmtDate = d => d ? String(d).substring(0,10).replace(/-/g,'/') : '';
const diffPct = (c, p) => p ? Math.round((c - p) / p * 100) : null;

function buildRepTable(repTable) {
  if (!repTable?.length) return [];
  const rows = TARGET_REPS.map(name => {
    const f = repTable.find(r => r.rep === name);
    return f ? { ...f } : { rep: name, wonCount: 0, meetingCount: 0, paymentAmount: 0 };
  });
  const others = repTable.filter(r => !TARGET_REPS.includes(r.rep));
  if (others.length > 0) rows.push({
    rep: 'その他', isOther: true,
    wonCount:      others.reduce((s, r) => s + r.wonCount, 0),
    meetingCount:  others.reduce((s, r) => s + r.meetingCount, 0),
    paymentAmount: others.reduce((s, r) => s + r.paymentAmount, 0),
  });
  return rows;
}

// ── ドリルダウンモーダル ──────────────────────────────
function Drilldown({ rep, type, start, end, onClose }) {
  const [rows, setRows] = useState(null);
  useEffect(() => {
    api.crmDashboardDrilldown({ rep, type, start, end })
      .then(d => setRows(d.rows || []))
      .catch(() => setRows([]));
  }, []);

  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.5)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
      onClick={onClose}>
      <div style={{ background:'#fff', borderRadius:16, width:'min(580px,90vw)', maxHeight:'75vh', overflow:'hidden', display:'flex', flexDirection:'column', boxShadow:'0 25px 50px rgba(0,0,0,0.25)' }}
        onClick={e => e.stopPropagation()}>
        <div style={{ padding:'14px 20px', borderBottom:'1px solid #f1f5f9', display:'flex', justifyContent:'space-between', alignItems:'center', background:'#f8fafc' }}>
          <div style={{ fontWeight:700, color:'#0f172a' }}>
            {rep} — {type === 'payments' ? '入金内訳' : '受注案件'}
          </div>
          <button onClick={onClose} style={{ background:'none', border:'none', cursor:'pointer', color:'#94a3b8', fontSize:20, lineHeight:1 }}>×</button>
        </div>
        <div style={{ overflowY:'auto' }}>
          {rows === null ? <div style={{ padding:32, textAlign:'center', color:'#94a3b8' }}>読み込み中…</div>
          : rows.length === 0 ? <div style={{ padding:32, textAlign:'center', color:'#94a3b8' }}>データなし</div>
          : (
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.82rem' }}>
              <thead>
                <tr style={{ background:'#f8fafc' }}>
                  {type === 'payments'
                    ? ['入金日','会社名','プラン','インセン'].map(h => <th key={h} style={{ padding:'8px 16px', textAlign: h==='インセン'?'right':'left', fontWeight:600, color:'#64748b', borderBottom:'1px solid #f1f5f9' }}>{h}</th>)
                    : ['受注日','顧客名'].map(h => <th key={h} style={{ padding:'8px 16px', textAlign:'left', fontWeight:600, color:'#64748b', borderBottom:'1px solid #f1f5f9' }}>{h}</th>)
                  }
                </tr>
              </thead>
              <tbody>
                {type === 'payments'
                  ? rows.map((r, i) => (
                      <tr key={i} style={{ borderBottom:'1px solid #f8fafc' }}>
                        <td style={{ padding:'8px 16px', color:'#64748b', whiteSpace:'nowrap' }}>
                          {r.payment_date ? String(r.payment_date).substring(5,10).replace('-','/') : '—'}
                        </td>
                        <td style={{ padding:'8px 16px', color:'#0f172a', fontWeight:500 }}>{r.company}</td>
                        <td style={{ padding:'8px 16px', color:'#64748b' }}>{r.plan || '—'}</td>
                        <td style={{ padding:'8px 16px', textAlign:'right', fontWeight:700, color:'#059669' }}>{fmtM(r.incentive_amount)}</td>
                      </tr>
                    ))
                  : rows.map((r, i) => (
                      <tr key={i} style={{ borderBottom:'1px solid #f8fafc' }}>
                        <td style={{ padding:'8px 16px', color:'#64748b', whiteSpace:'nowrap' }}>{fmtDate(r.order_date)}</td>
                        <td style={{ padding:'8px 16px', color:'#0f172a', fontWeight:500 }}>{r.customer_name}</td>
                      </tr>
                    ))
                }
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}

// ── メイン ────────────────────────────────────────────
export default function CrmDashboard() {
  const now = new Date();
  const currentMonth = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;

  const [data, setData]       = useState(null);
  const [summary, setSummary] = useState(null);
  const [trend, setTrend]     = useState([]);
  const [loading, setLoading] = useState(true);
  const [salesUser, setSalesUser] = useState('');
  const [period, setPeriod]   = useState('custom');
  const [customMonth, setCustomMonth] = useState(currentMonth);
  const [drill, setDrill]     = useState(null);
  const [alertOpen, setAlertOpen] = useState(false);

  const load = useCallback(async (u = salesUser, p = period, cm = customMonth) => {
    setLoading(true);
    try {
      const params = { period: p };
      if (u) params.salesUser = u;
      if (p === 'custom' && cm) params.customMonth = cm;
      // 収支見込みは指定月に対応
      const summaryMonth = p === 'custom' && cm ? cm : undefined;
      const [d, s, t] = await Promise.all([
        api.crmDashboard(params),
        api.crmMonthlySummary(summaryMonth),
        api.crmDashboardMonthlyTrend({ months: 6, ...(u ? { salesUser: u } : {}) }),
      ]);
      setData(d); setSummary(s); setTrend(t.rows || []);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { load(); }, []);

  if (loading && !data) return <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'100%', color:'#94a3b8', fontSize:'0.88rem' }}>読み込み中…</div>;
  if (!data) return null;

  const { curr, prev, repTable, yomiBreakdown, overdueAlerts, stagnantAlerts,
          rangeStart, rangeEnd, prevStart, prevEnd } = data;
  const reps = buildRepTable(repTable);
  const periodLabel = period === 'term' ? '今期' : period === 'custom' ? (customMonth ? customMonth.replace('-', '/') : '指定月') : '今月';
  const alertCount = (overdueAlerts?.length || 0) + (stagnantAlerts?.length || 0);

  // 収支見込み計算
  const forecast = summary ? {
    confirmed: summary.totals?.confirmed || 0,
    high:      summary.totals?.high      || 0,
    medium:    summary.totals?.medium    || 0,
    total:     summary.totals?.total     || 0,
    kpi:       summary.totals?.kpi       || 0,
  } : null;
  const forecastMax = forecast ? Math.max(forecast.confirmed, forecast.high, forecast.medium, 1) : 1;
  const kpiRate = forecast?.kpi > 0 ? Math.round(forecast.total / forecast.kpi * 100) : null;

  // トレンドデータ
  const trendData = trend.map(r => ({ month: r.month, amount: Number(r.amount) }));

  return (
    <div style={{ padding:'14px 18px', background:'#f1f5f9', minHeight:'100%', display:'flex', flexDirection:'column', gap:10 }}>

      {/* ── フィルターバー ── */}
      <div style={{ display:'flex', alignItems:'center', gap:10, flexWrap:'wrap' }}>
        <div style={{ display:'flex', background:'#fff', borderRadius:8, padding:3, border:'1px solid #e2e8f0' }}>
          {[['custom','指定月'], ['term','今期']].map(([v, l]) => (
            <button key={v} onClick={() => { setPeriod(v); load(salesUser, v, customMonth); }}
              style={{ padding:'4px 16px', borderRadius:6, border:'none', cursor:'pointer', fontSize:'0.8rem',
                fontWeight: period===v?700:400, background: period===v?'#1e40af':'transparent',
                color: period===v?'#fff':'#64748b', transition:'all 0.15s' }}>
              {l}
            </button>
          ))}
        </div>
        <input type="month" value={customMonth} onChange={e => { setCustomMonth(e.target.value); if (period === 'custom') load(salesUser, 'custom', e.target.value); }}
          style={{ padding:'4px 10px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.8rem', background: period==='custom' ? '#fff' : '#f8fafc', color: period==='custom' ? '#0f172a' : '#94a3b8', outline:'none', cursor: period==='custom' ? 'auto' : 'default' }} />
        <select value={salesUser} onChange={e => { setSalesUser(e.target.value); load(e.target.value, period); }}
          style={{ padding:'5px 12px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.82rem', background:'#fff', cursor:'pointer', color:'#0f172a' }}>
          <option value="">全員</option>
          {TARGET_REPS.map(u => <option key={u} value={u}>{u}</option>)}
        </select>
        <button onClick={() => load()}
          style={{ padding:'5px 12px', border:'1px solid #e2e8f0', borderRadius:8, background:'#fff', color:'#374151', fontSize:'0.8rem', cursor:'pointer' }}>
          更新
        </button>
        <div style={{ fontSize:'0.7rem', color:'#94a3b8', lineHeight:1.6 }}>
          <div>{periodLabel}: {fmtDate(rangeStart)} 〜 {fmtDate(rangeEnd)}</div>
          {period==='term' && prevStart && <div>前期: {fmtDate(prevStart)} 〜 {fmtDate(prevEnd)}</div>}
        </div>
        <div style={{ marginLeft:'auto', display:'flex', gap:10 }}>
          {[
            { label: `${periodLabel}入金額`, value: fmtM(curr.paymentAmount), diff: prev ? diffPct(curr.paymentAmount, prev.paymentAmount) : null, color:'#059669' },
            { label: `${periodLabel}受注件数`, value: `${curr.wonCount}件`, diff: prev ? diffPct(curr.wonCount, prev.wonCount) : null, color:'#1e40af' },
            { label: `${periodLabel}初回商談`, value: `${curr.meetingCount}件`, diff: prev ? diffPct(curr.meetingCount, prev.meetingCount) : null, color:'#d97706' },
          ].map(kpi => (
            <div key={kpi.label} style={{ background:'#fff', borderRadius:8, padding:'5px 14px', border:'1px solid #e2e8f0', textAlign:'center', minWidth:100 }}>
              <div style={{ fontSize:'0.65rem', color:'#94a3b8', marginBottom:2 }}>{kpi.label}</div>
              <div style={{ fontWeight:800, fontSize:'0.95rem', color:kpi.color, display:'flex', alignItems:'center', justifyContent:'center', gap:5 }}>
                {kpi.value}
                {kpi.diff != null && (
                  <span style={{ fontSize:'0.65rem', color: kpi.diff >= 0 ? '#059669' : '#dc2626' }}>
                    {kpi.diff >= 0 ? '▲' : '▼'}{Math.abs(kpi.diff)}%
                  </span>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── 今月収支見込み ── */}
      {forecast && (
        <div style={{ background:'#fff', borderRadius:12, padding:'14px 18px', border:'1px solid #e2e8f0' }}>
          <div style={{ display:'flex', alignItems:'center', gap:16, marginBottom:10 }}>
            <div style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>
              今月収支見込み
              <span style={{ fontSize:'0.68rem', color:'#94a3b8', fontWeight:400, marginLeft:8 }}>{currentMonth.replace('-', '/')}</span>
            </div>
            <div style={{ display:'flex', alignItems:'center', gap:14, marginLeft:'auto' }}>
              <div>
                <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>合計見込み </span>
                <span style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a' }}>{fmtM(forecast.total)}</span>
              </div>
              {kpiRate != null && (
                <div style={{ padding:'4px 12px', borderRadius:20,
                  background: kpiRate >= 100 ? '#f0fdf4' : kpiRate >= 70 ? '#fffbeb' : '#fef2f2',
                  border: `1px solid ${kpiRate >= 100 ? '#86efac' : kpiRate >= 70 ? '#fcd34d' : '#fca5a5'}` }}>
                  <span style={{ fontSize:'0.68rem', color:'#64748b' }}>KPI達成率 </span>
                  <span style={{ fontWeight:800, fontSize:'0.9rem', color: kpiRate >= 100 ? '#059669' : kpiRate >= 70 ? '#d97706' : '#dc2626' }}>
                    {kpiRate}%
                  </span>
                </div>
              )}
            </div>
          </div>
          <div style={{ display:'flex', flexDirection:'column', gap:5 }}>
            {[
              { label:'入金確定', amount: forecast.confirmed, color:'#059669', count: summary.payments?.length },
              { label:'締結ほぼ確実', amount: forecast.high, color:'#1e40af', count: summary.highDeals?.length },
              { label:'締結多分いける', amount: forecast.medium, color:'#d97706', count: summary.mediumDeals?.length },
            ].map(item => (
              <div key={item.label} style={{ display:'flex', alignItems:'center', gap:10 }}>
                <span style={{ fontSize:'0.7rem', color:'#64748b', width:90, flexShrink:0 }}>{item.label}</span>
                <div style={{ flex:1, height:7, background:'#f1f5f9', borderRadius:4, overflow:'hidden' }}>
                  <div style={{ height:'100%', width:`${Math.min(100, (item.amount / forecastMax) * 100)}%`, background:item.color, borderRadius:4, transition:'width 0.5s ease' }} />
                </div>
                <span style={{ fontSize:'0.78rem', fontWeight:700, color:item.color, width:70, textAlign:'right', flexShrink:0 }}>{fmtM(item.amount)}</span>
                {item.count != null && <span style={{ fontSize:'0.68rem', color:'#94a3b8', width:28, flexShrink:0 }}>{item.count}件</span>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── メイングリッド ── */}
      <div style={{ display:'grid', gridTemplateColumns:'1fr 320px', gap:10, flex:1, minHeight:0 }}>

        {/* 担当者テーブル */}
        <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', display:'flex', flexDirection:'column' }}>
          <div style={{ padding:'10px 16px', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
            <span style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>担当者別実績</span>
            <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>入金額・受注件数クリックで内訳</span>
          </div>
          <div style={{ overflowY:'auto' }}>
            <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.8rem' }}>
              <thead style={{ position:'sticky', top:0, zIndex:1 }}>
                <tr style={{ background:'#f8fafc' }}>
                  {[
                    { label:'担当者', sub: null },
                    { label:'入金額', sub: '入金日基準' },
                    { label:'受注件数', sub: '受注日基準' },
                    { label:'初回商談', sub: null },
                  ].map(({ label, sub }) => (
                    <th key={label} style={{ padding:'8px 14px', textAlign: label==='担当者'?'left':'right', fontWeight:600, color:'#64748b', borderBottom:'1px solid #f1f5f9', whiteSpace:'nowrap', fontSize:'0.75rem' }}>
                      <div>{label}</div>
                      {sub && <div style={{ fontSize:'0.62rem', color:'#94a3b8', fontWeight:400 }}>{sub}</div>}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {reps.map(r => (
                  <tr key={r.rep} style={{ borderBottom:'1px solid #f8fafc', transition:'background 0.1s' }}
                    onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
                    onMouseLeave={e => e.currentTarget.style.background=''}>
                    <td style={{ padding:'9px 14px', fontWeight:600, color: r.isOther ? '#94a3b8' : '#0f172a' }}>
                      {r.isOther ? r.rep : r.rep.split(/[\s　]/)[0]}
                      {!r.isOther && <span style={{ fontSize:'0.65rem', color:'#94a3b8', marginLeft:4 }}>{r.rep.split(/[\s　]/)[1] || ''}</span>}
                    </td>
                    <td style={{ padding:'9px 14px', textAlign:'right' }}>
                      {r.isOther
                        ? <span style={{ color:'#94a3b8' }}>{fmtM(r.paymentAmount)}</span>
                        : <button onClick={() => setDrill({ rep: r.rep, type: 'payments' })}
                            style={{ background:'none', border:'none', cursor:'pointer', fontWeight:700, color:'#059669', fontSize:'0.8rem', padding:0, borderBottom:'1px dotted #059669' }}>
                            {fmtM(r.paymentAmount)}
                          </button>}
                    </td>
                    <td style={{ padding:'9px 14px', textAlign:'right' }}>
                      {r.isOther
                        ? <span style={{ color:'#94a3b8' }}>{r.wonCount}件</span>
                        : <button onClick={() => setDrill({ rep: r.rep, type: 'won' })}
                            style={{ background:'none', border:'none', cursor:'pointer', fontWeight:600, color:'#1e40af', fontSize:'0.8rem', padding:0, borderBottom:'1px dotted #1e40af' }}>
                            {r.wonCount}件
                          </button>}
                    </td>
                    <td style={{ padding:'9px 14px', textAlign:'right', color: r.isOther ? '#94a3b8' : '#374151' }}>{r.meetingCount}件</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* 右カラム: チャート + ヨミ */}
        <div style={{ display:'flex', flexDirection:'column', gap:10 }}>

          {/* 月別推移 */}
          <div style={{ background:'#fff', borderRadius:12, padding:'12px 14px', border:'1px solid #e2e8f0', flex:'0 0 auto' }}>
            <div style={{ fontWeight:700, fontSize:'0.82rem', color:'#0f172a', marginBottom:8 }}>入金推移（6ヶ月）</div>
            <ResponsiveContainer width="100%" height={120}>
              <BarChart data={trendData} margin={{ top:0, right:0, left:0, bottom:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                <XAxis dataKey="month" tick={{ fontSize:9, fill:'#94a3b8' }} axisLine={false} tickLine={false} />
                <YAxis tickFormatter={v => fmtM(v)} tick={{ fontSize:9, fill:'#94a3b8' }} axisLine={false} tickLine={false} width={52} />
                <Tooltip formatter={v => [fmtM(v), '入金額']}
                  contentStyle={{ background:'#fff', border:'1px solid #e2e8f0', borderRadius:8, fontSize:11, color:'#0f172a', boxShadow:'0 4px 12px rgba(0,0,0,0.1)' }}
                  labelStyle={{ color:'#64748b', marginBottom:2 }} />
                <Bar dataKey="amount" radius={[3,3,0,0]}>
                  {trendData.map((_, i) => (
                    <Cell key={i} fill={i === trendData.length-1 ? '#1e40af' : '#bfdbfe'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* ヨミ内訳 */}
          <div style={{ background:'#fff', borderRadius:12, padding:'12px 14px', border:'1px solid #e2e8f0', flex:1, minHeight:0 }}>
            <div style={{ fontWeight:700, fontSize:'0.82rem', color:'#0f172a', marginBottom:10 }}>ヨミ内訳（進行中）</div>
            {yomiBreakdown?.length > 0 ? (
              <div style={{ display:'flex', flexDirection:'column', gap:5 }}>
                {(() => {
                  const maxCnt = Math.max(...yomiBreakdown.map(r => r.cnt), 1);
                  return yomiBreakdown.map(r => {
                    const color = YOMI_COLOR[r.yomi] || '#94a3b8';
                    return (
                      <div key={r.yomi} style={{ display:'flex', alignItems:'center', gap:8 }}>
                        <span style={{ fontSize:'0.65rem', color, width:78, flexShrink:0, textAlign:'right', fontWeight:600 }}>{r.yomi}</span>
                        <div style={{ flex:1, height:6, background:'#f1f5f9', borderRadius:3, overflow:'hidden' }}>
                          <div style={{ height:'100%', width:`${(r.cnt/maxCnt)*100}%`, background:color, borderRadius:3, transition:'width 0.4s' }} />
                        </div>
                        <span style={{ fontSize:'0.72rem', fontWeight:700, color, width:22, flexShrink:0, textAlign:'right' }}>{r.cnt}</span>
                      </div>
                    );
                  });
                })()}
              </div>
            ) : <div style={{ color:'#94a3b8', fontSize:'0.78rem', paddingTop:20, textAlign:'center' }}>データなし</div>}
          </div>
        </div>
      </div>

      {/* ── アラート（折りたたみ）── */}
      {alertCount > 0 && (
        <div style={{ background:'#fff', borderRadius:12, border:`1px solid ${alertOpen ? '#e2e8f0' : '#fca5a5'}`, overflow:'hidden' }}>
          <button onClick={() => setAlertOpen(v => !v)}
            style={{ width:'100%', padding:'8px 16px', background:'none', border:'none', cursor:'pointer', display:'flex', alignItems:'center', gap:10, textAlign:'left' }}>
            <span style={{ fontSize:'0.82rem', fontWeight:700, color: alertOpen ? '#0f172a' : '#dc2626' }}>
              {overdueAlerts?.length > 0 && `⚠ 期限切れ ${overdueAlerts.length}件`}
              {overdueAlerts?.length > 0 && stagnantAlerts?.length > 0 && '　'}
              {stagnantAlerts?.length > 0 && `⏸ 停滞中 ${stagnantAlerts.length}件`}
            </span>
            <span style={{ marginLeft:'auto', fontSize:'0.75rem', color:'#94a3b8' }}>{alertOpen ? '▲ 閉じる' : '▼ 開く'}</span>
          </button>
          {alertOpen && (
            <div style={{ padding:'0 16px 12px', display:'grid', gridTemplateColumns:'1fr 1fr', gap:8 }}>
              {overdueAlerts?.length > 0 && (
                <div>
                  <div style={{ fontSize:'0.72rem', fontWeight:600, color:'#dc2626', marginBottom:6 }}>アクション期限切れ</div>
                  <div style={{ display:'flex', flexDirection:'column', gap:3, maxHeight:160, overflowY:'auto' }}>
                    {overdueAlerts.map(d => (
                      <div key={d.id} style={{ fontSize:'0.72rem', padding:'5px 8px', background:'#fef2f2', borderRadius:6 }}>
                        <span style={{ fontWeight:600, color:'#0f172a' }}>{d.customer_name}</span>
                        <span style={{ color:'#94a3b8', marginLeft:6 }}>{fmtDate(d.next_action_date)} · {d.sales_person}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {stagnantAlerts?.length > 0 && (
                <div>
                  <div style={{ fontSize:'0.72rem', fontWeight:600, color:'#b45309', marginBottom:6 }}>停滞中案件</div>
                  <div style={{ display:'flex', flexDirection:'column', gap:3, maxHeight:160, overflowY:'auto' }}>
                    {stagnantAlerts.map(d => (
                      <div key={d.id} style={{ fontSize:'0.72rem', padding:'5px 8px', background:'#fffbeb', borderRadius:6 }}>
                        <span style={{ fontWeight:600, color:'#0f172a' }}>{d.customer_name}</span>
                        <span style={{ color:'#94a3b8', marginLeft:6 }}>{d.days_since_update}日未更新 · {d.sales_person}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {drill && (
        <Drilldown rep={drill.rep} type={drill.type} start={rangeStart} end={rangeEnd} onClose={() => setDrill(null)} />
      )}
    </div>
  );
}
