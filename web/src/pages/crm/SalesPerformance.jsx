import { useEffect, useMemo, useState } from 'react';
import { api } from '../../api/client';

const fmt = (n) => n ? `¥${Math.round(Number(n)).toLocaleString()}` : '—';
const fmtM = (n) => n ? `¥${(Math.round(Number(n)/10000)).toLocaleString()}万` : '—';
const pct = (n) => n != null ? `${Math.round(Number(n)*100)/100}%` : '—';

const ROLE_ORDER = ['役職無し','Lead','Sub Manager','Sub Chief','Chief','Sub Expert','Expert'];

function getUpperRoles(currentRole) {
  const idx = ROLE_ORDER.indexOf(currentRole);
  return {
    plus1: idx >= 0 && idx < ROLE_ORDER.length - 1 ? ROLE_ORDER[idx + 1] : null,
    plus2: idx >= 0 && idx < ROLE_ORDER.length - 2 ? ROLE_ORDER[idx + 2] : null,
  };
}

function calcLines(currentRole, roles, totalCurrMonths) {
  const getRoleTarget = (name) => roles.find(r => r.role_name === name)?.monthly_target || 0;
  const currentTarget = getRoleTarget(currentRole);
  const { plus1, plus2 } = getUpperRoles(currentRole);
  return {
    promotion2: plus2 ? Math.round(getRoleTarget(plus2) * 1.2 * totalCurrMonths) : null,
    promotion1: plus1 ? Math.round(getRoleTarget(plus1) * 1.2 * totalCurrMonths) : null,
    demotion1:  Math.round(currentTarget * 0.7 * totalCurrMonths),
    demotion2:  Math.round(currentTarget * 0.5 * totalCurrMonths),
    currentTarget,
    plus1Role: plus1,
    plus2Role: plus2,
  };
}

function calcRates(prevTotal, currTotal, currentTarget, totalCurrMonths, elapsedMonths, prevTarget) {
  const pt = prevTarget || currentTarget; // 前期役職目標（未設定なら現役職で代替）
  const prevRate = (pt > 0 && totalCurrMonths > 0) ? (prevTotal / (pt * totalCurrMonths)) * 100 : 0;
  const currRate = (currentTarget > 0 && elapsedMonths > 0) ? (currTotal / (currentTarget * elapsedMonths)) * 100 : 0;
  const totalTarget = (pt * totalCurrMonths) + (currentTarget * elapsedMonths);
  const avgRate  = totalTarget > 0 ? ((prevTotal + currTotal) / totalTarget) * 100 : 0;
  return { prevRate, currRate, avgRate, prevTarget: pt };
}

function getJudgment(avgRate) {
  if (avgRate >= 90) return { label: '現状維持以上', color: '#059669' };
  if (avgRate >= 70) return { label: '1段階降格水準', color: '#d97706' };
  return { label: '2段階降格水準', color: '#dc2626' };
}

// ── 個人成績詳細コンポーネント ──
function IndividualDetail({ staff, onClose }) {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [currentRole, setCurrentRole] = useState('');

  useEffect(() => {
    setLoading(true);
    Promise.all([api.crmIndividualPerformance(staff), api.crmRepRoles()])
      .then(([perf, rr]) => {
        setData(perf);
        const stored = (rr.repRoles || []).find(r => r.rep_name === staff);
        setCurrentRole(stored?.role_name || '役職無し');
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [staff]);

  if (loading) return <div style={{ padding:40, textAlign:'center', color:'#9ca3af' }}>読み込み中…</div>;
  if (!data) return null;

  const role = currentRole || '';
  const lines = role ? calcLines(role, data.roles, data.totalCurrMonths) : null;
  const rates = role && lines ? calcRates(data.prevTotal, data.currTotal, lines.currentTarget, data.totalCurrMonths, data.elapsedMonths, data.prevMonthlyTarget) : null;
  const judgment = rates ? getJudgment(rates.avgRate) : null;

  return (
    <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.4)', zIndex:1000, display:'flex', alignItems:'center', justifyContent:'center' }}
      onClick={e => e.target === e.currentTarget && onClose()}>
      <div style={{ background:'#fff', borderRadius:12, width:720, maxWidth:'94vw', maxHeight:'90vh', overflowY:'auto', boxShadow:'0 20px 60px rgba(0,0,0,0.2)' }}>
        {/* ヘッダー */}
        <div style={{ padding:'16px 24px', borderBottom:'1px solid #f3f4f6', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
          <h3 style={{ margin:0, fontSize:'1rem', fontWeight:800 }}>{staff} — 個人成績評価</h3>
          <button onClick={onClose} style={{ background:'none', border:'none', cursor:'pointer', color:'#9ca3af', fontSize:20 }}>✕</button>
        </div>

        <div style={{ padding:'20px 24px' }}>
          {/* 役職表示（設定タブから自動取得） */}
          <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:20, padding:'10px 16px', background:'#f8fafc', borderRadius:8 }}>
            <span style={{ fontSize:'0.82rem', fontWeight:700, color:'#374151' }}>役職</span>
            <span style={{ fontSize:'0.82rem', fontWeight:700, background:'#eef2ff', color:'#4f46e5', borderRadius:20, padding:'3px 14px', border:'1.5px solid #c7d2fe' }}>
              {currentRole || '役職無し'}
            </span>
            <span style={{ fontSize:'0.72rem', color:'#94a3b8', marginLeft:4 }}>※ 設定タブで変更できます</span>
          </div>

          {!role ? (
            <div style={{ textAlign:'center', padding:'32px 0', color:'#9ca3af' }}>上から役職を選択してください</div>
          ) : (
            <>
              {/* 昇格/降格ライン */}
              <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, marginBottom:20 }}>
                {[
                  lines.promotion2 != null && { label:`2段階昇格ライン (→${lines.plus2Role})`, value:lines.promotion2, color:'#7c3aed', bg:'#f5f3ff' },
                  lines.promotion1 != null && { label:`1段階昇格ライン (→${lines.plus1Role})`, value:lines.promotion1, color:'#2563eb', bg:'#eff6ff' },
                  { label:'1段階降格ライン (70%)', value:lines.demotion1, color:'#d97706', bg:'#fffbeb' },
                  { label:'2段階降格ライン (50%)', value:lines.demotion2, color:'#dc2626', bg:'#fef2f2' },
                ].filter(Boolean).map(c => (
                  <div key={c.label} style={{ padding:'12px 16px', background:c.bg, borderRadius:8, border:`1px solid ${c.color}33` }}>
                    <div style={{ fontSize:11, color:c.color, fontWeight:700, marginBottom:4 }}>{c.label}</div>
                    <div style={{ fontSize:'1.1rem', fontWeight:800, color:c.color }}>{fmt(c.value)}</div>
                    <div style={{ fontSize:10, color:'#9ca3af', marginTop:2 }}>月間目標 × 係数 × {data.totalCurrMonths}ヶ月</div>
                  </div>
                ))}
              </div>

              {/* 実績・達成率 */}
              <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:10, marginBottom:20 }}>
                {[
                  { label:`前期実績 (${data.period.prev_start?.split('T')[0]}〜${data.period.prev_end?.split('T')[0]})`, value:data.prevTotal, rate:rates.prevRate },
                  { label:`今期実績 (${data.elapsedMonths}ヶ月経過)`, value:data.currTotal, rate:rates.currRate },
                  { label:'2期平均達成率', value:null, rate:rates.avgRate, judgment },
                ].map(c => (
                  <div key={c.label} style={{ padding:'12px 16px', background:'#f8fafc', borderRadius:8, border:'1px solid #e5e7eb' }}>
                    <div style={{ fontSize:11, color:'#9ca3af', fontWeight:600, marginBottom:6 }}>{c.label}</div>
                    {c.value != null && <div style={{ fontSize:'1rem', fontWeight:700, color:'#374151', marginBottom:2 }}>{fmt(c.value)}</div>}
                    <div style={{ fontSize:'1.2rem', fontWeight:800, color: c.rate >= 100?'#059669':c.rate>=90?'#2563eb':c.rate>=70?'#d97706':'#dc2626' }}>
                      {Math.round(c.rate * 100) / 100}%
                    </div>
                    {c.judgment && <div style={{ fontSize:11, fontWeight:700, color:c.judgment.color, marginTop:4 }}>{c.judgment.label}</div>}
                  </div>
                ))}
              </div>

              {/* 月別明細 */}
              <div style={{ border:'1px solid #e5e7eb', borderRadius:8, overflow:'hidden' }}>
                <div style={{ padding:'10px 14px', background:'#f8fafc', borderBottom:'1px solid #f3f4f6', fontWeight:700, fontSize:'0.82rem' }}>今期 月別明細</div>
                <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.82rem' }}>
                  <thead>
                    <tr style={{ background:'#f9fafb' }}>
                      {['月','インセン合計','月間目標達成率'].map((h, i) => (
                        <th key={h} style={{ padding:'7px 14px', textAlign: i===0?'left':'right', color:'#9ca3af', fontWeight:600, borderBottom:'1px solid #f3f4f6' }}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(data.monthlyMap)
                      .filter(([m]) => /^\d{4}-\d{2}$/.test(m))
                      .sort()
                      .map(([month, amount]) => {
                        const [y, mo] = month.split('-');
                        const label = `${y}年${parseInt(mo, 10)}月`;
                        const r = lines.currentTarget > 0 ? (amount / lines.currentTarget) * 100 : 0;
                        const rColor = r >= 100 ? '#059669' : r >= 70 ? '#d97706' : '#dc2626';
                        return (
                          <tr key={month} style={{ borderBottom:'1px solid #f9fafb' }}
                            onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
                            onMouseLeave={e => e.currentTarget.style.background=''}>
                            <td style={{ padding:'8px 14px', fontWeight:600, color:'#111827' }}>{label}</td>
                            <td style={{ padding:'8px 14px', fontWeight:600, textAlign:'right', color:'#374151' }}>{fmt(amount)}</td>
                            <td style={{ padding:'8px 14px', textAlign:'right' }}>
                              <span style={{ fontWeight:700, color:rColor, background:rColor+'14', padding:'2px 10px', borderRadius:99, fontSize:'0.8rem' }}>
                                {Math.round(r)}%
                              </span>
                            </td>
                          </tr>
                        );
                      })}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

const CARD_COLORS = {
  confirmed: { bg: '#f0fdf4', border: '#86efac', text: '#15803d', label: '今月入金確定' },
  high:      { bg: '#eff6ff', border: '#93c5fd', text: '#1d4ed8', label: '今月締結ほぼ確実' },
  medium:    { bg: '#fefce8', border: '#fde047', text: '#854d0e', label: '今月締結多分いける' },
  total:     { bg: '#f8fafc', border: '#cbd5e1', text: '#334155', label: '当月合計' },
};

function SummaryCard({ type, amount, count }) {
  const c = CARD_COLORS[type];
  return (
    <div style={{ flex:'1 1 180px', padding:'16px 20px', background:c.bg, border:`1.5px solid ${c.border}`, borderRadius:10 }}>
      <div style={{ fontSize:'0.75rem', fontWeight:600, color:c.text, marginBottom:6 }}>{c.label}</div>
      <div style={{ fontSize:'1.5rem', fontWeight:800, color:c.text }}>{fmtM(amount)}</div>
      {count != null && <div style={{ fontSize:'0.72rem', color:'#9ca3af', marginTop:3 }}>{count}件</div>}
    </div>
  );
}

export default function SalesPerformance({ embedded }) {
  const now = new Date();
  const defaultMonth = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;

  const [detailStaff, setDetailStaff] = useState(null);
  const [perfStaff, setPerfStaff] = useState([]);
  const [perfStaffLoading, setPerfStaffLoading] = useState(false);

  useEffect(() => {
    if (perfStaff.length > 0) return;
    setPerfStaffLoading(true);
    api.crmPerfStaff().then(r => setPerfStaff(r.staff || [])).catch(() => {}).finally(() => setPerfStaffLoading(false));
  }, []);

  // 表示対象スタッフ（姓で絞り込み）
  const TARGET_STAFF_LAST = ['板金','野村','藤原','山本','萩原','荻原','添田'];
  const filteredPerfStaff = useMemo(() =>
    perfStaff.filter(s => TARGET_STAFF_LAST.some(n => s.name.includes(n)))
      .sort((a,b) => TARGET_STAFF_LAST.findIndex(n=>a.name.includes(n)) - TARGET_STAFF_LAST.findIndex(n=>b.name.includes(n)))
  , [perfStaff]);

  const wrapper = embedded
    ? { padding: '0' }
    : { padding: '24px', maxWidth: 1200, margin: '0 auto' };

  return (
    <div style={wrapper}>
      {detailStaff && <IndividualDetail staff={detailStaff} onClose={() => setDetailStaff(null)} />}

      {/* 個人成績 */}
      {(() => (
        <div>
          {perfStaffLoading ? (
            <div style={{ color:'#9ca3af', padding:24 }}>読み込み中…</div>
          ) : (
            <>
              {/* シンプルカードグリッド */}
              <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(200px, 1fr))', gap:12 }}>
                {filteredPerfStaff.map(s => {
                  const lastName = s.displayName;
                  const roleColor = { 'Expert':'#7c3aed','Sub Expert':'#6d28d9','Chief':'#1d4ed8','Sub Chief':'#0891b2','Sub Manager':'#0e7490','Lead':'#059669','役職無し':'#6b7280' }[s.role] || '#9ca3af';
                  return (
                    <div key={s.name}
                      onClick={() => setDetailStaff(s.name)}
                      style={{ border:'1px solid #e5e7eb', borderRadius:12, background:'#fff', cursor:'pointer', padding:'20px 16px',
                        display:'flex', flexDirection:'column', alignItems:'center', gap:8, transition:'box-shadow 0.15s',
                        ':hover':{ boxShadow:'0 4px 12px rgba(0,0,0,0.08)' } }}
                      onMouseEnter={e => e.currentTarget.style.boxShadow='0 4px 12px rgba(0,0,0,0.08)'}
                      onMouseLeave={e => e.currentTarget.style.boxShadow='none'}>
                      {/* アバター */}
                      {s.avatar_url
                        ? <img src={s.avatar_url} alt={lastName} style={{ width:52, height:52, borderRadius:'50%', objectFit:'cover' }} />
                        : <div style={{ width:52, height:52, borderRadius:'50%', background:'#f3f4f6', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'1.2rem', fontWeight:700, color:'#6b7280' }}>
                            {lastName[0]}
                          </div>
                      }
                      {/* 名前 */}
                      <div style={{ fontWeight:800, fontSize:'1rem', color:'#111827' }}>{lastName}</div>
                      {/* 役職バッジ */}
                      {s.role && (
                        <span style={{ fontSize:11, fontWeight:700, color:roleColor, background:roleColor+'15', padding:'2px 10px', borderRadius:99 }}>
                          {s.role}
                        </span>
                      )}
                      {/* 成績評価ボタン */}
                      <div style={{ fontSize:12, color:'#6366f1', marginTop:4 }}>成績評価を見る →</div>
                    </div>
                  );
                })}
              </div>
            </>
          )}
        </div>
      ))()}

    </div>
  );
}
