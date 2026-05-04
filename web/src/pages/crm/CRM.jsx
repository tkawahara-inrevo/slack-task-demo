import { useState, useEffect, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import CustomerList from './CustomerList';
import SalesPerformance from './SalesPerformance';
import CrmDashboard from './CrmDashboard';
import { api } from '../../api/client';

const TARGET_REPS  = ['山本 夏乃', '板金 慎太郎', '萩原 隼人', '藤原 一矢', '野村 尭弘'];
const ROLE_NAMES   = ['役職無し', 'Lead', 'Sub Manager', 'Sub Chief', 'Chief', 'Sub Expert', 'Expert'];
const ROLE_OPTIONS = ['', ...ROLE_NAMES];

const fmt = (n) => n ? `¥${Math.round(Number(n)).toLocaleString()}` : '—';
const YOMI_COLOR = { 'S 90％':'#7c3aed','A 70％':'#1d4ed8','B 50％':'#0891b2','C 30％':'#059669' };

// ── ヨミ管理パネル ──────────────────────────────────────────
function YomiPanel() {
  const [yomiKanri, setYomiKanri] = useState(null);
  const [loading, setLoading] = useState(true);
  const [editingMemo, setEditingMemo] = useState(null);
  const [memoValues, setMemoValues] = useState({});
  const [savingMemo, setSavingMemo] = useState({});

  useEffect(() => {
    api.crmYomiKanri().then(r => {
      setYomiKanri(r);
      const init = {};
      Object.values(r.byStaff).flat().forEach(d => { init[d.id] = d.sales_memo || ''; });
      setMemoValues(init);
    }).catch(() => {}).finally(() => setLoading(false));
  }, []);

  const handleMemoSave = async (dealId) => {
    setSavingMemo(p => ({ ...p, [dealId]: true }));
    try { await api.crmUpdateDeal(dealId, { salesMemo: memoValues[dealId] || '' }); }
    catch (e) { console.error(e); }
    finally { setSavingMemo(p => { const n={...p}; delete n[dealId]; return n; }); setEditingMemo(null); }
  };

  if (loading) return <div style={{ padding:24, textAlign:'center', color:'#94a3b8', fontSize:'0.82rem' }}>読み込み中…</div>;
  if (!yomiKanri) return null;

  return (
    <div style={{ display:'flex', flexDirection:'column', height:'100%' }}>
      <div style={{ padding:'10px 16px', background:'#fff', borderBottom:'1px solid #e2e8f0', flexShrink:0 }}>
        <div style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>ヨミ管理</div>
        <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>
          C以上 進行中 {yomiKanri.total}件
        </div>
      </div>
      <div style={{ flex:1, overflowY:'auto', padding:'8px 12px' }}>
        {Object.entries(yomiKanri.byStaff)
          .filter(([, d]) => d.length > 0)
          .sort((a,b) => b[1].length - a[1].length)
          .map(([staff, sDeals]) => (
            <div key={staff} style={{ marginBottom:10, background:'#fff', borderRadius:10, border:'1px solid #e2e8f0', overflow:'hidden' }}>
              <div style={{ padding:'8px 12px', background:'#f8fafc', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', gap:8 }}>
                <span style={{ fontWeight:700, fontSize:'0.82rem', color:'#374151' }}>{staff.split('/')[0].trim()}</span>
                <span style={{ fontSize:'0.7rem', color:'#94a3b8' }}>{sDeals.length}件</span>
                <span style={{ fontSize:'0.72rem', color:'#059669', marginLeft:'auto' }}>
                  {fmt(sDeals.reduce((s,d) => s + Number(d.monthly_fee||d.initial_fee||0)*1.1, 0))}
                </span>
              </div>
              <div>
                {sDeals.map(d => {
                  const yomiColor = YOMI_COLOR[d.yomi] || '#64748b';
                  return (
                    <div key={d.id} style={{ padding:'7px 12px', borderBottom:'1px solid #f8fafc', display:'grid', gridTemplateColumns:'1fr auto', gap:6, alignItems:'start' }}>
                      <div>
                        <div style={{ fontSize:'0.78rem', fontWeight:600, color:'#0f172a' }}>{d.customer_name}</div>
                        <div style={{ display:'flex', gap:6, marginTop:2, alignItems:'center', flexWrap:'wrap' }}>
                          <span style={{ fontSize:'0.65rem', fontWeight:700, color:yomiColor }}>{d.yomi}</span>
                          {d.contract_type && <span style={{ fontSize:'0.62rem', color:'#94a3b8' }}>{d.contract_type}</span>}
                          {d.conclusion_date && <span style={{ fontSize:'0.62rem', color:'#94a3b8' }}>{d.conclusion_date.split('T')[0]}</span>}
                        </div>
                        {/* メモ */}
                        {editingMemo === d.id ? (
                          <input autoFocus value={memoValues[d.id] ?? ''} onChange={e => setMemoValues(p => ({...p,[d.id]:e.target.value}))}
                            onBlur={() => handleMemoSave(d.id)}
                            onKeyDown={e => { if(e.key==='Enter') handleMemoSave(d.id); if(e.key==='Escape') setEditingMemo(null); }}
                            style={{ fontSize:'0.7rem', padding:'2px 6px', border:'1px solid #6366f1', borderRadius:5, outline:'none', width:'100%', boxSizing:'border-box', marginTop:3 }} />
                        ) : (
                          <div onClick={() => setEditingMemo(d.id)}
                            style={{ fontSize:'0.68rem', color: memoValues[d.id]?'#374151':'#cbd5e1', cursor:'text', marginTop:3,
                              padding:'2px 4px', borderRadius:4, border:'1px solid transparent' }}>
                            {savingMemo[d.id] ? '保存中…' : (memoValues[d.id] || '+ メモ')}
                          </div>
                        )}
                      </div>
                      <span style={{ fontSize:'0.7rem', color:'#059669', fontWeight:600, flexShrink:0 }}>
                        {fmt(Number(d.monthly_fee||d.initial_fee||0)*1.1)}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          ))}
      </div>
    </div>
  );
}

// ── CRM設定 ──────────────────────────────────────────────────
function CrmSettings() {
  const [roleTargetRows, setRoleTargetRows] = useState([]);
  const [roleTargets, setRoleTargets]       = useState({});
  const [repRoles, setRepRoles]             = useState({});
  const [period, setPeriod]                 = useState({ prevStart:'', prevEnd:'', currStart:'', currEnd:'' });
  const [saving, setSaving]                 = useState(false);
  const [notice, setNotice]                 = useState('');

  useEffect(() => {
    Promise.all([api.crmRoleTargets(), api.crmRepRoles(), api.crmPeriodSettings()]).then(([rt, rr, ps]) => {
      const rows = rt.targets || [];
      const merged = ROLE_NAMES.map((name, i) => {
        const ex = rows.find(r => r.role_name === name);
        return { role_name: name, monthly_target: Number(ex?.monthly_target || 0), sort_order: i };
      });
      setRoleTargetRows(merged);
      const rtMap = {};
      for (const r of merged) rtMap[r.role_name] = r.monthly_target;
      setRoleTargets(rtMap);
      const rrMap = {};
      for (const r of (rr.repRoles || [])) rrMap[r.rep_name] = r;
      setRepRoles(rrMap);
      const s = ps.settings || {};
      setPeriod({ prevStart: s.prev_start?.split('T')[0]||'', prevEnd: s.prev_end?.split('T')[0]||'', currStart: s.curr_start?.split('T')[0]||'', currEnd: s.curr_end?.split('T')[0]||'' });
    });
  }, []);

  const getEffective = (rep) => {
    const r = repRoles[rep];
    if (r?.monthly_target_override) return Number(r.monthly_target_override);
    const roleName = r?.role_name || '役職無し';
    return roleTargets[roleName] || 0;
  };

  const setRepRole = (rep, role) => setRepRoles(prev => ({ ...prev, [rep]: { ...(prev[rep]||{rep_name:rep}), role_name: role } }));
  const setOverride = (rep, wan) => setRepRoles(prev => ({ ...prev, [rep]: { ...(prev[rep]||{rep_name:rep}), monthly_target_override: wan===''?null:Number(wan)*10000 } }));
  const setRoleTarget = (roleName, wan) => {
    setRoleTargetRows(prev => prev.map(r => r.role_name===roleName ? {...r, monthly_target: Number(wan)*10000} : r));
    setRoleTargets(prev => ({ ...prev, [roleName]: Number(wan)*10000 }));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const repArr = TARGET_REPS.map(rep => ({ rep_name: rep, role_name: repRoles[rep]?.role_name||'', monthly_target_override: repRoles[rep]?.monthly_target_override||null }));
      const roleArr = roleTargetRows.map((r, i) => ({ role_name: r.role_name, monthly_target: r.monthly_target, sort_order: i }));
      await Promise.all([api.crmRepRolesSave(repArr), api.crmRoleTargetsSave(roleArr), api.crmPeriodSettingsSave({ prevStart:period.prevStart, prevEnd:period.prevEnd, currStart:period.currStart, currEnd:period.currEnd })]);
      setNotice('保存しました');
      setTimeout(() => setNotice(''), 2500);
    } catch { setNotice('保存に失敗しました'); setTimeout(() => setNotice(''), 3000); }
    setSaving(false);
  };

  const teamTotal = TARGET_REPS.reduce((s, rep) => s + getEffective(rep), 0);
  const sectionTitle = (label, sub) => (
    <div style={{ marginBottom:12 }}>
      <div style={{ fontWeight:700, fontSize:'0.92rem', color:'#0f172a' }}>{label}</div>
      {sub && <div style={{ fontSize:'0.75rem', color:'#94a3b8', marginTop:2 }}>{sub}</div>}
    </div>
  );

  return (
    <div style={{ padding:'28px 32px', maxWidth:640, overflowY:'auto' }}>
      {sectionTitle('集計期間', '前期・今期の開始日と終了日を設定します')}
      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', marginBottom:28 }}>
        {[
          [['prevStart','前期 開始'],['prevEnd','前期 終了']],
          [['currStart','今期 開始'],['currEnd','今期 終了']],
        ].map((row, ri) => (
          <div key={ri} style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:0, borderBottom: ri===0?'1px solid #f1f5f9':'none' }}>
            {row.map(([key, label]) => (
              <div key={key} style={{ padding:'10px 18px', borderRight:key.endsWith('Start')?'1px solid #f1f5f9':'none' }}>
                <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginBottom:4 }}>{label}</div>
                <input type="date" value={period[key]||''} onChange={e => setPeriod(p => ({...p,[key]:e.target.value}))}
                  style={{ width:'100%', padding:'5px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.85rem', outline:'none', color:'#0f172a', boxSizing:'border-box' }} />
              </div>
            ))}
          </div>
        ))}
      </div>

      {sectionTitle('役職別 月次目標', '成績ページの昇降格ラインにも使用されます')}
      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', marginBottom:28 }}>
        {roleTargetRows.map((row, i) => (
          <div key={row.role_name} style={{ display:'flex', alignItems:'center', gap:14, padding:'10px 18px', borderBottom:i<roleTargetRows.length-1?'1px solid #f8fafc':'none' }}>
            <span style={{ flex:1, fontSize:'0.85rem', fontWeight:600, color:'#374151' }}>{row.role_name}</span>
            <div style={{ display:'flex', alignItems:'center', gap:6 }}>
              <input type="number" min="0" step="10"
                value={row.monthly_target>0?Math.round(row.monthly_target/10000):''}
                onChange={e => setRoleTarget(row.role_name, e.target.value)} placeholder="0"
                style={{ width:90, padding:'5px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.88rem', textAlign:'right', outline:'none' }} />
              <span style={{ fontSize:'0.78rem', color:'#64748b' }}>万円 / 月</span>
            </div>
          </div>
        ))}
      </div>

      {sectionTitle('担当者別 役職・KPI目標', '役職未選択は「役職無し」として扱います。例外時のみ手動上書きを使用してください。')}
      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', marginBottom:16 }}>
        <div style={{ display:'grid', gridTemplateColumns:'1fr 130px 130px 80px', gap:10, padding:'8px 18px', background:'#f8fafc', borderBottom:'1px solid #f1f5f9' }}>
          {['担当者','役職','手動上書き','実効目標'].map(h => (
            <span key={h} style={{ fontSize:'0.7rem', color:'#94a3b8', fontWeight:600 }}>{h}</span>
          ))}
        </div>
        {TARGET_REPS.map((rep, i) => {
          const [fam, given] = rep.split(/[\s　]/);
          const roleName = repRoles[rep]?.role_name || '';
          const overrideRaw = repRoles[rep]?.monthly_target_override;
          const overrideWan = overrideRaw ? Math.round(Number(overrideRaw)/10000) : '';
          const effective = getEffective(rep);
          const fromOverride = !!overrideRaw;
          return (
            <div key={rep} style={{ display:'grid', gridTemplateColumns:'1fr 130px 130px 80px', gap:10, padding:'10px 18px', borderBottom:i<TARGET_REPS.length-1?'1px solid #f8fafc':'none', alignItems:'center' }}>
              <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                <span style={{ width:24, height:24, borderRadius:6, background:'#eef2ff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.65rem', fontWeight:800, color:'#4f46e5' }}>{fam?.[0]}</span>
                <span><span style={{ fontWeight:700, color:'#4f46e5', fontSize:'0.7rem' }}>{fam}</span><span style={{ color:'#374151', fontSize:'0.82rem', marginLeft:3 }}>{given}</span></span>
              </div>
              <select value={roleName} onChange={e => setRepRole(rep, e.target.value)}
                style={{ padding:'4px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', color:'#0f172a', outline:'none' }}>
                {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r||'— 未選択 —'}</option>)}
              </select>
              <div style={{ display:'flex', alignItems:'center', gap:4 }}>
                <input type="number" min="0" step="10" value={overrideWan}
                  onChange={e => setOverride(rep, e.target.value)} placeholder={effective>0?`${Math.round(effective/10000)}万`:''}
                  style={{ width:80, padding:'4px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', textAlign:'right', outline:'none' }} />
                <span style={{ fontSize:'0.7rem', color:'#94a3b8' }}>万</span>
              </div>
              <div style={{ textAlign:'right' }}>
                {effective>0
                  ? <span style={{ fontSize:'0.82rem', fontWeight:700, color:fromOverride?'#7c3aed':'#1e40af' }}>{Math.round(effective/10000)}万</span>
                  : <span style={{ fontSize:'0.78rem', color:'#cbd5e1' }}>—</span>}
              </div>
            </div>
          );
        })}
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', padding:'10px 18px', background:'#f8fafc', borderTop:'1px solid #f1f5f9' }}>
          <span style={{ fontSize:'0.78rem', color:'#64748b', fontWeight:600 }}>チーム月次目標合計</span>
          <span style={{ fontSize:'0.92rem', fontWeight:800, color:teamTotal>0?'#1e40af':'#cbd5e1' }}>
            {teamTotal>0?`${Math.round(teamTotal/10000)}万円`:'未設定'}
          </span>
        </div>
      </div>

      <div style={{ display:'flex', alignItems:'center', gap:12 }}>
        <button onClick={handleSave} disabled={saving}
          style={{ padding:'8px 24px', background:saving?'#94a3b8':'#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.85rem', fontWeight:700, cursor:saving?'default':'pointer' }}>
          {saving?'保存中…':'保存'}
        </button>
        {notice && <span style={{ fontSize:'0.8rem', color:notice.includes('失敗')?'#dc2626':'#059669', fontWeight:600 }}>{notice}</span>}
      </div>
    </div>
  );
}

// ── メイン CRM ────────────────────────────────────────────────
export default function CRM() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get('tab') || 'dashboard';
  const setTab = (t) => setSearchParams({ tab: t }, { replace: true });
  const [canViewPerf, setCanViewPerf] = useState(null); // null=loading

  useEffect(() => {
    api.crmPerformanceAccess()
      .then(r => setCanViewPerf(r.allowed))
      .catch(() => setCanViewPerf(false));
  }, []);

  const tabs = [
    { key:'dashboard',   label:'ダッシュボード' },
    { key:'customers',   label:'顧客一覧' },
    ...(canViewPerf ? [{ key:'performance', label:'成績' }] : []),
    { key:'settings',    label:'設定' },
  ];

  return (
    <div style={{ height:'100%', display:'flex', flexDirection:'column' }}>
      {/* タブバー */}
      <div style={{ display:'flex', borderBottom:'1px solid #e5e7eb', background:'#fff', paddingLeft:8, flexShrink:0 }}>
        {tabs.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            style={{ padding:'10px 20px', border:'none', background:'none', cursor:'pointer', fontSize:'0.88rem',
              fontWeight: tab===t.key?700:400, color:tab===t.key?'#1d4ed8':'#6b7280',
              borderBottom: tab===t.key?'2px solid #1d4ed8':'2px solid transparent', transition:'color 0.15s' }}>
            {t.label}
          </button>
        ))}
      </div>

      {/* タブコンテンツ */}
      <div style={{ flex:1, overflow:'hidden', display:'flex', flexDirection:'column' }}>
        {tab === 'dashboard'   && <div style={{ flex:1, overflow:'auto' }}><CrmDashboard /></div>}

        {/* 顧客一覧 + ヨミ管理 分割レイアウト */}
        {tab === 'customers' && (
          <div style={{ flex:1, display:'grid', gridTemplateColumns:'1fr 320px', overflow:'hidden' }}>
            <div style={{ overflow:'hidden', borderRight:'1px solid #e2e8f0', display:'flex', flexDirection:'column' }}>
              <CustomerList />
            </div>
            <div style={{ overflow:'hidden', display:'flex', flexDirection:'column', background:'#f8fafc' }}>
              <YomiPanel />
            </div>
          </div>
        )}

        {tab === 'performance' && canViewPerf && (
          <div style={{ flex:1, overflow:'auto' }}><SalesPerformance embedded /></div>
        )}
        {tab === 'performance' && canViewPerf === false && (
          <div style={{ flex:1, display:'flex', alignItems:'center', justifyContent:'center', color:'#94a3b8', flexDirection:'column', gap:8 }}>
            <div style={{ fontSize:'1.5rem' }}>🔒</div>
            <div style={{ fontWeight:700, color:'#374151' }}>アクセス権限がありません</div>
            <div style={{ fontSize:'0.82rem' }}>管理者またはBC Sub Manager以上のみ閲覧できます</div>
          </div>
        )}

        {tab === 'settings' && <div style={{ flex:1, overflow:'auto' }}><CrmSettings /></div>}
      </div>
    </div>
  );
}
