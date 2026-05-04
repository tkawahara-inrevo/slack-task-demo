import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import Pipeline from './Pipeline';
import CustomerList from './CustomerList';
import SalesPerformance from './SalesPerformance';
import CrmDashboard from './CrmDashboard';
import { api } from '../../api/client';

const TARGET_REPS = ['山本 夏乃', '板金 慎太郎', '萩原 隼人', '藤原 一矢', '野村 尭弘'];
const ROLE_NAMES  = ['役職無し', 'Lead', 'Sub Manager', 'Chief', 'Sub Expert', 'Expert'];
const ROLE_OPTIONS = ['', ...ROLE_NAMES];

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
      setPeriod({
        prevStart: s.prev_start?.split('T')[0] || '',
        prevEnd:   s.prev_end?.split('T')[0]   || '',
        currStart: s.curr_start?.split('T')[0] || '',
        currEnd:   s.curr_end?.split('T')[0]   || '',
      });
    });
  }, []);

  const getEffective = (rep) => {
    const r = repRoles[rep];
    if (r?.monthly_target_override) return Number(r.monthly_target_override);
    const roleName = r?.role_name || '役職無し';
    return roleTargets[roleName] || 0;
  };

  const setRepRole = (rep, role) => setRepRoles(prev => ({
    ...prev, [rep]: { ...(prev[rep] || { rep_name: rep }), role_name: role },
  }));

  const setOverride = (rep, wan) => setRepRoles(prev => ({
    ...prev, [rep]: {
      ...(prev[rep] || { rep_name: rep }),
      monthly_target_override: wan === '' ? null : Number(wan) * 10000,
    },
  }));

  const setRoleTarget = (roleName, wan) => {
    const val = Number(wan) * 10000;
    setRoleTargetRows(prev => prev.map(r => r.role_name === roleName ? { ...r, monthly_target: Number(wan) * 10000 } : r));
    setRoleTargets(prev => ({ ...prev, [roleName]: val }));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const repArr = TARGET_REPS.map(rep => ({
        rep_name: rep,
        role_name: repRoles[rep]?.role_name || '',
        monthly_target_override: repRoles[rep]?.monthly_target_override || null,
      }));
      const roleArr = roleTargetRows.map((r, i) => ({
        role_name: r.role_name,
        monthly_target: r.monthly_target,
        sort_order: i,
      }));
      await Promise.all([
        api.crmRepRolesSave(repArr),
        api.crmRoleTargetsSave(roleArr),
        api.crmPeriodSettingsSave({ prevStart: period.prevStart, prevEnd: period.prevEnd, currStart: period.currStart, currEnd: period.currEnd }),
      ]);
      setNotice('保存しました');
      setTimeout(() => setNotice(''), 2500);
    } catch {
      setNotice('保存に失敗しました');
      setTimeout(() => setNotice(''), 3000);
    }
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
    <div style={{ padding:'28px 32px', maxWidth:640 }}>

      {/* ── 期間設定 ── */}
      {sectionTitle('集計期間', '前期・今期の開始日と終了日を設定します（個人成績の評価期間に使用）')}
      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', marginBottom:28 }}>
        {[
          [['prevStart','前期 開始'],['prevEnd','前期 終了']],
          [['currStart','今期 開始'],['currEnd','今期 終了']],
        ].map((row, ri) => (
          <div key={ri} style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:0, borderBottom: ri === 0 ? '1px solid #f1f5f9' : 'none' }}>
            {row.map(([key, label]) => (
              <div key={key} style={{ padding:'10px 18px', borderRight: key.endsWith('Start') ? '1px solid #f1f5f9' : 'none' }}>
                <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginBottom:4 }}>{label}</div>
                <input type="date" value={period[key] || ''} onChange={e => setPeriod(p => ({ ...p, [key]: e.target.value }))}
                  style={{ width:'100%', padding:'5px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.85rem', outline:'none', color:'#0f172a', boxSizing:'border-box' }} />
              </div>
            ))}
          </div>
        ))}
      </div>

      {/* ── 役職別目標 ── */}
      {sectionTitle('役職別 月次目標', '成績ページの昇降格ラインにも使用されます')}
      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', marginBottom:28 }}>
        {roleTargetRows.map((row, i) => (
          <div key={row.role_name} style={{ display:'flex', alignItems:'center', gap:14, padding:'10px 18px', borderBottom: i < roleTargetRows.length - 1 ? '1px solid #f8fafc' : 'none' }}>
            <span style={{ flex:1, fontSize:'0.85rem', fontWeight:600, color:'#374151' }}>{row.role_name}</span>
            <div style={{ display:'flex', alignItems:'center', gap:6 }}>
              <input type="number" min="0" step="10"
                value={row.monthly_target > 0 ? Math.round(row.monthly_target / 10000) : ''}
                onChange={e => setRoleTarget(row.role_name, e.target.value)}
                placeholder="0"
                style={{ width:90, padding:'5px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.88rem', textAlign:'right', outline:'none', color:'#0f172a' }} />
              <span style={{ fontSize:'0.78rem', color:'#64748b', flexShrink:0 }}>万円 / 月</span>
            </div>
          </div>
        ))}
      </div>

      {/* ── 担当者別設定 ── */}
      {sectionTitle('担当者別 役職・KPI目標', '役職未選択は「役職無し」として扱います。例外時のみ手動上書きを使用してください。')}
      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden', marginBottom:16 }}>
        <div style={{ display:'grid', gridTemplateColumns:'1fr 130px 130px 80px', gap:10, padding:'8px 18px', background:'#f8fafc', borderBottom:'1px solid #f1f5f9' }}>
          {['担当者', '役職', '手動上書き', '実効目標'].map(h => (
            <span key={h} style={{ fontSize:'0.7rem', color:'#94a3b8', fontWeight:600 }}>{h}</span>
          ))}
        </div>

        {TARGET_REPS.map((rep, i) => {
          const [fam, given] = rep.split(/[\s　]/);
          const roleName     = repRoles[rep]?.role_name || '';
          const overrideRaw  = repRoles[rep]?.monthly_target_override;
          const overrideWan  = overrideRaw ? Math.round(Number(overrideRaw) / 10000) : '';
          const effective    = getEffective(rep);
          const fromOverride = !!overrideRaw;
          return (
            <div key={rep} style={{ display:'grid', gridTemplateColumns:'1fr 130px 130px 80px', gap:10, padding:'10px 18px', borderBottom: i < TARGET_REPS.length - 1 ? '1px solid #f8fafc' : 'none', alignItems:'center' }}>
              <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                <span style={{ width:24, height:24, borderRadius:6, background:'#eef2ff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.65rem', fontWeight:800, color:'#4f46e5', flexShrink:0 }}>
                  {fam?.[0]}
                </span>
                <span>
                  <span style={{ fontWeight:700, color:'#4f46e5', fontSize:'0.7rem' }}>{fam}</span>
                  <span style={{ color:'#374151', fontSize:'0.82rem', marginLeft:3 }}>{given}</span>
                </span>
              </div>
              <select value={roleName} onChange={e => setRepRole(rep, e.target.value)}
                style={{ padding:'4px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', color:'#0f172a', outline:'none' }}>
                {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r || '— 未選択 —'}</option>)}
              </select>
              <div style={{ display:'flex', alignItems:'center', gap:4 }}>
                <input type="number" min="0" step="10" value={overrideWan}
                  onChange={e => setOverride(rep, e.target.value)}
                  placeholder={effective > 0 ? `${Math.round(effective / 10000)}万（役職値）` : '未設定'}
                  style={{ width:80, padding:'4px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', textAlign:'right', outline:'none', color:'#0f172a' }} />
                <span style={{ fontSize:'0.7rem', color:'#94a3b8' }}>万</span>
              </div>
              <div style={{ textAlign:'right' }}>
                {effective > 0
                  ? <span style={{ fontSize:'0.82rem', fontWeight:700, color: fromOverride ? '#7c3aed' : '#1e40af' }}>
                      {Math.round(effective / 10000)}万
                    </span>
                  : <span style={{ fontSize:'0.78rem', color:'#cbd5e1' }}>—</span>
                }
              </div>
            </div>
          );
        })}

        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', padding:'10px 18px', background:'#f8fafc', borderTop:'1px solid #f1f5f9' }}>
          <span style={{ fontSize:'0.78rem', color:'#64748b', fontWeight:600 }}>チーム月次目標合計</span>
          <span style={{ fontSize:'0.92rem', fontWeight:800, color: teamTotal > 0 ? '#1e40af' : '#cbd5e1' }}>
            {teamTotal > 0 ? `${Math.round(teamTotal / 10000)}万円` : '未設定'}
          </span>
        </div>
      </div>

      <div style={{ display:'flex', alignItems:'center', gap:12 }}>
        <button onClick={handleSave} disabled={saving}
          style={{ padding:'8px 24px', background: saving ? '#94a3b8' : '#1e40af', color:'#fff', border:'none', borderRadius:8, fontSize:'0.85rem', fontWeight:700, cursor: saving ? 'default' : 'pointer' }}>
          {saving ? '保存中…' : '保存'}
        </button>
        {notice && (
          <span style={{ fontSize:'0.8rem', color: notice.includes('失敗') ? '#dc2626' : '#059669', fontWeight:600 }}>
            {notice}
          </span>
        )}
      </div>
    </div>
  );
}

export default function CRM() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get('tab') || 'dashboard';
  const setTab = (t) => setSearchParams({ tab: t }, { replace: true });

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div style={{ display: 'flex', borderBottom: '1px solid #e5e7eb', background: '#fff', paddingLeft: 8, flexShrink: 0 }}>
        {[
          { key: 'dashboard',   label: 'ダッシュボード' },
          { key: 'pipeline',    label: 'パイプライン' },
          { key: 'customers',   label: '顧客一覧' },
          { key: 'performance', label: '成績' },
          { key: 'settings',    label: '設定' },
        ].map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            style={{
              padding: '10px 20px', border: 'none', background: 'none', cursor: 'pointer',
              fontSize: '0.88rem', fontWeight: tab === t.key ? 700 : 400,
              color: tab === t.key ? '#1d4ed8' : '#6b7280',
              borderBottom: tab === t.key ? '2px solid #1d4ed8' : '2px solid transparent',
              transition: 'color 0.15s',
            }}>
            {t.label}
          </button>
        ))}
      </div>
      <div style={{ flex: 1, overflow: 'auto' }}>
        {tab === 'dashboard'   && <CrmDashboard />}
        {tab === 'pipeline'    && <Pipeline embedded />}
        {tab === 'customers'   && <CustomerList />}
        {tab === 'performance' && <SalesPerformance embedded />}
        {tab === 'settings'    && <CrmSettings />}
      </div>
    </div>
  );
}
