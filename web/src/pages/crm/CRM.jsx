import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import Pipeline from './Pipeline';
import CustomerList from './CustomerList';
import SalesPerformance from './SalesPerformance';
import CrmDashboard from './CrmDashboard';
import { api } from '../../api/client';

const TARGET_REPS  = ['山本 夏乃', '板金 慎太郎', '萩原 隼人', '藤原 一矢', '野村 尭弘'];
const ROLE_OPTIONS = ['', '役職無し', 'Lead', 'Sub Chief', 'Chief', 'Sub Expert', 'Expert'];

function CrmSettings() {
  const [roleTargets, setRoleTargets]   = useState({});  // role_name → monthly_target
  const [repRoles, setRepRoles]         = useState({});  // rep_name → { role_name, monthly_target_override }
  const [saving, setSaving]             = useState(false);
  const [notice, setNotice]             = useState('');

  useEffect(() => {
    Promise.all([api.crmRoleTargets(), api.crmRepRoles()]).then(([rt, rr]) => {
      const rtMap = {};
      for (const t of (rt.targets || [])) rtMap[t.role_name] = Number(t.monthly_target || 0);
      setRoleTargets(rtMap);
      const rrMap = {};
      for (const r of (rr.repRoles || [])) rrMap[r.rep_name] = r;
      setRepRoles(rrMap);
    });
  }, []);

  const getEffective = (rep) => {
    const r = repRoles[rep];
    if (r?.monthly_target_override) return Number(r.monthly_target_override);
    return roleTargets[r?.role_name || ''] || 0;
  };

  const setRole = (rep, role) => setRepRoles(prev => ({
    ...prev, [rep]: { ...(prev[rep] || { rep_name: rep }), role_name: role },
  }));

  const setOverride = (rep, wan) => setRepRoles(prev => ({
    ...prev, [rep]: {
      ...(prev[rep] || { rep_name: rep }),
      monthly_target_override: wan === '' ? null : Number(wan) * 10000,
    },
  }));

  const handleSave = async () => {
    setSaving(true);
    try {
      const arr = TARGET_REPS.map(rep => ({
        rep_name: rep,
        role_name: repRoles[rep]?.role_name || '',
        monthly_target_override: repRoles[rep]?.monthly_target_override || null,
      }));
      await api.crmRepRolesSave(arr);
      setNotice('保存しました');
      setTimeout(() => setNotice(''), 2500);
    } catch {
      setNotice('保存に失敗しました');
      setTimeout(() => setNotice(''), 3000);
    }
    setSaving(false);
  };

  const teamTotal = TARGET_REPS.reduce((s, rep) => s + getEffective(rep), 0);

  return (
    <div style={{ padding:'28px 32px', maxWidth:600 }}>
      <div style={{ marginBottom:20 }}>
        <div style={{ fontWeight:700, fontSize:'1rem', color:'#0f172a', marginBottom:4 }}>担当者別 役職・KPI目標</div>
        <div style={{ fontSize:'0.78rem', color:'#94a3b8' }}>
          役職を選択すると役職別目標（成績ページで設定）が自動適用されます。例外時のみ手動上書き欄に入力してください。
        </div>
      </div>

      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
        {/* ヘッダー */}
        <div style={{ display:'grid', gridTemplateColumns:'1fr 130px 120px 80px', gap:10, padding:'8px 18px', background:'#f8fafc', borderBottom:'1px solid #f1f5f9' }}>
          {['担当者', '役職', '手動上書き', '実効目標'].map(h => (
            <span key={h} style={{ fontSize:'0.7rem', color:'#94a3b8', fontWeight:600 }}>{h}</span>
          ))}
        </div>

        {TARGET_REPS.map((rep, i) => {
          const [fam, given]  = rep.split(/[\s　]/);
          const roleName      = repRoles[rep]?.role_name || '';
          const overrideRaw   = repRoles[rep]?.monthly_target_override;
          const overrideWan   = overrideRaw ? Math.round(Number(overrideRaw) / 10000) : '';
          const effective     = getEffective(rep);
          const fromRole      = !overrideRaw && effective > 0;
          return (
            <div key={rep} style={{ display:'grid', gridTemplateColumns:'1fr 130px 120px 80px', gap:10, padding:'10px 18px', borderBottom: i < TARGET_REPS.length - 1 ? '1px solid #f8fafc' : 'none', alignItems:'center' }}>
              <div style={{ display:'flex', alignItems:'center', gap:8 }}>
                <span style={{ width:24, height:24, borderRadius:6, background:'#eef2ff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.65rem', fontWeight:800, color:'#4f46e5', flexShrink:0 }}>
                  {fam?.[0]}
                </span>
                <span>
                  <span style={{ fontWeight:700, color:'#4f46e5', fontSize:'0.7rem' }}>{fam}</span>
                  <span style={{ color:'#374151', fontSize:'0.82rem', marginLeft:3 }}>{given}</span>
                </span>
              </div>
              <select value={roleName} onChange={e => setRole(rep, e.target.value)}
                style={{ padding:'4px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', color:'#0f172a', outline:'none' }}>
                {ROLE_OPTIONS.map(r => <option key={r} value={r}>{r || '— 未設定 —'}</option>)}
              </select>
              <div style={{ display:'flex', alignItems:'center', gap:4 }}>
                <input type="number" min="0" step="10" value={overrideWan}
                  onChange={e => setOverride(rep, e.target.value)}
                  placeholder="役職値を使用"
                  style={{ width:72, padding:'4px 8px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', textAlign:'right', outline:'none', color:'#0f172a' }} />
                <span style={{ fontSize:'0.7rem', color:'#94a3b8' }}>万</span>
              </div>
              <div style={{ textAlign:'right' }}>
                {effective > 0
                  ? <span style={{ fontSize:'0.82rem', fontWeight:700, color: fromRole ? '#1e40af' : '#7c3aed' }}>
                      {Math.round(effective / 10000)}万
                    </span>
                  : <span style={{ fontSize:'0.78rem', color:'#cbd5e1' }}>未設定</span>
                }
              </div>
            </div>
          );
        })}

        {/* フッター: チーム合計 */}
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', padding:'10px 18px', background:'#f8fafc', borderTop:'1px solid #f1f5f9' }}>
          <span style={{ fontSize:'0.78rem', color:'#64748b', fontWeight:600 }}>チーム月次目標合計</span>
          <span style={{ fontSize:'0.92rem', fontWeight:800, color: teamTotal > 0 ? '#1e40af' : '#cbd5e1' }}>
            {teamTotal > 0 ? `${Math.round(teamTotal / 10000)}万円` : '未設定'}
          </span>
        </div>
      </div>

      <div style={{ marginTop:16, display:'flex', alignItems:'center', gap:12 }}>
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
      {/* タブバー */}
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

      {/* タブコンテンツ */}
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

