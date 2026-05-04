import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import Pipeline from './Pipeline';
import CustomerList from './CustomerList';
import SalesPerformance from './SalesPerformance';
import CrmDashboard from './CrmDashboard';
import { api } from '../../api/client';

const TARGET_REPS = ['山本 夏乃', '板金 慎太郎', '萩原 隼人', '藤原 一矢', '野村 尭弘'];

function CrmSettings() {
  const [targets, setTargets] = useState({});
  const [saving, setSaving] = useState(false);
  const [notice, setNotice] = useState('');

  useEffect(() => {
    api.crmRoleTargets().then(d => {
      const map = {};
      for (const t of (d.targets || [])) map[t.role_name] = t;
      setTargets(map);
    });
  }, []);

  const getValue = (rep) => {
    const raw = targets[rep]?.monthly_target;
    return raw > 0 ? Math.round(raw / 10000) : '';
  };

  const setValue = (rep, wan) => {
    setTargets(prev => ({
      ...prev,
      [rep]: { ...(prev[rep] || {}), role_name: rep, monthly_target: Number(wan) * 10000 },
    }));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const arr = TARGET_REPS.map((rep, i) => ({
        role_name: rep,
        monthly_target: targets[rep]?.monthly_target || 0,
        sort_order: i,
      }));
      await api.crmRoleTargetsSave(arr);
      setNotice('保存しました');
      setTimeout(() => setNotice(''), 2500);
    } catch {
      setNotice('保存に失敗しました');
      setTimeout(() => setNotice(''), 3000);
    }
    setSaving(false);
  };

  return (
    <div style={{ padding:'28px 32px', maxWidth:480 }}>
      <div style={{ marginBottom:20 }}>
        <div style={{ fontWeight:700, fontSize:'1rem', color:'#0f172a', marginBottom:4 }}>担当者別 月次KPI目標</div>
        <div style={{ fontSize:'0.78rem', color:'#94a3b8' }}>設定した値がダッシュボードの達成率計算に使われます（万円単位で入力）</div>
      </div>

      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', overflow:'hidden' }}>
        {TARGET_REPS.map((rep, i) => {
          const [fam, given] = rep.split(/[\s　]/);
          return (
            <div key={rep} style={{ display:'flex', alignItems:'center', gap:14, padding:'12px 18px', borderBottom: i < TARGET_REPS.length - 1 ? '1px solid #f1f5f9' : 'none' }}>
              <div style={{ width:26, height:26, borderRadius:7, background:'#eef2ff', display:'flex', alignItems:'center', justifyContent:'center', fontSize:'0.68rem', fontWeight:800, color:'#4f46e5', flexShrink:0 }}>
                {fam?.[0]}
              </div>
              <div style={{ flex:1 }}>
                <span style={{ fontWeight:700, color:'#4f46e5', fontSize:'0.72rem' }}>{fam}</span>
                <span style={{ color:'#374151', fontSize:'0.85rem', marginLeft:4 }}>{given}</span>
              </div>
              <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                <input
                  type="number" min="0" step="10"
                  value={getValue(rep)}
                  onChange={e => setValue(rep, e.target.value)}
                  placeholder="未設定"
                  style={{ width:90, padding:'5px 10px', border:'1px solid #e2e8f0', borderRadius:8, fontSize:'0.88rem', textAlign:'right', outline:'none', color:'#0f172a' }}
                />
                <span style={{ fontSize:'0.78rem', color:'#64748b', flexShrink:0 }}>万円 / 月</span>
              </div>
            </div>
          );
        })}
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

