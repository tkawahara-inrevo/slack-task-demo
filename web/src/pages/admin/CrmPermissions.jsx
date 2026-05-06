import { useEffect, useState } from 'react';
import { api } from '../../api/client';

const TAB_LABELS = {
  dashboard:   { label: 'ダッシュボード', desc: '数値・KPI・アラートの表示' },
  customers:   { label: '顧客一覧',       desc: '顧客・商談の一覧と詳細' },
  yomi:        { label: 'ヨミ管理',       desc: '進行中案件のヨミ別管理' },
  performance: { label: '成績',           desc: '個人成績・KPI評価' },
  settings:    { label: '設定',           desc: 'CRM各種設定' },
};

const ACCESS_OPTIONS = [
  { value: 'all',         label: '全員',              desc: '役職・部署問わず全員がアクセス可' },
  { value: 'bc_all',      label: 'BC所属のみ',        desc: 'BCチーム所属なら役職問わずアクセス可' },
  { value: 'bc_manager',  label: 'BC管理職のみ',      desc: 'BCチーム所属 かつ Sub Chief以上' },
  { value: 'bc_and_above',label: 'BC所属（スコープ制限あり）', desc: 'BC管理職は全件、それ以外は自分のデータのみ表示' },
  { value: 'none',        label: 'アクセス不可',      desc: '（管理者を除く）誰もアクセスできない' },
];

export default function CrmPermissions() {
  const [config, setConfig] = useState(null);
  const [teams, setTeams] = useState([]);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([api.crmPermissions(), api.orgChart()])
      .then(([perm, org]) => {
        setConfig(perm);
        setTeams((org.teams || []).filter(t => !t.parent_id).map(t => t.name));
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const handleSave = async () => {
    setSaving(true);
    try {
      await api.crmPermissionsSave({ bcTeamName: config.bcTeamName, tabs: config.tabs });
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch { alert('保存に失敗しました'); }
    finally { setSaving(false); }
  };

  if (loading) return <div style={{ color:'#94a3b8', padding:24 }}>読み込み中…</div>;
  if (!config)  return null;

  return (
    <div>
      <div style={{ marginBottom:24 }}>
        <h2 style={{ margin:'0 0 4px', fontSize:'1rem', fontWeight:700 }}>CRM権限管理</h2>
        <p style={{ margin:0, fontSize:'0.82rem', color:'#6b7280' }}>
          各タブへのアクセス権限を設定します。管理者（admin）は常にすべてのタブにアクセスできます。
        </p>
      </div>

      {/* BCチーム名設定 */}
      <div style={{ background:'#f8fafc', border:'1px solid #e2e8f0', borderRadius:10, padding:'14px 18px', marginBottom:20 }}>
        <div style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a', marginBottom:10 }}>BCチームの設定</div>
        <div style={{ display:'flex', alignItems:'center', gap:10, flexWrap:'wrap' }}>
          <label style={{ fontSize:'0.82rem', color:'#374151', fontWeight:600 }}>チーム名：</label>
          <select value={config.bcTeamName}
            onChange={e => setConfig(c => ({...c, bcTeamName: e.target.value}))}
            style={{ padding:'6px 12px', border:'1.5px solid #e2e8f0', borderRadius:8, fontSize:'0.82rem', background:'#fff', minWidth:200 }}>
            {teams.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          <span style={{ fontSize:'0.72rem', color:'#94a3b8' }}>
            このチームに所属するメンバーを「BC所属」として扱います
          </span>
        </div>
      </div>

      {/* タブ別アクセス設定 */}
      <div style={{ display:'flex', flexDirection:'column', gap:10, marginBottom:24 }}>
        {Object.entries(TAB_LABELS).map(([tabKey, { label, desc }]) => {
          const current = config.tabs[tabKey]?.access || 'all';
          const isFixed = tabKey === 'customers'; // 顧客一覧は変更不可
          return (
            <div key={tabKey} style={{ background:'#fff', border:'1px solid #e2e8f0', borderRadius:10, padding:'14px 18px' }}>
              <div style={{ display:'flex', alignItems:'flex-start', gap:16 }}>
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>{label}</div>
                  <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginTop:2 }}>{desc}</div>
                </div>
                <div style={{ flexShrink:0 }}>
                  {isFixed ? (
                    <span style={{ fontSize:'0.78rem', color:'#059669', background:'#f0fdf4', border:'1px solid #86efac', borderRadius:6, padding:'5px 12px', fontWeight:600 }}>
                      全員（変更不可）
                    </span>
                  ) : (
                    <select value={current}
                      onChange={e => setConfig(c => ({ ...c, tabs: { ...c.tabs, [tabKey]: { access: e.target.value } } }))}
                      style={{ padding:'6px 10px', border:`1.5px solid ${current==='none'?'#fca5a5':current==='all'?'#86efac':'#93c5fd'}`, borderRadius:8, fontSize:'0.82rem', background:'#fff', minWidth:200, cursor:'pointer' }}>
                      {ACCESS_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                    </select>
                  )}
                </div>
              </div>
              {/* 選択中の説明 */}
              {!isFixed && (
                <div style={{ marginTop:8, fontSize:'0.72rem', color:'#64748b', background:'#f8fafc', borderRadius:6, padding:'5px 10px' }}>
                  {ACCESS_OPTIONS.find(o => o.value === current)?.desc}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* スコープ説明 */}
      <div style={{ background:'#eff6ff', border:'1px solid #bfdbfe', borderRadius:10, padding:'12px 16px', marginBottom:20, fontSize:'0.78rem', color:'#1e40af' }}>
        <strong>「BC所属（スコープ制限あり）」について</strong><br />
        BC所属の管理職（Sub Chief以上）は全データを閲覧できます。管理職以外のBC所属メンバーは、自分が担当している案件のデータのみ表示されます。
      </div>

      <div style={{ display:'flex', justifyContent:'flex-end' }}>
        <button onClick={handleSave} disabled={saving}
          style={{ padding:'9px 28px', background: saved?'#059669':'#1e40af', color:'#fff', border:'none', borderRadius:9, fontSize:'0.85rem', fontWeight:700, cursor:'pointer' }}>
          {saved ? '✓ 保存しました' : saving ? '保存中…' : '設定を保存'}
        </button>
      </div>
    </div>
  );
}
