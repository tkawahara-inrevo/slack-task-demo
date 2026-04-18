import { useEffect, useState } from 'react';
import { api } from '../../api/client';

export default function RpoAdmin() {
  const [teams, setTeams]   = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving]   = useState(null);

  // Apps Script URL設定
  const [scriptUrl,     setScriptUrl]     = useState('');
  const [scriptSaving,  setScriptSaving]  = useState(false);
  const [scriptSaved,   setScriptSaved]   = useState(false);

  const load = () =>
    Promise.all([
      api.rpoTeams().catch(() => ({ teams: [] })),
      api.rpoSettings().catch(() => ({ settings: {} })),
    ]).then(([teamsRes, settingsRes]) => {
      setTeams(teamsRes.teams || []);
      setScriptUrl(settingsRes.settings?.appsScriptUrl || '');
    }).finally(() => setLoading(false));

  useEffect(() => { load(); }, []);

  const saveScriptUrl = async () => {
    setScriptSaving(true);
    try {
      await api.rpoSaveSettings({ appsScriptUrl: scriptUrl.trim() || null });
      setScriptSaved(true);
      setTimeout(() => setScriptSaved(false), 2000);
    } catch { alert('保存に失敗しました'); }
    finally { setScriptSaving(false); }
  };

  const toggle = async (team) => {
    setSaving(team.id);
    try {
      await api.rpoToggleHrTeam(team.id, !team.is_hr_dept);
      setTeams(prev =>
        prev.map(t => t.id === team.id ? { ...t, is_hr_dept: !t.is_hr_dept } : t)
      );
    } catch {
      alert('更新に失敗しました');
    } finally {
      setSaving(null);
    }
  };

  if (loading) return <div className="page-loading">読み込み中...</div>;

  const hrTeams     = teams.filter(t => t.is_hr_dept);
  const normalTeams = teams.filter(t => !t.is_hr_dept);

  return (
    <div>
      <div className="page-header">
        <h1>案件管理 権限設定</h1>
        <p className="page-subtitle">
          HR部署として設定されたチームのメンバーが案件管理にアクセスできます。<br />
          チーム内で <strong>manager</strong> ロールを持つメンバーは全案件を閲覧できます。
        </p>
      </div>

      {/* Google Apps Script URL設定 */}
      <div className="rpo-admin-section">
        <h2 className="rpo-admin-section-title">Google スプレッドシート連携</h2>
        <p style={{ fontSize: '0.875rem', color: '#6b7280', marginBottom: '12px' }}>
          <a href="https://script.google.com" target="_blank" rel="noreferrer">script.google.com</a> で作成したApps ScriptウェブアプリのURLを登録してください。<br />
          登録後、各案件でスプシURLを貼るだけで応募者データを同期できます。
        </p>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <input
            type="url"
            value={scriptUrl}
            onChange={e => setScriptUrl(e.target.value)}
            placeholder="https://script.google.com/macros/s/XXXXX/exec"
            style={{ flex: 1, padding: '8px 10px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.875rem' }}
          />
          <button className="btn-primary small" onClick={saveScriptUrl} disabled={scriptSaving}>
            {scriptSaved ? '保存済み ✓' : scriptSaving ? '保存中...' : '保存'}
          </button>
        </div>
        {scriptUrl && (
          <p style={{ fontSize: '0.75rem', color: '#10b981', marginTop: '6px' }}>✓ 設定済み</p>
        )}
      </div>

      <div className="rpo-admin-section">
        <h2 className="rpo-admin-section-title">
          HR部署チーム
          <span className="rpo-admin-badge hr">{hrTeams.length}</span>
        </h2>
        {hrTeams.length === 0 ? (
          <p className="empty-hint">HR部署として設定されたチームがありません</p>
        ) : (
          <div className="rpo-admin-team-list">
            {hrTeams.map(t => (
              <div key={t.id} className="rpo-admin-team-row hr-active">
                <span className="rpo-admin-team-name">{t.name}</span>
                <span className="rpo-admin-team-badge">HR部署</span>
                <button
                  className="btn-secondary btn-sm"
                  disabled={saving === t.id}
                  onClick={() => toggle(t)}
                >
                  {saving === t.id ? '更新中...' : 'HR解除'}
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="rpo-admin-section">
        <h2 className="rpo-admin-section-title">
          その他のチーム
          <span className="rpo-admin-badge">{normalTeams.length}</span>
        </h2>
        {normalTeams.length === 0 ? (
          <p className="empty-hint">チームがありません</p>
        ) : (
          <div className="rpo-admin-team-list">
            {normalTeams.map(t => (
              <div key={t.id} className="rpo-admin-team-row">
                <span className="rpo-admin-team-name">{t.name}</span>
                <button
                  className="btn-primary btn-sm"
                  disabled={saving === t.id}
                  onClick={() => toggle(t)}
                >
                  {saving === t.id ? '更新中...' : 'HR部署に設定'}
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
