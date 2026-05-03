import { useEffect, useState } from 'react';
import { api } from '../../api/client';

export default function DailyReportAdmin() {
  const [members, setMembers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [lastSync, setLastSync] = useState(null);
  const [search, setSearch] = useState('');

  const load = async () => {
    try {
      const r = await api.dailyReportMembers();
      setMembers(r.members || []);
    } catch {}
    finally { setLoading(false); }
  };

  useEffect(() => { load(); }, []);

  const handleSync = async () => {
    setSyncing(true);
    try {
      const r = await api.dailyReportSync();
      setMembers(r.members || []);
      setLastSync(new Date().toLocaleTimeString('ja-JP'));
    } catch { alert('同期に失敗しました'); }
    finally { setSyncing(false); }
  };

  const handleToggle = async (userId, current) => {
    setMembers(prev => prev.map(m => m.user_id === userId ? { ...m, is_target: !current } : m));
    await api.dailyReportToggle(userId, !current).catch(() => load());
  };

  const filtered = members.filter(m =>
    !search || m.display_name.toLowerCase().includes(search.toLowerCase())
  );
  const targetCount = members.filter(m => m.is_target).length;
  const excludeCount = members.filter(m => !m.is_target).length;

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 16 }}>
        <div>
          <h2 style={{ margin: '0 0 4px', fontSize: '1rem', fontWeight: 700 }}>日報対象メンバー管理</h2>
          <p style={{ margin: 0, fontSize: '0.82rem', color: '#6b7280' }}>
            チェックが入っているメンバーが日報の対象になります。
            対象外の人は退職者や除外したいメンバーのチェックを外してください。
          </p>
        </div>
        <button className="btn-primary" onClick={handleSync} disabled={syncing} style={{ flexShrink: 0 }}>
          {syncing ? '同期中...' : 'Slack同期'}
        </button>
      </div>

      {lastSync && (
        <p style={{ fontSize: '0.78rem', color: '#10b981', marginBottom: 10 }}>✓ {lastSync} に同期しました</p>
      )}

      <div style={{ display: 'flex', gap: 12, marginBottom: 12, alignItems: 'center', flexWrap: 'wrap' }}>
        <input
          type="text" value={search} placeholder="名前で検索…"
          onChange={e => setSearch(e.target.value)}
          style={{ padding: '5px 10px', border: '1px solid #d1d5db', borderRadius: 6, fontSize: '0.82rem', width: 200 }}
        />
        <span style={{ fontSize: '0.8rem', color: '#6b7280' }}>
          対象: <b style={{ color: '#10b981' }}>{targetCount}</b>名 ／
          対象外: <b style={{ color: '#ef4444' }}>{excludeCount}</b>名
        </span>
      </div>

      {loading ? (
        <p style={{ color: '#9ca3af' }}>読み込み中...</p>
      ) : members.length === 0 ? (
        <div style={{ textAlign: 'center', padding: 32, color: '#9ca3af' }}>
          <p>メンバーがいません。「Slack同期」を押してください。</p>
        </div>
      ) : (
        <div style={{ border: '1px solid #e5e7eb', borderRadius: 8, overflow: 'hidden', background: '#fff' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
            <thead>
              <tr style={{ background: '#f9fafb' }}>
                <th style={{ padding: '8px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb', width: 48 }}>対象</th>
                <th style={{ padding: '8px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>名前</th>
                <th style={{ padding: '8px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>Slack ID</th>
                <th style={{ padding: '8px 14px', textAlign: 'left', fontWeight: 600, color: '#6b7280', borderBottom: '1px solid #e5e7eb' }}>更新日時</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(m => (
                <tr key={m.user_id}
                  style={{ borderBottom: '1px solid #f3f4f6', opacity: m.is_target ? 1 : 0.5 }}
                  onClick={() => handleToggle(m.user_id, m.is_target)}
                  onMouseEnter={e => e.currentTarget.style.background = '#f9fafb'}
                  onMouseLeave={e => e.currentTarget.style.background = ''}
                >
                  <td style={{ padding: '10px 14px', cursor: 'pointer', textAlign: 'center' }}>
                    <input type="checkbox" checked={!!m.is_target} onChange={() => {}}
                      style={{ width: 16, height: 16, cursor: 'pointer', accentColor: '#10b981' }} />
                  </td>
                  <td style={{ padding: '10px 14px', cursor: 'pointer' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      {m.avatar_url
                        ? <img src={m.avatar_url} alt="" style={{ width: 24, height: 24, borderRadius: '50%' }} />
                        : <div style={{ width: 24, height: 24, borderRadius: '50%', background: '#e5e7eb', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, color: '#6b7280', flexShrink: 0 }}>
                            {m.display_name[0]?.toUpperCase()}
                          </div>
                      }
                      <span style={{ fontWeight: m.is_target ? 600 : 400, color: m.is_target ? '#111827' : '#9ca3af' }}>
                        {m.display_name}
                      </span>
                      {!m.is_target && <span style={{ fontSize: '0.72rem', color: '#ef4444', background: '#fef2f2', padding: '1px 6px', borderRadius: 4 }}>対象外</span>}
                    </div>
                  </td>
                  <td style={{ padding: '10px 14px', color: '#9ca3af', fontSize: '0.78rem', fontFamily: 'monospace' }}>{m.user_id}</td>
                  <td style={{ padding: '10px 14px', color: '#9ca3af', fontSize: '0.78rem' }}>
                    {new Date(m.updated_at).toLocaleDateString('ja-JP')}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div style={{ marginTop: 16, padding: 12, background: '#f9fafb', borderRadius: 6, fontSize: '0.78rem', color: '#6b7280' }}>
        <b>daily-report-watcher への連携：</b><br/>
        除外ユーザーリストのAPIエンドポイント：<code style={{ background: '#e5e7eb', padding: '1px 6px', borderRadius: 3 }}>/api/admin/daily-report/excludes</code><br/>
        daily-report-watcherの<code style={{ background: '#e5e7eb', padding: '1px 6px', borderRadius: 3 }}>EXCLUDE_USER_IDS</code>の代わりにこのAPIを参照するよう変更してください。
      </div>
    </div>
  );
}
