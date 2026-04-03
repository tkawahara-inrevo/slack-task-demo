import { useCallback, useEffect, useState } from 'react';
import { api } from '../../api/client';

function formatTimestamp(value) {
  if (!value) return '-';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '-' : date.toLocaleString('ja-JP');
}

export default function UserMappingAdmin() {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [members, setMembers] = useState([]);

  const load = useCallback(async (nextQuery = query) => {
    setLoading(true);
    try {
      const res = await api.adminUserMapping(nextQuery);
      setMembers(res.members || []);
    } catch (error) {
      console.error(error);
    } finally {
      setLoading(false);
    }
  }, [query]);

  useEffect(() => {
    load('');
  }, [load]);

  const handleSearch = async (event) => {
    event?.preventDefault();
    await load(query);
  };

  const handleSync = async () => {
    setSyncing(true);
    try {
      await api.adminSyncUserMapping();
      await load(query);
    } catch (error) {
      console.error(error);
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div>
      <div className="page-header">
        <div>
          <h2>ユーザーマッピング</h2>
          <p className="page-subtitle">Slack の表示名と user ID を確認するための管理者専用一覧です。</p>
        </div>
        <button className="btn-primary" onClick={handleSync} disabled={syncing}>
          {syncing ? '同期中...' : 'Slack から同期'}
        </button>
      </div>

      <form className="admin-form-row" onSubmit={handleSearch}>
        <input
          type="text"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="表示名・実名・Slack ID で検索"
        />
        <button className="btn-primary" type="submit">検索</button>
      </form>

      {loading ? (
        <p className="empty-text">読み込み中...</p>
      ) : (
        <div className="admin-table-wrap">
          <table className="admin-table">
            <thead>
              <tr>
                <th>表示名</th>
                <th>実名</th>
                <th>Slack ID</th>
                <th>状態</th>
                <th>最終同期</th>
              </tr>
            </thead>
            <tbody>
              {members.map((member) => (
                <tr key={member.user_id}>
                  <td>{member.display_name || '-'}</td>
                  <td>{member.real_name || '-'}</td>
                  <td><code>{member.user_id}</code></td>
                  <td>{member.is_active ? '有効' : '無効'}</td>
                  <td>{formatTimestamp(member.last_synced_at)}</td>
                </tr>
              ))}
              {members.length === 0 && (
                <tr>
                  <td colSpan="5" className="empty-text">データがありません</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
