import { useEffect, useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { api } from '../api/client';

const STATUS_LABELS = {
  in_progress: '進行中',
  done: '完了',
  cancelled: 'キャンセル',
  pending: '保留',
};

function formatDate(d) {
  if (!d) return '-';
  const s = typeof d === 'string' ? d : new Date(d).toISOString();
  return s.slice(0, 10);
}

export default function ProjectView() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [project, setProject] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [allTasks, setAllTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [addingTaskId, setAddingTaskId] = useState('');
  const [showSearch, setShowSearch] = useState(false);
  const [searchResults, setSearchResults] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [user, setUser] = useState(null);

  const load = async () => {
    try {
      const [me, projs, projTasks] = await Promise.all([
        api.me(),
        api.projects(),
        api.projectTasks(id),
      ]);
      setUser(me);
      setProject(projs.projects.find((p) => p.id === id) || { name: 'プロジェクト' });
      setTasks(projTasks.tasks);
    } catch (e) { console.error(e); }
    setLoading(false);
  };

  useEffect(() => { load(); }, [id]);

  const handleSearch = async () => {
    if (!searchQuery.trim()) return;
    try {
      const r = await api.tasks({ limit: 20 });
      const taskIds = new Set(tasks.map((t) => t.id));
      setSearchResults(r.tasks.filter((t) =>
        !taskIds.has(t.id) &&
        t.title?.toLowerCase().includes(searchQuery.toLowerCase())
      ));
    } catch (e) { console.error(e); }
  };

  const handleAddTask = async (taskId) => {
    if (!taskId) return;
    try {
      await api.adminAddProjectTask(id, taskId);
      setSearchResults((prev) => prev.filter((t) => t.id !== taskId));
      load();
    } catch (e) { console.error(e); }
  };

  const handleRemoveTask = async (taskId) => {
    if (!confirm('このタスクをプロジェクトから外しますか？')) return;
    try {
      await api.adminRemoveProjectTask(id, taskId);
      load();
    } catch (e) { console.error(e); }
  };

  if (loading) return <div className="loading">読み込み中...</div>;

  const summary = {
    total: tasks.length,
    in_progress: tasks.filter((t) => t.status === 'in_progress').length,
    done: tasks.filter((t) => t.status === 'done').length,
    overdue: tasks.filter((t) => t.due_date && !['done', 'cancelled'].includes(t.status) && formatDate(t.due_date) < formatDate(new Date())).length,
  };

  return (
    <div className="project-view">
      <div className="task-detail-nav">
        <Link to="/" className="back-btn">← ダッシュボード</Link>
      </div>

      <div className="project-header">
        <h1>{project?.name}</h1>
        <div className="project-stats">
          <span className="stat-item">全 {summary.total}</span>
          <span className="stat-item blue">進行中 {summary.in_progress}</span>
          <span className="stat-item green">完了 {summary.done}</span>
          {summary.overdue > 0 && <span className="stat-item red">期限切れ {summary.overdue}</span>}
        </div>
      </div>

      {user?.role === 'admin' && (
        <div className="project-actions">
          <button className="btn-outline" onClick={() => setShowSearch(!showSearch)}>
            {showSearch ? '閉じる' : '＋ タスクを追加'}
          </button>
          {showSearch && (
            <div className="task-search-box">
              <div className="admin-form-row">
                <input
                  placeholder="タスク名で検索..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                />
                <button className="btn-sm btn-primary" onClick={handleSearch}>検索</button>
              </div>
              {searchResults.length > 0 && (
                <div className="search-results">
                  {searchResults.map((t) => (
                    <div key={t.id} className="search-result-item">
                      <span>{t.title?.slice(0, 50)}</span>
                      <button className="btn-sm btn-primary" onClick={() => handleAddTask(t.id)}>追加</button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      <div className="task-table-wrap">
        <table className="task-table">
          <thead>
            <tr>
              <th>タイトル</th>
              <th>ステータス</th>
              <th>期限</th>
              <th>種別</th>
              {user?.role === 'admin' && <th></th>}
            </tr>
          </thead>
          <tbody>
            {tasks.length === 0 && (
              <tr><td colSpan={5} className="empty">タスクがありません</td></tr>
            )}
            {tasks.map((t) => {
              const isOverdue = t.due_date && !['done', 'cancelled'].includes(t.status)
                && formatDate(t.due_date) < formatDate(new Date());
              return (
                <tr
                  key={t.id}
                  className={`clickable-row ${isOverdue ? 'overdue-row' : ''}`}
                  onClick={() => navigate(`/tasks/${t.id}`)}
                >
                  <td className="task-title">{t.title?.length > 50 ? t.title.slice(0, 50) + '...' : t.title}</td>
                  <td><span className={`status-badge ${t.status}`}>{STATUS_LABELS[t.status] || t.status}</span></td>
                  <td className={isOverdue ? 'overdue-date' : ''}>{formatDate(t.due_date)}</td>
                  <td>{t.task_type === 'broadcast' ? '一斉' : '個人'}</td>
                  {user?.role === 'admin' && (
                    <td>
                      <button
                        className="btn-sm btn-danger"
                        onClick={(e) => { e.stopPropagation(); handleRemoveTask(t.id); }}
                      >外す</button>
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
