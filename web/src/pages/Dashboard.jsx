import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api/client';
import StatusSummary from '../components/StatusSummary';
import MemberList from '../components/MemberList';
import TaskTable from '../components/TaskTable';

export default function Dashboard() {
  const [user, setUser] = useState(null);
  const [summary, setSummary] = useState(null);
  const [members, setMembers] = useState([]);
  const [projects, setProjects] = useState([]);
  const [usergroups, setUsergroups] = useState([]);
  const [tasks, setTasks] = useState({ tasks: [], total: 0, page: 1 });
  const [filter, setFilter] = useState({ status: '', assignee: '', project: '', usergroup: '', overdue: false, page: 1 });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([api.me(), api.summary(), api.members(), api.projects(), api.usergroups()])
      .then(([me, sum, mem, proj, ug]) => {
        setUser(me);
        setSummary(sum.summary);
        setMembers(mem.members);
        setProjects(proj.projects);
        setUsergroups(ug.usergroups || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    const params = { page: filter.page, limit: 50 };
    if (filter.status) params.status = filter.status;
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.project) params.project = filter.project;
    if (filter.usergroup) params.usergroup = filter.usergroup;
    if (filter.overdue) params.overdue = '1';
    api.tasks(params).then(setTasks).catch(console.error);
  }, [filter]);

  const setF = (patch) => setFilter(f => ({ ...f, ...patch, page: 1 }));

  const activeFilterCount = [filter.status, filter.assignee, filter.project, filter.usergroup, filter.overdue].filter(Boolean).length;

  if (loading) return <div className="loading">読み込み中...</div>;

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>タスク管理ダッシュボード</h1>
        <div className="header-right">
          <Link to="/analytics" className="analytics-link">分析</Link>
          <Link to="/tasks/new" className="create-task-link">＋ タスク作成</Link>
          {user?.role === 'admin' && (
            <Link to="/admin" className="admin-link">管理設定</Link>
          )}
          <span className="user-info">
            {user?.displayName}
            {user?.role === 'admin' && <span className="role-badge">admin</span>}
          </span>
        </div>
      </header>

      <div className="dashboard-layout">
        <main className="dashboard-main">
          {/* Filter panel */}
          <div className="filter-panel">
            <div className="filter-panel-row">
              <select
                className="filter-select"
                value={filter.assignee}
                onChange={(e) => setF({ assignee: e.target.value })}
              >
                <option value="">担当者：全員</option>
                {members.map((m) => (
                  <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>
                ))}
              </select>

              {usergroups.length > 0 && (
                <select
                  className="filter-select"
                  value={filter.usergroup}
                  onChange={(e) => setF({ usergroup: e.target.value })}
                >
                  <option value="">チーム：すべて</option>
                  {usergroups.map((g) => (
                    <option key={g.id} value={g.id}>@{g.handle}</option>
                  ))}
                </select>
              )}

              {projects.length > 0 && (
                <select
                  className="filter-select"
                  value={filter.project}
                  onChange={(e) => setF({ project: e.target.value })}
                >
                  <option value="">プロジェクト：すべて</option>
                  {projects.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
                </select>
              )}

              <label className="filter-checkbox-label">
                <input
                  type="checkbox"
                  checked={filter.overdue}
                  onChange={(e) => setF({ overdue: e.target.checked })}
                />
                期限切れのみ
              </label>

              {activeFilterCount > 0 && (
                <button
                  className="filter-clear-btn"
                  onClick={() => setFilter({ status: '', assignee: '', project: '', usergroup: '', overdue: false, page: 1 })}
                >
                  クリア
                </button>
              )}
            </div>

            {filter.status && (
              <div className="filter-tags">
                <span className="filter-tag" onClick={() => setF({ status: '' })}>
                  ステータス: {filter.status} &times;
                </span>
              </div>
            )}
          </div>

          <TaskTable
            tasks={tasks.tasks}
            total={tasks.total}
            page={tasks.page}
            onPageChange={(p) => setFilter(f => ({ ...f, page: p }))}
          />
        </main>

        <aside className="dashboard-sidebar">
          <section className="sidebar-section">
            <h2>状態</h2>
            {summary && (
              <StatusSummary
                summary={summary}
                onFilter={(s) => setF({ status: s })}
                activeStatus={filter.status}
              />
            )}
          </section>

          <section className="sidebar-section">
            <h2>メンバー</h2>
            <MemberList
              members={members}
              onSelect={(id) => setF({ assignee: filter.assignee === id ? '' : id })}
              selectedId={filter.assignee}
            />
          </section>

          {projects.length > 0 && (
            <section className="sidebar-section">
              <h2>プロジェクト</h2>
              <div className="sidebar-project-list">
                {projects.map((p) => (
                  <Link key={p.id} to={`/projects/${p.id}`} className="sidebar-project-link">
                    {p.name}
                  </Link>
                ))}
              </div>
            </section>
          )}
        </aside>
      </div>
    </div>
  );
}
