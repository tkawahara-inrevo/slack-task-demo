import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { api } from '../api/client';
import StatusSummary from '../components/StatusSummary';
import MemberList from '../components/MemberList';
import TaskTable from '../components/TaskTable';

export default function Dashboard() {
  const [user, setUser] = useState(null);
  const [summary, setSummary] = useState(null);
  const [members, setMembers] = useState([]);
  const [projects, setProjects] = useState([]);
  const [tasks, setTasks] = useState({ tasks: [], total: 0, page: 1 });
  const [filter, setFilter] = useState({ status: '', assignee: '', project: '', page: 1 });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([api.me(), api.summary(), api.members(), api.projects()])
      .then(([me, sum, mem, proj]) => {
        setUser(me);
        setSummary(sum.summary);
        setMembers(mem.members);
        setProjects(proj.projects);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    const params = { page: filter.page, limit: 50 };
    if (filter.status) params.status = filter.status;
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.project) params.project = filter.project;
    api.tasks(params).then(setTasks).catch(console.error);
  }, [filter]);

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

      <section className="summary-section">
        <h2>ステータス概要</h2>
        {summary && <StatusSummary summary={summary} onFilter={(s) => setFilter(f => ({ ...f, status: s, page: 1 }))} />}
      </section>

      <section className="members-section">
        <h2>メンバー別タスク数</h2>
        <MemberList
          members={members}
          onSelect={(id) => setFilter(f => ({ ...f, assignee: f.assignee === id ? '' : id, page: 1 }))}
          selectedId={filter.assignee}
        />
      </section>

      {projects.length > 0 && (
        <section className="projects-section">
          <h2>プロジェクト</h2>
          <div className="project-cards">
            {projects.map((p) => (
              <Link key={p.id} to={`/projects/${p.id}`} className="project-card">
                <span className="project-card-name">{p.name}</span>
              </Link>
            ))}
          </div>
        </section>
      )}

      <section className="tasks-section">
        <div className="tasks-header">
          <h2>タスク一覧</h2>
          <div className="filter-controls">
            {projects.length > 0 && (
              <select
                className="project-filter"
                value={filter.project}
                onChange={(e) => setFilter(f => ({ ...f, project: e.target.value, page: 1 }))}
              >
                <option value="">全プロジェクト</option>
                {projects.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
              </select>
            )}
            <div className="filter-info">
              {filter.status && <span className="filter-tag" onClick={() => setFilter(f => ({ ...f, status: '', page: 1 }))}>ステータス: {filter.status} &times;</span>}
              {filter.assignee && <span className="filter-tag" onClick={() => setFilter(f => ({ ...f, assignee: '', page: 1 }))}>担当者フィルター &times;</span>}
              {filter.project && <span className="filter-tag" onClick={() => setFilter(f => ({ ...f, project: '', page: 1 }))}>プロジェクト &times;</span>}
            </div>
          </div>
        </div>
        <TaskTable
          tasks={tasks.tasks}
          total={tasks.total}
          page={tasks.page}
          onPageChange={(p) => setFilter(f => ({ ...f, page: p }))}
        />
      </section>
    </div>
  );
}
