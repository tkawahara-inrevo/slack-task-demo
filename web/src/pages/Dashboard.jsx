import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api/client';
import StatusSummary from '../components/StatusSummary';
import MemberList from '../components/MemberList';
import TaskTable from '../components/TaskTable';

const PIPELINE_STAGES = [
  { value: 'mk', label: 'MK', color: '#6c8ebf' },
  { value: 'bc', label: 'BC', color: '#d79b00' },
  { value: 'contracted', label: '受注済', color: '#00897b' },
  { value: 'hr', label: 'HR分析', color: '#7b1fa2' },
  { value: 'direction', label: 'DIR', color: '#e65100' },
  { value: 'cs', label: 'CS', color: '#0277bd' },
  { value: 'completed', label: '完了', color: '#388e3c' },
  { value: 'lost', label: '失注', color: '#9e9e9e' },
];

export default function Dashboard() {
  const [user, setUser] = useState(null);
  const [summary, setSummary] = useState(null);
  const [members, setMembers] = useState([]);
  const [projects, setProjects] = useState([]);
  const [dashTeams, setDashTeams] = useState([]);
  const [pipeline, setPipeline] = useState([]);
  const [tasks, setTasks] = useState({ tasks: [], total: 0, page: 1 });
  const [personalFilters, setPersonalFilters] = useState([]);
  const [filter, setFilter] = useState({ status: '', assignee: '', project: '', dashDept: '', dashTeam: '', personalFilter: '', overdue: false, sort: '', page: 1 });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([api.me(), api.summary(), api.members(), api.projects(), api.workloadTeams(), api.personalFilters()])
      .then(([me, sum, mem, proj, dt, pf]) => {
        setUser(me);
        setSummary(sum.summary);
        setMembers(mem.members);
        setProjects(proj.projects);
        setDashTeams(dt.teams || []);
        setPersonalFilters(pf.filters || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    const params = { page: filter.page, limit: 50 };
    if (filter.status) params.status = filter.status;
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.project) params.project = filter.project;
    if (filter.dashTeam) params.dashTeam = filter.dashTeam;
    else if (filter.dashDept) params.dashTeam = filter.dashDept;
    if (filter.personalFilter) params.personalFilter = filter.personalFilter;
    if (filter.overdue) params.overdue = '1';
    if (filter.sort) params.sort = filter.sort;
    api.tasks(params).then(setTasks).catch(console.error);
  }, [filter]);

  const setF = (patch) => setFilter(f => ({ ...f, ...patch, page: 1 }));

  const deptTeams = useMemo(() => dashTeams.filter(t => !t.parent_id), [dashTeams]);
  const childTeamsOf = useMemo(() => {
    const map = {};
    for (const t of dashTeams) {
      if (t.parent_id) { if (!map[t.parent_id]) map[t.parent_id] = []; map[t.parent_id].push(t); }
    }
    return map;
  }, [dashTeams]);

  const activeFilterCount = [filter.status, filter.assignee, filter.project, filter.dashDept, filter.dashTeam, filter.personalFilter, filter.overdue, filter.sort].filter(Boolean).length;

  if (loading) return <div className="loading">読み込み中...</div>;

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>タスク管理ダッシュボード</h1>
      </header>

      <div className="dashboard-layout">
        <main className="dashboard-main">
          {/* Filter panel */}
          <div className="filter-panel">
            <div className="filter-panel-row">
              <select
                className="filter-select"
                value={filter.status}
                onChange={(e) => setF({ status: e.target.value })}
              >
                <option value="">ステータス：すべて</option>
                <option value="in_progress">進行中のみ</option>
                <option value="done">完了済みのみ</option>
                <option value="pending">保留のみ</option>
                <option value="cancelled">キャンセルのみ</option>
              </select>

              {personalFilters.length > 0 && (
                <select
                  className="filter-select"
                  value={filter.personalFilter}
                  onChange={(e) => setF({ personalFilter: e.target.value, assignee: '', dashDept: '', dashTeam: '' })}
                >
                  <option value="">個人フィルター：なし</option>
                  {personalFilters.map((f) => (
                    <option key={f.id} value={f.id}>{f.name}</option>
                  ))}
                </select>
              )}

              {deptTeams.length > 0 && (
                <select
                  className="filter-select"
                  value={filter.dashDept}
                  onChange={(e) => setF({ dashDept: e.target.value, dashTeam: '', assignee: '', personalFilter: '' })}
                >
                  <option value="">部署：すべて</option>
                  {deptTeams.map((t) => (
                    <option key={t.id} value={t.id}>{t.name}</option>
                  ))}
                </select>
              )}

              {filter.dashDept && childTeamsOf[filter.dashDept]?.length > 0 && (
                <select
                  className="filter-select"
                  value={filter.dashTeam}
                  onChange={(e) => setF({ dashTeam: e.target.value })}
                >
                  <option value="">チーム：すべて</option>
                  {childTeamsOf[filter.dashDept].map((t) => (
                    <option key={t.id} value={t.id}>{t.name}</option>
                  ))}
                </select>
              )}

              <select
                className="filter-select"
                value={filter.assignee}
                onChange={(e) => setF({ assignee: e.target.value })}
              >
                <option value="">担当者：全員</option>
                {members
                  .map((m) => (
                    <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>
                  ))}
              </select>

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

              <select
                className="filter-select"
                value={filter.sort}
                onChange={(e) => setF({ sort: e.target.value })}
              >
                <option value="">並び順：作成日</option>
                <option value="due_date_asc">期限：近い順</option>
                <option value="due_date_desc">期限：遠い順</option>
              </select>

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
                  onClick={() => setFilter({ status: '', assignee: '', project: '', dashDept: '', dashTeam: '', personalFilter: '', overdue: false, sort: '', page: 1 })}
                >
                  クリア
                </button>
              )}
            </div>
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
