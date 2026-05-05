import { useEffect, useMemo, useState } from 'react';
import { PieChart, Pie, Cell, Tooltip, BarChart, Bar, XAxis, YAxis, CartesianGrid, ResponsiveContainer } from 'recharts';
import { api } from '../api/client';

const STATUS_CFG = {
  in_progress: { label: '進行中', color: '#3b82f6' },
  done:        { label: '完了',   color: '#10b981' },
  pending:     { label: '保留',   color: '#f59e0b' },
  cancelled:   { label: 'キャンセル', color: '#94a3b8' },
};

// 先頭のSlackメンション・@mention を除去してタイトルとして使える文字列を返す
const cleanTitle = (title, content) => {
  const raw = title || content || '';
  return raw
    .replace(/^(<@[^>]+>\s*)+/g, '')           // <@UXXXXXX> 形式
    .replace(/^(@[\w./　-鿿]+\s*)+/g, '') // @name/surname 形式
    .replace(/^\s+/, '')
    .split('\n')[0]
    .slice(0, 80)
    || '（タイトルなし）';
};

const fmtDate = (d) => {
  if (!d) return '—';
  const dt = new Date(d);
  return `${dt.getMonth()+1}/${dt.getDate()}`;
};
const daysSince = (d) => Math.floor((Date.now() - new Date(d)) / 86400000);
const isOverdueTask = (t) => t.due_date && new Date(t.due_date) < new Date() && t.status !== 'done' && t.status !== 'cancelled';

// ── タスクスライドパネル ──────────────────────────────────────────
function TaskPanel({ task, onClose, onStatusChange }) {
  const [status, setStatus] = useState(task.status);
  const [saving, setSaving] = useState(false);

  const handleStatus = async (s) => {
    setSaving(true);
    try {
      await api.taskSetStatus(task.id, s);
      setStatus(s);
      onStatusChange?.(task.id, s);
    } catch (e) { console.error(e); }
    finally { setSaving(false); }
  };

  const st = STATUS_CFG[status] || { label: status, color: '#94a3b8' };
  const title = cleanTitle(task.title, task.content);
  const overdue = isOverdueTask({ ...task, status });

  return (
    <>
      {/* オーバーレイ */}
      <div onClick={onClose}
        style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.3)', zIndex:400 }} />
      {/* パネル */}
      <div style={{ position:'fixed', top:0, right:0, bottom:0, width:'min(480px,90vw)', background:'#fff', zIndex:401, boxShadow:'-8px 0 32px rgba(0,0,0,0.12)', display:'flex', flexDirection:'column' }}>
        {/* ヘッダー */}
        <div style={{ padding:'16px 20px', borderBottom:'1px solid #f1f5f9', display:'flex', justifyContent:'space-between', alignItems:'flex-start' }}>
          <div style={{ flex:1, minWidth:0 }}>
            <span style={{ fontSize:'0.72rem', fontWeight:700, padding:'2px 9px', borderRadius:99, background:st.color+'18', color:st.color, display:'inline-block', marginBottom:6 }}>{st.label}</span>
            <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a', lineHeight:1.4 }}>{title}</div>
            {task.assignee_label && <div style={{ fontSize:'0.75rem', color:'#94a3b8', marginTop:4 }}>担当: {task.assignee_label}</div>}
          </div>
          <button onClick={onClose} style={{ background:'#f1f5f9', border:'none', borderRadius:8, width:30, height:30, cursor:'pointer', color:'#64748b', fontSize:16, flexShrink:0, marginLeft:10 }}>×</button>
        </div>

        {/* ステータス変更 */}
        <div style={{ padding:'12px 20px', borderBottom:'1px solid #f1f5f9' }}>
          <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginBottom:8, fontWeight:600 }}>ステータス変更</div>
          <div style={{ display:'flex', gap:6, flexWrap:'wrap' }}>
            {Object.entries(STATUS_CFG).map(([key, cfg]) => (
              <button key={key} onClick={() => handleStatus(key)} disabled={saving || status === key}
                style={{ padding:'5px 14px', borderRadius:99, border:`1.5px solid ${status===key ? cfg.color : '#e2e8f0'}`, cursor: status===key ? 'default' : 'pointer', fontSize:'0.78rem', fontWeight: status===key ? 700 : 500,
                  background: status===key ? cfg.color : '#fff', color: status===key ? '#fff' : '#64748b', transition:'all 0.12s' }}>
                {cfg.label}
              </button>
            ))}
          </div>
        </div>

        {/* コンテンツ */}
        <div style={{ flex:1, overflowY:'auto', padding:'16px 20px' }}>
          {task.due_date && (
            <div style={{ marginBottom:14, padding:'10px 14px', background: overdue ? '#fef2f2' : '#f8fafc', borderRadius:9, border:`1px solid ${overdue ? '#fca5a5' : '#f1f5f9'}` }}>
              <div style={{ fontSize:'0.68rem', color:'#94a3b8', marginBottom:2 }}>期限</div>
              <div style={{ fontWeight:700, color: overdue ? '#dc2626' : '#374151' }}>
                {new Date(task.due_date).toLocaleDateString('ja-JP')}
                {overdue && <span style={{ marginLeft:8, fontSize:'0.75rem' }}>{daysSince(task.due_date)}日超過</span>}
              </div>
            </div>
          )}
          {task.content && (
            <div>
              <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginBottom:6, fontWeight:600 }}>内容</div>
              <div style={{ fontSize:'0.85rem', color:'#374151', lineHeight:1.8, whiteSpace:'pre-wrap', wordBreak:'break-word' }}>
                {task.content.replace(/^(<@[^>]+>\s*)+/g, '').replace(/^(@[\w./　-鿿]+\s*)+/g, '')}
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

// ── メインダッシュボード ──────────────────────────────────────────
export default function Dashboard() {
  const [user,    setUser]    = useState(null);
  const [summary, setSummary] = useState(null);
  const [members, setMembers] = useState([]);
  const [projects, setProjects] = useState([]);
  const [dashTeams, setDashTeams] = useState([]);
  const [recentTasks, setRecentTasks] = useState([]);
  const [personalFilters, setPersonalFilters] = useState([]);
  const [filter, setFilter] = useState({ status:'', assignee:'', project:'', dashDept:'', dashTeam:'', personalFilter:'', overdue:false, sort:'', page:1 });
  const [filteredTasks, setFilteredTasks] = useState({ tasks:[], total:0 });
  const [filterApplied, setFilterApplied] = useState(false);
  const [loading, setLoading] = useState(true);
  const [selectedTask, setSelectedTask] = useState(null);

  useEffect(() => {
    Promise.all([
      api.me(), api.summary(), api.members(), api.projects(),
      api.workloadTeams(), api.personalFilters(),
      api.tasks({ limit: 8, sort: 'created_desc' }),
    ]).then(([me, sum, mem, proj, dt, pf, recent]) => {
      setUser(me);
      setSummary(sum.summary);
      setMembers(mem.members);
      setProjects(proj.projects);
      setDashTeams(dt.teams || []);
      setPersonalFilters(pf.filters || []);
      setRecentTasks(recent.tasks || []);
    }).catch(console.error).finally(() => setLoading(false));
  }, []);

  const setF = (patch) => setFilter(f => ({ ...f, ...patch, page:1 }));

  const deptTeams  = useMemo(() => dashTeams.filter(t => !t.parent_id), [dashTeams]);
  const childTeamsOf = useMemo(() => {
    const map = {};
    for (const t of dashTeams) {
      if (t.parent_id) { if (!map[t.parent_id]) map[t.parent_id] = []; map[t.parent_id].push(t); }
    }
    return map;
  }, [dashTeams]);

  const applyFilter = async () => {
    const params = { page: filter.page, limit: 50 };
    if (filter.status) params.status = filter.status;
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.project) params.project = filter.project;
    if (filter.dashTeam) params.dashTeam = filter.dashTeam;
    else if (filter.dashDept) params.dashTeam = filter.dashDept;
    if (filter.personalFilter) params.personalFilter = filter.personalFilter;
    if (filter.overdue) params.overdue = '1';
    if (filter.sort) params.sort = filter.sort;
    const res = await api.tasks(params);
    setFilteredTasks(res);
    setFilterApplied(true);
  };

  const clearFilter = () => {
    setFilter({ status:'', assignee:'', project:'', dashDept:'', dashTeam:'', personalFilter:'', overdue:false, sort:'', page:1 });
    setFilterApplied(false);
    setFilteredTasks({ tasks:[], total:0 });
  };

  const handleStatusChange = (taskId, newStatus) => {
    setRecentTasks(prev => prev.map(t => t.id === taskId ? { ...t, status: newStatus } : t));
    setFilteredTasks(prev => ({ ...prev, tasks: prev.tasks.map(t => t.id === taskId ? { ...t, status: newStatus } : t) }));
  };

  // KPIデータ
  const pieData = useMemo(() => summary
    ? Object.entries(STATUS_CFG).map(([k, c]) => ({ name: c.label, value: summary[k] || 0, color: c.color })).filter(d => d.value > 0)
    : [], [summary]);
  const totalTasks = pieData.reduce((s, d) => s + d.value, 0);

  // 期限切れ件数（membersデータから合計）
  const overdueCount = useMemo(() => members.reduce((s, m) => s + (m.overdue || 0), 0), [members]);

  // メンバーバーチャート（タスクのある上位8名）
  const memberBar = useMemo(() =>
    members.filter(m => m.total > 0).slice(0, 8).map(m => ({
      name: (m.displayName || '?').split(/[\s　/]/)[0],
      進行中: m.in_progress || 0,
      完了: m.done || 0,
    }))
  , [members]);

  if (loading) return <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'60vh', color:'#94a3b8' }}>読み込み中…</div>;

  const TaskRow = ({ t, borderBottom = true }) => {
    const title = cleanTitle(t.title, t.content);
    const st = STATUS_CFG[t.status] || { label: t.status, color: '#94a3b8' };
    const overdue = isOverdueTask(t);
    return (
      <div onClick={() => setSelectedTask(t)}
        style={{ display:'flex', alignItems:'center', gap:12, padding:'9px 0', borderBottom: borderBottom ? '1px solid #f1f5f9' : 'none', cursor:'pointer', transition:'background 0.1s' }}
        onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
        onMouseLeave={e => e.currentTarget.style.background=''}>
        <span style={{ width:8, height:8, borderRadius:'50%', background:st.color, flexShrink:0 }} />
        <div style={{ flex:1, minWidth:0 }}>
          <div style={{ fontSize:'0.85rem', fontWeight:600, color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{title}</div>
          <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>{t.assignee_label || '担当者なし'}</div>
        </div>
        <div style={{ textAlign:'right', flexShrink:0 }}>
          <span style={{ fontSize:'0.7rem', fontWeight:600, padding:'2px 8px', borderRadius:20, background:st.color+'18', color:st.color }}>{st.label}</span>
          {t.due_date && (
            <div style={{ fontSize:'0.65rem', marginTop:2, color: overdue ? '#dc2626' : '#94a3b8', fontWeight: overdue ? 700 : 400 }}>
              {overdue ? `${daysSince(t.due_date)}日超過` : `期限 ${fmtDate(t.due_date)}`}
            </div>
          )}
        </div>
      </div>
    );
  };

  const card = (children, style = {}) => (
    <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'16px 20px', ...style }}>{children}</div>
  );
  const sh = (label, sub) => (
    <div style={{ marginBottom:10 }}>
      <div style={{ fontWeight:700, fontSize:'0.88rem', color:'#0f172a' }}>{label}</div>
      {sub && <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>{sub}</div>}
    </div>
  );

  return (
    <div style={{ padding:'20px 24px', background:'#f8fafc', minHeight:'100%', display:'flex', flexDirection:'column', gap:14 }}>

      {/* KPIカード */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:12 }}>
        {[
          { label:'総タスク数', value: totalTasks, color:'#6366f1' },
          { label:'進行中',    value: summary?.in_progress || 0, color:'#3b82f6' },
          { label:'期限切れ',  value: overdueCount, color: overdueCount > 0 ? '#dc2626' : '#94a3b8' },
          { label:'完了',      value: summary?.done || 0, color:'#10b981' },
        ].map(k => (
          <div key={k.label} style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'14px 18px' }}>
            <div style={{ fontSize:'0.72rem', color:'#64748b', fontWeight:500, marginBottom:6 }}>{k.label}</div>
            <div style={{ fontSize:'1.8rem', fontWeight:900, color:k.color, lineHeight:1 }}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* グラフエリア */}
      <div style={{ display:'grid', gridTemplateColumns:'260px 1fr', gap:12 }}>
        {card(<>
          {sh('ステータス分布')}
          <div style={{ display:'flex', alignItems:'center', gap:12 }}>
            <PieChart width={120} height={120}>
              <Pie data={pieData} dataKey="value" cx="50%" cy="50%" outerRadius={54} innerRadius={30} paddingAngle={2}>
                {pieData.map((d, i) => <Cell key={i} fill={d.color} />)}
              </Pie>
              <Tooltip formatter={(v, n) => [`${v}件`, n]} contentStyle={{ fontSize:11, borderRadius:8 }} />
            </PieChart>
            <div style={{ flex:1, display:'flex', flexDirection:'column', gap:6 }}>
              {pieData.map(d => (
                <div key={d.name} style={{ display:'flex', alignItems:'center', gap:6 }}>
                  <span style={{ width:9, height:9, borderRadius:2, background:d.color, flexShrink:0 }} />
                  <span style={{ fontSize:'0.75rem', color:'#374151', flex:1 }}>{d.name}</span>
                  <span style={{ fontSize:'0.78rem', fontWeight:700, color:d.color }}>{d.value}</span>
                </div>
              ))}
            </div>
          </div>
        </>)}

        {card(<>
          {sh('メンバー別タスク数', 'タスクのある担当者')}
          {memberBar.length > 0 ? (
            <ResponsiveContainer width="100%" height={130}>
              <BarChart data={memberBar} margin={{ top:0, right:0, left:-20, bottom:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize:10, fill:'#94a3b8' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize:10, fill:'#94a3b8' }} axisLine={false} tickLine={false} allowDecimals={false} />
                <Tooltip contentStyle={{ fontSize:11, borderRadius:8 }} />
                <Bar dataKey="進行中" fill="#3b82f6" radius={[3,3,0,0]} maxBarSize={22} />
                <Bar dataKey="完了"   fill="#10b981" radius={[3,3,0,0]} maxBarSize={22} />
              </BarChart>
            </ResponsiveContainer>
          ) : <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', paddingTop:20 }}>データなし</div>}
        </>)}
      </div>

      {/* フィルター検索 */}
      {card(<>
        {sh('タスク検索')}
        <div style={{ display:'flex', gap:8, flexWrap:'wrap', marginBottom: filterApplied ? 12 : 0 }}>
          <select value={filter.status} onChange={e => setF({status:e.target.value})}
            style={{ padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' }}>
            <option value="">ステータス：すべて</option>
            <option value="in_progress">進行中</option>
            <option value="done">完了</option>
            <option value="pending">保留</option>
            <option value="cancelled">キャンセル</option>
          </select>

          <select value={filter.assignee} onChange={e => setF({assignee:e.target.value})}
            style={{ padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' }}>
            <option value="">担当者：全員</option>
            {members.map(m => <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>)}
          </select>

          {deptTeams.length > 0 && (
            <select value={filter.dashDept} onChange={e => setF({dashDept:e.target.value, dashTeam:''})}
              style={{ padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' }}>
              <option value="">部署：すべて</option>
              {deptTeams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          )}

          {filter.dashDept && childTeamsOf[filter.dashDept]?.length > 0 && (
            <select value={filter.dashTeam} onChange={e => setF({dashTeam:e.target.value})}
              style={{ padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' }}>
              <option value="">チーム：すべて</option>
              {childTeamsOf[filter.dashDept].map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          )}

          {projects.length > 0 && (
            <select value={filter.project} onChange={e => setF({project:e.target.value})}
              style={{ padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' }}>
              <option value="">プロジェクト：すべて</option>
              {projects.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
            </select>
          )}

          <select value={filter.sort} onChange={e => setF({sort:e.target.value})}
            style={{ padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' }}>
            <option value="">並び順：作成日</option>
            <option value="due_date_asc">期限：近い順</option>
            <option value="due_date_desc">期限：遠い順</option>
          </select>

          <label style={{ display:'flex', alignItems:'center', gap:5, fontSize:'0.8rem', color:'#374151', cursor:'pointer' }}>
            <input type="checkbox" checked={filter.overdue} onChange={e => setF({overdue:e.target.checked})} />
            期限切れのみ
          </label>

          <button onClick={applyFilter}
            style={{ padding:'6px 18px', background:'#1e40af', color:'#fff', border:'none', borderRadius:7, fontSize:'0.82rem', fontWeight:700, cursor:'pointer' }}>
            検索
          </button>
          {filterApplied && (
            <button onClick={clearFilter}
              style={{ padding:'6px 12px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', color:'#64748b', cursor:'pointer' }}>
              クリア
            </button>
          )}
        </div>

        {filterApplied && (
          filteredTasks.tasks.length === 0 ? (
            <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'16px 0' }}>該当するタスクがありません</div>
          ) : (
            <>
              <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginBottom:6 }}>{filteredTasks.total}件</div>
              <div style={{ maxHeight:400, overflowY:'auto' }}>
                {filteredTasks.tasks.map((t, i) => <TaskRow key={t.id} t={t} borderBottom={i < filteredTasks.tasks.length-1} />)}
              </div>
            </>
          )
        )}
      </>)}

      {/* 最新タスク（フィルター適用時は非表示） */}
      {!filterApplied && card(<>
        {sh('最新タスク', '直近8件')}
        {recentTasks.length === 0
          ? <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'12px 0' }}>タスクがありません</div>
          : recentTasks.map((t, i) => <TaskRow key={t.id} t={t} borderBottom={i < recentTasks.length-1} />)
        }
      </>)}

      {/* タスクスライドパネル */}
      {selectedTask && (
        <TaskPanel
          task={selectedTask}
          onClose={() => setSelectedTask(null)}
          onStatusChange={handleStatusChange}
        />
      )}
    </div>
  );
}
