import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { PieChart, Pie, Cell, Tooltip, BarChart, Bar, XAxis, YAxis, CartesianGrid, ResponsiveContainer, LineChart, Line } from 'recharts';
import { api } from '../api/client';

const STATUS_CFG = {
  in_progress: { label: '進行中', color: '#3b82f6' },
  done:        { label: '完了',   color: '#10b981' },
  pending:     { label: '保留',   color: '#f59e0b' },
  cancelled:   { label: 'キャンセル', color: '#94a3b8' },
};

const fmtDate = (d) => {
  if (!d) return '—';
  const dt = new Date(d);
  return `${dt.getMonth()+1}/${dt.getDate()}`;
};

const daysSince = (d) => Math.floor((Date.now() - new Date(d)) / 86400000);

export default function Dashboard() {
  const navigate = useNavigate();
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

  const activeFilterCount = [filter.status, filter.assignee, filter.project, filter.dashDept, filter.dashTeam, filter.personalFilter, filter.overdue, filter.sort].filter(Boolean).length;

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

  // サマリーデータ整形
  const pieData = useMemo(() => {
    if (!summary) return [];
    return Object.entries(STATUS_CFG)
      .map(([key, cfg]) => ({ name: cfg.label, value: summary[key] || 0, color: cfg.color }))
      .filter(d => d.value > 0);
  }, [summary]);

  const totalTasks = pieData.reduce((s, d) => s + d.value, 0);

  // メンバー別タスク数（棒グラフ）
  const memberBar = useMemo(() =>
    members.slice(0, 8).map(m => ({
      name: m.displayName?.split(/[\s　]/)[0] || m.displayName || '?',
      進行中: m.in_progress_count || 0,
      完了: m.done_count || 0,
    }))
  , [members]);

  // 期限切れタスク数
  const overdueCount = recentTasks.filter(t => t.due_date && new Date(t.due_date) < new Date() && t.status !== 'done' && t.status !== 'cancelled').length;

  if (loading) return (
    <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'60vh', color:'#94a3b8', fontSize:'0.9rem' }}>
      読み込み中…
    </div>
  );

  const card = (children, style = {}) => (
    <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'16px 20px', ...style }}>
      {children}
    </div>
  );

  const sectionHead = (label, sub) => (
    <div style={{ marginBottom:12 }}>
      <div style={{ fontWeight:700, fontSize:'0.88rem', color:'#0f172a' }}>{label}</div>
      {sub && <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginTop:1 }}>{sub}</div>}
    </div>
  );

  return (
    <div style={{ padding:'20px 24px', background:'#f8fafc', minHeight:'100%', display:'flex', flexDirection:'column', gap:16 }}>

      {/* ── KPIカード ── */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:12 }}>
        {[
          { label:'総タスク数', value: totalTasks, color:'#6366f1', bg:'#eef2ff' },
          { label:'進行中', value: summary?.in_progress || 0, color:'#3b82f6', bg:'#eff6ff' },
          { label:'期限切れ', value: overdueCount, color: overdueCount > 0 ? '#dc2626' : '#94a3b8', bg: overdueCount > 0 ? '#fef2f2' : '#f8fafc' },
          { label:'完了', value: summary?.done || 0, color:'#10b981', bg:'#f0fdf4' },
        ].map(k => (
          <div key={k.label} style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'14px 18px' }}>
            <div style={{ fontSize:'0.72rem', color:'#64748b', fontWeight:500, marginBottom:6 }}>{k.label}</div>
            <div style={{ fontSize:'1.8rem', fontWeight:900, color:k.color, lineHeight:1 }}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* ── グラフエリア ── */}
      <div style={{ display:'grid', gridTemplateColumns:'280px 1fr', gap:12 }}>

        {/* ステータス円グラフ */}
        {card(<>
          {sectionHead('ステータス分布')}
          <div style={{ display:'flex', alignItems:'center', gap:16 }}>
            <PieChart width={130} height={130}>
              <Pie data={pieData} dataKey="value" cx="50%" cy="50%" outerRadius={58} innerRadius={34} paddingAngle={2}>
                {pieData.map((d, i) => <Cell key={i} fill={d.color} />)}
              </Pie>
              <Tooltip formatter={(v, name) => [`${v}件`, name]} contentStyle={{ fontSize:11, borderRadius:8 }} />
            </PieChart>
            <div style={{ flex:1, display:'flex', flexDirection:'column', gap:7 }}>
              {pieData.map(d => (
                <div key={d.name} style={{ display:'flex', alignItems:'center', gap:7 }}>
                  <span style={{ width:10, height:10, borderRadius:3, background:d.color, flexShrink:0 }} />
                  <span style={{ fontSize:'0.78rem', color:'#374151', flex:1 }}>{d.name}</span>
                  <span style={{ fontSize:'0.82rem', fontWeight:700, color:d.color }}>{d.value}</span>
                </div>
              ))}
            </div>
          </div>
        </>, {})}

        {/* メンバー別棒グラフ */}
        {card(<>
          {sectionHead('メンバー別タスク数', '進行中 / 完了')}
          {memberBar.length > 0 ? (
            <ResponsiveContainer width="100%" height={130}>
              <BarChart data={memberBar} margin={{ top:0, right:0, left:-20, bottom:0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                <XAxis dataKey="name" tick={{ fontSize:10, fill:'#94a3b8' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize:10, fill:'#94a3b8' }} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={{ fontSize:11, borderRadius:8 }} />
                <Bar dataKey="進行中" fill="#3b82f6" radius={[3,3,0,0]} maxBarSize={24} />
                <Bar dataKey="完了"   fill="#10b981" radius={[3,3,0,0]} maxBarSize={24} />
              </BarChart>
            </ResponsiveContainer>
          ) : <div style={{ color:'#94a3b8', fontSize:'0.82rem', paddingTop:16, textAlign:'center' }}>データなし</div>}
        </>, {})}
      </div>

      {/* ── 最新タスク ── */}
      {card(<>
        {sectionHead('最新タスク', '直近8件')}
        {recentTasks.length === 0 ? (
          <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'12px 0' }}>タスクがありません</div>
        ) : (
          <div style={{ display:'flex', flexDirection:'column', gap:0 }}>
            {recentTasks.map((t, i) => {
              const st = STATUS_CFG[t.status] || { label:t.status, color:'#94a3b8' };
              const isOverdue = t.due_date && new Date(t.due_date) < new Date() && t.status !== 'done' && t.status !== 'cancelled';
              return (
                <div key={t.id} onClick={() => navigate(`/tasks/${t.id}`)}
                  style={{ display:'flex', alignItems:'center', gap:12, padding:'9px 0', borderBottom: i < recentTasks.length-1 ? '1px solid #f1f5f9' : 'none', cursor:'pointer' }}
                  onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
                  onMouseLeave={e => e.currentTarget.style.background=''}>
                  <span style={{ width:8, height:8, borderRadius:'50%', background:st.color, flexShrink:0 }} />
                  <div style={{ flex:1, minWidth:0 }}>
                    <div style={{ fontSize:'0.85rem', fontWeight:600, color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{t.title}</div>
                    <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>{t.assignee_name || '担当者なし'}</div>
                  </div>
                  <div style={{ textAlign:'right', flexShrink:0 }}>
                    <span style={{ fontSize:'0.72rem', fontWeight:600, padding:'2px 8px', borderRadius:20, background:st.color+'18', color:st.color }}>{st.label}</span>
                    {t.due_date && (
                      <div style={{ fontSize:'0.68rem', marginTop:2, color: isOverdue ? '#dc2626' : '#94a3b8', fontWeight: isOverdue ? 700 : 400 }}>
                        {isOverdue ? `${daysSince(t.due_date)}日超過` : `期限 ${fmtDate(t.due_date)}`}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </>)}

      {/* ── フィルター検索 ── */}
      {card(<>
        {sectionHead('タスク検索', 'フィルターを入力して検索')}
        <div style={{ display:'flex', gap:8, flexWrap:'wrap', marginBottom:12 }}>
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
            <button onClick={() => { setFilter({ status:'', assignee:'', project:'', dashDept:'', dashTeam:'', personalFilter:'', overdue:false, sort:'', page:1 }); setFilterApplied(false); setFilteredTasks({tasks:[],total:0}); }}
              style={{ padding:'6px 12px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', color:'#64748b', cursor:'pointer' }}>
              クリア
            </button>
          )}
        </div>

        {/* 検索結果 */}
        {filterApplied && (
          filteredTasks.tasks.length === 0 ? (
            <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'16px 0' }}>該当するタスクがありません</div>
          ) : (
            <>
              <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginBottom:8 }}>{filteredTasks.total}件</div>
              <div style={{ display:'flex', flexDirection:'column', gap:0, maxHeight:400, overflowY:'auto' }}>
                {filteredTasks.tasks.map((t, i) => {
                  const st = STATUS_CFG[t.status] || { label:t.status, color:'#94a3b8' };
                  const isOverdue = t.due_date && new Date(t.due_date) < new Date() && t.status !== 'done' && t.status !== 'cancelled';
                  return (
                    <div key={t.id} onClick={() => navigate(`/tasks/${t.id}`)}
                      style={{ display:'flex', alignItems:'center', gap:12, padding:'9px 0', borderBottom: i < filteredTasks.tasks.length-1 ? '1px solid #f1f5f9' : 'none', cursor:'pointer' }}
                      onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
                      onMouseLeave={e => e.currentTarget.style.background=''}>
                      <span style={{ width:8, height:8, borderRadius:'50%', background:st.color, flexShrink:0 }} />
                      <div style={{ flex:1, minWidth:0 }}>
                        <div style={{ fontSize:'0.85rem', fontWeight:600, color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{t.title}</div>
                        <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>{t.assignee_name || '担当者なし'}</div>
                      </div>
                      <div style={{ textAlign:'right', flexShrink:0 }}>
                        <span style={{ fontSize:'0.72rem', fontWeight:600, padding:'2px 8px', borderRadius:20, background:st.color+'18', color:st.color }}>{st.label}</span>
                        {t.due_date && (
                          <div style={{ fontSize:'0.68rem', marginTop:2, color: isOverdue ? '#dc2626' : '#94a3b8', fontWeight: isOverdue ? 700 : 400 }}>
                            {isOverdue ? `${daysSince(t.due_date)}日超過` : `期限 ${fmtDate(t.due_date)}`}
                          </div>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </>
          )
        )}
      </>)}
    </div>
  );
}
