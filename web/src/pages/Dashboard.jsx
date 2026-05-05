import { useEffect, useMemo, useState } from 'react';
import { api } from '../api/client';

// キャンセルを除外したステータス設定
const STATUS_CFG = {
  in_progress: { label: '進行中', color: '#3b82f6' },
  done:        { label: '完了',   color: '#10b981' },
  pending:     { label: '保留',   color: '#f59e0b' },
};
// ステータス変更ボタンは進行中・完了のみ
const STATUS_CHANGE = ['in_progress', 'done'];

// タイトル候補から「アドレス行」（名前/英名形式・短い・句読点なし）をスキップして
// 最初の意味ある行をタイトルとして取得し、その後ろを本文として返す
const parseTitleAndBody = (title, content) => {
  const raw = title || content || '';
  const lines = raw.split('\n');
  let titleLine = '（タイトルなし）';
  let titleIdx = 0;

  for (let i = 0; i < lines.length; i++) {
    // メンション・（Cc:）・fyi: を除去してタイトル候補として評価
    // @\S+ でハイフン含むチャンネル名(@hr-direction等)も除去
    const candidate = lines[i]
      .replace(/<@[^>]+>/g, '')
      .replace(/@\S+/g, '')
      .replace(/（[^）]*）/g, '')
      .replace(/\([^)]*\)/g, '')
      .replace(/fyi\s*:/gi, '')
      .replace(/cc\s*:/gi, '')
      .replace(/\s+/g, ' ')
      .trim();

    if (!candidate) continue;

    // 「名前/英名」形式の短いアドレス行はスキップ（例: 煌/Kagari Doi）
    const isAddressLine =
      /[一-鿿A-Za-z]+\/[A-Za-z一-鿿 ]+/.test(candidate) &&
      candidate.length < 35 &&
      !/[。！？、：]/.test(candidate);
    if (isAddressLine) continue;

    titleLine = candidate.slice(0, 100);
    titleIdx = i;
    break;
  }

  // 本文：タイトル行の次から最後まで（メンショントリミングなし）
  const body = lines.slice(titleIdx + 1).join('\n').trim();
  return { title: titleLine, body };
};

const cleanTitle = (title, content) => parseTitleAndBody(title, content).title;

// 担当者名をクリーン（@除去・先頭の1名だけ）
const cleanAssigneeName = (label) => {
  if (!label) return null;
  const first = label.trim().replace(/^@/, '').split(/\s+@/)[0];
  return first.split('/')[0].trim() || first.trim() || null;
};

// 検索プレビュー用本文（メンション除去あり）
const getContentBody = (rawText) => {
  const { body } = parseTitleAndBody(rawText, null);
  return body
    .replace(/<@[^>]+>/g, '')
    .replace(/@\S+/g, '')
    .trim();
};

const fmtDate = (d) => {
  if (!d) return '—';
  const dt = new Date(d);
  return `${dt.getFullYear()}/${dt.getMonth()+1}/${dt.getDate()}`;
};
const daysSince = (d) => Math.floor((Date.now() - new Date(d)) / 86400000);
const isOverdueTask = (t) => t.due_date && new Date(t.due_date) < new Date() && t.status !== 'done' && t.status !== 'cancelled';

// Slackメッセージテキストの簡易フォーマット（<@U...>→@名前、URLリンク化）
function SlackText({ text, nameMap }) {
  if (!text) return null;
  const parts = text.split(/(<@[^>]+>|<https?:[^>]+>|https?:\/\/\S+)/g);
  return (
    <>
      {parts.map((p, i) => {
        const mention = p.match(/^<@([^|>]+)(?:\|([^>]+))?>$/);
        if (mention) {
          const name = mention[2] || nameMap?.[mention[1]] || mention[1];
          return <span key={i} style={{ color:'#3b82f6', fontWeight:600 }}>@{name}</span>;
        }
        const linkTag = p.match(/^<(https?:[^|>]+)(?:\|([^>]+))?>$/);
        if (linkTag) {
          return <a key={i} href={linkTag[1]} target="_blank" rel="noopener noreferrer" style={{ color:'#3b82f6' }}>{linkTag[2] || linkTag[1]}</a>;
        }
        if (/^https?:\/\/\S+$/.test(p)) {
          return <a key={i} href={p} target="_blank" rel="noopener noreferrer" style={{ color:'#3b82f6' }}>{p}</a>;
        }
        return <span key={i}>{p}</span>;
      })}
    </>
  );
}

// ── タスクスライドパネル ──────────────────────────────────────────
function TaskPanel({ task, members, onClose, onStatusChange }) {
  const [status, setStatus] = useState(task.status);
  const [fullTask, setFullTask] = useState(null);
  const [thread, setThread] = useState(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.taskGet?.(task.id).then(r => setFullTask(r.task || r)).catch(() => {});
    api.taskThread?.(task.id).then(r => setThread(r.messages || [])).catch(() => setThread([]));
  }, [task.id]);

  const handleStatus = async (s) => {
    setSaving(true);
    try {
      await api.taskSetStatus(task.id, s);
      setStatus(s);
      onStatusChange?.(task.id, s);
    } catch (e) { console.error(e); }
    finally { setSaving(false); }
  };

  const displayTask = fullTask || task;
  const st = STATUS_CFG[status] || { label: status, color: '#94a3b8' };
  const title = cleanTitle(displayTask.title, displayTask.content);
  const overdue = isOverdueTask({ ...displayTask, status });
  // パネル本文：一切加工しない（content優先、なければtitle全文）
  const contentBody = displayTask.content || displayTask.title || '';

  // 担当者名を解決（cleanAssigneeNameでクリーン → なければmembersでルックアップ）
  const assigneeName = cleanAssigneeName(displayTask.assignee_label) ||
    members.find(m => m.assignee_id === displayTask.assignee_id)?.displayName?.split('/')[0]?.trim() || null;

  return (
    <>
      <div onClick={onClose} style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.35)', zIndex:400 }} />
      <div style={{ position:'fixed', top:0, right:0, bottom:0, width:'min(520px,90vw)', background:'#fff', zIndex:401, boxShadow:'-8px 0 32px rgba(0,0,0,0.14)', display:'flex', flexDirection:'column' }}>

        {/* ヘッダー */}
        <div style={{ padding:'18px 22px 14px', borderBottom:'1px solid #f1f5f9', background:'#fafafa' }}>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:8 }}>
            <span style={{ fontSize:'0.72rem', fontWeight:700, padding:'3px 10px', borderRadius:99, background:st.color+'18', color:st.color }}>
              {st.label}
            </span>
            <button onClick={onClose} style={{ background:'#f1f5f9', border:'none', borderRadius:8, width:28, height:28, cursor:'pointer', color:'#64748b', fontSize:15 }}>×</button>
          </div>
          <div style={{ fontWeight:800, fontSize:'1rem', color:'#0f172a', lineHeight:1.5, marginBottom:6 }}>{title}</div>
          <div style={{ display:'flex', gap:12, flexWrap:'wrap' }}>
            {assigneeName && (
              <span style={{ fontSize:'0.75rem', color:'#64748b', display:'flex', alignItems:'center', gap:4 }}>
                <span style={{ fontSize:'0.8rem' }}>👤</span>{assigneeName}
              </span>
            )}
            {displayTask.due_date && (
              <span style={{ fontSize:'0.75rem', fontWeight: overdue?700:400, color: overdue?'#dc2626':'#64748b', display:'flex', alignItems:'center', gap:4 }}>
                <span style={{ fontSize:'0.8rem' }}>📅</span>
                {fmtDate(displayTask.due_date)}
                {overdue && <span style={{ color:'#dc2626' }}>（{daysSince(displayTask.due_date)}日超過）</span>}
              </span>
            )}
            {displayTask.project_name && (
              <span style={{ fontSize:'0.75rem', color:'#6366f1', display:'flex', alignItems:'center', gap:4 }}>
                <span>📁</span>{displayTask.project_name}
              </span>
            )}
          </div>
        </div>

        {/* ステータス変更（進行中・完了のみ） */}
        <div style={{ padding:'12px 22px', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', gap:10 }}>
          <span style={{ fontSize:'0.72rem', color:'#94a3b8', flexShrink:0 }}>変更</span>
          {STATUS_CHANGE.map(key => {
            const cfg = STATUS_CFG[key];
            const active = status === key;
            return (
              <button key={key} onClick={() => handleStatus(key)} disabled={saving || active}
                style={{ padding:'5px 16px', borderRadius:99, border:`1.5px solid ${active ? cfg.color : '#e2e8f0'}`, cursor: active ? 'default' : 'pointer', fontSize:'0.8rem', fontWeight: active ? 700 : 500,
                  background: active ? cfg.color : '#fff', color: active ? '#fff' : '#64748b', transition:'all 0.12s' }}>
                {cfg.label}
              </button>
            );
          })}
        </div>

        {/* 本文 + Slackスレッド */}
        <div style={{ flex:1, overflowY:'auto', padding:'18px 22px', display:'flex', flexDirection:'column', gap:16 }}>
          {contentBody ? (
            <div>
              <div style={{ fontSize:'0.72rem', color:'#94a3b8', fontWeight:600, marginBottom:8 }}>内容</div>
              <div style={{ fontSize:'0.85rem', color:'#374151', lineHeight:1.9, whiteSpace:'pre-wrap', wordBreak:'break-word', background:'#f8fafc', borderRadius:10, padding:'14px 16px', border:'1px solid #f1f5f9' }}>
                {contentBody}
              </div>
            </div>
          ) : (
            <div style={{ color:'#cbd5e1', fontSize:'0.82rem', textAlign:'center', paddingTop:4 }}>（本文なし）</div>
          )}

          {/* Slackスレッド */}
          {thread === null && (
            <div style={{ fontSize:'0.75rem', color:'#cbd5e1', textAlign:'center' }}>スレッド読み込み中…</div>
          )}
          {thread !== null && thread.length > 0 && (() => {
            const nameMap = Object.fromEntries(thread.filter(m => m.user_id).map(m => [m.user_id, m.displayName?.split(/[\s　/]/)[0] || m.user_id]));
            return (
              <div>
                <div style={{ fontSize:'0.72rem', color:'#94a3b8', fontWeight:600, marginBottom:10 }}>
                  Slackスレッド（{thread.length}件）
                </div>
                <div style={{ display:'flex', flexDirection:'column', gap:10 }}>
                  {thread.map((m, i) => (
                    <div key={m.ts} style={{ display:'flex', gap:10, alignItems:'flex-start' }}>
                      <div style={{ width:30, height:30, borderRadius:'50%', background: m.is_root ? '#dbeafe' : '#f1f5f9', display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0, fontSize:'0.7rem', fontWeight:700, color: m.is_root ? '#1d4ed8' : '#64748b' }}>
                        {(m.displayName || '?').split(/[\s　/]/)[0].slice(0, 2)}
                      </div>
                      <div style={{ flex:1, minWidth:0 }}>
                        <div style={{ display:'flex', alignItems:'baseline', gap:6, marginBottom:3 }}>
                          <span style={{ fontSize:'0.78rem', fontWeight:700, color: m.is_root ? '#1d4ed8' : '#374151' }}>
                            {m.displayName?.split('/')[0]?.trim() || '?'}
                          </span>
                          {m.is_root && <span style={{ fontSize:'0.65rem', color:'#94a3b8' }}>元メッセージ</span>}
                        </div>
                        <div style={{ fontSize:'0.82rem', color:'#374151', lineHeight:1.7, whiteSpace:'pre-wrap', wordBreak:'break-word' }}>
                          <SlackText text={m.text} nameMap={nameMap} />
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            );
          })()}
        </div>
      </div>
    </>
  );
}

// ── タスクカード行（検索結果・最新タスク共通） ──────────────────────
function TaskCard({ t, members, onClick, compact = false }) {
  const title = cleanTitle(t.title, t.content);
  const st = STATUS_CFG[t.status] || { label: t.status, color: '#94a3b8' };
  const overdue = isOverdueTask(t);
  const assigneeName = cleanAssigneeName(t.assignee_label) ||
    members.find(m => m.assignee_id === t.assignee_id)?.displayName?.split('/')[0]?.trim() || null;
  const contentPreview = getContentBody(t.title || t.content).slice(0, 80);

  if (compact) {
    // 最新タスク（シンプル行）
    return (
      <div onClick={onClick} style={{ display:'flex', alignItems:'center', gap:12, padding:'10px 0', borderBottom:'1px solid #f1f5f9', cursor:'pointer' }}
        onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
        onMouseLeave={e => e.currentTarget.style.background=''}>
        <span style={{ width:8, height:8, borderRadius:'50%', background:st.color, flexShrink:0 }} />
        <div style={{ flex:1, minWidth:0 }}>
          <div style={{ fontSize:'0.85rem', fontWeight:600, color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{title}</div>
          <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>
            {assigneeName ? assigneeName : <span style={{ color:'#e2e8f0' }}>担当者未設定</span>}
          </div>
        </div>
        <div style={{ textAlign:'right', flexShrink:0 }}>
          <span style={{ fontSize:'0.7rem', fontWeight:600, padding:'2px 8px', borderRadius:20, background:st.color+'18', color:st.color }}>{st.label}</span>
          {t.due_date && <div style={{ fontSize:'0.65rem', marginTop:2, color: overdue?'#dc2626':'#94a3b8', fontWeight: overdue?700:400 }}>{overdue ? `${daysSince(t.due_date)}日超過` : fmtDate(t.due_date)}</div>}
        </div>
      </div>
    );
  }

  // 検索結果（カードスタイル）
  return (
    <div onClick={onClick}
      style={{ background:'#fff', borderRadius:10, border:'1px solid #e2e8f0', padding:'12px 16px', cursor:'pointer', transition:'box-shadow 0.15s, border-color 0.15s' }}
      onMouseEnter={e => { e.currentTarget.style.boxShadow='0 4px 12px rgba(0,0,0,0.08)'; e.currentTarget.style.borderColor='#c7d2fe'; }}
      onMouseLeave={e => { e.currentTarget.style.boxShadow='none'; e.currentTarget.style.borderColor='#e2e8f0'; }}>
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', gap:10, marginBottom:6 }}>
        <div style={{ fontWeight:700, fontSize:'0.88rem', color:'#0f172a', flex:1, minWidth:0, lineHeight:1.4 }}>{title}</div>
        <span style={{ fontSize:'0.7rem', fontWeight:700, padding:'3px 10px', borderRadius:99, background:st.color+'18', color:st.color, flexShrink:0, whiteSpace:'nowrap' }}>{st.label}</span>
      </div>
      {contentPreview && (
        <div style={{ fontSize:'0.75rem', color:'#64748b', lineHeight:1.5, marginBottom:8, overflow:'hidden', textOverflow:'ellipsis', display:'-webkit-box', WebkitLineClamp:2, WebkitBoxOrient:'vertical' }}>
          {contentPreview}
        </div>
      )}
      <div style={{ display:'flex', gap:12, alignItems:'center', flexWrap:'wrap' }}>
        {assigneeName && (
          <span style={{ fontSize:'0.72rem', color:'#94a3b8', display:'flex', alignItems:'center', gap:3 }}>
            👤 {assigneeName}
          </span>
        )}
        {t.due_date && (
          <span style={{ fontSize:'0.72rem', fontWeight: overdue?700:400, color: overdue?'#dc2626':'#94a3b8', display:'flex', alignItems:'center', gap:3 }}>
            📅 {fmtDate(t.due_date)}{overdue && ` （${daysSince(t.due_date)}日超過）`}
          </span>
        )}
        {t.project_name && (
          <span style={{ fontSize:'0.72rem', color:'#6366f1' }}>📁 {t.project_name}</span>
        )}
      </div>
    </div>
  );
}

// ── メインダッシュボード ──────────────────────────────────────────
export default function Dashboard() {
  const [me, setMe] = useState(null);
  const [mySummary, setMySummary] = useState(null);
  const [members, setMembers] = useState([]);
  const [projects, setProjects] = useState([]);
  const [dashTeams, setDashTeams] = useState([]);
  const [myTasks, setMyTasks] = useState([]);
  const [filter, setFilter] = useState({ status:'', assignee:'', project:'', team:'', overdue:false, page:1 });
  const [filteredTasks, setFilteredTasks] = useState({ tasks:[], total:0 });
  const [filterApplied, setFilterApplied] = useState(false);
  const [loading, setLoading] = useState(true);
  const [selectedTask, setSelectedTask] = useState(null);

  useEffect(() => {
    api.me()
      .then(m => Promise.all([
        Promise.resolve(m),
        api.summary({ scope: 'self' }),
        api.members(),
        api.projects(),
        api.workloadTeams(),
        api.tasks({ assignee: m.userId, limit: 100 }),
      ]))
      .then(([meRes, sumRes, memRes, projRes, dtRes, myTasksRes]) => {
        setMe(meRes);
        setMySummary(sumRes.summary);
        setMembers(memRes.members);
        setProjects(projRes.projects);
        setDashTeams(dtRes.teams || []);
        setMyTasks(myTasksRes.tasks || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const setF = (patch) => setFilter(f => ({ ...f, ...patch, page:1 }));

  // 自分が属する部署の配下チーム（MKなど複数チームある部署向けフィルター）
  const myDeptTeams = useMemo(() => {
    if (!me || !dashTeams.length) return [];
    const myTeamIds = new Set((me.dashTeams || []).map(t => t.id));
    const parentIds = new Set();
    for (const t of dashTeams) {
      if (!myTeamIds.has(t.id)) continue;
      if (t.parent_id) parentIds.add(t.parent_id);
      else parentIds.add(t.id);
    }
    return dashTeams.filter(t => t.parent_id && parentIds.has(t.parent_id));
  }, [me, dashTeams]);

  // 自分のアクティブタスク（完了・キャンセル除外、期限切れ→期限近い順）
  const activeTasks = useMemo(() =>
    myTasks
      .filter(t => t.status !== 'done' && t.status !== 'cancelled')
      .sort((a, b) => {
        const aOv = a.due_date && new Date(a.due_date) < new Date();
        const bOv = b.due_date && new Date(b.due_date) < new Date();
        if (aOv && !bOv) return -1;
        if (!aOv && bOv) return 1;
        if (a.due_date && b.due_date) return new Date(a.due_date) - new Date(b.due_date);
        if (a.due_date) return -1;
        if (b.due_date) return 1;
        return 0;
      })
  , [myTasks]);

  const applyFilter = async () => {
    const params = { page: filter.page, limit: 50 };
    if (filter.status)   params.status   = filter.status;
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.project)  params.project  = filter.project;
    if (filter.team)     params.dashTeam = filter.team;
    if (filter.overdue)  params.overdue  = '1';
    const res = await api.tasks(params);
    setFilteredTasks(res);
    setFilterApplied(true);
  };

  const clearFilter = () => {
    setFilter({ status:'', assignee:'', project:'', team:'', overdue:false, page:1 });
    setFilterApplied(false);
    setFilteredTasks({ tasks:[], total:0 });
  };

  const handleStatusChange = (taskId, newStatus) => {
    setMyTasks(prev => prev.map(t => t.id===taskId ? {...t, status:newStatus} : t));
    setFilteredTasks(prev => ({ ...prev, tasks: prev.tasks.map(t => t.id===taskId ? {...t, status:newStatus} : t) }));
    if (selectedTask?.id === taskId) setSelectedTask(s => s ? {...s, status:newStatus} : s);
  };

  const myOverdue = mySummary?._overdue || 0;
  const myTotal = useMemo(() =>
    Object.entries(STATUS_CFG).reduce((s, [k]) => s + (mySummary?.[k] || 0), 0)
  , [mySummary]);

  if (loading) return <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'60vh', color:'#94a3b8' }}>読み込み中…</div>;

  const card = (children, style={}) => (
    <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'16px 20px', ...style }}>{children}</div>
  );
  const sh = (label, sub) => (
    <div style={{ marginBottom:10 }}>
      <div style={{ fontWeight:700, fontSize:'0.88rem', color:'#0f172a' }}>{label}</div>
      {sub && <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>{sub}</div>}
    </div>
  );
  const selStyle = { padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' };
  const myName = me?.displayName?.split(/[\s　/]/)[0] || '';

  return (
    <div style={{ padding:'20px 24px', background:'#f8fafc', minHeight:'100%', display:'flex', flexDirection:'column', gap:14 }}>

      {myName && (
        <div style={{ fontSize:'0.82rem', color:'#64748b', fontWeight:600 }}>{myName} のタスク</div>
      )}

      {/* KPIカード */}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:12 }}>
        {[
          { label:'総タスク数', value: myTotal,                  color:'#6366f1' },
          { label:'進行中',    value: mySummary?.in_progress||0,  color:'#3b82f6' },
          { label:'期限切れ',  value: myOverdue,                  color: myOverdue > 0 ? '#dc2626' : '#94a3b8' },
          { label:'完了',      value: mySummary?.done||0,         color:'#10b981' },
        ].map(k => (
          <div key={k.label} style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'14px 18px' }}>
            <div style={{ fontSize:'0.72rem', color:'#64748b', fontWeight:500, marginBottom:6 }}>{k.label}</div>
            <div style={{ fontSize:'1.8rem', fontWeight:900, color:k.color, lineHeight:1 }}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* マイタスク一覧（デフォルト表示） */}
      {card(<>
        {sh('マイタスク', `進行中・保留 ${activeTasks.length}件`)}
        {activeTasks.length === 0 ? (
          <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'16px 0' }}>
            アクティブなタスクはありません
          </div>
        ) : (
          activeTasks.map(t => (
            <TaskCard key={t.id} t={t} members={members} onClick={() => setSelectedTask(t)} compact />
          ))
        )}
      </>)}

      {/* 部署タスク検索 */}
      {card(<>
        {sh('タスク検索', '同部署のタスクを検索')}
        <div style={{ display:'flex', gap:8, flexWrap:'wrap', marginBottom: filterApplied ? 14 : 0 }}>
          <select value={filter.status} onChange={e => setF({status:e.target.value})} style={selStyle}>
            <option value="">ステータス：すべて</option>
            <option value="in_progress">進行中</option>
            <option value="done">完了</option>
            <option value="pending">保留</option>
          </select>

          {myDeptTeams.length > 0 && (
            <select value={filter.team} onChange={e => setF({team:e.target.value})} style={selStyle}>
              <option value="">チーム：すべて</option>
              {myDeptTeams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          )}

          <select value={filter.assignee} onChange={e => setF({assignee:e.target.value})} style={selStyle}>
            <option value="">担当者：全員</option>
            {members.map(m => <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>)}
          </select>

          {projects.length > 0 && (
            <select value={filter.project} onChange={e => setF({project:e.target.value})} style={selStyle}>
              <option value="">プロジェクト：すべて</option>
              {projects.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
            </select>
          )}

          <label style={{ display:'flex', alignItems:'center', gap:5, fontSize:'0.8rem', color:'#374151', cursor:'pointer' }}>
            <input type="checkbox" checked={filter.overdue} onChange={e => setF({overdue:e.target.checked})} />
            期限切れのみ
          </label>

          <button onClick={applyFilter}
            style={{ padding:'6px 20px', background:'#1e40af', color:'#fff', border:'none', borderRadius:7, fontSize:'0.82rem', fontWeight:700, cursor:'pointer' }}>
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
            <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'24px 0' }}>該当するタスクがありません</div>
          ) : (
            <>
              <div style={{ fontSize:'0.72rem', color:'#94a3b8', marginBottom:10 }}>{filteredTasks.total}件</div>
              <div style={{ display:'flex', flexDirection:'column', gap:8 }}>
                {filteredTasks.tasks.map(t => (
                  <TaskCard key={t.id} t={t} members={members} onClick={() => setSelectedTask(t)} />
                ))}
              </div>
            </>
          )
        )}
      </>)}

      {selectedTask && (
        <TaskPanel task={selectedTask} members={members} onClose={() => setSelectedTask(null)} onStatusChange={handleStatusChange} />
      )}
    </div>
  );
}
