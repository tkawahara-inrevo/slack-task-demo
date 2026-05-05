import { useEffect, useState, useCallback } from 'react';
import { api } from '../api/client';

const STATUS_CFG = {
  in_progress: { label: '進行中', color: '#3b82f6' },
  done:        { label: '完了',   color: '#10b981' },
  pending:     { label: '保留',   color: '#f59e0b' },
};
const STATUS_CHANGE = ['in_progress', 'done'];

const parseTitleLine = (raw) => {
  if (!raw) return '（タイトルなし）';
  const lines = raw.split('\n');
  for (const line of lines) {
    const c = line.replace(/<@[^>]+>/g, '').replace(/@\S+/g, '')
      .replace(/（[^）]*）/g, '').replace(/fyi\s*:/gi, '').replace(/\s+/g, ' ').trim();
    if (!c) continue;
    const slashes = (c.match(/\//g) || []).length;
    const isAddr = !/[。！？、：「」\d（）()]/.test(c) && /[一-鿿]+\/[A-Za-z]/.test(c) &&
      (c.length < 40 || (slashes >= 2 && /^[぀-鿿＀-￯a-zA-Z \/]+$/.test(c)));
    if (isAddr) continue;
    return c.slice(0, 80);
  }
  return '（タイトルなし）';
};

const fmtDate = (d) => {
  if (!d) return null;
  const dt = new Date(d);
  return `${dt.getFullYear()}/${dt.getMonth()+1}/${dt.getDate()}`;
};

const isOverdue = (t) => t.due_date && new Date(t.due_date) < new Date() && t.status !== 'done' && t.status !== 'cancelled';

// ── タスク詳細パネル（ポップアップ内） ──────────────────────────────
function TaskDetailPanel({ task, onClose, onStatusChange }) {
  const [status, setStatus] = useState(task.status);
  const [fullTask, setFullTask] = useState(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.taskGet?.(task.id).then(r => setFullTask(r.task || r)).catch(() => {});
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

  const d = fullTask || task;
  const st = STATUS_CFG[status] || { label: status, color: '#94a3b8' };
  const title = parseTitleLine(d.title || d.content);
  const body = d.content || d.title || '';
  const ov = isOverdue({ ...d, status });

  return (
    <div style={{ position:'fixed', inset:0, display:'flex', flexDirection:'column', background:'#fff', zIndex:50 }}>
      {/* ヘッダー */}
      <div style={{ padding:'12px 14px 10px', borderBottom:'1px solid #f1f5f9', background:'#fafafa' }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:6 }}>
          <span style={{ fontSize:'0.68rem', fontWeight:700, padding:'2px 8px', borderRadius:99,
            background:st.color+'18', color:st.color }}>{st.label}</span>
          <button onClick={onClose}
            style={{ background:'#f1f5f9', border:'none', borderRadius:6, width:26, height:26, cursor:'pointer', color:'#64748b', fontSize:14 }}>
            ←
          </button>
        </div>
        <div style={{ fontWeight:800, fontSize:'0.92rem', color:'#0f172a', lineHeight:1.45, marginBottom:6 }}>{title}</div>
        <div style={{ display:'flex', gap:10, flexWrap:'wrap' }}>
          {d.due_date && (
            <span style={{ fontSize:'0.72rem', fontWeight: ov?700:400, color: ov?'#dc2626':'#64748b' }}>
              📅 {fmtDate(d.due_date)}{ov ? '（超過）' : ''}
            </span>
          )}
          {d.project_name && <span style={{ fontSize:'0.72rem', color:'#6366f1' }}>📁 {d.project_name}</span>}
        </div>
      </div>

      {/* ステータス変更 */}
      <div style={{ padding:'10px 14px', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', gap:8 }}>
        <span style={{ fontSize:'0.7rem', color:'#94a3b8' }}>変更</span>
        {STATUS_CHANGE.map(key => {
          const cfg = STATUS_CFG[key];
          const active = status === key;
          return (
            <button key={key} onClick={() => handleStatus(key)} disabled={saving || active}
              style={{ padding:'4px 14px', borderRadius:99, border:`1.5px solid ${active ? cfg.color : '#e2e8f0'}`,
                cursor: active ? 'default' : 'pointer', fontSize:'0.78rem', fontWeight: active ? 700 : 500,
                background: active ? cfg.color : '#fff', color: active ? '#fff' : '#64748b' }}>
              {cfg.label}
            </button>
          );
        })}
      </div>

      {/* 本文 */}
      <div style={{ flex:1, overflowY:'auto', padding:'14px' }}>
        {body ? (
          <div style={{ fontSize:'0.82rem', color:'#374151', lineHeight:1.8, whiteSpace:'pre-wrap',
            wordBreak:'break-word', background:'#f8fafc', borderRadius:8, padding:'12px', border:'1px solid #f1f5f9' }}>
            {body}
          </div>
        ) : (
          <div style={{ color:'#cbd5e1', fontSize:'0.8rem', textAlign:'center', paddingTop:20 }}>（本文なし）</div>
        )}
      </div>
    </div>
  );
}

// ── メイン ──────────────────────────────────────────────────────────
export default function FloatingTasks() {
  const [me, setMe] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('active');
  const [selectedTask, setSelectedTask] = useState(null);

  const load = useCallback(() => {
    setLoading(true);
    api.me()
      .then(m => { setMe(m); return api.tasks({ assignee: m.userId, limit: 200 }); })
      .then(r => setTasks(r.tasks || []))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleStatusChange = (id, newStatus) => {
    setTasks(prev => prev.map(t => t.id === id ? { ...t, status: newStatus } : t));
    if (selectedTask?.id === id) setSelectedTask(s => s ? { ...s, status: newStatus } : s);
  };

  const filtered = tasks.filter(t => {
    if (filter === 'active') return t.status !== 'done' && t.status !== 'cancelled';
    if (filter === 'done') return t.status === 'done';
    return t.status !== 'cancelled';
  }).sort((a, b) => {
    const aOv = isOverdue(a), bOv = isOverdue(b);
    if (aOv && !bOv) return -1;
    if (!aOv && bOv) return 1;
    if (a.due_date && b.due_date) return new Date(a.due_date) - new Date(b.due_date);
    if (a.due_date) return -1;
    if (b.due_date) return 1;
    return new Date(b.created_at) - new Date(a.created_at);
  });

  return (
    <div style={{ fontFamily:'-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif', background:'#f8fafc', height:'100vh', display:'flex', flexDirection:'column', overflow:'hidden' }}>
      {/* ヘッダー */}
      <div style={{ background:'#1e2127', color:'#fff', padding:'10px 14px', display:'flex', alignItems:'center', justifyContent:'space-between', flexShrink:0 }}>
        <div style={{ fontWeight:700, fontSize:'0.88rem' }}>
          {me ? `${me.displayName?.split(/[\s　/]/)[0]} のタスク` : 'マイタスク'}
        </div>
        <div style={{ display:'flex', gap:6, alignItems:'center' }}>
          <span style={{ fontSize:'0.72rem', color:'#9ba1ad' }}>{filtered.length}件</span>
          <button onClick={load} style={{ background:'none', border:'none', color:'#9ba1ad', cursor:'pointer', fontSize:14, padding:'2px 4px' }} title="更新">↻</button>
        </div>
      </div>

      {/* タブ */}
      <div style={{ display:'flex', background:'#fff', borderBottom:'1px solid #e2e8f0', flexShrink:0 }}>
        {[['active','進行中・保留'],['done','完了'],['all','すべて']].map(([v,l]) => (
          <button key={v} onClick={() => setFilter(v)}
            style={{ flex:1, padding:'7px 0', border:'none', background:'none', cursor:'pointer', fontSize:'0.75rem',
              fontWeight: filter===v ? 700 : 400, color: filter===v ? '#1d4ed8' : '#64748b',
              borderBottom: filter===v ? '2px solid #1d4ed8' : '2px solid transparent' }}>
            {l}
          </button>
        ))}
      </div>

      {/* タスク一覧 */}
      <div style={{ flex:1, overflowY:'auto' }}>
        {loading ? (
          <div style={{ textAlign:'center', padding:'24px 0', color:'#94a3b8', fontSize:'0.82rem' }}>読み込み中…</div>
        ) : filtered.length === 0 ? (
          <div style={{ textAlign:'center', padding:'24px 0', color:'#94a3b8', fontSize:'0.82rem' }}>タスクなし</div>
        ) : (
          filtered.map(t => {
            const title = parseTitleLine(t.title || t.content);
            const st = STATUS_CFG[t.status] || { label: t.status, color: '#94a3b8' };
            const ov = isOverdue(t);
            return (
              <div key={t.id}
                style={{ padding:'10px 14px', borderBottom:'1px solid #f1f5f9', cursor:'pointer', background:'#fff' }}
                onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
                onMouseLeave={e => e.currentTarget.style.background='#fff'}
                onClick={() => setSelectedTask(t)}>
                <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', gap:8 }}>
                  <div style={{ fontSize:'0.82rem', fontWeight:600, color:'#0f172a', lineHeight:1.4, flex:1, minWidth:0,
                    overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                    {title}
                  </div>
                  <span style={{ fontSize:'0.65rem', fontWeight:700, padding:'2px 7px', borderRadius:20,
                    background:st.color+'18', color:st.color, flexShrink:0, whiteSpace:'nowrap' }}>
                    {st.label}
                  </span>
                </div>
                <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginTop:4 }}>
                  {t.due_date ? (
                    <span style={{ fontSize:'0.7rem', color: ov ? '#dc2626' : '#94a3b8', fontWeight: ov ? 700 : 400 }}>
                      📅 {fmtDate(t.due_date)}{ov ? ' 超過' : ''}
                    </span>
                  ) : <span />}
                  <button onClick={e => { e.stopPropagation(); if (t.status !== 'done') { api.taskSetStatus(t.id, 'done').then(() => handleStatusChange(t.id, 'done')).catch(console.error); } }}
                    style={{ fontSize:'0.65rem', padding:'2px 8px', border:'1px solid #d1d5db', borderRadius:4,
                      background:'#fff', color: t.status === 'done' ? '#94a3b8' : '#64748b', cursor: t.status === 'done' ? 'default' : 'pointer',
                      opacity: t.status === 'done' ? 0.5 : 1 }}
                    disabled={t.status === 'done'}>
                    {t.status === 'done' ? '完了済' : '完了にする'}
                  </button>
                </div>
              </div>
            );
          })
        )}
      </div>

      {/* タスク詳細パネル（ポップアップ内） */}
      {selectedTask && (
        <TaskDetailPanel
          task={selectedTask}
          onClose={() => setSelectedTask(null)}
          onStatusChange={handleStatusChange}
        />
      )}
    </div>
  );
}
