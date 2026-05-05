import { useEffect, useState, useCallback } from 'react';
import { api } from '../api/client';

const STATUS_CFG = {
  in_progress: { label: '進行中', color: '#3b82f6' },
  done:        { label: '完了',   color: '#10b981' },
  pending:     { label: '保留',   color: '#f59e0b' },
};

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
  return `${dt.getMonth()+1}/${dt.getDate()}`;
};

const isOverdue = (t) => t.due_date && new Date(t.due_date) < new Date() && t.status !== 'done' && t.status !== 'cancelled';

export default function FloatingTasks() {
  const [me, setMe] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('active'); // active | all | done

  const load = useCallback(() => {
    setLoading(true);
    api.me()
      .then(m => { setMe(m); return api.tasks({ assignee: m.userId, limit: 200 }); })
      .then(r => setTasks(r.tasks || []))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleStatusChange = async (id, status) => {
    await api.taskSetStatus(id, status).catch(console.error);
    setTasks(prev => prev.map(t => t.id === id ? { ...t, status } : t));
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

  const openInMain = (id) => {
    const target = window.opener || window.parent;
    if (target && target !== window) {
      try { target.location.href = `/?task=${id}`; target.focus(); return; } catch {}
    }
    window.open(`/?task=${id}`, '_blank');
  };

  return (
    <div style={{ fontFamily:'-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif', background:'#f8fafc', minHeight:'100vh', display:'flex', flexDirection:'column' }}>
      {/* ヘッダー */}
      <div style={{ background:'#1e2127', color:'#fff', padding:'10px 14px', display:'flex', alignItems:'center', justifyContent:'space-between', position:'sticky', top:0, zIndex:10 }}>
        <div style={{ fontWeight:700, fontSize:'0.88rem' }}>
          {me ? `${me.displayName?.split(/[\s　/]/)[0]} のタスク` : 'マイタスク'}
        </div>
        <div style={{ display:'flex', gap:6, alignItems:'center' }}>
          <span style={{ fontSize:'0.72rem', color:'#9ba1ad' }}>{filtered.length}件</span>
          <button onClick={load} style={{ background:'none', border:'none', color:'#9ba1ad', cursor:'pointer', fontSize:14, padding:'2px 4px' }} title="更新">↻</button>
        </div>
      </div>

      {/* タブ */}
      <div style={{ display:'flex', background:'#fff', borderBottom:'1px solid #e2e8f0' }}>
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
                onClick={() => openInMain(t.id)}>
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
                  {t.status !== 'done' && (
                    <button onClick={e => { e.stopPropagation(); handleStatusChange(t.id, 'done'); }}
                      style={{ fontSize:'0.65rem', padding:'2px 8px', border:'1px solid #d1d5db', borderRadius:4,
                        background:'#fff', color:'#64748b', cursor:'pointer' }}>
                      完了にする
                    </button>
                  )}
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
