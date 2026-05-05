import { useEffect, useState, useCallback, useMemo } from 'react';
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
      .replace(/\*([^*]*)\*/g, '$1')  // Slack bold
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

const today = () => { const d = new Date(); d.setHours(0,0,0,0); return d; };
const isOverdue = (t) => t.due_date && new Date(t.due_date) < today() && t.status !== 'done' && t.status !== 'cancelled';
const isDueToday = (t) => {
  if (!t.due_date || t.status === 'done' || t.status === 'cancelled') return false;
  const d = new Date(t.due_date); d.setHours(0,0,0,0);
  return d.getTime() === today().getTime();
};

// Slackメンション簡易変換
function SlackText({ text, nameMap }) {
  if (!text) return null;
  const TOKEN = /(<@[^>]+>|<!subteam\^[^>]+>|<!(?:channel|here|everyone)>|<https?:[^>]+>|https?:\/\/[^\s<>]+|@U[A-Z0-9]{6,})/g;
  const parts = text.split(TOKEN);
  return (
    <>
      {parts.map((p, i) => {
        const um = p.match(/^<@([^|>]+)(?:\|([^>]+))?>$/);
        if (um) {
          const raw = um[2] || nameMap?.[um[1]] || null;
          const name = raw ? raw.split('/')[0].trim() : um[1];
          return <span key={i} style={{ color:'#3b82f6', fontWeight:600 }}>@{name}</span>;
        }
        const sm = p.match(/^<!subteam\^([^|>]+)(?:\|([^>]+))?>$/);
        if (sm) return <span key={i} style={{ color:'#8b5cf6', fontWeight:600 }}>@{(sm[2]||'').replace(/^@/,'') || sm[1].slice(0,8)}</span>;
        if (/^<!(channel|here|everyone)>$/i.test(p)) return <span key={i} style={{ color:'#f59e0b', fontWeight:600 }}>@{p.replace(/[<>!]/g,'')}</span>;
        const lm = p.match(/^<(https?:[^|>]+)(?:\|([^>]+))?>$/);
        if (lm) return <a key={i} href={lm[1]} target="_blank" rel="noopener noreferrer" style={{ color:'#3b82f6', wordBreak:'break-all' }}>{lm[2]||lm[1]}</a>;
        if (/^https?:\/\//.test(p)) return <a key={i} href={p} target="_blank" rel="noopener noreferrer" style={{ color:'#3b82f6', wordBreak:'break-all' }}>{p}</a>;
        const bm = p.match(/^@(U[A-Z0-9]{6,})$/);
        if (bm) return <span key={i} style={{ color:'#3b82f6', fontWeight:600 }}>@{nameMap?.[bm[1]] ? nameMap[bm[1]].split('/')[0].trim() : bm[1]}</span>;
        return <span key={i}>{p}</span>;
      })}
    </>
  );
}

const fmtTs = (ts) => {
  if (!ts) return '';
  const d = new Date(parseFloat(ts) * 1000);
  return `${d.getMonth()+1}/${d.getDate()} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
};

// ── タスク詳細パネル（ポップアップ内） ──────────────────────────────
function TaskDetailPanel({ task, onClose, onStatusChange }) {
  const [status, setStatus] = useState(task.status);
  const [fullTask, setFullTask] = useState(null);
  const [threadData, setThreadData] = useState(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.taskGet?.(task.id).then(r => setFullTask(r.task || r)).catch(() => {});
    api.taskThread?.(task.id)
      .then(r => setThreadData({ messages: r.messages || [], nameMap: r.nameMap || {}, channel: r.channel || null }))
      .catch(() => setThreadData({ messages: [], nameMap: {}, channel: null }));
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
  const thread = threadData?.messages || null;
  const nameMap = useMemo(() => {
    const base = { ...(threadData?.nameMap || {}) };
    for (const m of thread || []) {
      if (m.user_id && m.displayName && !base[m.user_id]) base[m.user_id] = m.displayName.split('/')[0].trim();
    }
    return base;
  }, [threadData]);
  const slackUrl = d.source_permalink || null;

  return (
    <div style={{ position:'fixed', inset:0, display:'flex', flexDirection:'column', background:'#fff', zIndex:50 }}>
      {/* ヘッダー */}
      <div style={{ padding:'10px 12px 8px', borderBottom:'1px solid #f1f5f9', background:'#fafafa', flexShrink:0 }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:5 }}>
          <span style={{ fontSize:'0.65rem', fontWeight:700, padding:'2px 7px', borderRadius:99, background:st.color+'18', color:st.color }}>{st.label}</span>
          <button onClick={onClose} style={{ background:'#f1f5f9', border:'none', borderRadius:6, width:24, height:24, cursor:'pointer', color:'#64748b', fontSize:13 }}>←</button>
        </div>
        <div style={{ fontWeight:800, fontSize:'0.88rem', color:'#0f172a', lineHeight:1.4, marginBottom:4 }}>{title}</div>
        <div style={{ display:'flex', gap:8, flexWrap:'wrap', alignItems:'center' }}>
          {d.due_date && (
            <span style={{ fontSize:'0.7rem', fontWeight: ov?700:400, color: ov?'#dc2626':'#64748b' }}>
              📅 {fmtDate(d.due_date)}{ov ? '（超過）' : ''}
            </span>
          )}
          {threadData?.channel && (
            <span style={{ fontSize:'0.7rem', color:'#64748b' }}>
              {threadData.channel.is_private ? '🔒' : '#'}{threadData.channel.name}
            </span>
          )}
          {slackUrl && (
            <a href={slackUrl} target="_blank" rel="noopener noreferrer"
              style={{ fontSize:'0.68rem', color:'#3b82f6', textDecoration:'none', fontWeight:600 }}>Slackで開く ↗</a>
          )}
        </div>
      </div>

      {/* ステータス変更 */}
      <div style={{ padding:'8px 12px', borderBottom:'1px solid #f1f5f9', display:'flex', alignItems:'center', gap:7, flexShrink:0 }}>
        <span style={{ fontSize:'0.68rem', color:'#94a3b8' }}>変更</span>
        {STATUS_CHANGE.map(key => {
          const cfg = STATUS_CFG[key];
          const active = status === key;
          return (
            <button key={key} onClick={() => handleStatus(key)} disabled={saving || active}
              style={{ padding:'3px 12px', borderRadius:99, border:`1.5px solid ${active ? cfg.color : '#e2e8f0'}`,
                cursor: active ? 'default' : 'pointer', fontSize:'0.75rem', fontWeight: active ? 700 : 500,
                background: active ? cfg.color : '#fff', color: active ? '#fff' : '#64748b' }}>
              {cfg.label}
            </button>
          );
        })}
      </div>

      {/* 本文 + スレッド */}
      <div style={{ flex:1, overflowY:'auto', padding:'12px' }}>
        {body && (
          <div style={{ fontSize:'0.8rem', color:'#374151', lineHeight:1.75, whiteSpace:'pre-wrap',
            wordBreak:'break-word', background:'#f8fafc', borderRadius:8, padding:'10px 12px',
            border:'1px solid #f1f5f9', marginBottom:12 }}>
            <SlackText text={body} nameMap={nameMap} />
          </div>
        )}

        {/* スレッド */}
        {thread === null && <div style={{ fontSize:'0.72rem', color:'#cbd5e1', textAlign:'center' }}>スレッド読み込み中…</div>}
        {thread !== null && thread.length > 0 && (
          <div>
            <div style={{ fontSize:'0.68rem', color:'#94a3b8', fontWeight:600, marginBottom:8 }}>
              スレッド（{thread.length}件）
            </div>
            <div style={{ display:'flex', flexDirection:'column', gap:10 }}>
              {thread.map(m => {
                const jaName = (m.displayName || '?').split('/')[0].trim();
                return (
                  <div key={m.ts} style={{ display:'flex', gap:8, alignItems:'flex-start' }}>
                    {m.avatar_url
                      ? <img src={m.avatar_url} alt={jaName} style={{ width:26, height:26, borderRadius:'50%', flexShrink:0, objectFit:'cover', border: m.is_root ? '2px solid #3b82f6' : '1px solid #e2e8f0' }} />
                      : <div style={{ width:26, height:26, borderRadius:'50%', background: m.is_root ? '#dbeafe' : '#f1f5f9', display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0, fontSize:'0.6rem', fontWeight:700, color: m.is_root ? '#1d4ed8' : '#64748b' }}>{jaName.slice(0,2)}</div>
                    }
                    <div style={{ flex:1, minWidth:0 }}>
                      <div style={{ display:'flex', alignItems:'baseline', gap:5, marginBottom:2, flexWrap:'wrap' }}>
                        <span style={{ fontSize:'0.72rem', fontWeight:700, color: m.is_root ? '#1e40af' : '#374151' }}>{jaName}</span>
                        {m.is_root && <span style={{ fontSize:'0.58rem', background:'#dbeafe', color:'#1d4ed8', padding:'1px 5px', borderRadius:3, fontWeight:600 }}>元メッセージ</span>}
                        <span style={{ fontSize:'0.6rem', color:'#cbd5e1', marginLeft:'auto' }}>{fmtTs(m.ts)}</span>
                      </div>
                      <div style={{ fontSize:'0.78rem', color:'#374151', lineHeight:1.65, whiteSpace:'pre-wrap', wordBreak:'break-word' }}>
                        <SlackText text={m.text} nameMap={nameMap} />
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
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
  const [hideSelfDone, setHideSelfDone] = useState(true);
  const [selectedTask, setSelectedTask] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const load = useCallback((silent = false) => {
    if (!silent) setLoading(true);
    const doLoad = (userId) => api.tasks({ assignee: userId, limit: 200 })
      .then(r => { setTasks(r.tasks || []); setLastUpdated(new Date()); })
      .catch(console.error)
      .finally(() => { if (!silent) setLoading(false); });

    if (me) {
      doLoad(me.userId);
      if (!silent) setLoading(false);
    } else {
      api.me()
        .then(m => { setMe(m); return doLoad(m.userId); })
        .catch(console.error)
        .finally(() => setLoading(false));
    }
  }, [me]);

  // 初回ロード
  useEffect(() => { load(); }, []); // eslint-disable-line

  // 60秒ごとの自動更新
  useEffect(() => {
    const id = setInterval(() => load(true), 60000);
    return () => clearInterval(id);
  }, [load]);

  // ウィンドウサイズ・位置を記憶（beforeunload）
  useEffect(() => {
    const save = () => {
      try {
        localStorage.setItem('float_w', window.outerWidth);
        localStorage.setItem('float_h', window.outerHeight);
        localStorage.setItem('float_x', window.screenX);
        localStorage.setItem('float_y', window.screenY);
      } catch {}
    };
    window.addEventListener('beforeunload', save);
    return () => window.removeEventListener('beforeunload', save);
  }, []);

  const handleStatusChange = (id, newStatus) => {
    setTasks(prev => prev.map(t => t.id === id ? { ...t, status: newStatus } : t));
    if (selectedTask?.id === id) setSelectedTask(s => s ? { ...s, status: newStatus } : s);
  };

  const filtered = tasks.filter(t => {
    if (filter === 'active') {
      if (t.status === 'done' || t.status === 'cancelled') return false;
      if (hideSelfDone && t.self_completed) return false;
      return true;
    }
    if (filter === 'done') return t.status === 'done';
    return t.status !== 'cancelled';
  }).sort((a, b) => {
    // self_completed は下に
    if (a.self_completed && !b.self_completed) return 1;
    if (!a.self_completed && b.self_completed) return -1;
    const aOv = isOverdue(a), bOv = isOverdue(b);
    const aT = isDueToday(a), bT = isDueToday(b);
    if (aOv && !bOv) return -1;
    if (!aOv && bOv) return 1;
    if (aT && !bT) return -1;
    if (!aT && bT) return 1;
    if (a.due_date && b.due_date) return new Date(a.due_date) - new Date(b.due_date);
    if (a.due_date) return -1;
    if (b.due_date) return 1;
    return new Date(b.created_at) - new Date(a.created_at);
  });

  const updatedStr = lastUpdated
    ? `${String(lastUpdated.getHours()).padStart(2,'0')}:${String(lastUpdated.getMinutes()).padStart(2,'0')} 更新`
    : '';

  return (
    <div style={{ fontFamily:'-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif', background:'#f8fafc', height:'100vh', display:'flex', flexDirection:'column', overflow:'hidden' }}>
      {/* ヘッダー */}
      <div style={{ background:'#1e2127', color:'#fff', padding:'9px 12px', display:'flex', alignItems:'center', justifyContent:'space-between', flexShrink:0 }}>
        <div style={{ fontWeight:700, fontSize:'0.85rem' }}>
          {me ? `${me.displayName?.split(/[\s　/]/)[0]} のタスク` : 'マイタスク'}
        </div>
        <div style={{ display:'flex', gap:6, alignItems:'center' }}>
          <span style={{ fontSize:'0.65rem', color:'#6b7280' }}>{updatedStr}</span>
          {filter === 'active' && (
            <button onClick={() => setHideSelfDone(v => !v)}
              style={{ fontSize:'0.62rem', padding:'2px 7px', border:`1px solid ${hideSelfDone ? '#10b981' : '#4b5563'}`, borderRadius:4,
                background: hideSelfDone ? '#d1fae5' : 'rgba(255,255,255,0.1)', color: hideSelfDone ? '#065f46' : '#9ba1ad', cursor:'pointer' }}
              title="自分完了済みを非表示/表示">
              {hideSelfDone ? '完了済み非表示' : '完了済み表示'}
            </button>
          )}
          <span style={{ fontSize:'0.7rem', color:'#9ba1ad' }}>{filtered.length}件</span>
          <button onClick={() => load(false)} style={{ background:'none', border:'none', color:'#9ba1ad', cursor:'pointer', fontSize:14, padding:'2px 4px' }} title="今すぐ更新">↻</button>
        </div>
      </div>

      {/* タブ */}
      <div style={{ display:'flex', background:'#fff', borderBottom:'1px solid #e2e8f0', flexShrink:0 }}>
        {[['active','進行中・保留'],['done','完了'],['all','すべて']].map(([v,l]) => (
          <button key={v} onClick={() => setFilter(v)}
            style={{ flex:1, padding:'6px 0', border:'none', background:'none', cursor:'pointer', fontSize:'0.72rem',
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
            const tod = isDueToday(t);
            const selfDone = !!t.self_completed;
            const dateColor = ov ? '#dc2626' : tod ? '#ea580c' : '#94a3b8';
            const dateFw = (ov || tod) ? 700 : 400;
            const borderColor = selfDone ? '#10b981' : ov ? '#dc2626' : tod ? '#ea580c' : 'transparent';
            return (
              <div key={t.id}
                style={{ padding:'9px 12px', borderBottom:'1px solid #f1f5f9', cursor:'pointer',
                  background: selfDone ? '#f0fdf4' : '#fff',
                  borderLeft: `3px solid ${borderColor}`, opacity: selfDone ? 0.75 : 1 }}
                onMouseEnter={e => e.currentTarget.style.background= selfDone ? '#dcfce7' : '#f8fafc'}
                onMouseLeave={e => e.currentTarget.style.background= selfDone ? '#f0fdf4' : '#fff'}
                onClick={() => setSelectedTask(t)}>
                <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', gap:8 }}>
                  <div style={{ fontSize:'0.8rem', fontWeight:600, color: selfDone ? '#64748b' : '#0f172a', lineHeight:1.35, flex:1, minWidth:0,
                    overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                    {title}
                  </div>
                  <span style={{ fontSize:'0.62rem', fontWeight:700, padding:'2px 6px', borderRadius:20,
                    background:st.color+'18', color:st.color, flexShrink:0, whiteSpace:'nowrap' }}>
                    {st.label}
                  </span>
                </div>
                <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginTop:3 }}>
                  <div style={{ display:'flex', gap:8, alignItems:'center' }}>
                    {t.due_date && (
                      <span style={{ fontSize:'0.68rem', color: dateColor, fontWeight: dateFw }}>
                        📅 {fmtDate(t.due_date)}{ov ? ' 超過' : tod ? ' 今日' : ''}
                      </span>
                    )}
                    {selfDone && (
                      <span style={{ fontSize:'0.62rem', color:'#059669', fontWeight:700 }}>✓ 自分完了済み</span>
                    )}
                  </div>
                  {!selfDone && (
                    <button
                      onClick={e => { e.stopPropagation(); if (t.status !== 'done') { api.taskSetStatus(t.id, 'done').then(() => handleStatusChange(t.id, 'done')).catch(console.error); } }}
                      style={{ fontSize:'0.62rem', padding:'2px 7px', border:'1px solid #d1d5db', borderRadius:4,
                        background:'#fff', color: t.status === 'done' ? '#94a3b8' : '#64748b',
                        cursor: t.status === 'done' ? 'default' : 'pointer', opacity: t.status === 'done' ? 0.5 : 1 }}
                      disabled={t.status === 'done'}>
                      {t.status === 'done' ? '完了済' : '完了'}
                    </button>
                  )}
                </div>
              </div>
            );
          })
        )}
      </div>

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
