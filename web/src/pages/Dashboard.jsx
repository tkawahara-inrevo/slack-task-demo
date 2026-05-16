import React, { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api/client';
import { useBreakpoint } from '../hooks/useWindowWidth';

// ── デザイントークン（CSS変数参照でダークモード対応） ───────────────
const T = {
  card:      { background:'var(--surface)',   border:'1px solid var(--gray-200)', borderRadius:12 },
  cardInner: { background:'var(--surface-2)', border:'1px solid var(--gray-200)', borderRadius:8  },
  text:      'var(--gray-900)',
  textSub:   'var(--gray-500)',
  textMid:   'var(--gray-600)',
  border:    'var(--gray-200)',
  bg:        'var(--gray-50)',
  surface:   'var(--surface)',
  surface2:  'var(--surface-2)',
};
const wCard = (extra = {}) => ({ ...T.card, padding:'16px', ...extra });
const wHead = { display:'flex', alignItems:'center', gap:8, marginBottom:12 };
const wTitle = { fontSize:'0.88rem', fontWeight:700, color:T.text, flex:1 };

// ── 分析タブ用コンポーネント ──────────────────────────────────────
const STATUS_COLORS = { done:'#10b981', in_progress:'#3b82f6', pending:'#f59e0b', cancelled:'#94a3b8', overdue:'#dc2626' };

function AnalyticsBar({ value, max, color, label }) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:4 }}>
      {label && <span style={{ fontSize:'0.7rem', color:'#64748b', width:36, flexShrink:0 }}>{label}</span>}
      <div style={{ flex:1, height:6, background:'#f1f5f9', borderRadius:3, overflow:'hidden' }}>
        <div style={{ width:`${pct}%`, height:'100%', background:color, borderRadius:3 }} />
      </div>
      <span style={{ fontSize:'0.7rem', color:'#64748b', width:24, textAlign:'right' }}>{value}</span>
    </div>
  );
}

function ProgressRing({ rate }) {
  const r = 32, c = 2 * Math.PI * r;
  const color = rate >= 80 ? '#10b981' : rate >= 50 ? '#f59e0b' : '#dc2626';
  return (
    <svg width="80" height="80" viewBox="0 0 80 80">
      <circle cx="40" cy="40" r={r} fill="none" stroke="#f1f5f9" strokeWidth="7" />
      <circle cx="40" cy="40" r={r} fill="none" stroke={color} strokeWidth="7"
        strokeDasharray={c} strokeDashoffset={c - (rate/100)*c}
        strokeLinecap="round" transform="rotate(-90 40 40)" />
      <text x="40" y="40" textAnchor="middle" dominantBaseline="central" fontSize="14" fontWeight="700" fill={color}>{rate}%</text>
    </svg>
  );
}

function AnalyticsTab({ members, usergroups }) {
  const [periodSummary, setPeriodSummary] = useState(null);
  const [memberData, setMemberData] = useState(null);
  const [dueData, setDueData] = useState(null);
  const [projectData, setProjectData] = useState(null);
  const [filter, setFilter] = useState({ assignee:'', usergroup:'' });
  const [filterLoading, setFilterLoading] = useState(false);

  useEffect(() => {
    Promise.all([
      api.analyticsMemberCompletion(),
      api.analyticsDueCompliance(),
      api.analyticsProjectProgress(),
      api.analyticsPeriodSummary({ scope: 'self' }),
    ]).then(([m, d, p, ps]) => {
      setMemberData(m.members || []);
      setDueData(d.weeks || []);
      setProjectData(p.projects || []);
      setPeriodSummary(ps);
    }).catch(console.error);
  }, []);

  useEffect(() => {
    if (!memberData) return;
    setFilterLoading(true);
    const params = {};
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.usergroup) params.usergroup = filter.usergroup;
    Promise.all([api.analyticsMemberCompletion(params), api.analyticsDueCompliance(params)])
      .then(([m, d]) => { setMemberData(m.members || []); setDueData(d.weeks || []); })
      .catch(console.error)
      .finally(() => setFilterLoading(false));
  }, [filter]);

  if (!memberData) return <div style={{ textAlign:'center', padding:'40px 0', color:'#94a3b8' }}>読み込み中…</div>;

  const selStyle = { padding:'6px 10px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', outline:'none' };

  const PeriodCard = ({ label, data }) => {
    if (!data) return null;
    const cr = data.compliance_rate;
    const metrics = [
      { label: '新規割当', value: data.assigned,   unit: '件', color: '#6366f1' },
      { label: '期間完了', value: data.completed,  unit: '件', color: '#10b981' },
      { label: '遵守率',   value: cr ?? '—',       unit: cr != null ? '%' : '', color: cr == null ? '#94a3b8' : cr >= 80 ? '#10b981' : cr >= 50 ? '#f59e0b' : '#dc2626' },
      { label: '平均処理', value: data.avg_days,   unit: '日', color: '#64748b' },
    ];
    return (
      <div style={{ background:'#f8fafc', borderRadius:10, padding:'14px 16px' }}>
        <div style={{ fontSize:'0.78rem', fontWeight:700, color:'#64748b', marginBottom:12 }}>{label}</div>
        <div style={{ display:'grid', gridTemplateColumns:'repeat(4,1fr)', gap:8 }}>
          {metrics.map(m => (
            <div key={m.label} style={{ textAlign:'center' }}>
              <div style={{ fontSize:'1.4rem', fontWeight:900, color:m.color, lineHeight:1 }}>{m.value}<span style={{ fontSize:'0.7rem', fontWeight:500 }}>{m.unit}</span></div>
              <div style={{ fontSize:'0.65rem', color:'#94a3b8', marginTop:3 }}>{m.label}</div>
            </div>
          ))}
        </div>
      </div>
    );
  };
  const card = (children, style={}) => <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'16px 20px', ...style }}>{children}</div>;
  const sh = (label) => <div style={{ fontWeight:700, fontSize:'0.88rem', color:'#0f172a', marginBottom:12 }}>{label}</div>;
  const maxTotal = Math.max(1, ...memberData.map(m => m.total));
  const filteredMembers = members.filter(m => {
    if (!filter.usergroup) return true;
    const ug = usergroups.find(g => g.id === filter.usergroup);
    return ug?.memberIds?.includes(m.assignee_id);
  });

  return (
    <div style={{ display:'flex', flexDirection:'column', gap:14 }}>
      {/* フィルター */}
      <div style={{ display:'flex', gap:8, flexWrap:'wrap', alignItems:'center' }}>
        {usergroups.length > 0 && (
          <select value={filter.usergroup} onChange={e => setFilter(f => ({...f, usergroup:e.target.value, assignee:''}))} style={selStyle}>
            <option value="">チーム：すべて</option>
            {usergroups.map(g => <option key={g.id} value={g.id}>@{g.handle}</option>)}
          </select>
        )}
        <select value={filter.assignee} onChange={e => setFilter(f => ({...f, assignee:e.target.value}))} style={selStyle}>
          <option value="">担当者：全員</option>
          {filteredMembers.map(m => <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>)}
        </select>
        {(filter.assignee || filter.usergroup) && (
          <button onClick={() => setFilter({assignee:'', usergroup:''})}
            style={{ padding:'6px 12px', border:'1px solid #e2e8f0', borderRadius:7, fontSize:'0.8rem', background:'#fff', color:'#64748b', cursor:'pointer' }}>
            クリア
          </button>
        )}
        {filterLoading && <span style={{ fontSize:'0.75rem', color:'#94a3b8' }}>更新中…</span>}
      </div>

      {/* 自分の期間別サマリー */}
      {periodSummary && card(<>
        {sh('自分のタスクサマリー')}
        <div style={{ display:'flex', flexDirection:'column', gap:8 }}>
          <PeriodCard label="直近7日" data={periodSummary.d7} />
          <PeriodCard label="直近30日" data={periodSummary.d30} />
        </div>
      </>)}

      {/* メンバー別完了率 */}
      {card(<>
        {sh('メンバー別完了率')}
        {memberData.length === 0 ? (
          <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'12px 0' }}>データなし</div>
        ) : (
          <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(220px, 1fr))', gap:12 }}>
            {memberData.map(m => (
              <div key={m.assignee_id} style={{ background:'#f8fafc', borderRadius:10, padding:'12px 14px' }}>
                <div style={{ display:'flex', alignItems:'center', gap:10, marginBottom:10 }}>
                  <ProgressRing rate={m.completion_rate} />
                  <div>
                    <div style={{ fontWeight:700, fontSize:'0.85rem', color:'#0f172a' }}>{m.displayName?.split('/')[0]}</div>
                    <div style={{ fontSize:'0.7rem', color:'#94a3b8' }}>全 {m.total} タスク</div>
                  </div>
                </div>
                <AnalyticsBar value={m.done}        max={maxTotal} color={STATUS_COLORS.done}       label="完了" />
                <AnalyticsBar value={m.in_progress} max={maxTotal} color={STATUS_COLORS.in_progress} label="進行中" />
                <AnalyticsBar value={m.pending}     max={maxTotal} color={STATUS_COLORS.pending}     label="保留" />
                {m.overdue > 0 && <AnalyticsBar value={m.overdue} max={maxTotal} color={STATUS_COLORS.overdue} label="期限切れ" />}
              </div>
            ))}
          </div>
        )}
      </>)}

      {/* 期限遵守率の推移 */}
      {dueData && card(<>
        {sh('期限遵守率の推移（週別・過去12週）')}
        {dueData.length === 0 ? (
          <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'12px 0' }}>データなし</div>
        ) : (
          <div style={{ display:'flex', alignItems:'flex-end', gap:4, height:120, paddingBottom:20, position:'relative' }}>
            {dueData.map((w, i) => {
              const rate = w.compliance_rate ?? 0;
              const color = rate >= 80 ? STATUS_COLORS.done : rate >= 50 ? STATUS_COLORS.pending : STATUS_COLORS.overdue;
              return (
                <div key={i} style={{ flex:1, display:'flex', flexDirection:'column', alignItems:'center', gap:2 }}>
                  <div style={{ width:'100%', background:color, borderRadius:'3px 3px 0 0', height:`${rate}%`, minHeight: rate > 0 ? 2 : 0, transition:'height 0.2s' }}
                    title={`${rate}% (${w.on_time}/${w.with_due})`} />
                  <span style={{ fontSize:'0.55rem', color:'#94a3b8', transform:'rotate(-45deg)', whiteSpace:'nowrap', marginTop:2 }}>
                    {w.week_start?.slice(5,10).replace('-','/')}
                  </span>
                </div>
              );
            })}
          </div>
        )}
        <div style={{ display:'flex', gap:12, marginTop:4 }}>
          {[['期限内完了', STATUS_COLORS.done], ['要改善', STATUS_COLORS.pending], ['期限超過', STATUS_COLORS.overdue]].map(([l,c]) => (
            <span key={l} style={{ display:'flex', alignItems:'center', gap:4, fontSize:'0.7rem', color:'#64748b' }}>
              <span style={{ width:8, height:8, borderRadius:2, background:c, display:'inline-block' }} />{l}
            </span>
          ))}
        </div>
      </>)}

      {/* プロジェクト別進捗 */}
      {projectData && card(<>
        {sh('プロジェクト別進捗')}
        {projectData.length === 0 ? (
          <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'12px 0' }}>プロジェクトなし</div>
        ) : (
          <div style={{ display:'flex', flexDirection:'column', gap:10 }}>
            {projectData.map(p => (
              <Link key={p.project_id} to={`/projects/${p.project_id}`} style={{ textDecoration:'none' }}>
                <div style={{ background:'#f8fafc', borderRadius:10, padding:'10px 14px' }}>
                  <div style={{ display:'flex', justifyContent:'space-between', marginBottom:6 }}>
                    <span style={{ fontSize:'0.85rem', fontWeight:600, color:'#0f172a' }}>{p.project_name}</span>
                    <span style={{ fontSize:'0.82rem', fontWeight:700, color: p.progress_rate >= 80 ? '#10b981' : '#64748b' }}>{p.progress_rate}%</span>
                  </div>
                  <div style={{ height:6, background:'#e2e8f0', borderRadius:3, overflow:'hidden', display:'flex' }}>
                    {p.total > 0 && <>
                      <div style={{ width:`${(p.done/p.total)*100}%`, background:STATUS_COLORS.done }} />
                      <div style={{ width:`${(p.in_progress/p.total)*100}%`, background:STATUS_COLORS.in_progress }} />
                      <div style={{ width:`${(p.pending/p.total)*100}%`, background:STATUS_COLORS.pending }} />
                    </>}
                  </div>
                  <div style={{ display:'flex', gap:10, marginTop:4 }}>
                    {[['完了', p.done, STATUS_COLORS.done], ['進行中', p.in_progress, STATUS_COLORS.in_progress], ['保留', p.pending, STATUS_COLORS.pending]].map(([l,v,c]) => (
                      <span key={l} style={{ fontSize:'0.68rem', color:c }}>
                        {l} {v}
                      </span>
                    ))}
                    {p.overdue > 0 && <span style={{ fontSize:'0.68rem', color:STATUS_COLORS.overdue }}>期限切れ {p.overdue}</span>}
                  </div>
                </div>
              </Link>
            ))}
          </div>
        )}
      </>)}
    </div>
  );
}

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

    // 「名前/英名」形式のアドレス行をスキップ
    // 複数人（真有/Mayu 満奈実/Manami...）も対象：スラッシュが2個以上かつ日英文字・スペース・スラッシュだけ
    const slashCount = (candidate.match(/\//g) || []).length;
    const isAddressLine =
      !/[。！？、：「」\d（）()]/.test(candidate) &&
      /[一-鿿]+\/[A-Za-z]/.test(candidate) &&
      (candidate.length < 40 || (slashCount >= 2 && /^[぀-鿿＀-￯a-zA-Z \/]+$/.test(candidate)));
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

const fmtSlackTs = (ts) => {
  if (!ts) return '';
  const d = new Date(parseFloat(ts) * 1000);
  return `${d.getMonth()+1}/${d.getDate()} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
};

// Slackメッセージテキストの簡易フォーマット
// subteamMap: { [subteamId]: 'handle or name' } でユーザーグループ名を解決
function SlackText({ text, nameMap, subteamMap }) {
  if (!text) return null;
  const TOKEN = /(<@[^>]+>|<!subteam\^[^>]+>|<!(?:channel|here|everyone)>|<https?:[^>]+>|https?:\/\/[^\s<>]+|@U[A-Z0-9]{6,})/g;
  const parts = text.split(TOKEN);
  return (
    <>
      {parts.map((p, i) => {
        // ユーザーメンション <@UXXX> or <@UXXX|name>
        const userMention = p.match(/^<@([^|>]+)(?:\|([^>]+))?>$/);
        if (userMention) {
          const uid = userMention[1];
          const raw = userMention[2] || nameMap?.[uid] || null;
          const name = raw ? raw.split('/')[0].trim() : uid;
          return <span key={i} style={{ color:'#3b82f6', fontWeight:600 }}>@{name}</span>;
        }
        // 角括弧なしの bare @UXXX 形式
        const bareId = p.match(/^@(U[A-Z0-9]{6,})$/);
        if (bareId) {
          const name = nameMap?.[bareId[1]] ? nameMap[bareId[1]].split('/')[0].trim() : bareId[1];
          return <span key={i} style={{ color:'#3b82f6', fontWeight:600 }}>@{name}</span>;
        }
        // サブチームメンション <!subteam^SXXX|@handle> — subteamMapで名前解決
        const subteamMatch = p.match(/^<!subteam\^([^|>]+)(?:\|([^>]+))?>$/);
        if (subteamMatch) {
          const subteamId = subteamMatch[1];
          const labelInMsg = subteamMatch[2]?.replace(/^@/, '');
          const handle = subteamMap?.[subteamId] || labelInMsg || subteamId.slice(0, 8) + '…';
          return <span key={i} style={{ color:'#8b5cf6', fontWeight:600 }}>@{handle}</span>;
        }
        // <!channel> <!here> <!everyone>
        if (/^<!(channel|here|everyone)>$/i.test(p)) {
          const word = p.replace(/[<>!]/g, '');
          return <span key={i} style={{ color:'#f59e0b', fontWeight:600 }}>@{word}</span>;
        }
        // リンク <https://...> or <https://...|label>
        const linkTag = p.match(/^<(https?:[^|>]+)(?:\|([^>]+))?>$/);
        if (linkTag) {
          return <a key={i} href={linkTag[1]} target="_blank" rel="noopener noreferrer" style={{ color:'#3b82f6', wordBreak:'break-all' }}>{linkTag[2] || linkTag[1]}</a>;
        }
        // 生URL
        if (/^https?:\/\//.test(p)) {
          return <a key={i} href={p} target="_blank" rel="noopener noreferrer" style={{ color:'#3b82f6', wordBreak:'break-all' }}>{p}</a>;
        }
        return <span key={i}>{p}</span>;
      })}
    </>
  );
}

// ── タスクスライドパネル ──────────────────────────────────────────
function TaskPanel({ task, members, usergroups, onClose, onStatusChange }) {
  const [status, setStatus] = useState(task.status);
  const [fullTask, setFullTask] = useState(null);
  const [threadData, setThreadData] = useState(null);
  const [saving, setSaving] = useState(false);
  // 期日inline編集
  const [editingDue, setEditingDue] = useState(false);
  const [editDue, setEditDue] = useState('');
  // コメント
  const [comment, setComment] = useState('');
  const [mentionQuery, setMentionQuery] = useState('');
  const [showMentions, setShowMentions] = useState(false);
  const [mentionMap, setMentionMap] = useState({});
  const [commentSaving, setCommentSaving] = useState(false);
  const textareaRef = React.useRef(null);

  useEffect(() => {
    api.taskGet?.(task.id).then(r => {
      const t = r.task || r;
      setFullTask(t);
      setEditDue(t.due_date ? t.due_date.slice(0,10) : '');
    }).catch(() => {});
    api.taskThread?.(task.id)
      .then(r => setThreadData({ messages: r.messages || [], nameMap: r.nameMap || {}, subteamMap: r.subteamMap || {}, channel: r.channel || null }))
      .catch(() => setThreadData({ messages: [], nameMap: {}, subteamMap: {}, channel: null }));
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

  const handleDueSave = async (val) => {
    setEditingDue(false);
    const current = (fullTask?.due_date || '').slice(0,10);
    if (val === current) return;
    try {
      const r = await api.taskUpdateFields(task.id, { due_date: val || null });
      setFullTask(r.task || r);
    } catch (e) { console.error(e); }
  };

  const mentionCandidates = useMemo(() => {
    const q = mentionQuery.toLowerCase();
    const ms = members.filter(m => { const full=(m.displayName||'').toLowerCase(); return !q||full.includes(q); })
      .slice(0,8).map(m => ({ type:'user', id:m.assignee_id, label:m.displayName?.split('/')[0].trim() }));
    const gs = (usergroups||[]).filter(g => !q||g.handle.toLowerCase().includes(q))
      .slice(0,5).map(g => ({ type:'group', id:g.id, handle:g.handle, label:g.handle }));
    return [...ms, ...gs];
  }, [mentionQuery, members, usergroups]);

  const insertMention = (item) => {
    const insertion = `@${item.label} `;
    const ta = textareaRef.current;
    const textBefore = ta ? comment.slice(0, ta.selectionStart) : comment;
    const atIdx = textBefore.lastIndexOf('@');
    const before = atIdx >= 0 ? comment.slice(0, atIdx) : comment;
    const after = ta ? comment.slice(ta.selectionStart) : '';
    const newVal = before + insertion + after;
    setComment(newVal);
    setMentionMap(prev => ({ ...prev, [item.label]: item }));
    setShowMentions(false); setMentionQuery('');
    setTimeout(() => { if (ta) { ta.focus(); const p=before.length+insertion.length; ta.setSelectionRange(p,p); }}, 0);
  };

  const handleCommentChange = (e) => {
    const val = e.target.value; setComment(val);
    const before = val.slice(0, e.target.selectionStart);
    const m = before.match(/@([^@\s]*)$/);
    if (m) { setMentionQuery(m[1]); setShowMentions(true); } else { setShowMentions(false); setMentionQuery(''); }
  };

  const handleComment = async () => {
    if (!comment.trim()) return;
    setCommentSaving(true);
    try {
      let text = comment;
      for (const [label, item] of Object.entries(mentionMap)) {
        const esc = label.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
        text = text.replace(new RegExp(`@${esc}`,'g'), item.type==='group' ? `<!subteam^${item.id}|@${item.handle}>` : `<@${item.id}>`);
      }
      await api.taskAddComment(task.id, text.trim());
      setComment(''); setMentionMap({});
      api.taskThread?.(task.id).then(r => setThreadData({ messages: r.messages||[], nameMap: r.nameMap||{}, subteamMap: r.subteamMap||{}, channel: r.channel||null })).catch(() => {});
    } catch (e) { console.error(e); } finally { setCommentSaving(false); }
  };

  const displayTask = fullTask || task;
  const st = STATUS_CFG[status] || { label: status, color: '#94a3b8' };
  const title = cleanTitle(displayTask.title, displayTask.content);
  const overdue = isOverdueTask({ ...displayTask, status });
  // パネル本文：一切加工しない（content優先、なければtitle全文）
  const contentBody = displayTask.content || displayTask.title || '';
  // nameMap: サーバー側マップ + messages配列のuser_id→displayNameを補完（確実な変換のため）
  const thread = threadData?.messages || null;
  const nameMap = useMemo(() => {
    const base = { ...(threadData?.nameMap || {}) };
    for (const m of thread || []) {
      if (m.user_id && m.displayName && !base[m.user_id]) {
        base[m.user_id] = m.displayName.split('/')[0].trim();
      }
    }
    return base;
  }, [threadData]);
  const channel = threadData?.channel || null;

  // Slack deep link: source_permalink があればそれを使う、なければ構築
  const slackUrl = displayTask.source_permalink ||
    (displayTask.channel_id && displayTask.message_ts
      ? `https://app.slack.com/client/${displayTask.channel_id}`
      : null);
  // subteamId → handle: サーバー側キャッシュ（高精度）＋フロント側usergroups（フォールバック）
  const subteamMap = useMemo(() => ({
    ...Object.fromEntries((usergroups || []).map(g => [g.id, g.handle || g.name || g.id])),
    ...(threadData?.subteamMap || {}),
  }), [usergroups, threadData?.subteamMap]);

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
            {editingDue ? (
              <input type="date" autoFocus value={editDue}
                onChange={e => setEditDue(e.target.value)}
                onBlur={e => handleDueSave(e.target.value)}
                onKeyDown={e => { if (e.key==='Enter') handleDueSave(editDue); if (e.key==='Escape') setEditingDue(false); }}
                style={{ fontSize:'0.75rem', border:'1px solid #3b82f6', borderRadius:6, padding:'2px 8px', outline:'none' }} />
            ) : (
              <span onClick={() => setEditingDue(true)} title="クリックで期日変更"
                style={{ fontSize:'0.75rem', fontWeight: overdue?700:400, color: overdue?'#dc2626':'#64748b',
                  display:'flex', alignItems:'center', gap:4, cursor:'pointer',
                  padding:'2px 8px', borderRadius:99, background: overdue?'#fef2f2':'#f8fafc', border:'1px solid #e2e8f0' }}>
                <span>📅</span>
                {displayTask.due_date ? fmtDate(displayTask.due_date) : '期日なし'}
                {overdue && <span style={{ color:'#dc2626' }}>（{daysSince(displayTask.due_date)}日超過）</span>}
              </span>
            )}
            {displayTask.project_name && (
              <span style={{ fontSize:'0.75rem', color:'#6366f1', display:'flex', alignItems:'center', gap:4 }}>
                <span>📁</span>{displayTask.project_name}
              </span>
            )}
            {channel && (
              <span style={{ fontSize:'0.75rem', color:'#64748b', display:'flex', alignItems:'center', gap:4 }}>
                <span>{channel.is_private ? '🔒' : '#'}</span>{channel.name}
              </span>
            )}
          </div>
          {slackUrl && (
            <div style={{ marginTop:10 }}>
              <a href={slackUrl} target="_blank" rel="noopener noreferrer"
                style={{ display:'inline-flex', alignItems:'center', gap:6, padding:'5px 12px',
                  background:'#fff', border:'1px solid #e2e8f0', borderRadius:7, textDecoration:'none',
                  color:'#374151', fontSize:'0.78rem', fontWeight:600 }}>
                <svg width="14" height="14" viewBox="0 0 122 122" fill="none" xmlns="http://www.w3.org/2000/svg">
                  <path d="M25.7 77.5a12.8 12.8 0 1 1-25.6 0 12.8 12.8 0 0 1 25.6 0zm-6.4-38.2a12.8 12.8 0 1 0 0-25.6 12.8 12.8 0 0 0 0 25.6z" fill="#E01E5A"/>
                  <path d="M83.9 19.1a12.8 12.8 0 1 1 0 25.6 12.8 12.8 0 0 1 0-25.6zm6.4 58.4a12.8 12.8 0 1 0 25.6 0 12.8 12.8 0 0 0-25.6 0z" fill="#36C5F0"/>
                  <path d="M19.3 57.7a12.8 12.8 0 1 1 0 25.6 12.8 12.8 0 0 1 0-25.6zm64.6 6.4a12.8 12.8 0 1 0 25.6 0 12.8 12.8 0 0 0-25.6 0z" fill="#2EB67D"/>
                  <path d="M57.7 102.7a12.8 12.8 0 1 1 25.6 0 12.8 12.8 0 0 1-25.6 0zM64.1 19.1a12.8 12.8 0 1 0 0-25.6 12.8 12.8 0 0 0 0 25.6z" fill="#ECB22E"/>
                </svg>
                Slackで開く
              </a>
            </div>
          )}
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
        <div style={{ flex:1, overflowY:'auto', padding:'18px 22px 8px', display:'flex', flexDirection:'column', gap:16 }}>
          {contentBody ? (
            <div>
              <div style={{ fontSize:'0.72rem', color:'#94a3b8', fontWeight:600, marginBottom:8 }}>内容</div>
              <div style={{ fontSize:'0.85rem', color:'#374151', lineHeight:1.9, whiteSpace:'pre-wrap', wordBreak:'break-word', background:'#f8fafc', borderRadius:10, padding:'14px 16px', border:'1px solid #f1f5f9' }}>
                <SlackText text={contentBody} nameMap={nameMap} subteamMap={subteamMap} />
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
            return (
              <div>
                <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:10 }}>
                  <span style={{ fontSize:'0.72rem', color:'#94a3b8', fontWeight:600 }}>
                    Slackスレッド（{thread.length}件）{channel ? ` · ${channel.is_private ? '🔒' : '#'}${channel.name}` : ''}
                  </span>
                </div>
                <div style={{ display:'flex', flexDirection:'column', gap:12 }}>
                  {thread.map(m => {
                    const jaName = (m.displayName || '?').split('/')[0].trim();
                    const initials = jaName.slice(0, 2);
                    return (
                      <div key={m.ts} style={{ display:'flex', gap:10, alignItems:'flex-start' }}>
                        {/* アバター */}
                        {m.avatar_url ? (
                          <img src={m.avatar_url} alt={jaName}
                            style={{ width:32, height:32, borderRadius:'50%', flexShrink:0, objectFit:'cover', border: m.is_root ? '2px solid #3b82f6' : '1px solid #e2e8f0' }} />
                        ) : (
                          <div style={{ width:32, height:32, borderRadius:'50%', background: m.is_root ? '#dbeafe' : '#f1f5f9', display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0, fontSize:'0.68rem', fontWeight:700, color: m.is_root ? '#1d4ed8' : '#64748b', border: m.is_root ? '2px solid #3b82f6' : '1px solid #e2e8f0' }}>
                            {initials}
                          </div>
                        )}
                        <div style={{ flex:1, minWidth:0 }}>
                          <div style={{ display:'flex', alignItems:'baseline', gap:6, marginBottom:3, flexWrap:'wrap' }}>
                            <span style={{ fontSize:'0.78rem', fontWeight:700, color: m.is_root ? '#1e40af' : '#374151' }}>
                              {jaName}
                            </span>
                            {m.is_root && (
                              <span style={{ fontSize:'0.62rem', background:'#dbeafe', color:'#1d4ed8', padding:'1px 6px', borderRadius:4, fontWeight:600 }}>
                                元メッセージ
                              </span>
                            )}
                            <span style={{ fontSize:'0.65rem', color:'#cbd5e1', marginLeft:'auto' }}>
                              {fmtSlackTs(m.ts)}
                            </span>
                          </div>
                          <div style={{ fontSize:'0.82rem', color:'#374151', lineHeight:1.75, whiteSpace:'pre-wrap', wordBreak:'break-word' }}>
                            <SlackText text={m.text} nameMap={nameMap} subteamMap={subteamMap} />
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            );
          })()}
        </div>

        {/* コメントボックス（固定下部・Slackスタイル） */}
        <div style={{ borderTop:'1px solid #e2e8f0', background:'#fff', flexShrink:0, position:'relative' }}>
          {showMentions && mentionCandidates.length > 0 && (
            <div style={{ position:'absolute', bottom:'100%', left:0, right:0, background:'#fff', border:'1px solid #e2e8f0',
              borderRadius:'8px 8px 0 0', maxHeight:200, overflowY:'auto', boxShadow:'0 -4px 12px rgba(0,0,0,0.1)', zIndex:10 }}>
              {mentionCandidates.map(item => (
                <div key={`${item.type}:${item.id}`} onMouseDown={e => { e.preventDefault(); insertMention(item); }}
                  style={{ padding:'8px 16px', cursor:'pointer', fontSize:'0.82rem', display:'flex', alignItems:'center', gap:8 }}
                  onMouseEnter={e => e.currentTarget.style.background='#f8fafc'}
                  onMouseLeave={e => e.currentTarget.style.background=''}>
                  {item.type==='group'
                    ? <><span style={{ fontSize:'0.68rem', background:'#ede9fe', color:'#6d28d9', padding:'1px 6px', borderRadius:3, fontWeight:600 }}>グループ</span>@{item.label}</>
                    : <><span style={{ fontSize:'0.68rem', background:'#dbeafe', color:'#1d4ed8', padding:'1px 6px', borderRadius:3, fontWeight:600 }}>メンバー</span>{item.label}</>
                  }
                </div>
              ))}
            </div>
          )}
          <div style={{ padding:'10px 16px', display:'flex', gap:8, alignItems:'flex-end' }}>
            <textarea ref={textareaRef} value={comment} onChange={handleCommentChange}
              placeholder="返信する… （@でメンション）"
              rows={1}
              style={{ flex:1, padding:'8px 12px', border:'1px solid #e2e8f0', borderRadius:10, fontSize:'0.85rem',
                lineHeight:1.5, resize:'none', outline:'none', fontFamily:'inherit', maxHeight:100, overflowY:'auto' }}
              onKeyDown={e => {
                if (e.key==='Escape') { setShowMentions(false); return; }
                if (showMentions && (e.key==='Enter'||e.key==='Tab') && mentionCandidates.length>0) { e.preventDefault(); insertMention(mentionCandidates[0]); return; }
                if (e.key==='Enter' && !e.shiftKey && !showMentions) { e.preventDefault(); handleComment(); }
              }}
            />
            <button onClick={handleComment} disabled={commentSaving || !comment.trim()}
              style={{ padding:'8px 14px', border:'none', borderRadius:8, fontSize:'0.82rem', fontWeight:700, flexShrink:0,
                background: (!comment.trim()||commentSaving) ? '#f1f5f9' : '#2563eb',
                color: (!comment.trim()||commentSaving) ? '#94a3b8' : '#fff',
                cursor: (!comment.trim()||commentSaving) ? 'default' : 'pointer' }}>
              {commentSaving ? '…' : '送信'}
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

// ── タスクカード（グリッドレイアウト共通） ──────────────────────────
function TaskCard({ t, members, onClick }) {
  const title = cleanTitle(t.title, t.content);
  const preview = getContentBody(t.title || t.content).slice(0, 120);
  const st = STATUS_CFG[t.status] || { label: t.status, color: '#94a3b8' };
  const overdue = isOverdueTask(t);
  const assigneeName = cleanAssigneeName(t.assignee_label) ||
    members.find(m => m.assignee_id === t.assignee_id)?.displayName?.split('/')[0]?.trim() || null;
  const selfDone = t.task_type === 'broadcast' && t.self_completed;

  return (
    <div onClick={onClick}
      style={{ background:'var(--surface)', borderRadius:10, border:`1px solid var(--gray-200)`, padding:'11px 13px', cursor:'pointer',
        transition:'box-shadow 0.15s', display:'flex', flexDirection:'column', gap:7, minHeight:115,
        borderLeft: `3px solid ${selfDone ? '#10b981' : st.color}` }}
      onMouseEnter={e => { e.currentTarget.style.boxShadow='0 4px 12px rgba(0,0,0,0.08)'; }}
      onMouseLeave={e => { e.currentTarget.style.boxShadow='none'; }}>

      {/* ステータスバッジ */}
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', gap:6 }}>
        <span style={{ fontSize:'0.67rem', fontWeight:700, padding:'2px 7px', borderRadius:20,
          background:st.color+'20', color:st.color, whiteSpace:'nowrap' }}>{st.label}</span>
        {t.task_type === 'broadcast' && (
          <span style={{ fontSize:'0.62rem', padding:'1px 6px', borderRadius:3,
            background: selfDone ? 'var(--success-light)' : 'var(--warning-light)', color: selfDone ? '#065f46' : '#92400e', fontWeight:600 }}>
            {selfDone ? '自分完了済' : '一斉配信'}
          </span>
        )}
      </div>

      {/* タイトル */}
      <div style={{ fontWeight:600, fontSize:'0.85rem', color:'var(--gray-900)', lineHeight:1.45,
        overflow:'hidden', display:'-webkit-box', WebkitLineClamp:2, WebkitBoxOrient:'vertical' }}>
        {title}
      </div>
      {/* 内容プレビュー or 作成元ラベル */}
      {preview ? (
        <div style={{ fontSize:'0.74rem', color:'var(--gray-500)', lineHeight:1.5,
          overflow:'hidden', display:'-webkit-box', WebkitLineClamp:2, WebkitBoxOrient:'vertical' }}>
          {preview}
        </div>
      ) : !t.from_slack ? (
        <div style={{ fontSize:'0.67rem', color:'var(--gray-400)', background:'var(--surface-2)', borderRadius:4, padding:'2px 7px', display:'inline-block' }}>
          手動作成
        </div>
      ) : null}

      {/* メタ情報 */}
      <div style={{ display:'flex', gap:8, flexWrap:'wrap', marginTop:'auto', alignItems:'center' }}>
        {assigneeName && (
          <span style={{ fontSize:'0.7rem', color:'var(--gray-400)', display:'flex', alignItems:'center', gap:3 }}>
            👤 {assigneeName}
          </span>
        )}
        {t.due_date && (
          <span style={{ fontSize:'0.7rem', fontWeight: overdue?700:400, color: overdue?'#ef4444':'var(--gray-400)', display:'flex', alignItems:'center', gap:3 }}>
            📅 {fmtDate(t.due_date)}{overdue && ` (${daysSince(t.due_date)}日超過)`}
          </span>
        )}
        {t.project_name && (
          <span style={{ fontSize:'0.7rem', color:'#6366f1' }}>📁 {t.project_name}</span>
        )}
      </div>
    </div>
  );
}

// ── 折りたたみラッパー ──────────────────────────────────────────────
function Foldable({ id, label, badge, badgeColor = '#ef4444', defaultOpen = true, children }) {
  const [open, setOpen] = useState(() => {
    const v = localStorage.getItem(`fold_${id}`);
    return v === null ? defaultOpen : v === '1';
  });
  const toggle = () => setOpen(v => {
    localStorage.setItem(`fold_${id}`, v ? '0' : '1');
    return !v;
  });
  return (
    <div>
      <div onClick={toggle} style={{ display:'flex', alignItems:'center', gap:8, marginBottom: open ? 8 : 0, cursor:'pointer', userSelect:'none', padding:'4px 2px' }}>
        <span style={{ fontSize:'0.75rem', fontWeight:700, color:T.textMid, flex:1 }}>{label}</span>
        {badge > 0 && <span style={{ fontSize:'0.63rem', background:badgeColor, color:'#fff', borderRadius:99, padding:'1px 7px', fontWeight:700 }}>{badge}</span>}
        <span style={{ fontSize:'0.68rem', color:T.textSub }}>{open ? '▲ 閉じる' : '▼ 開く'}</span>
      </div>
      {open && children}
    </div>
  );
}

// ── メンションウィジェット ──────────────────────────────────────────
function MentionWidget() {
  const [mentions, setMentions] = useState(null);
  const [directOnly, setDirectOnly] = useState(() => localStorage.getItem('mention_direct_only') === '1');

  const load = () => api.myMentions().then(d => setMentions(d.mentions || [])).catch(() => setMentions([]));

  useEffect(() => {
    load();
    const t = setInterval(load, 30000);
    return () => clearInterval(t);
  }, []);

  const dismiss = (id, e) => {
    e.preventDefault();
    e.stopPropagation();
    api.dismissMention(id).catch(() => {});
    setMentions(prev => prev.filter(m => m.id !== id));
  };

  const fmtAgo = (iso) => {
    const m = Math.floor((Date.now() - new Date(iso)) / 60000);
    if (m < 60) return `${m}分前`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}時間前`;
    return `${Math.floor(h / 24)}日前`;
  };

  const toggleDirectOnly = () => setDirectOnly(v => {
    localStorage.setItem('mention_direct_only', v ? '0' : '1');
    return !v;
  });

  if (!mentions || mentions.length === 0) return null;

  const isDirectMention = m => m.mention_type !== 'channel' && m.mention_type !== 'group';
  const displayed = directOnly ? mentions.filter(isDirectMention) : mentions;
  const directCount = mentions.filter(isDirectMention).length;

  const count = directOnly ? directCount : mentions.length;

  if (count === 0 && directOnly) {
    return (
      <div style={{ ...T.card, padding:'10px 14px' }}>
        <div style={{ display:'flex', alignItems:'center', gap:8 }}>
          <span style={wTitle}>未確認メンション</span>
          <label style={{ display:'flex', alignItems:'center', gap:4, marginLeft:'auto', cursor:'pointer', fontSize:'0.7rem', color:T.textSub }}>
            <input type="checkbox" checked={directOnly} onChange={toggleDirectOnly} />個別のみ
          </label>
        </div>
        <div style={{ fontSize:'0.75rem', color:T.textSub, marginTop:6, textAlign:'center' }}>
          個別メンションなし（@ch等 {mentions.length}件）
        </div>
      </div>
    );
  }

  return (
    <div style={{ background:'rgba(239,68,68,0.06)', border:'1px solid rgba(239,68,68,0.25)', borderRadius:12, padding:'10px 14px', borderLeft:'3px solid #ef4444' }}>
      {/* ヘッダー */}
      <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:8 }}>
        <span style={{ fontSize:'0.82rem', fontWeight:700, color:'#b91c1c' }}>未確認メンション</span>
        <span style={{ background:'#ef4444', color:'#fff', borderRadius:10, fontSize:'0.63rem', fontWeight:700, padding:'1px 7px', flexShrink:0 }}>{count}</span>
        <label style={{ display:'flex', alignItems:'center', gap:3, marginLeft:'auto', cursor:'pointer', fontSize:'0.7rem', color: directOnly ? 'var(--primary)' : T.textSub, fontWeight: directOnly ? 600 : 400, flexShrink:0 }}>
          <input type="checkbox" checked={directOnly} onChange={toggleDirectOnly} />個別のみ
        </label>
        <button onClick={load} style={{ background:'none', border:'none', cursor:'pointer', color:T.textSub, fontSize:'0.8rem', padding:'1px 4px', flexShrink:0 }} title="更新">↻</button>
      </div>
      {/* アイテムリスト */}
      <div style={{ display:'flex', flexDirection:'column', gap:4, maxHeight:260, overflowY:'auto' }}>
        {displayed.map(m => {
          // thread_ts_root があればスレッドビューで開く
          const threadTs = m.thread_ts_root || m.message_ts;
          const slackUrl = `https://slack.com/archives/${m.channel_id}/p${m.message_ts.replace('.', '')}?thread_ts=${threadTs}&cid=${m.channel_id}`;
          return (
            <div key={m.id} style={{ display:'flex', alignItems:'center', gap:5 }}>
              <a href={slackUrl} target="_blank" rel="noreferrer"
                style={{ flex:1, display:'flex', alignItems:'center', gap:8, padding:'6px 8px', borderRadius:7, background:'var(--surface-2)', border:'1px solid var(--gray-200)', textDecoration:'none', minWidth:0, transition:'opacity 0.1s' }}
                onMouseEnter={e => e.currentTarget.style.opacity='0.75'}
                onMouseLeave={e => e.currentTarget.style.opacity='1'}>
                {m.sender_avatar
                  ? <img src={m.sender_avatar} alt="" style={{ width:24, height:24, borderRadius:'50%', flexShrink:0 }} />
                  : <div style={{ width:24, height:24, borderRadius:'50%', background:'var(--gray-200)', flexShrink:0 }} />}
                <div style={{ flex:1, minWidth:0 }}>
                  <div style={{ display:'flex', alignItems:'center', gap:4, marginBottom:1 }}>
                    <span style={{ fontSize:'0.73rem', fontWeight:600, color:T.text, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                      {m.sender_name || 'Unknown'}
                    </span>
                    {m.mention_type === 'channel' && <span style={{ fontSize:'0.6rem', color:T.textSub, background:'var(--surface-3)', padding:'0px 4px', borderRadius:3, flexShrink:0 }}>@ch</span>}
                    {m.mention_type === 'group' && <span style={{ fontSize:'0.6rem', color:T.textSub, background:'var(--surface-3)', padding:'0px 4px', borderRadius:3, flexShrink:0 }}>GR</span>}
                    <span style={{ fontSize:'0.63rem', color:T.textSub, marginLeft:'auto', flexShrink:0 }}>{fmtAgo(m.created_at)}</span>
                  </div>
                  <div style={{ fontSize:'0.75rem', color:T.textMid, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{m.text_preview}</div>
                </div>
              </a>
              <button onClick={(e) => dismiss(m.id, e)}
                style={{ flexShrink:0, background:'none', border:'none', cursor:'pointer', color:T.textSub, fontSize:'0.85rem', width:20, height:20, display:'flex', alignItems:'center', justifyContent:'center', opacity:0.5 }}
                title="既読">×</button>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── 勤怠ウィジェット（onTeamClick でチーム稼働ボタンを表示）──────
function AttendanceWidget({ onTeamClick }) {
  const [myAtt, setMyAtt] = useState(null);
  useEffect(() => { api.myAttendance().then(setMyAtt).catch(() => {}); }, []);
  const myIn  = myAtt?.clockIn?.ok;
  const myOut = myAtt?.clockOut?.ok;
  return (
    <div style={{ ...T.card, padding:'9px 16px', display:'flex', alignItems:'center', gap:14 }}>
      <span style={{ fontSize:'0.72rem', fontWeight:700, color:T.textMid, flexShrink:0 }}>本日の勤怠</span>
      <span style={{ fontSize:'0.78rem', color: myIn ? '#16a34a' : T.textSub }}>{myIn ? '🟢 出勤済み' : '⬜ 未出勤'}</span>
      <span style={{ fontSize:'0.78rem', color: myOut ? '#8b5cf6' : T.textSub }}>{myOut ? '🟣 退勤済み' : '⬜ 未退勤'}</span>
      {onTeamClick && (
        <button onClick={onTeamClick} style={{ marginLeft:'auto', padding:'4px 12px', border:'1px solid var(--gray-200)', borderRadius:7, background:'var(--surface)', color:T.textSub, fontSize:'0.72rem', cursor:'pointer', flexShrink:0, display:'flex', alignItems:'center', gap:4 }}>
          チーム稼働状況 →
        </button>
      )}
    </div>
  );
}

// ── チームメンバー稼働詳細ウィジェット（Chief以上用）──────────────
function TeamDetailWidget() {
  const [data, setData]         = useState(null);
  const [taskPanel, setTaskPanel] = useState(null); // { userId, name }
  const [tasks, setTasks]       = useState([]);
  const [expandCal, setExpandCal] = useState(null); // userId

  useEffect(() => { api.teamCalendarDetail().then(setData).catch(() => {}); }, []);

  const openTasks = (m) => {
    setTaskPanel(m);
    setTasks([]);
    api.tasks({ assignee: m.userId, status: 'in_progress', limit: 50 })
      .then(d => setTasks(d.tasks || []))
      .catch(() => {});
  };

  const fmtTime = (iso) => iso ? new Date(iso).toLocaleTimeString('ja-JP', { hour:'2-digit', minute:'2-digit', timeZone:'Asia/Tokyo' }) : '';
  const nowIso = new Date().toISOString();

  if (!data || !data.canView || !data.members.length) return null;

  return (
    <>
      <div style={{ ...T.card, overflow:'hidden' }}>
        <div style={{ padding:'10px 16px', borderBottom:'1px solid var(--gray-200)', display:'flex', alignItems:'center', gap:8 }}>
          <span style={wTitle}>チームメンバー</span>
          <span style={{ fontSize:'0.72rem', color:T.textSub }}>{data.members.length}人</span>
          {!data.calendarConnected && <span style={{ fontSize:'0.7rem', color:'#f59e0b', marginLeft:'auto' }}>カレンダー未連携</span>}
        </div>
        <div style={{ overflowX:'auto' }}>
          <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.8rem' }}>
            <thead>
              <tr style={{ background:'var(--surface-2)' }}>
                <th style={{ padding:'7px 12px', textAlign:'left', fontWeight:600, color:T.textSub, fontSize:'0.72rem', whiteSpace:'nowrap' }}>メンバー</th>
                <th style={{ padding:'7px 10px', textAlign:'left', fontWeight:600, color:T.textSub, fontSize:'0.72rem', whiteSpace:'nowrap' }}>出勤状況</th>
                <th style={{ padding:'7px 10px', textAlign:'right', fontWeight:600, color:T.textSub, fontSize:'0.72rem' }}>タスク</th>
                {data.calendarConnected && <>
                  <th style={{ padding:'7px 10px', textAlign:'left', fontWeight:600, color:T.textSub, fontSize:'0.72rem' }}>今日の予定</th>
                </>}
              </tr>
            </thead>
            <tbody>
              {data.members.map((m) => {
                const inMeeting = m.currentEvent != null;
                const isExpanded = expandCal === m.userId;
                return (
                  <React.Fragment key={m.userId}>
                    <tr style={{ borderBottom: isExpanded ? 'none' : '1px solid var(--gray-200)', background: inMeeting ? 'var(--warning-light)' : 'transparent' }}>
                      <td style={{ padding:'7px 12px' }}>
                        <div style={{ display:'flex', alignItems:'center', gap:7 }}>
                          {m.avatarUrl
                            ? <img src={m.avatarUrl} alt="" style={{ width:22, height:22, borderRadius:'50%', flexShrink:0 }} />
                            : <div style={{ width:22, height:22, borderRadius:'50%', background:'var(--gray-200)', flexShrink:0 }} />}
                          <span style={{ color:T.text, whiteSpace:'nowrap' }}>{m.displayName}</span>
                        </div>
                      </td>
                      <td style={{ padding:'7px 10px' }}>
                        {m.clockedIn
                          ? <span style={{ color:'#16a34a', fontSize:'0.75rem', fontWeight:600 }}>● 出勤中{m.clockedOut ? '（退勤済）' : ''}</span>
                          : <span style={{ color:'#94a3b8', fontSize:'0.75rem' }}>○ 未出勤</span>}
                      </td>
                      <td style={{ padding:'7px 10px', textAlign:'right' }}>
                        {m.taskCount > 0
                          ? <button onClick={() => openTasks(m)} style={{ background:'#eff6ff', color:'#3b82f6', border:'none', borderRadius:10, padding:'2px 9px', fontSize:'0.72rem', cursor:'pointer', fontWeight:600 }}>{m.taskCount}件</button>
                          : <span style={{ color:'#cbd5e1', fontSize:'0.75rem' }}>—</span>}
                      </td>
                      {data.calendarConnected && <>
                        <td style={{ padding:'7px 10px', maxWidth:260 }}>
                          <div style={{ display:'flex', flexDirection:'column', gap:2 }}>
                            {inMeeting && (
                              <span style={{ color:'#d97706', fontSize:'0.75rem', fontWeight:600 }}>🟡 {m.currentEvent.title} 〜{fmtTime(m.currentEvent.end)}</span>
                            )}
                            {m.todayEvents?.filter(e => e.start > nowIso).slice(0, isExpanded ? 99 : 2).map((e, i) => (
                              <span key={i} style={{ color:'#64748b', fontSize:'0.72rem' }}>{fmtTime(e.start)} {e.title}</span>
                            ))}
                            {!inMeeting && !m.todayEvents?.length && (
                              <span style={{ color:'#cbd5e1', fontSize:'0.72rem' }}>予定なし</span>
                            )}
                            {m.todayEvents?.filter(e => e.start > nowIso).length > 2 && (
                              <button onClick={() => setExpandCal(isExpanded ? null : m.userId)} style={{ background:'none', border:'none', cursor:'pointer', color:'#94a3b8', fontSize:'0.68rem', textAlign:'left', padding:0 }}>
                                {isExpanded ? '▲ 折りたたむ' : `▼ +${m.todayEvents.filter(e => e.start > nowIso).length - 2}件`}
                              </button>
                            )}
                          </div>
                        </td>
                      </>}
                    </tr>
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* タスク一覧パネル */}
      {taskPanel && (
        <>
          <div onClick={() => setTaskPanel(null)} style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.3)', zIndex:1000 }} />
          <div style={{ position:'fixed', top:0, right:0, bottom:0, width:480, maxWidth:'94vw', background:'#fff', zIndex:1001, display:'flex', flexDirection:'column', boxShadow:'-4px 0 24px rgba(0,0,0,0.12)' }}>
            <div style={{ padding:'16px 20px', borderBottom:'1px solid #e5e7eb', display:'flex', alignItems:'center', gap:12 }}>
              <div>
                <div style={{ fontSize:'0.75rem', color:'#6b7280' }}>{taskPanel.displayName} の進行中タスク</div>
                <div style={{ fontWeight:700, fontSize:'0.95rem' }}>{tasks.length}件</div>
              </div>
              <button onClick={() => setTaskPanel(null)} style={{ marginLeft:'auto', background:'none', border:'none', fontSize:'1.2rem', cursor:'pointer', color:'#6b7280' }}>✕</button>
            </div>
            <div style={{ flex:1, overflowY:'auto' }}>
              {tasks.length === 0
                ? <div style={{ padding:24, color:'#9ca3af', textAlign:'center' }}>読み込み中...</div>
                : tasks.map(t => (
                  <div key={t.id} style={{ padding:'10px 20px', borderBottom:'1px solid #f3f4f6' }}>
                    <div style={{ fontSize:'0.85rem', color:'#111827', fontWeight:500 }}>{t.title}</div>
                    {t.due_date && <div style={{ fontSize:'0.72rem', color: new Date(t.due_date) < new Date() ? '#ef4444' : '#9ca3af', marginTop:3 }}>期限: {t.due_date}</div>}
                  </div>
                ))
              }
            </div>
          </div>
        </>
      )}
    </>
  );
}

// ── カレンダーウィジェット ─────────────────────────────────────────
function CalendarWidget({ role }) {
  const [data, setData] = useState(null);
  const [status, setStatus] = useState(null);

  useEffect(() => {
    api.gcalStatus().then(setStatus).catch(() => {});
    api.myCalendar().then(setData).catch(() => {});
  }, []);

  const fmtTime = (iso) => {
    if (!iso) return '';
    const d = new Date(iso);
    return d.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit', timeZone: 'Asia/Tokyo' });
  };

  const now = new Date();

  if (!status) return null;

  // 未連携の場合（adminのみ連携ボタンを表示）
  if (!status.connected) {
    if (role !== 'admin') return null;
    return (
      <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'12px 16px', display:'flex', alignItems:'center', gap:10 }}>
        <span style={{ fontSize:'0.8rem', color:'#64748b' }}>Googleカレンダー未連携</span>
        <a href="/api/google/oauth/start" target="_blank" rel="noreferrer"
          style={{ marginLeft:'auto', background:'#3b82f6', color:'#fff', borderRadius:8, padding:'5px 14px', fontSize:'0.8rem', fontWeight:600, textDecoration:'none' }}>
          連携する
        </a>
      </div>
    );
  }

  if (!data?.events) return null;

  const events = data.events.filter(e => !e.allDay);
  if (events.length === 0) return (
    <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'12px 16px' }}>
      <div style={{ fontSize:'0.8rem', fontWeight:700, color:'#374151', marginBottom:4 }}>今日の予定</div>
      <div style={{ fontSize:'0.8rem', color:'#94a3b8' }}>予定なし</div>
    </div>
  );

  return (
    <div style={{ background:'#fff', borderRadius:12, border:'1px solid #e2e8f0', padding:'12px 16px' }}>
      <div style={{ fontSize:'0.82rem', fontWeight:700, color:'#0f172a', marginBottom:8 }}>今日の予定</div>
      <div style={{ display:'flex', flexDirection:'column', gap:5 }}>
        {events.map(e => {
          const start = new Date(e.start);
          const end   = new Date(e.end);
          const active = now >= start && now <= end;
          const past   = now > end;
          return (
            <div key={e.id} style={{
              display:'flex', alignItems:'center', gap:8, padding:'6px 10px',
              borderRadius:8, borderLeft:`3px solid ${active ? '#3b82f6' : past ? '#e5e7eb' : '#94a3b8'}`,
              background: active ? '#eff6ff' : '#f8fafc',
              opacity: past ? 0.5 : 1,
            }}>
              <span style={{ fontSize:'0.72rem', color:'#64748b', whiteSpace:'nowrap', minWidth:80 }}>
                {fmtTime(e.start)}〜{fmtTime(e.end)}
              </span>
              <span style={{ flex:1, fontSize:'0.8rem', fontWeight: active ? 700 : 400, color:'#0f172a', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                {active && <span style={{ color:'#3b82f6', marginRight:4 }}>▶</span>}
                {e.title}
              </span>
              {e.meetUrl && (
                <a href={e.meetUrl} target="_blank" rel="noreferrer"
                  style={{ fontSize:'0.7rem', color:'#3b82f6', textDecoration:'none', flexShrink:0 }}>Meet</a>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── メインダッシュボード ──────────────────────────────────────────
export default function Dashboard() {
  const [me, setMe] = useState(null);
  const [showTeamModal, setShowTeamModal] = useState(false);
  const [teamOverdue, setTeamOverdue] = useState([]);
  const [alertSelected, setAlertSelected] = useState(new Set());
  const [alertNotifying, setAlertNotifying] = useState(false);
  const [incompleteModal, setIncompleteModal] = useState(null); // { task, users, total, incomplete }
  const [mySummary, setMySummary] = useState(null);
  const [members, setMembers] = useState([]);
  const [usergroups, setUsergroups] = useState([]);
  const [projects, setProjects] = useState([]);
  const [dashTeams, setDashTeams] = useState([]);
  const [myTasks, setMyTasks] = useState([]);
  const [filter, setFilter] = useState({ status:'in_progress', assignee:'', project:'', dept:'', team:'', overdue:false, page:1 });
  const [filteredTasks, setFilteredTasks] = useState({ tasks:[], total:0 });
  const [filterApplied, setFilterApplied] = useState(false);
  const [loading, setLoading] = useState(true);
  const [selectedTask, setSelectedTask] = useState(null);
  const [teamMemberIds, setTeamMemberIds] = useState(null);

  useEffect(() => {
    api.me()
      .then(m => Promise.all([
        Promise.resolve(m),
        api.summary({ scope: 'self' }),
        api.members(),
        api.projects(),
        api.workloadTeams(),
        api.tasks({ assignee: m.userId, limit: 100 }),
        api.usergroups(),
        api.teamOverdue(),
      ]))
      .then(([meRes, sumRes, memRes, projRes, dtRes, myTasksRes, ugRes, toRes]) => {
        setMe(meRes);
        setMySummary(sumRes.summary);
        setMembers(memRes.members);
        setTeamOverdue(toRes.tasks || []);
        setProjects(projRes.projects);
        setDashTeams(dtRes.teams || []);
        setMyTasks(myTasksRes.tasks || []);
        setUsergroups(ugRes.usergroups || []);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const setF = (patch) => {
    setFilter(f => {
      const next = { ...f, ...patch, page:1 };
      // 部署が変わったらチームと担当者もリセット
      if ('dept' in patch) { next.team = ''; next.assignee = ''; }
      if ('team' in patch) next.assignee = '';
      return next;
    });
    const teamId = 'team' in patch ? patch.team : ('dept' in patch ? null : undefined);
    if (teamId !== undefined) {
      if (teamId) {
        api.teamMembers(teamId).then(r => setTeamMemberIds(new Set(r.memberIds || []))).catch(() => setTeamMemberIds(null));
      } else {
        setTeamMemberIds(null);
      }
    }
  };

  // 親チーム（部署レベル）: admin は全トップレベル、一般は自分の部署のみ
  const deptTeams = useMemo(() => {
    if (!me || !dashTeams.length) return [];
    const topLevel = dashTeams.filter(t => !t.parent_id);
    if (me.role === 'admin' || me.role === 'it') return topLevel;
    const myTeamIds = new Set((me.dashTeams || []).map(t => t.id));
    const myParentIds = new Set();
    for (const t of dashTeams) {
      if (!myTeamIds.has(t.id)) continue;
      if (t.parent_id) myParentIds.add(t.parent_id);
      else myParentIds.add(t.id);
    }
    return topLevel.filter(t => myParentIds.has(t.id));
  }, [me, dashTeams]);

  // 選択中部署の子チーム
  const childTeams = useMemo(() =>
    filter.dept ? dashTeams.filter(t => t.parent_id === filter.dept) : []
  , [dashTeams, filter.dept]);

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

  // 期限当日＋期限切れのカウント（バッジ用）
  const urgentTaskCount = useMemo(() => {
    const tod = new Date(); tod.setHours(0, 0, 0, 0);
    return activeTasks.filter(t => {
      if (!t.due_date) return false;
      const due = new Date(t.due_date); due.setHours(0, 0, 0, 0);
      return due <= tod;
    }).length;
  }, [activeTasks]);

  const applyFilter = async () => {
    const params = { page: filter.page, limit: 50 };
    if (filter.status)   params.status   = filter.status;
    if (filter.assignee) params.assignee = filter.assignee;
    if (filter.project)  params.project  = filter.project;
    // 子チーム > 親部署の優先順位
    if (filter.team)     params.dashTeam = filter.team;
    else if (filter.dept) params.dashTeam = filter.dept;
    if (filter.overdue)  params.overdue  = '1';
    const res = await api.tasks(params);
    setFilteredTasks(res);
    setFilterApplied(true);
  };

  const clearFilter = () => {
    setFilter({ status:'in_progress', assignee:'', project:'', dept:'', team:'', overdue:false, page:1 });
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

  const { isMobile } = useBreakpoint();

  if (loading) return <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:'60vh', color:T.textSub }}>読み込み中…</div>;

  const card = (children, style={}) => (
    <div style={{ ...T.card, padding: isMobile ? '12px 14px' : '16px 20px', ...style }}>{children}</div>
  );
  const sh = (label, sub) => (
    <div style={{ marginBottom:10 }}>
      <div style={{ fontWeight:700, fontSize:'0.88rem', color:T.text }}>{label}</div>
      {sub && <div style={{ fontSize:'0.7rem', color:T.textSub, marginTop:1 }}>{sub}</div>}
    </div>
  );
  const myName = me?.displayName?.split(/[\s　/]/)[0] || '';

  // チーム期限切れアラートの本体（右サイドバーで共有）
  const overdueAlertBody = teamOverdue.length > 0 && (
    <Foldable id="team-overdue" label="チーム期限切れ" badge={teamOverdue.length} defaultOpen={true}>
      <div style={{ ...T.card, padding:'12px 14px', borderLeft:'3px solid #ef4444' }}>
        <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:8, flexWrap:'wrap' }}>
          {alertSelected.size > 0 && (
            <button onClick={async () => { setAlertNotifying(true); await Promise.all([...alertSelected].map(id => api.taskNotifyOverdue(id).catch(() => {}))); setAlertNotifying(false); setAlertSelected(new Set()); }}
              disabled={alertNotifying}
              style={{ padding:'3px 10px', background: alertNotifying ? 'var(--gray-200)' : '#ef4444', color:'#fff', border:'none', borderRadius:6, fontSize:'0.72rem', fontWeight:700, cursor: alertNotifying ? 'default' : 'pointer' }}>
              {alertNotifying ? '投稿中…' : `${alertSelected.size}件に通知`}
            </button>
          )}
          <label style={{ fontSize:'0.72rem', color:'#ef4444', cursor:'pointer', display:'flex', alignItems:'center', gap:4, marginLeft:'auto' }}>
            <input type="checkbox" checked={alertSelected.size === teamOverdue.length && teamOverdue.length > 0} onChange={e => setAlertSelected(e.target.checked ? new Set(teamOverdue.map(t => t.id)) : new Set())} />
            全選択
          </label>
        </div>
        <div style={{ display:'flex', flexDirection:'column', gap:5 }}>
          {teamOverdue.map(t => {
            const days = Math.floor((Date.now() - new Date(t.due_date)) / 86400000);
            const rawTitle = t.title || '';
            const titleLine = (() => { for (const line of rawTitle.split('\n')) { const c = line.replace(/<@[^>]+>/g,'').replace(/@\S+/g,'').replace(/（[^）]*）/g,'').replace(/\s+/g,' ').trim(); if (!c) continue; if (!/[。！？、：]/.test(c) && /[一-鿿]+\/[A-Za-z]/.test(c) && (c.length<40 || (c.match(/\//g)||[]).length>=2)) continue; return c.slice(0,55); } return rawTitle.slice(0,55); })();
            const checked = alertSelected.has(t.id);
            return (
              <div key={t.id} style={{ display:'flex', alignItems:'center', gap:7, padding:'5px 8px', background: checked ? 'var(--danger-light)' : 'var(--surface-2)', borderRadius:7, border:`1px solid ${checked ? 'rgba(239,68,68,0.3)' : 'var(--gray-200)'}` }}>
                <input type="checkbox" checked={checked} onChange={e => { const s=new Set(alertSelected); e.target.checked?s.add(t.id):s.delete(t.id); setAlertSelected(s); }} style={{ cursor:'pointer', flexShrink:0 }} />
                <span style={{ fontSize:'0.7rem', fontWeight:700, color:'#ef4444', background:'var(--danger-light)', padding:'1px 6px', borderRadius:99, whiteSpace:'nowrap', cursor:'pointer', flexShrink:0 }} onClick={() => setSelectedTask(t)}>{t.assigneeName}</span>
                <span style={{ fontSize:'0.78rem', color:T.text, flex:1, minWidth:0, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', cursor:'pointer' }} onClick={() => setSelectedTask(t)}>{titleLine}</span>
                <span style={{ fontSize:'0.68rem', color:'#ef4444', fontWeight:700, whiteSpace:'nowrap', flexShrink:0 }}>{days}日超過</span>
                {t.task_type==='broadcast' && <button onClick={() => api.taskIncompleteTargets(t.id).then(r=>setIncompleteModal({task:t,...r})).catch(()=>{})} style={{ fontSize:'0.62rem', padding:'2px 7px', border:'1px solid rgba(239,68,68,0.3)', borderRadius:5, background:'var(--surface)', color:'#ef4444', cursor:'pointer', whiteSpace:'nowrap', flexShrink:0 }}>未完了者</button>}
              </div>
            );
          })}
        </div>
      </div>
    </Foldable>
  );

  const SectionDivider = ({ label }) => (
    <div style={{ display:'flex', alignItems:'center', gap:10, margin:'4px 0 12px' }}>
      <span style={{ fontSize:'0.68rem', fontWeight:700, letterSpacing:'0.08em', color:T.textSub, textTransform:'uppercase', flexShrink:0 }}>{label}</span>
      <span style={{ flex:1, height:'1px', background:'var(--gray-200)', display:'block' }} />
    </div>
  );

  return (
    <div style={{ padding: isMobile ? '12px' : '20px 24px', background:'var(--gray-50)', minHeight:'100%' }}>
      {/* 勤怠（右にチーム稼働ボタン） */}
      <div style={{ marginBottom:16 }}>
        <AttendanceWidget onTeamClick={() => setShowTeamModal(true)} />
      </div>

      {/* ── 自分のこと ── */}
      <SectionDivider label="自分のこと" />
      <div style={{ display:'flex', flexDirection:'column', gap:14, marginBottom:28 }}>
        {/* 今日の予定 */}
        <Foldable id="calendar" label="今日の予定" defaultOpen={true}>
          <CalendarWidget role={me?.role} />
        </Foldable>

        {/* マイタスク（KPIなし・タイトル行 + カードグリッド） */}
        {card(<>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:12 }}>
            <span style={{ fontWeight:700, fontSize:'0.88rem', color:T.text }}>
              {myName ? `${myName} のタスク` : 'マイタスク'}
            </span>
            <button onClick={() => { const w=localStorage.getItem('float_w')||380,h=localStorage.getItem('float_h')||640,x=localStorage.getItem('float_x'),y=localStorage.getItem('float_y'),pos=x&&y?`,left=${x},top=${y}`:''; window.open('/dashboard/floating','taskhub-float',`width=${w},height=${h},resizable=yes,scrollbars=yes${pos}`); }}
              style={{ padding:'4px 10px', border:'1px solid var(--gray-200)', borderRadius:7, background:'var(--surface)', color:T.textSub, fontSize:'0.72rem', cursor:'pointer', display:'flex', alignItems:'center', gap:4, position:'relative' }}>
              ↗ ポップアップ
              {urgentTaskCount > 0 && (
                <span style={{ position:'absolute', top:-6, right:-6, background:'#dc2626', color:'#fff', borderRadius:'50%', minWidth:16, height:16, fontSize:'0.62rem', fontWeight:700, display:'flex', alignItems:'center', justifyContent:'center', padding:'0 3px', lineHeight:1 }}>
                  {urgentTaskCount}
                </span>
              )}
            </button>
          </div>
          {activeTasks.length === 0
            ? <div style={{ color:T.textSub, fontSize:'0.82rem', textAlign:'center', padding:'20px 0' }}>アクティブなタスクはありません</div>
            : <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(200px,1fr))', gap:10 }}>
                {activeTasks.map(t => <TaskCard key={t.id} t={t} members={members} onClick={() => setSelectedTask(t)} />)}
              </div>
          }
        </>)}

        {/* 未確認メンション */}
        <MentionWidget />
      </div>

      {/* ── チームのこと ── */}
      <SectionDivider label="チームのこと" />
      {overdueAlertBody || (
        <div style={{ ...T.card, padding:'12px 16px', color:T.textSub, fontSize:'0.82rem', textAlign:'center' }}>
          チーム期限切れタスクなし
        </div>
      )}

      {/* チーム稼働状況モーダル */}
      {showTeamModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.45)', zIndex:600, display:'flex', alignItems:'flex-start', justifyContent:'flex-end', padding: isMobile ? '56px 0 0' : '56px 24px 20px' }}
          onClick={() => setShowTeamModal(false)}>
          <div style={{ background:'var(--surface)', borderRadius:14, width: isMobile ? '100%' : 'min(720px,85vw)', maxHeight:'85vh', display:'flex', flexDirection:'column', boxShadow:'0 20px 60px rgba(0,0,0,0.2)' }}
            onClick={e => e.stopPropagation()}>
            <div style={{ padding:'14px 20px', borderBottom:'1px solid var(--gray-200)', display:'flex', justifyContent:'space-between', alignItems:'center', flexShrink:0 }}>
              <span style={{ fontWeight:700, fontSize:'0.95rem', color:T.text }}>チーム稼働状況</span>
              <button onClick={() => setShowTeamModal(false)} style={{ background:'var(--surface-2)', border:'none', borderRadius:8, width:28, height:28, cursor:'pointer', color:T.textSub, fontSize:16 }}>×</button>
            </div>
            <div style={{ overflowY:'auto', flex:1 }}>
              <TeamDetailWidget />
            </div>
          </div>
        </div>
      )}

      {/* モーダル（タブ外） */}
      {selectedTask && <TaskPanel task={selectedTask} members={members} usergroups={usergroups} onClose={() => setSelectedTask(null)} onStatusChange={handleStatusChange} />}
      {incompleteModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(15,23,42,0.5)', zIndex:600, display:'flex', alignItems:'center', justifyContent:'center' }} onClick={() => setIncompleteModal(null)}>
          <div style={{ background:'var(--surface)', borderRadius:14, width:'min(440px,90vw)', maxHeight:'80vh', display:'flex', flexDirection:'column', boxShadow:'0 20px 60px rgba(0,0,0,0.2)' }} onClick={e => e.stopPropagation()}>
            <div style={{ padding:'16px 20px', borderBottom:'1px solid var(--gray-200)', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
              <div>
                <div style={{ fontWeight:700, fontSize:'0.9rem', color:T.text }}>未完了者一覧</div>
                <div style={{ fontSize:'0.72rem', color:T.textSub, marginTop:2 }}>{incompleteModal.total}名中 {incompleteModal.incomplete}名が未完了</div>
              </div>
              <button onClick={() => setIncompleteModal(null)} style={{ background:'var(--surface-2)', border:'none', borderRadius:8, width:28, height:28, cursor:'pointer', color:T.textSub, fontSize:16 }}>×</button>
            </div>
            <div style={{ overflowY:'auto', padding:'12px 16px', display:'flex', flexDirection:'column', gap:8 }}>
              {incompleteModal.users.length === 0
                ? <div style={{ textAlign:'center', color:T.textSub, padding:'20px 0', fontSize:'0.85rem' }}>全員完了済み</div>
                : incompleteModal.users.map(u => (
                  <div key={u.user_id} style={{ display:'flex', alignItems:'center', gap:10, padding:'8px 10px', background:'var(--danger-light)', borderRadius:8, border:'1px solid rgba(239,68,68,0.2)' }}>
                    {u.avatar_url ? <img src={u.avatar_url} alt="" style={{ width:32, height:32, borderRadius:'50%', objectFit:'cover', flexShrink:0 }} /> : <div style={{ width:32, height:32, borderRadius:'50%', background:'rgba(239,68,68,0.3)', display:'flex', alignItems:'center', justifyContent:'center', flexShrink:0, fontSize:'0.7rem', fontWeight:700, color:'#ef4444' }}>{(u.displayName||'?').split('/')[0].slice(0,2)}</div>}
                    <span style={{ fontSize:'0.85rem', fontWeight:600, color:T.text }}>{(u.displayName||'').split('/')[0].trim()}</span>
                  </div>
                ))
              }
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
