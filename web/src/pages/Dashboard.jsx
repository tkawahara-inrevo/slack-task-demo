import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api/client';

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
  const [threadData, setThreadData] = useState(null); // { messages, nameMap }
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    api.taskGet?.(task.id).then(r => setFullTask(r.task || r)).catch(() => {});
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
        <div style={{ flex:1, overflowY:'auto', padding:'18px 22px', display:'flex', flexDirection:'column', gap:16 }}>
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
      style={{ background:'#fff', borderRadius:10, border:'1px solid #e2e8f0', padding:'12px 14px', cursor:'pointer',
        transition:'box-shadow 0.15s, border-color 0.15s', display:'flex', flexDirection:'column', gap:8,
        borderLeft: `3px solid ${selfDone ? '#10b981' : st.color}` }}
      onMouseEnter={e => { e.currentTarget.style.boxShadow='0 3px 10px rgba(0,0,0,0.08)'; e.currentTarget.style.borderColor='#c7d2fe'; }}
      onMouseLeave={e => { e.currentTarget.style.boxShadow='none'; e.currentTarget.style.borderColor='#e2e8f0'; }}>

      {/* ステータスバッジ */}
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', gap:6 }}>
        <span style={{ fontSize:'0.68rem', fontWeight:700, padding:'2px 8px', borderRadius:20,
          background:st.color+'18', color:st.color, whiteSpace:'nowrap' }}>{st.label}</span>
        {t.task_type === 'broadcast' && (
          <span style={{ fontSize:'0.62rem', padding:'1px 6px', borderRadius:3,
            background: selfDone ? '#d1fae5' : '#fef3c7', color: selfDone ? '#065f46' : '#92400e', fontWeight:600 }}>
            {selfDone ? '自分完了済' : '一斉配信'}
          </span>
        )}
      </div>

      {/* タイトル */}
      <div style={{ fontWeight:600, fontSize:'0.85rem', color:'#0f172a', lineHeight:1.45,
        overflow:'hidden', display:'-webkit-box', WebkitLineClamp:2, WebkitBoxOrient:'vertical' }}>
        {title}
      </div>
      {/* 内容プレビュー or 作成元ラベル */}
      {preview ? (
        <div style={{ fontSize:'0.75rem', color:'#64748b', lineHeight:1.5,
          overflow:'hidden', display:'-webkit-box', WebkitLineClamp:3, WebkitBoxOrient:'vertical' }}>
          {preview}
        </div>
      ) : !t.from_slack ? (
        <div style={{ fontSize:'0.68rem', color:'#94a3b8', background:'#f8fafc', borderRadius:4, padding:'3px 8px', display:'inline-block' }}>
          手動作成
        </div>
      ) : null}

      {/* メタ情報 */}
      <div style={{ display:'flex', flexDirection:'column', gap:3, marginTop:'auto' }}>
        {assigneeName && (
          <span style={{ fontSize:'0.7rem', color:'#94a3b8', display:'flex', alignItems:'center', gap:3 }}>
            👤 {assigneeName}
          </span>
        )}
        {t.due_date && (
          <span style={{ fontSize:'0.7rem', fontWeight: overdue?700:400, color: overdue?'#dc2626':'#94a3b8', display:'flex', alignItems:'center', gap:3 }}>
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

// ── メインダッシュボード ──────────────────────────────────────────
export default function Dashboard() {
  const [tab, setTab] = useState('tasks');
  const [me, setMe] = useState(null);
  const [teamOverdue, setTeamOverdue] = useState([]);
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
    if (me.role === 'admin') return topLevel;
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

  const tabBtnStyle = (active) => ({
    padding:'6px 20px', border:'none', borderRadius:8, cursor:'pointer', fontSize:'0.85rem', fontWeight: active ? 700 : 500,
    background: active ? '#1e40af' : 'transparent', color: active ? '#fff' : '#64748b', transition:'all 0.12s',
  });

  return (
    <div style={{ padding:'20px 24px', background:'#f8fafc', minHeight:'100%', display:'flex', flexDirection:'column', gap:14 }}>

      {/* ヘッダー + タブ */}
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', flexWrap:'wrap', gap:8 }}>
        {myName && <div style={{ fontSize:'0.82rem', color:'#64748b', fontWeight:600 }}>{myName} のタスク</div>}
        <div style={{ display:'flex', gap:4, background:'#f1f5f9', borderRadius:10, padding:3 }}>
          <button style={tabBtnStyle(tab==='tasks')}   onClick={() => setTab('tasks')}>マイタスク</button>
          <button style={tabBtnStyle(tab==='analytics')} onClick={() => setTab('analytics')}>分析</button>
        </div>
      </div>

      {tab === 'analytics' && <AnalyticsTab members={members} usergroups={usergroups} />}
      {tab === 'tasks' && <>

      {/* チームアラート（期限切れ） */}
      {teamOverdue.length > 0 && (
        <div style={{ background:'#fef2f2', borderRadius:12, border:'1px solid #fca5a5', padding:'12px 16px' }}>
          <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:10 }}>
            <span style={{ fontSize:'0.82rem', fontWeight:700, color:'#dc2626' }}>⚠ チーム 期限切れタスク</span>
            <span style={{ fontSize:'0.72rem', background:'#dc2626', color:'#fff', borderRadius:99, padding:'1px 8px', fontWeight:700 }}>{teamOverdue.length}件</span>
          </div>
          <div style={{ display:'flex', flexDirection:'column', gap:6 }}>
            {teamOverdue.map(t => {
              const days = Math.floor((Date.now() - new Date(t.due_date)) / 86400000);
              const rawTitle = t.title || '';
              const titleLine = (() => {
                for (const line of rawTitle.split('\n')) {
                  const c = line.replace(/<@[^>]+>/g, '').replace(/@\S+/g, '').replace(/（[^）]*）/g, '').replace(/\s+/g, ' ').trim();
                  if (!c) continue;
                  const slashes = (c.match(/\//g) || []).length;
                  const isAddr = !/[。！？、：]/.test(c) && /[一-鿿]+\/[A-Za-z]/.test(c) && (c.length < 40 || slashes >= 2);
                  if (isAddr) continue;
                  return c.slice(0, 60);
                }
                return rawTitle.slice(0, 60);
              })();
              return (
                <div key={t.id} onClick={() => setSelectedTask(t)}
                  style={{ display:'flex', alignItems:'center', gap:10, padding:'6px 10px', background:'#fff', borderRadius:8,
                    border:'1px solid #fecaca', cursor:'pointer' }}
                  onMouseEnter={e => e.currentTarget.style.background='#fff7f7'}
                  onMouseLeave={e => e.currentTarget.style.background='#fff'}>
                  <span style={{ fontSize:'0.72rem', fontWeight:700, color:'#dc2626', background:'#fee2e2', padding:'2px 8px', borderRadius:99, whiteSpace:'nowrap' }}>
                    {t.assigneeName}
                  </span>
                  <span style={{ fontSize:'0.8rem', color:'#374151', flex:1, minWidth:0, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                    {titleLine}
                  </span>
                  <span style={{ fontSize:'0.7rem', color:'#dc2626', fontWeight:700, whiteSpace:'nowrap' }}>{days}日超過</span>
                </div>
              );
            })}
          </div>
        </div>
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
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:10 }}>
          <div>
            <div style={{ fontWeight:700, fontSize:'0.88rem', color:'#0f172a' }}>マイタスク</div>
            <div style={{ fontSize:'0.7rem', color:'#94a3b8', marginTop:1 }}>進行中・保留 {activeTasks.length}件</div>
          </div>
          <button
            onClick={() => {
              const w = localStorage.getItem('float_w') || 380;
              const h = localStorage.getItem('float_h') || 640;
              const x = localStorage.getItem('float_x');
              const y = localStorage.getItem('float_y');
              const pos = x && y ? `,left=${x},top=${y}` : '';
              window.open('/dashboard/floating', 'taskhub-float', `width=${w},height=${h},resizable=yes,scrollbars=yes${pos}`);
            }}
            style={{ padding:'5px 12px', border:'1px solid #e2e8f0', borderRadius:7, background:'#fff', color:'#64748b', fontSize:'0.75rem', cursor:'pointer', display:'flex', alignItems:'center', gap:4 }}
            title="ポップアップで開く">
            ↗ ポップアップ
          </button>
        </div>
        {activeTasks.length === 0 ? (
          <div style={{ color:'#94a3b8', fontSize:'0.82rem', textAlign:'center', padding:'16px 0' }}>
            アクティブなタスクはありません
          </div>
        ) : (
          <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(240px, 1fr))', gap:10 }}>
            {activeTasks.map(t => (
              <TaskCard key={t.id} t={t} members={members} onClick={() => setSelectedTask(t)} />
            ))}
          </div>
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
          </select>

          {deptTeams.length > 0 && (
            <select value={filter.dept} onChange={e => setF({dept:e.target.value})} style={selStyle}>
              <option value="">部署：すべて</option>
              {deptTeams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          )}
          {childTeams.length > 0 && (
            <select value={filter.team} onChange={e => setF({team:e.target.value})} style={selStyle}>
              <option value="">チーム：すべて</option>
              {childTeams.map(t => <option key={t.id} value={t.id}>{t.name}</option>)}
            </select>
          )}

          <select value={filter.assignee} onChange={e => setF({assignee:e.target.value})} style={selStyle}>
            <option value="">担当者：全員</option>
            {(teamMemberIds ? members.filter(m => teamMemberIds.has(m.assignee_id)) : members)
              .map(m => <option key={m.assignee_id} value={m.assignee_id}>{m.displayName}</option>)}
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
              <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(240px, 1fr))', gap:10 }}>
                {filteredTasks.tasks.map(t => (
                  <TaskCard key={t.id} t={t} members={members} onClick={() => setSelectedTask(t)} />
                ))}
              </div>
            </>
          )
        )}
      </>)}

      {selectedTask && (
        <TaskPanel task={selectedTask} members={members} usergroups={usergroups} onClose={() => setSelectedTask(null)} onStatusChange={handleStatusChange} />
      )}
      </>}
    </div>
  );
}
