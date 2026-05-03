import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../../api/client';

const GROUPS = [
  { label: '#all系',          test: n => n.startsWith('all') },
  { label: '#bc系',           test: n => n.startsWith('bc') },
  { label: '#corp系',         test: n => n.startsWith('corp') },
  { label: '#hr系',           test: n => n.startsWith('hr') },
  { label: '#mk系',           test: n => n.startsWith('mk') },
  { label: '#nb系',           test: n => n.startsWith('nb') },
  { label: 'その他チャンネル', test: () => true },
];

function getGroup(ch) {
  if (ch.type === 'group') return 'ユーザーグループ';
  return (GROUPS.find(g => g.test(ch.name)) || GROUPS[GROUPS.length - 1]).label;
}

const STEPS = ['準備中...','メンバー情報を取得中...','チャンネル一覧を取得中...','ユーザーグループを取得中...','チャンネルメンバーを取得中...','ユーザーグループメンバーを取得中...','データベースに保存中...','同期完了'];
const stepPct = step => { const i = STEPS.indexOf(step); return i < 0 ? 5 : Math.round(((i+1)/STEPS.length)*100); };

export default function ChannelMapping() {
  const [status, setStatus]           = useState(null);
  const [syncing, setSyncing]         = useState(false);
  const [syncJob, setSyncJob]         = useState(null);
  const [data, setData]               = useState(null);
  const [loading, setLoading]         = useState(false);
  const [nameFilter, setNameFilter]   = useState('');
  const [selectedChs, setSelectedChs] = useState(new Set());
  const [membership, setMembership]   = useState('all');
  const [hiddenChs, setHiddenChs]     = useState(new Set());
  const [showHidden, setShowHidden]   = useState(false);
  const [viewMode, setViewMode]       = useState('channel'); // 'channel' | 'member'
  const [selectedMember, setSelectedMember] = useState('');
  const [memberSearch, setMemberSearch] = useState('');
  const [memberSuggestions, setMemberSuggestions] = useState(false);
  const pollRef = useRef(null);

  const loadStatus = async () => { try { setStatus(await api.channelMappingStatus()); } catch {} };
  const loadData   = async () => { setLoading(true); try { setData(await api.channelMappingData()); } catch {} finally { setLoading(false); } };
  const loadHidden = async () => { try { const r = await api.channelMappingHidden(); setHiddenChs(new Set(r.hidden||[])); } catch {} };

  useEffect(() => { loadStatus(); loadData(); loadHidden(); }, []);

  const handleSync = async () => {
    setSyncing(true); setSyncJob({ step:'準備中...', detail:'', status:'running' });
    try {
      const { jobId } = await api.channelMappingSync();
      pollRef.current = setInterval(async () => {
        try {
          const job = await api.channelMappingSyncStatus(jobId);
          setSyncJob(job);
          if (job.status === 'done' || job.status === 'error') {
            clearInterval(pollRef.current); setSyncing(false);
            if (job.status === 'done') { await loadStatus(); await loadData(); }
          }
        } catch { clearInterval(pollRef.current); setSyncing(false); await loadStatus(); await loadData(); }
      }, 2000);
    } catch (e) { setSyncing(false); setSyncJob({ step:'エラーが発生しました', detail:e.message, status:'error' }); }
  };

  const toggleCh     = id => setSelectedChs(p => { const n=new Set(p); n.has(id)?n.delete(id):n.add(id); return n; });
  const hideChannel  = async id => { setHiddenChs(p=>{const n=new Set(p);n.add(id);return n;}); setSelectedChs(s=>{const n=new Set(s);n.delete(id);return n;}); await api.channelMappingHide(id).catch(()=>{}); };
  const unhideCh     = async id => { setHiddenChs(p=>{const n=new Set(p);n.delete(id);return n;}); await api.channelMappingUnhide(id).catch(()=>{}); };
  const restoreAll   = async () => { setHiddenChs(new Set()); await api.channelMappingUnhideAll().catch(()=>{}); };

  const grouped = useMemo(() => {
    if (!data) return {};
    const g = {};
    for (const ch of data.channels) { const l=getGroup(ch); if (!g[l]) g[l]=[]; g[l].push(ch); }
    return g;
  }, [data]);

  const allGroupLabels = [...new Set(data?.channels.map(ch => getGroup(ch)) || [])];

  const filteredMembers = useMemo(() => {
    if (!data) return [];
    return data.members.filter(m => {
      if (nameFilter && !m.display_name.toLowerCase().includes(nameFilter.toLowerCase())) return false;
      if (selectedChs.size === 0) return true;
      const userChs = new Set(data.memberMap[m.user_id]||[]);
      const inAny = [...selectedChs].some(id => userChs.has(id));
      if (membership==='in' && !inAny) return false;
      if (membership==='out' && inAny) return false;
      return true;
    });
  }, [data, nameFilter, selectedChs, membership]);

  return (
    <div>
      {/* ヘッダー */}
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:16 }}>
        <div>
          <h2 style={{ margin:'0 0 4px', fontSize:'1rem', fontWeight:700 }}>チャンネルマッピング</h2>
          <p style={{ margin:0, fontSize:'0.82rem', color:'#6b7280' }}>Slackのチャンネル・グループへの参加状況を確認できます</p>
        </div>
        <div style={{ display:'flex', gap:8, alignItems:'center' }}>
          {status?.synced_at && (
            <span style={{ fontSize:'0.75rem', color:'#9ca3af' }}>
              最終同期: {new Date(status.synced_at).toLocaleString('ja-JP')}
              {status.members>0 && ` (${status.members}名 / CH${status.channels} / UG${status.usergroups})`}
            </span>
          )}
          <button className="btn-primary" onClick={handleSync} disabled={syncing} style={{ fontSize:'0.82rem' }}>
            {syncing ? '同期中...' : 'Slack同期'}
          </button>
        </div>
      </div>

      {/* 進捗バー */}
      {syncJob && (
        <div style={{ marginBottom:16, padding:'12px 16px', background:'#f9fafb', border:`1px solid ${syncJob.status==='error'?'#fca5a5':syncJob.status==='done'?'#86efac':'#e5e7eb'}`, borderRadius:8 }}>
          <div style={{ display:'flex', justifyContent:'space-between', marginBottom:6 }}>
            <span style={{ fontSize:'0.85rem', fontWeight:600, color:syncJob.status==='error'?'#dc2626':syncJob.status==='done'?'#15803d':'#374151' }}>
              {syncJob.status==='done'?'✓ ':syncJob.status==='error'?'✕ ':'⟳ '}{syncJob.step}
            </span>
            <span style={{ fontSize:'0.78rem', color:'#6b7280' }}>{stepPct(syncJob.step)}%</span>
          </div>
          {syncJob.status!=='error' && (
            <div style={{ height:5, background:'#e5e7eb', borderRadius:3, overflow:'hidden', marginBottom:4 }}>
              <div style={{ height:'100%', width:`${stepPct(syncJob.step)}%`, background:syncJob.status==='done'?'#10b981':'#3b82f6', borderRadius:3, transition:'width 0.4s ease' }} />
            </div>
          )}
          {syncJob.detail && <div style={{ fontSize:'0.75rem', color:'#6b7280' }}>{syncJob.detail}</div>}
        </div>
      )}

      {(!data || data.channels.length === 0) ? (
        <div style={{ textAlign:'center', padding:48, color:'#9ca3af' }}>
          {loading ? '読み込み中...' : '「Slack同期」を押してデータを取得してください'}
        </div>
      ) : (
        <div>
        {/* モード切替 */}
        <div style={{ display:'flex', gap:8, marginBottom:16, alignItems:'center' }}>
          {[['channel','チャンネル軸（チャンネルを選んでメンバーを確認）'],['member','メンバー軸（メンバーを選んでチャンネルを確認）']].map(([v,l]) => (
            <button key={v} onClick={() => setViewMode(v)}
              style={{ padding:'6px 14px', border:'1.5px solid', borderRadius:8, fontSize:'0.82rem', fontWeight:600, cursor:'pointer',
                borderColor:viewMode===v?'#3b82f6':'#e5e7eb', background:viewMode===v?'#eff6ff':'#fff', color:viewMode===v?'#1d4ed8':'#6b7280' }}>
              {l}
            </button>
          ))}
        </div>

        {viewMode === 'member' ? (
          /* ── メンバー軸 ── */
          <div>
            <div style={{ display:'flex', gap:8, marginBottom:16, alignItems:'center', position:'relative' }}>
              <div style={{ flex:1, maxWidth:320, position:'relative' }}>
                <input
                  value={memberSearch}
                  onChange={e => { setMemberSearch(e.target.value); setMemberSuggestions(true); if (!e.target.value) setSelectedMember(''); }}
                  onFocus={() => setMemberSuggestions(true)}
                  placeholder="名前で検索..."
                  style={{ width:'100%', boxSizing:'border-box', padding:'8px 12px', border:'1px solid #d1d5db', borderRadius:8, fontSize:'0.85rem', outline:'none' }}
                  onFocus={e => { e.target.style.borderColor='#6366f1'; setMemberSuggestions(true); }}
                  onBlur={e => { e.target.style.borderColor='#d1d5db'; setTimeout(() => setMemberSuggestions(false), 150); }}
                />
                {memberSuggestions && memberSearch && (
                  <div style={{ position:'absolute', top:'100%', left:0, right:0, zIndex:100, background:'#fff', border:'1px solid #e5e7eb', borderRadius:8, boxShadow:'0 4px 16px rgba(0,0,0,0.1)', maxHeight:240, overflowY:'auto', marginTop:4 }}>
                    {data.members.filter(m => m.display_name.toLowerCase().includes(memberSearch.toLowerCase())).map(m => (
                      <div key={m.user_id}
                        onMouseDown={() => { setSelectedMember(m.user_id); setMemberSearch(m.display_name); setMemberSuggestions(false); }}
                        style={{ padding:'8px 12px', cursor:'pointer', fontSize:'0.85rem', borderBottom:'1px solid #f3f4f6' }}
                        onMouseEnter={e => e.target.style.background='#f9fafb'}
                        onMouseLeave={e => e.target.style.background='transparent'}>
                        <span style={{ fontWeight:600, color:'#111827' }}>{m.display_name}</span>
                        {m.title && <span style={{ color:'#9ca3af', fontSize:'0.78rem', marginLeft:8 }}>{m.title}</span>}
                      </div>
                    ))}
                    {data.members.filter(m => m.display_name.toLowerCase().includes(memberSearch.toLowerCase())).length === 0 && (
                      <div style={{ padding:'12px', color:'#9ca3af', fontSize:'0.82rem', textAlign:'center' }}>見つかりません</div>
                    )}
                  </div>
                )}
              </div>
              {selectedMember && (
                <button onClick={() => { setSelectedMember(''); setMemberSearch(''); }}
                  style={{ background:'none', border:'none', color:'#9ca3af', cursor:'pointer', fontSize:13 }}>×</button>
              )}
            </div>
            {selectedMember && (() => {
              const userChs = new Set(data.memberMap[selectedMember] || []);
              const visibleChannels = allGroupLabels.flatMap(label =>
                (grouped[label] || []).filter(ch => !hiddenChs.has(ch.channel_id))
              );
              const inCount  = visibleChannels.filter(ch => userChs.has(ch.channel_id)).length;
              const outCount = visibleChannels.filter(ch => !userChs.has(ch.channel_id)).length;
              return (
                <div>
                  <div style={{ display:'flex', gap:12, marginBottom:12, fontSize:'0.82rem', color:'#6b7280' }}>
                    <span>参加中: <b style={{ color:'#059669' }}>{inCount}</b>件</span>
                    <span>未参加: <b style={{ color:'#9ca3af' }}>{outCount}</b>件</span>
                  </div>
                  {allGroupLabels.map(label => {
                    const items = (grouped[label] || []).filter(ch => !hiddenChs.has(ch.channel_id));
                    if (!items.length) return null;
                    return (
                      <div key={label} style={{ marginBottom:16 }}>
                        <div style={{ fontSize:'0.72rem', fontWeight:700, color:'#9ca3af', textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:6 }}>{label}</div>
                        <div style={{ display:'flex', flexWrap:'wrap', gap:6 }}>
                          {items.map(ch => {
                            const isMember = userChs.has(ch.channel_id);
                            return (
                              <span key={ch.channel_id} style={{
                                display:'inline-flex', alignItems:'center', gap:4,
                                padding:'4px 10px', borderRadius:99, fontSize:'0.78rem', fontWeight:600,
                                background:isMember?'#dcfce7':'#f3f4f6',
                                color:isMember?'#15803d':'#9ca3af',
                                border:`1px solid ${isMember?'#86efac':'#e5e7eb'}`,
                              }}>
                                <span>{isMember?'○':'×'}</span>
                                {ch.type==='group'?'@':'#'}{ch.name}
                              </span>
                            );
                          })}
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
            })()}
            {!selectedMember && (
              <p style={{ textAlign:'center', padding:32, color:'#9ca3af' }}>メンバーを選択してください</p>
            )}
          </div>
        ) : (
        <div style={{ display:'flex', gap:12, alignItems:'flex-start' }}>
          {/* 左: チャンネル選択 */}
          <div style={{ flexShrink:0, width:240, background:'#fff', border:'1px solid #e5e7eb', borderRadius:8, overflow:'hidden' }}>
            <div style={{ padding:'8px 12px', background:'#f9fafb', borderBottom:'1px solid #e5e7eb', display:'flex', alignItems:'center', gap:6 }}>
              <span style={{ fontSize:'0.78rem', fontWeight:700, color:'#374151', flex:1 }}>CH / グループ選択</span>
              {selectedChs.size > 0 && (
                <button onClick={() => setSelectedChs(new Set())}
                  style={{ background:'none', border:'none', color:'#6b7280', cursor:'pointer', fontSize:'0.72rem' }}>クリア</button>
              )}
            </div>
            <div style={{ maxHeight:'60vh', overflowY:'auto', padding:'6px 0' }}>
              {allGroupLabels.map(label => {
                const items = (grouped[label]||[]).filter(ch => showHidden || !hiddenChs.has(ch.channel_id));
                if (!items.length) return null;
                return (
                  <div key={label}>
                    <div style={{ padding:'4px 10px', fontSize:'0.7rem', fontWeight:700, color:'#9ca3af', textTransform:'uppercase', letterSpacing:'0.06em', background:'#f9fafb' }}>
                      {label}
                    </div>
                    {items.map(ch => {
                      const isHidden = hiddenChs.has(ch.channel_id);
                      const isSelected = selectedChs.has(ch.channel_id);
                      return (
                        <div key={ch.channel_id} style={{ display:'flex', alignItems:'center', padding:'2px 6px 2px 10px',
                          background:isSelected?'#eff6ff':isHidden?'#fafafa':'transparent',
                          borderLeft:isSelected?'2px solid #3b82f6':'2px solid transparent', opacity:isHidden?0.4:1 }}>
                          <label style={{ display:'flex', alignItems:'center', gap:5, flex:1, cursor:isHidden?'default':'pointer', fontSize:'0.78rem', overflow:'hidden' }}>
                            {!isHidden && <input type="checkbox" checked={isSelected} onChange={() => toggleCh(ch.channel_id)} style={{ cursor:'pointer', flexShrink:0 }} />}
                            <span style={{ color:ch.type==='group'?'#7c3aed':'#374151', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                              {ch.type==='group'?'@':'#'}{ch.name}
                            </span>
                          </label>
                          <button onClick={() => isHidden ? unhideCh(ch.channel_id) : hideChannel(ch.channel_id)}
                            title={isHidden?'表示に戻す':'非表示'}
                            style={{ background:'none', border:'none', cursor:'pointer', color:'#d1d5db', fontSize:11, padding:'0 3px', lineHeight:1, flexShrink:0 }}
                            onMouseEnter={e=>e.target.style.color=isHidden?'#10b981':'#ef4444'}
                            onMouseLeave={e=>e.target.style.color='#d1d5db'}>
                            {isHidden?'↩':'×'}
                          </button>
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
            {hiddenChs.size > 0 && (
              <div style={{ padding:'7px 10px', borderTop:'1px solid #f3f4f6', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
                <span style={{ fontSize:'0.72rem', color:'#9ca3af' }}>{hiddenChs.size}件非表示</span>
                <div style={{ display:'flex', gap:5 }}>
                  <button onClick={() => setShowHidden(v=>!v)}
                    style={{ fontSize:'0.72rem', padding:'2px 7px', border:'1px solid #e5e7eb', borderRadius:4, background:'#fff', cursor:'pointer', color:'#6b7280' }}>
                    {showHidden?'隠す':'表示'}
                  </button>
                  <button onClick={restoreAll}
                    style={{ fontSize:'0.72rem', padding:'2px 7px', border:'1px solid #e5e7eb', borderRadius:4, background:'#fff', cursor:'pointer', color:'#10b981' }}>
                    全復元
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* 右: 結果 */}
          <div style={{ flex:1, minWidth:0 }}>
            <div style={{ display:'flex', gap:8, marginBottom:12, alignItems:'center', flexWrap:'wrap' }}>
              <input type="text" value={nameFilter} onChange={e=>setNameFilter(e.target.value)}
                placeholder="名前で絞り込み..."
                style={{ flex:1, minWidth:160, maxWidth:240, padding:'6px 10px', border:'1px solid #d1d5db', borderRadius:6, fontSize:'0.82rem' }} />
              {selectedChs.size > 0 && (
                <div style={{ display:'flex', gap:4 }}>
                  {[['all','全員'],['in','入っている'],['out','入っていない']].map(([v,l]) => (
                    <button key={v} onClick={() => setMembership(v)}
                      style={{ padding:'4px 10px', border:'1.5px solid', borderRadius:6, fontSize:'0.75rem', fontWeight:600, cursor:'pointer',
                        borderColor:membership===v?'#3b82f6':'#e5e7eb', background:membership===v?'#eff6ff':'#fff', color:membership===v?'#1d4ed8':'#6b7280' }}>
                      {l}
                    </button>
                  ))}
                </div>
              )}
              <span style={{ fontSize:'0.78rem', color:'#9ca3af', marginLeft:'auto' }}>{filteredMembers.length}名</span>
            </div>
            <div style={{ border:'1px solid #e5e7eb', borderRadius:8, overflow:'hidden', background:'#fff' }}>
              <table style={{ width:'100%', borderCollapse:'collapse', fontSize:'0.82rem' }}>
                <thead>
                  <tr style={{ background:'#f9fafb' }}>
                    <th style={{ padding:'8px 12px', textAlign:'left', fontWeight:600, color:'#6b7280', borderBottom:'1px solid #e5e7eb', width:160 }}>名前</th>
                    <th style={{ padding:'8px 12px', textAlign:'left', fontWeight:600, color:'#6b7280', borderBottom:'1px solid #e5e7eb' }}>役職・部署</th>
                    {[...selectedChs].map(chId => {
                      const ch = data.channels.find(c=>c.channel_id===chId);
                      return (
                        <th key={chId} style={{ padding:'6px', textAlign:'center', fontWeight:600, color:'#6b7280', borderBottom:'1px solid #e5e7eb', width:64, fontSize:'0.7rem', wordBreak:'break-all' }}>
                          {ch ? `${ch.type==='group'?'@':'#'}${ch.name}` : chId}
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {filteredMembers.length === 0 ? (
                    <tr><td colSpan={99} style={{ padding:24, textAlign:'center', color:'#9ca3af' }}>
                      {selectedChs.size===0 ? 'チャンネルを選択してください' : '該当なし'}
                    </td></tr>
                  ) : filteredMembers.map((m,i) => {
                    const userChs = new Set(data.memberMap[m.user_id]||[]);
                    return (
                      <tr key={m.user_id} style={{ borderBottom:'1px solid #f3f4f6', background:i%2===1?'#fafafa':'#fff' }}>
                        <td style={{ padding:'7px 12px', fontWeight:600, color:'#111827' }}>{m.display_name}</td>
                        <td style={{ padding:'7px 12px', color:'#6b7280', fontSize:'0.78rem' }}>{m.title}</td>
                        {[...selectedChs].map(chId => {
                          const ok = userChs.has(chId);
                          return (
                            <td key={chId} style={{ padding:'7px 6px', textAlign:'center' }}>
                              <span style={{ fontWeight:700, color:ok?'#059669':'#d1d5db' }}>{ok?'○':'×'}</span>
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
        )} {/* end channel mode */}
        </div>
      )}
    </div>
  );
}
