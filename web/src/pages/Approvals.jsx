import { useEffect, useMemo, useState } from 'react';
import { api } from '../api/client';
import { CheckCircle, XCircle, Clock, ExternalLink, User, FileText, ChevronRight } from 'lucide-react';

const STATUS_LABEL = {
  pending:   { label: '承認待ち', color: '#d97706', bg: '#fffbeb', border: '#fde68a', Icon: Clock },
  approved:  { label: '決裁完了', color: '#16a34a', bg: '#f0fdf4', border: '#86efac', Icon: CheckCircle },
  rejected:  { label: '否認',     color: '#dc2626', bg: '#fef2f2', border: '#fca5a5', Icon: XCircle },
  cancelled: { label: '取り下げ', color: '#6b7280', bg: '#f3f4f6', border: '#d1d5db', Icon: XCircle },
};

const fmt = (d) => d ? new Date(d).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' }) : '-';

export default function Approvals() {
  const [list, setList] = useState([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState('all'); // all | pending | approved | rejected
  const [selected, setSelected] = useState(null);
  const [userMap, setUserMap] = useState({});

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.approvals(),
      api.adminUserMapping().catch(() => ({ members: [] })),
    ]).then(([r, dir]) => {
      setList(r.approvals || []);
      const m = {};
      for (const u of (dir.members || [])) m[u.user_id] = u.display_name || u.real_name || u.user_id;
      setUserMap(m);
    }).finally(() => setLoading(false));
  }, []);

  const nameOf = (uid) => userMap[uid] || uid;

  const filtered = useMemo(() => {
    if (tab === 'all') return list;
    return list.filter(a => a.status === tab);
  }, [list, tab]);

  const slackLink = (a) => a.message_ts
    ? `https://slack.com/app_redirect?channel=${a.channel_id}&message_ts=${a.message_ts}`
    : null;

  return (
    <div style={{ padding: 24, maxWidth: 1200, margin: '0 auto' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
        <FileText size={22} color="#2563eb"/>
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 800, color: '#111827' }}>電子決裁</h1>
        <span style={{ fontSize: 12, color: '#6b7280', marginLeft: 8 }}>
          Slackで <code style={{ background:'#f3f4f6', padding:'1px 6px', borderRadius:4 }}>/pochi-approval</code> から起票
        </span>
      </div>

      {/* タブ */}
      <div style={{ display: 'flex', gap: 4, background: '#f3f4f6', borderRadius: 10, padding: 4, marginBottom: 16, width: 'fit-content' }}>
        {[['all','全件'],['pending','承認待ち'],['approved','決裁完了'],['rejected','否認']].map(([v,l]) => (
          <button key={v} onClick={() => setTab(v)}
            style={{ padding: '6px 14px', fontSize: 12, fontWeight: 700, border: 'none', borderRadius: 7, cursor: 'pointer',
              background: tab === v ? '#fff' : 'transparent',
              color: tab === v ? '#1d4ed8' : '#6b7280',
              boxShadow: tab === v ? '0 1px 2px rgba(0,0,0,0.06)' : 'none' }}>
            {l} {v !== 'all' && (
              <span style={{ marginLeft: 4, color: '#9ca3af' }}>
                {list.filter(a => a.status === v).length}
              </span>
            )}
          </button>
        ))}
      </div>

      {loading ? (
        <div style={{ padding: 40, textAlign: 'center', color: '#9ca3af' }}>読み込み中…</div>
      ) : filtered.length === 0 ? (
        <div style={{ padding: 40, textAlign: 'center', color: '#9ca3af', background: '#fff', border: '1px solid #e5e7eb', borderRadius: 12 }}>
          該当する決裁はありません
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          {filtered.map(a => {
            const s = STATUS_LABEL[a.status] || STATUS_LABEL.pending;
            const Icon = s.Icon;
            const link = slackLink(a);
            const approvedN = (a.voters || []).filter(v => v.status === 'approved').length;
            const totalN = (a.voters || []).length;
            const rejectedN = (a.voters || []).filter(v => v.status === 'rejected').length;
            return (
              <div key={a.id} onClick={() => setSelected(a)}
                style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 12, padding: '14px 18px', cursor: 'pointer', boxShadow: '0 1px 2px rgba(0,0,0,0.03)' }}
                onMouseEnter={e => e.currentTarget.style.background = '#f9fafb'}
                onMouseLeave={e => e.currentTarget.style.background = '#fff'}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                  <div style={{ background: s.bg, color: s.color, padding: 6, borderRadius: 8, display: 'flex' }}>
                    <Icon size={16}/>
                  </div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontWeight: 700, color: '#111827', fontSize: 14, marginBottom: 2 }}>{a.title}</div>
                    <div style={{ fontSize: 11, color: '#6b7280', display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                      <span>起票: <strong>{nameOf(a.requester_user_id)}</strong></span>
                      <span>・</span>
                      <span>{fmt(a.created_at)}</span>
                      <span>・</span>
                      <span>方式: {a.mode === 'sequential' ? '順次' : '並列'}</span>
                    </div>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span style={{ fontSize: 12, fontWeight: 700, color: s.color, background: s.bg, border: `1px solid ${s.border}`, padding: '3px 10px', borderRadius: 999 }}>
                      {s.label}
                    </span>
                    <span style={{ fontSize: 12, color: '#6b7280', fontWeight: 700 }}>
                      {rejectedN > 0 ? `❌${rejectedN}` : `${approvedN}/${totalN}`}
                    </span>
                    {link && <a href={link} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()}
                      style={{ color: '#6b7280', padding: 4, display: 'flex' }}><ExternalLink size={14}/></a>}
                    <ChevronRight size={16} color="#9ca3af"/>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* 詳細モーダル */}
      {selected && (
        <div onClick={() => setSelected(null)}
          style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50, padding: 20 }}>
          <div onClick={e => e.stopPropagation()}
            style={{ background: '#fff', borderRadius: 14, maxWidth: 720, width: '100%', maxHeight: '90vh', overflow: 'auto', padding: 24, boxShadow: '0 20px 50px rgba(0,0,0,0.2)' }}>
            <ApprovalDetail approval={selected} nameOf={nameOf} onClose={() => setSelected(null)}/>
          </div>
        </div>
      )}
    </div>
  );
}

function ApprovalDetail({ approval: a, nameOf, onClose }) {
  const s = STATUS_LABEL[a.status] || STATUS_LABEL.pending;
  const Icon = s.Icon;
  const link = a.message_ts
    ? `https://slack.com/app_redirect?channel=${a.channel_id}&message_ts=${a.message_ts}`
    : null;
  return (
    <>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 18, paddingBottom: 14, borderBottom: '1px solid #f3f4f6' }}>
        <div style={{ background: s.bg, color: s.color, padding: 8, borderRadius: 10, display: 'flex' }}>
          <Icon size={20}/>
        </div>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 17, fontWeight: 800, color: '#111827' }}>{a.title}</div>
          <div style={{ fontSize: 12, color: '#6b7280', marginTop: 2 }}>
            <span style={{ fontWeight: 700, color: s.color }}>{s.label}</span>　・　起票: <strong>{nameOf(a.requester_user_id)}</strong>　・　{fmt(a.created_at)}
          </div>
        </div>
        <button onClick={onClose} style={{ background: 'none', border: 'none', cursor: 'pointer', color: '#9ca3af', fontSize: 20, padding: 8 }}>×</button>
      </div>

      {a.description && (
        <div style={{ background: '#f9fafb', border: '1px solid #f3f4f6', borderRadius: 10, padding: 14, marginBottom: 18 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: '#6b7280', marginBottom: 6, textTransform: 'uppercase' }}>内容</div>
          <div style={{ fontSize: 14, color: '#1f2937', whiteSpace: 'pre-wrap', lineHeight: 1.6 }}>{a.description}</div>
        </div>
      )}

      <div style={{ marginBottom: 18 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: '#6b7280', marginBottom: 8, textTransform: 'uppercase' }}>
          承認ログ ({a.mode === 'sequential' ? '順次承認' : '並列承認'})
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
          {(a.voters || []).map(v => {
            const vs = STATUS_LABEL[v.status] || STATUS_LABEL.pending;
            const VIcon = vs.Icon;
            return (
              <div key={v.user_id} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '10px 14px', background: '#fff', border: `1px solid ${vs.border}`, borderRadius: 8 }}>
                <div style={{ color: vs.color, display: 'flex' }}><VIcon size={16}/></div>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 13, fontWeight: 700, color: '#111827' }}><User size={12} style={{ display:'inline', verticalAlign:'middle', marginRight:4 }}/>{nameOf(v.user_id)}</div>
                  {v.comment && <div style={{ fontSize: 12, color: '#6b7280', marginTop: 2 }}>{v.comment}</div>}
                </div>
                <div style={{ textAlign: 'right' }}>
                  <div style={{ fontSize: 12, fontWeight: 700, color: vs.color }}>{vs.label}</div>
                  {v.decided_at && <div style={{ fontSize: 11, color: '#9ca3af' }}>{fmt(v.decided_at)}</div>}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {link && (
        <a href={link} target="_blank" rel="noopener noreferrer"
          style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: 13, fontWeight: 700, color: '#2563eb', textDecoration: 'none', padding: '8px 16px', background: '#eff6ff', border: '1px solid #bfdbfe', borderRadius: 8 }}>
          <ExternalLink size={14}/> Slackで開く
        </a>
      )}
      {a.completed_at && (
        <div style={{ marginTop: 14, fontSize: 11, color: '#9ca3af' }}>完了: {fmt(a.completed_at)}</div>
      )}
    </>
  );
}
