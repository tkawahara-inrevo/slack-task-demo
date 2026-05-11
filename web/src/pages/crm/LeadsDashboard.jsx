import { useState, useEffect, useCallback } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Cell,
} from 'recharts';
import { api } from '../../api/client';

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#84cc16','#ec4899','#6366f1'];
const fmtM = n => { if (!n) return '—'; const m = Math.round(Number(n)); return m >= 10000 ? `${Math.round(m/10000).toLocaleString()}万` : m.toLocaleString(); };
const TICK = { fontSize: 11, fill: '#6b7280' };

const YOMI_COLOR = {
  'リード（アポ化前）': '#94a3b8',
  'アポ取得済': '#3b82f6',
  '商談中': '#10b981',
};

export default function LeadsDashboard() {
  const [data, setData]   = useState(null);
  const [loading, setLoading] = useState(false);
  const [from, setFrom]   = useState('');
  const [to, setTo]       = useState('');

  const load = useCallback(async (f, t) => {
    setLoading(true);
    try {
      const d = await api.crmLeadsDashboard({ from: f || undefined, to: t || undefined });
      setData(d);
    } catch (e) { console.error(e); }
    setLoading(false);
  }, []);

  useEffect(() => { load('', ''); }, []);

  const handleSearch = () => load(from, to);
  const handleReset  = () => { setFrom(''); setTo(''); load('', ''); };

  const total = data?.funnel?.reduce((s, r) => s + r.cnt, 0) || 0;

  return (
    <div style={{ padding: '0 0 32px' }}>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ margin: 0, fontSize: '1.1rem', fontWeight: 800, color: '#111827' }}>リード管理</h2>
        {data?.period && (
          <p style={{ margin: '3px 0 0', fontSize: '0.8rem', color: '#6b7280' }}>
            集計期間: {data.period.from} 〜 {data.period.to}
          </p>
        )}
      </div>

      {/* 期間フィルター */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 20, flexWrap: 'wrap', alignItems: 'center' }}>
        <label style={{ fontSize: '0.82rem', fontWeight: 600, color: '#374151' }}>流入日</label>
        <input type="date" value={from} onChange={e => setFrom(e.target.value)}
          style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '5px 10px', fontSize: '0.82rem' }} />
        <span style={{ color: '#9ca3af' }}>〜</span>
        <input type="date" value={to} onChange={e => setTo(e.target.value)}
          style={{ border: '1px solid #d1d5db', borderRadius: 6, padding: '5px 10px', fontSize: '0.82rem' }} />
        <button onClick={handleSearch}
          style={{ background: '#3b82f6', color: '#fff', border: 'none', borderRadius: 6, padding: '6px 16px', fontSize: '0.82rem', cursor: 'pointer', fontWeight: 600 }}>
          絞り込む
        </button>
        {(from || to) && (
          <button onClick={handleReset}
            style={{ background: 'none', border: '1px solid #d1d5db', borderRadius: 6, padding: '5px 12px', fontSize: '0.8rem', cursor: 'pointer', color: '#6b7280' }}>
            リセット
          </button>
        )}
      </div>

      {loading && <div style={{ color: '#9ca3af', marginBottom: 16 }}>読み込み中...</div>}

      {data && !loading && (
        <>
          {/* ファネル KPI */}
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 20 }}>
            {data.funnel.map((f, i) => {
              const rate = i > 0 && data.funnel[i-1].cnt > 0
                ? Math.round(f.cnt / data.funnel[i-1].cnt * 100) : null;
              return (
                <div key={f.label} style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '14px 20px', minWidth: 150, flex: 1 }}>
                  <div style={{ fontSize: '0.72rem', color: '#6b7280', marginBottom: 4 }}>{f.label}</div>
                  <div style={{ fontSize: '2rem', fontWeight: 800, color: YOMI_COLOR[f.label] || '#374151' }}>
                    {f.cnt.toLocaleString()}
                  </div>
                  {rate !== null && (
                    <div style={{ fontSize: '0.72rem', color: '#9ca3af', marginTop: 2 }}>
                      転換率 {rate}%
                    </div>
                  )}
                </div>
              );
            })}
            <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '14px 20px', minWidth: 150, flex: 1 }}>
              <div style={{ fontSize: '0.72rem', color: '#6b7280', marginBottom: 4 }}>期間内合計</div>
              <div style={{ fontSize: '2rem', fontWeight: 800, color: '#374151' }}>
                {(data.bySource.reduce((s, r) => s + r.cnt, 0)).toLocaleString()}
              </div>
            </div>
          </div>

          {/* 月次推移 + 流入経路 */}
          <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 20, marginBottom: 20 }}>
            <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px' }}>
              <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 12, paddingLeft: 8 }}>月次リード推移</div>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={data.trend} margin={{ top: 4, right: 16, left: 0, bottom: 4 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
                  <XAxis dataKey="month" tick={TICK} />
                  <YAxis tick={TICK} allowDecimals={false} />
                  <Tooltip formatter={v => [`${v}件`]} />
                  <Line type="monotone" dataKey="cnt" stroke="#3b82f6" strokeWidth={2} dot={{ r: 3 }} name="リード数" />
                </LineChart>
              </ResponsiveContainer>
            </div>

            <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px' }}>
              <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 12, paddingLeft: 8 }}>流入経路 TOP10</div>
              <ResponsiveContainer width="100%" height={Math.max(200, data.bySource.slice(0,10).length * 26)}>
                <BarChart data={data.bySource.slice(0, 10)} layout="vertical" margin={{ top: 0, right: 16, left: 4, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                  <XAxis type="number" tick={TICK} allowDecimals={false} />
                  <YAxis type="category" dataKey="source" tick={{ fontSize: 11, fill: '#6b7280' }} width={120} />
                  <Tooltip formatter={v => [`${v}件`]} />
                  <Bar dataKey="cnt" radius={[0, 4, 4, 0]} name="件数">
                    {data.bySource.slice(0,10).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* 担当者別 */}
          <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, padding: '16px 12px', marginBottom: 20 }}>
            <div style={{ fontWeight: 700, fontSize: '0.85rem', marginBottom: 12, paddingLeft: 8 }}>担当者別リード数</div>
            <ResponsiveContainer width="100%" height={Math.max(120, data.byRep.length * 28)}>
              <BarChart data={data.byRep} layout="vertical" margin={{ top: 0, right: 16, left: 4, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" horizontal={false} />
                <XAxis type="number" tick={TICK} allowDecimals={false} />
                <YAxis type="category" dataKey="rep" tick={{ fontSize: 11, fill: '#6b7280' }} width={100} />
                <Tooltip formatter={v => [`${v}件`]} />
                <Bar dataKey="cnt" fill="#3b82f6" radius={[0, 4, 4, 0]} name="件数" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* リード一覧 */}
          <div style={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 10, overflow: 'hidden' }}>
            <div style={{ padding: '12px 16px', borderBottom: '1px solid #f3f4f6', fontWeight: 700, fontSize: '0.85rem' }}>
              リード一覧（最新50件）
            </div>
            <div style={{ overflowX: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
                <thead>
                  <tr style={{ background: '#f8fafc' }}>
                    {['流入日', '会社名', 'ヨミ', '流入経路', '担当者'].map(h => (
                      <th key={h} style={{ padding: '8px 12px', textAlign: 'left', fontWeight: 600, color: '#64748b', whiteSpace: 'nowrap', borderBottom: '1px solid #e5e7eb', fontSize: '0.72rem' }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.recent.map((r, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid #f8fafc', background: i % 2 === 0 ? '#fff' : '#fafafa' }}>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#6b7280' }}>{r.inflow_date?.slice(0,10) || '—'}</td>
                      <td style={{ padding: '7px 12px', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontWeight: 500 }}>{r.customer || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap' }}>
                        <span style={{ fontSize: '0.72rem', background: r.yomi === 'アポ化前' ? '#f1f5f9' : '#eff6ff', color: r.yomi === 'アポ化前' ? '#64748b' : '#3b82f6', borderRadius: 4, padding: '2px 6px' }}>
                          {r.yomi}
                        </span>
                      </td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#374151' }}>{r.source || '—'}</td>
                      <td style={{ padding: '7px 12px', whiteSpace: 'nowrap', color: '#374151' }}>{r.rep || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
