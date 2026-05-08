import { useState } from 'react';

const WORKFLOWS = [
  {
    id: 'wf-01',
    name: '面接日程通知',
    emoji: '📅',
    category: '採用',
    description: '候補者と面接官に日時・場所・準備事項を自動通知します',
    tags: ['採用', '通知'],
    active: true,
    lastRun: '2026-05-07 14:32',
    inputs: [
      { key: 'candidate', label: '候補者名', type: 'text', placeholder: '例: 山田 太郎' },
      { key: 'datetime', label: '面接日時', type: 'datetime-local' },
      { key: 'location', label: '場所 / URL', type: 'text', placeholder: '例: 会議室A / Zoom URL' },
    ],
  },
  {
    id: 'wf-02',
    name: '新入社員ウェルカム',
    emoji: '🎉',
    category: '人事',
    description: '#generalと#introductionチャンネルにウェルカムメッセージを投稿し、各ツールの招待リンクを本人にDMします',
    tags: ['人事', 'オンボーディング'],
    active: true,
    lastRun: '2026-05-01 09:00',
    inputs: [
      { key: 'name', label: '氏名', type: 'text', placeholder: '例: 田中 花子' },
      { key: 'team', label: '所属チーム', type: 'select', options: ['営業', 'マーケ', 'CS', 'エンジニア', 'コーポレート'] },
      { key: 'start_date', label: '入社日', type: 'date' },
    ],
  },
  {
    id: 'wf-03',
    name: '週次MTGリマインダー',
    emoji: '🔔',
    category: '業務',
    description: '対象チャンネルに翌日のMTGアジェンダ入力を促すリマインダーを送信します',
    tags: ['業務', 'リマインダー'],
    active: true,
    lastRun: '2026-05-05 17:00',
    inputs: [
      { key: 'channel', label: '対象チャンネル', type: 'select', options: ['#営業チーム', '#マーケチーム', '#全社'] },
      { key: 'meeting_time', label: 'MTG時刻', type: 'time' },
    ],
  },
  {
    id: 'wf-04',
    name: '有給申請承認フロー',
    emoji: '📝',
    category: '人事',
    description: '申請者→直属上長→人事の承認ルートで有給申請を処理し、承認完了を勤怠システムに連携します',
    tags: ['人事', '承認'],
    active: false,
    lastRun: '2026-04-28 10:15',
    inputs: [
      { key: 'applicant', label: '申請者', type: 'text', placeholder: '例: 鈴木 一郎' },
      { key: 'date_from', label: '取得開始日', type: 'date' },
      { key: 'date_to', label: '取得終了日', type: 'date' },
      { key: 'reason', label: '理由（任意）', type: 'text', placeholder: '例: 私用のため' },
    ],
  },
  {
    id: 'wf-05',
    name: '月次レポート配信',
    emoji: '📊',
    category: '業務',
    description: '指定月のKPI・タスク完了率・採用進捗をまとめて経営陣・各部門長にDMで配信します',
    tags: ['業務', 'レポート'],
    active: true,
    lastRun: '2026-05-01 08:00',
    inputs: [
      { key: 'month', label: '対象月', type: 'month' },
      { key: 'scope', label: '配信範囲', type: 'select', options: ['全部門', '営業のみ', 'コーポレートのみ'] },
    ],
  },
  {
    id: 'wf-06',
    name: '遅刻・欠勤アラート',
    emoji: '⚠️',
    category: '勤怠',
    description: '打刻なし・遅刻検知時に上長と人事にSlack通知を送り、本人にも確認メッセージを送信します',
    tags: ['勤怠', 'アラート'],
    active: false,
    lastRun: null,
    inputs: [
      { key: 'member', label: '対象メンバー', type: 'text', placeholder: '例: 高橋 次郎' },
      { key: 'type', label: '種別', type: 'select', options: ['遅刻', '欠勤', '早退'] },
      { key: 'note', label: '備考', type: 'text', placeholder: '任意' },
    ],
  },
];

const CATEGORIES = ['すべて', '採用', '人事', '業務', '勤怠'];
const CATEGORY_COLORS = {
  '採用': '#3b82f6', '人事': '#10b981', '業務': '#f59e0b', '勤怠': '#ef4444',
};

function StatusBadge({ active }) {
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 4,
      fontSize: '0.72rem', fontWeight: 600, padding: '2px 8px', borderRadius: 20,
      background: active ? '#f0fdf4' : '#f9fafb',
      color: active ? '#16a34a' : '#9ca3af',
      border: `1px solid ${active ? '#bbf7d0' : '#e5e7eb'}`,
    }}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: active ? '#22c55e' : '#d1d5db', display: 'inline-block' }} />
      {active ? 'アクティブ' : '停止中'}
    </span>
  );
}

function ExecModal({ wf, onClose }) {
  const [values, setValues] = useState({});
  const [state, setState] = useState('idle'); // idle | running | done

  const set = (k, v) => setValues(p => ({ ...p, [k]: v }));

  const handleRun = () => {
    setState('running');
    setTimeout(() => setState('done'), 1800);
  };

  return (
    <>
      <div onClick={onClose} style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', zIndex: 1000 }} />
      <div style={{
        position: 'fixed', top: '50%', left: '50%', transform: 'translate(-50%,-50%)',
        width: 440, maxWidth: '94vw', background: '#fff', borderRadius: 14,
        boxShadow: '0 8px 40px rgba(0,0,0,0.18)', zIndex: 1001, overflow: 'hidden',
      }}>
        {/* ヘッダー */}
        <div style={{ padding: '20px 24px 16px', borderBottom: '1px solid #f3f4f6' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
            <span style={{ fontSize: '1.5rem' }}>{wf.emoji}</span>
            <div>
              <div style={{ fontWeight: 800, fontSize: '1rem', color: '#111827' }}>{wf.name}</div>
              <span style={{ fontSize: '0.75rem', color: CATEGORY_COLORS[wf.category] || '#6b7280', fontWeight: 600 }}>{wf.category}</span>
            </div>
            <button onClick={onClose} style={{ marginLeft: 'auto', background: 'none', border: 'none', fontSize: '1.1rem', cursor: 'pointer', color: '#9ca3af' }}>✕</button>
          </div>
          <div style={{ fontSize: '0.82rem', color: '#6b7280', lineHeight: 1.5 }}>{wf.description}</div>
        </div>

        {/* フォーム */}
        {state === 'idle' && (
          <div style={{ padding: '20px 24px' }}>
            {wf.inputs.map(inp => (
              <div key={inp.key} style={{ marginBottom: 14 }}>
                <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 600, color: '#374151', marginBottom: 5 }}>{inp.label}</label>
                {inp.type === 'select' ? (
                  <select value={values[inp.key] || ''} onChange={e => set(inp.key, e.target.value)}
                    style={{ width: '100%', border: '1px solid #d1d5db', borderRadius: 7, padding: '7px 10px', fontSize: '0.85rem' }}>
                    <option value="">選択してください</option>
                    {inp.options.map(o => <option key={o} value={o}>{o}</option>)}
                  </select>
                ) : (
                  <input type={inp.type} value={values[inp.key] || ''} onChange={e => set(inp.key, e.target.value)}
                    placeholder={inp.placeholder || ''}
                    style={{ width: '100%', border: '1px solid #d1d5db', borderRadius: 7, padding: '7px 10px', fontSize: '0.85rem', boxSizing: 'border-box' }} />
                )}
              </div>
            ))}
            <button onClick={handleRun}
              style={{ width: '100%', background: '#3b82f6', color: '#fff', border: 'none', borderRadius: 8, padding: '10px', fontSize: '0.9rem', fontWeight: 700, cursor: 'pointer', marginTop: 4 }}>
              実行する
            </button>
          </div>
        )}

        {/* 実行中 */}
        {state === 'running' && (
          <div style={{ padding: '40px 24px', textAlign: 'center' }}>
            <div style={{ fontSize: '2rem', marginBottom: 12, animation: 'spin 1s linear infinite', display: 'inline-block' }}>⚙️</div>
            <div style={{ fontWeight: 600, color: '#374151' }}>ワークフローを実行中...</div>
            <div style={{ fontSize: '0.8rem', color: '#9ca3af', marginTop: 6 }}>Slackに送信しています</div>
          </div>
        )}

        {/* 完了 */}
        {state === 'done' && (
          <div style={{ padding: '40px 24px', textAlign: 'center' }}>
            <div style={{ fontSize: '2.5rem', marginBottom: 12 }}>✅</div>
            <div style={{ fontWeight: 700, fontSize: '1rem', color: '#15803d', marginBottom: 6 }}>実行完了</div>
            <div style={{ fontSize: '0.82rem', color: '#6b7280', marginBottom: 20 }}>
              Slackへの送信が完了しました
            </div>
            <button onClick={onClose}
              style={{ background: '#f3f4f6', color: '#374151', border: 'none', borderRadius: 8, padding: '8px 24px', fontSize: '0.88rem', fontWeight: 600, cursor: 'pointer' }}>
              閉じる
            </button>
          </div>
        )}
      </div>
      <style>{`@keyframes spin { from{transform:rotate(0deg)} to{transform:rotate(360deg)} }`}</style>
    </>
  );
}

export default function SlackWorkflows() {
  const [category, setCategory] = useState('すべて');
  const [execTarget, setExecTarget] = useState(null);
  const [search, setSearch] = useState('');

  const filtered = WORKFLOWS.filter(wf => {
    const matchCat = category === 'すべて' || wf.category === category;
    const matchQ   = !search || wf.name.includes(search) || wf.description.includes(search);
    return matchCat && matchQ;
  });

  return (
    <div style={{ maxWidth: 900, margin: '0 auto', padding: '24px 16px' }}>
      {/* ページヘッダー */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <div style={{ fontSize: '1.6rem' }}>⚡</div>
        <div>
          <h1 style={{ margin: 0, fontSize: '1.2rem', fontWeight: 800, color: '#111827' }}>Slack ワークフロー</h1>
          <p style={{ margin: '2px 0 0', fontSize: '0.82rem', color: '#6b7280' }}>ワークフローを選んで実行できます</p>
        </div>
      </div>

      {/* 検索 + カテゴリーフィルター */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 20, flexWrap: 'wrap', alignItems: 'center' }}>
        <input value={search} onChange={e => setSearch(e.target.value)} placeholder="ワークフローを検索..."
          style={{ border: '1px solid #d1d5db', borderRadius: 8, padding: '7px 12px', fontSize: '0.85rem', minWidth: 200 }} />
        <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
          {CATEGORIES.map(c => (
            <button key={c} onClick={() => setCategory(c)} style={{
              padding: '5px 14px', borderRadius: 20, fontSize: '0.82rem', fontWeight: 600,
              border: '1px solid ' + (category === c ? (CATEGORY_COLORS[c] || '#3b82f6') : '#e5e7eb'),
              background: category === c ? (CATEGORY_COLORS[c] || '#3b82f6') : '#fff',
              color: category === c ? '#fff' : '#6b7280', cursor: 'pointer',
            }}>{c}</button>
          ))}
        </div>
      </div>

      {/* ワークフロー一覧 */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: 16 }}>
        {filtered.map(wf => (
          <div key={wf.id} style={{
            background: '#fff', border: '1px solid #e5e7eb', borderRadius: 12, padding: '18px 20px',
            display: 'flex', flexDirection: 'column', gap: 10,
            transition: 'box-shadow 0.15s',
          }}
          onMouseEnter={e => e.currentTarget.style.boxShadow = '0 2px 12px rgba(0,0,0,0.08)'}
          onMouseLeave={e => e.currentTarget.style.boxShadow = 'none'}>
            {/* カードヘッダー */}
            <div style={{ display: 'flex', alignItems: 'flex-start', gap: 10 }}>
              <span style={{ fontSize: '1.4rem', lineHeight: 1 }}>{wf.emoji}</span>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontWeight: 700, fontSize: '0.92rem', color: '#111827', marginBottom: 2 }}>{wf.name}</div>
                <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                  <span style={{ fontSize: '0.72rem', color: CATEGORY_COLORS[wf.category] || '#6b7280', fontWeight: 600 }}>{wf.category}</span>
                  <StatusBadge active={wf.active} />
                </div>
              </div>
            </div>

            {/* 説明 */}
            <div style={{ fontSize: '0.8rem', color: '#6b7280', lineHeight: 1.5, flex: 1 }}>{wf.description}</div>

            {/* フッター */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginTop: 4 }}>
              <span style={{ fontSize: '0.72rem', color: '#9ca3af' }}>
                {wf.lastRun ? `最終実行: ${wf.lastRun}` : '未実行'}
              </span>
              <button onClick={() => setExecTarget(wf)}
                disabled={!wf.active}
                style={{
                  background: wf.active ? '#3b82f6' : '#f3f4f6',
                  color: wf.active ? '#fff' : '#9ca3af',
                  border: 'none', borderRadius: 7, padding: '6px 14px',
                  fontSize: '0.82rem', fontWeight: 700,
                  cursor: wf.active ? 'pointer' : 'not-allowed',
                }}>
                実行
              </button>
            </div>
          </div>
        ))}
      </div>

      {filtered.length === 0 && (
        <div style={{ textAlign: 'center', padding: '60px 20px', color: '#9ca3af' }}>
          <div style={{ fontSize: '2rem', marginBottom: 8 }}>🔍</div>
          <div>該当するワークフローがありません</div>
        </div>
      )}

      {execTarget && <ExecModal wf={execTarget} onClose={() => setExecTarget(null)} />}
    </div>
  );
}
