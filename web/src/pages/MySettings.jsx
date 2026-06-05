import { useEffect, useState } from 'react';
import { api } from '../api/client';

export default function MySettings() {
  return (
    <div style={{ padding: '20px 28px', maxWidth: 720, margin: '0 auto' }}>
      <div style={{ fontWeight: 800, fontSize: '1.2rem', color: '#0f172a', marginBottom: 4 }}>個人設定</div>
      <div style={{ fontSize: '0.8rem', color: '#64748b', marginBottom: 24 }}>あなただけに適用されるTaskHub設定</div>

      <TaskTriggerSection />
    </div>
  );
}

// 自動タスク化キーワード
function TaskTriggerSection() {
  const [list, setList] = useState(null);
  const [keyword, setKeyword] = useState('');
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState('');

  const reload = () => api.myTaskTriggers().then(r => setList(r.triggers || [])).catch(() => setList([]));
  useEffect(() => { reload(); }, []);

  const add = async () => {
    const k = keyword.trim();
    if (!k) return;
    setAdding(true); setError('');
    try {
      await api.myTaskTriggerAdd(k);
      setKeyword('');
      reload();
    } catch (e) { setError(e.message || '追加失敗'); }
    finally { setAdding(false); }
  };

  const toggle = async (t) => {
    try { await api.myTaskTriggerToggle(t.id, !t.enabled); reload(); }
    catch (e) { alert('変更失敗: ' + e.message); }
  };

  const remove = async (t) => {
    if (!window.confirm(`「${t.keyword}」を削除しますか？`)) return;
    try { await api.myTaskTriggerDelete(t.id); reload(); }
    catch (e) { alert('削除失敗: ' + e.message); }
  };

  return (
    <section style={{ background: '#fff', border: '1px solid #e2e8f0', borderRadius: 12, padding: '18px 20px', boxShadow: '0 1px 3px rgba(0,0,0,0.04)' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <span style={{ fontSize: '1rem' }}>🐶</span>
        <h2 style={{ margin: 0, fontSize: '0.95rem', fontWeight: 800, color: '#0f172a' }}>自動タスク化キーワード（Pochi）</h2>
      </div>
      <p style={{ fontSize: '0.76rem', color: '#64748b', margin: '4px 0 16px', lineHeight: 1.6 }}>
        <b>あなた自身がSlackに投稿したメッセージ</b>に、ここで登録したキーワードが含まれていると<br />
        Pochi が自動でタスク化します（他の人の発言には反応しません）。<br />
        標準で <code style={{ background: '#f1f5f9', padding: '1px 6px', borderRadius: 4, color: '#0f172a' }}>{'<タスク化>'}</code> と <code style={{ background: '#f1f5f9', padding: '1px 6px', borderRadius: 4, color: '#0f172a' }}>＜タスク化＞</code> はチーム全員で有効です。
      </p>

      {/* 追加フォーム */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
        <input value={keyword} onChange={e => { setKeyword(e.target.value); setError(''); }}
          onKeyDown={e => { if (e.key === 'Enter') add(); }}
          placeholder="例: TODO: / メモ / 📝 / やること"
          style={{ flex: 1, padding: '8px 12px', border: '1px solid #cbd5e1', borderRadius: 8, fontSize: '0.86rem', outline: 'none' }} />
        <button onClick={add} disabled={adding || !keyword.trim()}
          style={{ padding: '6px 18px', border: 'none', background: '#4f46e5', color: '#fff', borderRadius: 8, cursor: adding || !keyword.trim() ? 'not-allowed' : 'pointer', fontSize: '0.82rem', fontWeight: 700, opacity: adding || !keyword.trim() ? 0.5 : 1 }}>
          {adding ? '...' : '＋ 追加'}
        </button>
      </div>
      {error && <div style={{ fontSize: '0.74rem', color: '#dc2626', marginBottom: 10 }}>{error}</div>}

      {/* リスト */}
      {list === null && <div style={{ textAlign: 'center', color: '#94a3b8', padding: 16, fontSize: '0.78rem' }}>読み込み中…</div>}
      {list?.length === 0 && (
        <div style={{ textAlign: 'center', color: '#94a3b8', padding: 18, fontSize: '0.78rem', border: '1px dashed #e2e8f0', borderRadius: 8 }}>
          まだ自分用キーワードはありません。<br />
          上のフォームから追加すると、自分の発言時にだけ自動タスク化されるようになります。
        </div>
      )}
      {list?.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
          {list.map(t => (
            <div key={t.id} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 12px', background: t.enabled ? '#f8fafc' : '#fafafa', border: '1px solid #f1f5f9', borderRadius: 8, opacity: t.enabled ? 1 : 0.55 }}>
              <span style={{ fontWeight: 700, color: '#0f172a', fontSize: '0.85rem' }}>{t.keyword}</span>
              {!t.enabled && <span style={{ fontSize: '0.66rem', color: '#94a3b8', background: '#f1f5f9', padding: '1px 8px', borderRadius: 99 }}>OFF</span>}
              <span style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
                <button onClick={() => toggle(t)}
                  style={{ fontSize: '0.72rem', padding: '3px 10px', border: '1px solid #cbd5e1', background: '#fff', color: t.enabled ? '#64748b' : '#059669', borderRadius: 6, cursor: 'pointer', fontWeight: 600 }}>
                  {t.enabled ? '無効化' : '有効化'}
                </button>
                <button onClick={() => remove(t)}
                  style={{ fontSize: '0.72rem', padding: '3px 10px', border: '1px solid #fca5a5', background: '#fef2f2', color: '#dc2626', borderRadius: 6, cursor: 'pointer', fontWeight: 600 }}>
                  削除
                </button>
              </span>
            </div>
          ))}
        </div>
      )}

      <details style={{ marginTop: 16, fontSize: '0.74rem', color: '#64748b' }}>
        <summary style={{ cursor: 'pointer', userSelect: 'none' }}>💡 使い方のヒント</summary>
        <div style={{ marginTop: 8, padding: '8px 12px', background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 6, lineHeight: 1.7 }}>
          • <b>独自の合言葉</b>を設定すると、その単語を含めて投稿するだけで自動タスク化されます<br />
          • キーワードはあなたが書いたメッセージにのみ反応します<br />
          • キーワード自体はタスクのタイトルから自動除去されます<br />
          • Pochiが入っているチャンネル全てで有効です
        </div>
      </details>
    </section>
  );
}
