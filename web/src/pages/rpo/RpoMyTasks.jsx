import { useEffect, useRef, useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../../api/client';

function ClientSearchCell({ value, clients, onChange }) {
  const [open, setOpen]     = useState(false);
  const [query, setQuery]   = useState('');
  const [pos, setPos]       = useState({ top: 0, left: 0 });
  const ref                 = useRef(null);
  const inputRef            = useRef(null);

  const current = clients.find(c => c.id === value);
  const filtered = query
    ? clients.filter(c => c.name.toLowerCase().includes(query.toLowerCase()))
    : clients;

  useEffect(() => {
    if (!open) return;
    const handler = e => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const handleOpen = () => {
    const rect = ref.current?.getBoundingClientRect();
    if (rect) setPos({ top: rect.bottom + window.scrollY, left: rect.left + window.scrollX });
    setOpen(true);
    setQuery('');
    setTimeout(() => inputRef.current?.focus(), 0);
  };
  const handleSelect = (id) => { onChange(id); setOpen(false); };

  return (
    <div ref={ref}>
      <div
        onClick={handleOpen}
        style={{ cursor: 'pointer', padding: '4px 6px', borderRadius: '4px', fontSize: '0.82rem',
          color: current ? '#111827' : '#9ca3af', minHeight: '28px', display: 'flex', alignItems: 'center' }}
        className="client-search-trigger"
      >
        {current ? current.name : '— 案件を選択'}
      </div>
      {open && (
        <div style={{ position: 'absolute', top: pos.top, left: pos.left, zIndex: 9999, background: 'white',
          border: '1px solid #d1d5db', borderRadius: '6px', boxShadow: '0 4px 16px rgba(0,0,0,0.12)',
          width: '220px', maxHeight: '260px', display: 'flex', flexDirection: 'column' }}>
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder="案件名で検索…"
            style={{ margin: '6px', padding: '5px 8px', border: '1px solid #d1d5db', borderRadius: '4px',
              fontSize: '0.82rem', outline: 'none' }}
          />
          <div style={{ overflowY: 'auto', flex: 1 }}>
            <div
              onMouseDown={() => handleSelect('')}
              style={{ padding: '6px 12px', fontSize: '0.82rem', color: '#9ca3af', cursor: 'pointer' }}
              className="client-search-option"
            >—</div>
            {filtered.map(c => (
              <div
                key={c.id}
                onMouseDown={() => handleSelect(c.id)}
                style={{ padding: '6px 12px', fontSize: '0.82rem', cursor: 'pointer',
                  background: c.id === value ? '#eff6ff' : 'transparent' }}
                className="client-search-option"
              >{c.name}</div>
            ))}
            {filtered.length === 0 && (
              <div style={{ padding: '8px 12px', color: '#9ca3af', fontSize: '0.8rem' }}>見つかりません</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

const STATUS_OPTIONS = ['未着手', '対応中', '完了', '依頼中'];
const STATUS_COLORS  = { '未着手': '#9ca3af', '対応中': '#f59e0b', '完了': '#10b981', '依頼中': '#6366f1' };

const COLOR_MAP = {
  Ocean:'#3b82f6', Emerald:'#10b981', Amber:'#f59e0b', Rose:'#ef4444',
  Violet:'#8b5cf6', Pink:'#ec4899', Teal:'#14b8a6', Slate:'#64748b',
};
const colorOf = name => COLOR_MAP[name] || '#6b7280';

function toSnake(patch) {
  const m = { taskStatus: 'task_status', statusMemo: 'status_memo', dueDate: 'due_date', isDone: 'is_done', rpoClientId: 'rpo_client_id' };
  const r = {};
  for (const [k, v] of Object.entries(patch)) r[m[k] ?? k] = v;
  return r;
}

function TaskRow({ item, clients, addingId, setAddingId, onSave, onDelete, todayStr }) {
  const [clientId,   setClientId]   = useState(item.rpo_client_id  ?? '');
  const [dueDate,    setDueDate]    = useState(item.due_date        ? item.due_date.slice(0, 10) : '');
  const [status,     setStatus]     = useState(item.task_status     ?? '未着手');
  const [title,      setTitle]      = useState(item.title           ?? '');
  const [notes,      setNotes]      = useState(item.notes           ?? '');
  const [statusMemo, setStatusMemo] = useState(item.status_memo     ?? '');

  const latestRef = useRef({ clientId, dueDate, status, title, notes, statusMemo });
  latestRef.current = { clientId, dueDate, status, title, notes, statusMemo };

  const timers = useRef({});
  const schedule = (key, patch, delay = 600) => {
    clearTimeout(timers.current[key]);
    timers.current[key] = setTimeout(() => onSave(item.id, patch), delay);
  };

  useEffect(() => {
    return () => {
      Object.values(timers.current).forEach(clearTimeout);
      const { clientId, dueDate, status, title, notes, statusMemo } = latestRef.current;
      onSave(item.id, {
        rpo_client_id: clientId || null, due_date: dueDate || null,
        task_status: status, title, notes: notes || null, status_memo: statusMemo || null,
      });
    };
  }, []);

  const isOverdue = dueDate && dueDate < todayStr && status !== '完了';
  const client = clients.find(c => c.id === clientId);

  return (
    <tr className={`mytask-row${isOverdue ? ' overdue' : ''}${status === '完了' ? ' done' : ''}`}>
      {/* CL名 */}
      <td className="mytask-cell client-cell">
        <ClientSearchCell
          value={clientId}
          clients={clients}
          onChange={id => { setClientId(id); onSave(item.id, { rpo_client_id: id || null }); }}
        />
      </td>

      {/* 締切 */}
      <td className="mytask-cell due-cell">
        <input
          type="date"
          value={dueDate}
          onChange={e => { setDueDate(e.target.value); schedule('due', { due_date: e.target.value || null }, 0); }}
        />
      </td>

      {/* 進捗 */}
      <td className="mytask-cell status-cell">
        <select
          value={status}
          onChange={e => { setStatus(e.target.value); onSave(item.id, { task_status: e.target.value }); }}
          style={{ color: STATUS_COLORS[status], fontWeight: 600 }}
        >
          {STATUS_OPTIONS.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </td>

      {/* やること */}
      <td className="mytask-cell title-cell">
        <textarea
          ref={el => { if (el && item.id === addingId) { el.focus(); setAddingId(null); } autoGrow(el); }}
          value={title}
          placeholder="やることを入力…"
          onInput={e => autoGrow(e.target)}
          onChange={e => { setTitle(e.target.value); schedule('title', { title: e.target.value }); }}
        />
      </td>

      {/* 補足 */}
      <td className="mytask-cell notes-cell">
        <textarea
          ref={el => autoGrow(el)}
          value={notes}
          onInput={e => autoGrow(e.target)}
          onChange={e => { setNotes(e.target.value); schedule('notes', { notes: e.target.value || null }); }}
        />
      </td>

      {/* 状況詳細 */}
      <td className="mytask-cell memo-cell">
        <textarea
          ref={el => autoGrow(el)}
          value={statusMemo}
          onInput={e => autoGrow(e.target)}
          onChange={e => { setStatusMemo(e.target.value); schedule('memo', { status_memo: e.target.value || null }); }}
        />
      </td>

      {/* 削除 */}
      <td className="mytask-cell action-cell">
        <button className="ts-delete-btn" onClick={() => onDelete(item.id)}>✕</button>
      </td>
    </tr>
  );
}

function autoGrow(el) {
  if (!el) return;
  el.style.height = 'auto';
  el.style.height = el.scrollHeight + 'px';
}

export default function RpoMyTasks() {
  const navigate = useNavigate();
  const [tasks,    setTasks]    = useState([]);
  const [clients,  setClients]  = useState([]);
  const [loading,  setLoading]  = useState(true);
  const [addingId, setAddingId] = useState(null);
  const [filterStatus, setFilterStatus] = useState('active');
  const [filterClient, setFilterClient] = useState('');

  useEffect(() => {
    api.rpoMyTasks()
      .then(r => { setTasks(r.tasks || []); setClients(r.clients || []); })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const saveTask = async (itemId, patch) => {
    setTasks(prev => prev.map(t => t.id === itemId ? { ...t, ...patch } : t));
    await api.rpoUpdateMyTask(itemId, patch).catch(() => {});
  };

  const handleAdd = async () => {
    try {
      const r = await api.rpoCreateMyTask({ title: '', taskStatus: '未着手' });
      setTasks(prev => [...prev, r.item]);
      setAddingId(r.item.id);
    } catch { alert('作成に失敗しました'); }
  };

  const handleDelete = async (itemId) => {
    if (!window.confirm('このタスクを削除しますか？')) return;
    await api.rpoDeleteMyTask(itemId).catch(() => {});
    setTasks(prev => prev.filter(t => t.id !== itemId));
  };

  const todayStr = new Date().toISOString().slice(0, 10);

  const displayed = tasks.filter(t => {
    const isOverdue = t.due_date && t.due_date.slice(0,10) < todayStr && t.task_status !== '完了';
    if (filterStatus === 'active'  && t.task_status === '完了') return false;
    if (filterStatus === 'done'    && t.task_status !== '完了') return false;
    if (filterStatus === 'overdue' && !isOverdue) return false;
    if (filterClient && t.rpo_client_id !== filterClient) return false;
    return true;
  });

  if (loading) return <div className="page-loading">読み込み中...</div>;

  return (
    <div className="rpo-page">
      <div className="rpo-header">
        <div>
          <button className="btn-back-inline" onClick={() => navigate('/rpo')}>← 案件一覧</button>
          <h1 className="rpo-title">マイタスク</h1>
          <p className="rpo-subtitle">担当案件のタスクを横断管理</p>
        </div>
      </div>

      {/* フィルターバー */}
      <div style={{ display: 'flex', gap: '8px', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap' }}>
        <div className="task-filter-tabs">
          {[['active','未完了'], ['done','完了'], ['overdue','期限切れ'], ['all','全て']].map(([v, label]) => (
            <button key={v} className={`task-filter-tab ${filterStatus === v ? 'active' : ''}`}
              onClick={() => setFilterStatus(v)}>{label}</button>
          ))}
        </div>
        <select
          value={filterClient}
          onChange={e => setFilterClient(e.target.value)}
          style={{ padding: '4px 8px', border: '1px solid #d1d5db', borderRadius: '6px', fontSize: '0.82rem' }}
        >
          <option value="">全案件</option>
          {clients.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
        <span style={{ fontSize: '0.8rem', color: '#9ca3af', marginLeft: 'auto' }}>{displayed.length}件</span>
      </div>

      {/* テーブル */}
      <div className="mytask-table-wrap">
        <table className="mytask-table">
          <thead>
            <tr>
              <th className="mytask-th client-cell">CL名</th>
              <th className="mytask-th due-cell">締切</th>
              <th className="mytask-th status-cell">進捗</th>
              <th className="mytask-th title-cell">やること</th>
              <th className="mytask-th notes-cell">補足</th>
              <th className="mytask-th memo-cell">状況詳細</th>
              <th className="mytask-th action-cell"></th>
            </tr>
          </thead>
          <tbody>
            {displayed.length === 0 ? (
              <tr><td colSpan={7} style={{ textAlign: 'center', padding: '24px', color: '#9ca3af', fontSize: '0.875rem' }}>
                タスクがありません
              </td></tr>
            ) : displayed.map(item => (
              <TaskRow
                key={item.id}
                item={item}
                clients={clients}
                addingId={addingId}
                setAddingId={setAddingId}
                onSave={saveTask}
                onDelete={handleDelete}
                todayStr={todayStr}
              />
            ))}
          </tbody>
        </table>
      </div>

      <button className="btn-primary" style={{ marginTop: '12px' }} onClick={handleAdd}>
        ＋ タスクを追加
      </button>
    </div>
  );
}
