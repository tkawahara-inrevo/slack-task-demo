import { useNavigate } from 'react-router-dom';

// <@U123> や <#C123|channel> などのSlackメンションを除去
function stripMentions(text) {
  return (text || '')
    .replace(/<@[A-Z0-9]+>/g, '')
    .replace(/<#[A-Z0-9]+\|[^>]*>/g, '')
    .replace(/<[^>]+>/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function trimTitle(title) {
  const t = stripMentions(title || '');
  return t.length > 60 ? t.slice(0, 60) + '…' : t || '（タイトルなし）';
}

// "名前 太郎/Tarou Name" → "名前 太郎"
function trimDisplayName(name) {
  if (!name) return '-';
  const slash = name.indexOf('/');
  return slash > 0 ? name.slice(0, slash).trim() : name;
}

const STATUS_LABELS = {
  in_progress: '進行中',
  done: '完了',
  cancelled: 'キャンセル',
  pending: '保留',
};

function formatDate(d) {
  if (!d) return '-';
  const s = typeof d === 'string' ? d : new Date(d).toISOString();
  return s.slice(0, 10);
}

export default function TaskTable({ tasks, total, page, onPageChange }) {
  const navigate = useNavigate();
  const limit = 50;
  const totalPages = Math.max(1, Math.ceil(total / limit));

  return (
    <div className="task-table-wrap">
      <table className="task-table">
        <thead>
          <tr>
            <th>タイトル</th>
            <th>担当者</th>
            <th>ステータス</th>
            <th>期限</th>
            <th>作成日</th>
          </tr>
        </thead>
        <tbody>
          {tasks.length === 0 && (
            <tr><td colSpan={5} className="empty">タスクがありません</td></tr>
          )}
          {tasks.map((t) => {
            const isOverdue = t.due_date && t.status !== 'done' && t.status !== 'cancelled'
              && formatDate(t.due_date) < formatDate(new Date());
            const assigneeLabel = t.task_type === 'broadcast'
              ? (t.assignee_label ? `一斉（${t.assignee_label}）` : `一斉（${t.total_count ?? '?'}名）`)
              : trimDisplayName(t.assigneeDisplayName);
            return (
              <tr
                key={t.id}
                className={`clickable-row ${isOverdue ? 'overdue-row' : ''}`}
                onClick={() => navigate(`/tasks/${t.id}`)}
              >
                <td className="task-title">{trimTitle(t.title)}</td>
                <td className="task-assignee">{assigneeLabel}</td>
                <td><span className={`status-badge ${t.status}`}>{STATUS_LABELS[t.status] || t.status}</span></td>
                <td className={isOverdue ? 'overdue-date' : ''}>{formatDate(t.due_date)}</td>
                <td>{formatDate(t.created_at)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {totalPages > 1 && (
        <div className="pagination">
          <button disabled={page <= 1} onClick={() => onPageChange(page - 1)}>前へ</button>
          <span>{page} / {totalPages}（{total}件）</span>
          <button disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>次へ</button>
        </div>
      )}
    </div>
  );
}
