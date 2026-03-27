import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { api } from '../api/client';

const STATUS_OPTIONS = [
  { value: 'in_progress', label: '進行中', color: '#1565c0', bg: '#e3f2fd' },
  { value: 'done',        label: '完了',   color: '#27ae60', bg: '#e8faf0' },
];

function formatDate(d) {
  if (!d) return '';
  const s = typeof d === 'string' ? d : new Date(d).toISOString();
  return s.slice(0, 10);
}

function formatDateTime(d) {
  if (!d) return '';
  return new Date(d).toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' });
}

export default function TaskDetail() {
  const { id } = useParams();
  const [task, setTask] = useState(null);
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.taskDetail(id)
      .then((r) => {
        setTask(r.task);
        setComments(r.comments);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [id]);

  const handleStatusChange = async (status) => {
    try {
      const r = await api.taskSetStatus(id, status);
      setTask(r.task);
    } catch (e) { console.error(e); }
  };

  if (loading) return <div className="loading">読み込み中...</div>;
  if (!task) return <div className="loading">タスクが見つかりません</div>;

  const isOverdue = task.due_date && task.status !== 'done' &&
    formatDate(task.due_date) < formatDate(new Date());

  return (
    <div className="task-detail">
      <div className="task-detail-nav">
        <Link to="/" className="back-btn">← 一覧に戻る</Link>
      </div>

      <div className="task-detail-main">
        <div className="task-detail-left">
          <div className="task-detail-header">
            <h1 className="task-detail-title">{task.title || '（タイトルなし）'}</h1>
          </div>

          <div className="task-detail-section">
            <h3>説明</h3>
            <div className="task-description">
              {task.description ? (
                <pre className="desc-pre">{task.description}</pre>
              ) : (
                <span className="empty-text">説明なし</span>
              )}
            </div>
          </div>

          <div className="task-detail-section">
            <h3>コメント ({comments.length})</h3>
            <div className="comments-list">
              {comments.map((c, i) => (
                <div key={i} className="comment-item">
                  <div className="comment-header">
                    <span className="comment-author">{c.displayName}</span>
                    <span className="comment-time">{formatDateTime(c.created_at)}</span>
                  </div>
                  <div className="comment-body">{c.comment}</div>
                </div>
              ))}
              {comments.length === 0 && <p className="empty-text">コメントなし</p>}
            </div>
          </div>
        </div>

        <div className="task-detail-sidebar">
          <div className="sidebar-section">
            <label>ステータス</label>
            <div className="status-select-group">
              {STATUS_OPTIONS.map((s) => (
                <button
                  key={s.value}
                  className={`status-btn ${task.status === s.value ? 'active' : ''}`}
                  style={task.status === s.value ? { background: s.bg, color: s.color, borderColor: s.color } : {}}
                  onClick={() => handleStatusChange(s.value)}
                >
                  {s.label}
                </button>
              ))}
            </div>
          </div>

          <div className="sidebar-section">
            <label>担当者</label>
            {task.task_type === 'broadcast' ? (
              <div className="sidebar-value">
                <div className="broadcast-targets-label">
                  一斉（{task.assignee_label || `${task.total_count ?? '?'}名`}）
                </div>
                {task.targets?.length > 0 && (
                  <ul className="broadcast-targets-list">
                    {task.targets.map((t) => (
                      <li key={t.user_id}>{t.displayName}</li>
                    ))}
                  </ul>
                )}
              </div>
            ) : (
              <div className="sidebar-value">{task.assigneeDisplayName || task.assignee_id || '-'}</div>
            )}
          </div>

          <div className="sidebar-section">
            <label>期限</label>
            <div className={`sidebar-value ${isOverdue ? 'overdue-text' : ''}`}>
              {formatDate(task.due_date) || '未設定'}
              {isOverdue && ' (期限切れ)'}
            </div>
          </div>

          <div className="sidebar-section">
            <label>依頼者</label>
            <div className="sidebar-value">{task.requesterDisplayName || task.requester_user_id || '-'}</div>
          </div>

          <div className="sidebar-section">
            <label>種別</label>
            <div className="sidebar-value">{task.task_type === 'broadcast' ? '一斉タスク' : '個人タスク'}</div>
          </div>

          <div className="sidebar-section">
            <label>作成日</label>
            <div className="sidebar-value small">{formatDateTime(task.created_at)}</div>
          </div>
        </div>
      </div>
    </div>
  );
}
