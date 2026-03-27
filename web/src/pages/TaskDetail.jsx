import { useEffect, useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import { api } from '../api/client';

const STATUS_OPTIONS = [
  { value: 'in_progress', label: '進行中', color: '#1565c0', bg: '#e3f2fd' },
  { value: 'done', label: '完了', color: '#27ae60', bg: '#e8faf0' },
  { value: 'cancelled', label: 'キャンセル', color: '#868e96', bg: '#f1f3f5' },
  { value: 'pending', label: '保留', color: '#e67e22', bg: '#fff8e1' },
];

function formatDate(d) {
  if (!d) return '';
  const s = typeof d === 'string' ? d : new Date(d).toISOString();
  return s.slice(0, 10);
}

function formatDateTime(d) {
  if (!d) return '';
  const dt = new Date(d);
  return dt.toLocaleString('ja-JP', { timeZone: 'Asia/Tokyo' });
}

export default function TaskDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [task, setTask] = useState(null);
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [editing, setEditing] = useState(false);
  const [form, setForm] = useState({});
  const [saving, setSaving] = useState(false);
  const [newComment, setNewComment] = useState('');
  const [commentSaving, setCommentSaving] = useState(false);

  const load = () => {
    api.taskDetail(id)
      .then((r) => {
        setTask(r.task);
        setComments(r.comments);
        setForm({
          title: r.task.title || '',
          description: r.task.description || '',
          due_date: formatDate(r.task.due_date),
          assignee_id: r.task.assignee_id || '',
        });
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, [id]);

  const handleStatusChange = async (status) => {
    try {
      const r = await api.taskSetStatus(id, status);
      setTask(r.task);
    } catch (e) { console.error(e); }
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const body = {};
      if (form.title !== task.title) body.title = form.title;
      if (form.description !== task.description) body.description = form.description;
      if (form.due_date !== formatDate(task.due_date)) body.due_date = form.due_date || null;
      if (form.assignee_id !== (task.assignee_id || '')) body.assignee_id = form.assignee_id || null;

      if (Object.keys(body).length > 0) {
        const r = await api.taskUpdate(id, body);
        setTask(r.task);
      }
      setEditing(false);
    } catch (e) { console.error(e); }
    setSaving(false);
  };

  const handleComment = async () => {
    if (!newComment.trim()) return;
    setCommentSaving(true);
    try {
      const r = await api.taskAddComment(id, newComment.trim());
      setComments(r.comments);
      setNewComment('');
    } catch (e) { console.error(e); }
    setCommentSaving(false);
  };

  if (loading) return <div className="loading">読み込み中...</div>;
  if (!task) return <div className="loading">タスクが見つかりません</div>;

  const statusCfg = STATUS_OPTIONS.find((s) => s.value === task.status) || STATUS_OPTIONS[0];
  const isOverdue = task.due_date && !['done', 'cancelled'].includes(task.status) &&
    formatDate(task.due_date) < formatDate(new Date());

  return (
    <div className="task-detail">
      <div className="task-detail-nav">
        <Link to="/" className="back-btn">← 一覧に戻る</Link>
      </div>

      <div className="task-detail-main">
        <div className="task-detail-left">
          {/* Title */}
          <div className="task-detail-header">
            {editing ? (
              <input
                className="edit-title-input"
                value={form.title}
                onChange={(e) => setForm({ ...form, title: e.target.value })}
              />
            ) : (
              <h1 className="task-detail-title">{task.title || '（タイトルなし）'}</h1>
            )}
          </div>

          {/* Description */}
          <div className="task-detail-section">
            <h3>説明</h3>
            {editing ? (
              <textarea
                className="edit-desc-input"
                rows={6}
                value={form.description}
                onChange={(e) => setForm({ ...form, description: e.target.value })}
              />
            ) : (
              <div className="task-description">
                {task.description ? (
                  <pre className="desc-pre">{task.description}</pre>
                ) : (
                  <span className="empty-text">説明なし</span>
                )}
              </div>
            )}
          </div>

          {/* Comments */}
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
            <div className="comment-form">
              <textarea
                rows={3}
                placeholder="コメントを入力..."
                value={newComment}
                onChange={(e) => setNewComment(e.target.value)}
              />
              <button
                className="btn-primary"
                onClick={handleComment}
                disabled={commentSaving || !newComment.trim()}
              >
                {commentSaving ? '送信中...' : 'コメント'}
              </button>
            </div>
          </div>
        </div>

        {/* Sidebar */}
        <div className="task-detail-sidebar">
          {/* Status */}
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

          {/* Assignee */}
          <div className="sidebar-section">
            <label>担当者</label>
            {editing ? (
              <input
                className="sidebar-input"
                value={form.assignee_id}
                onChange={(e) => setForm({ ...form, assignee_id: e.target.value })}
                placeholder="Slack User ID"
              />
            ) : (
              <div className="sidebar-value">{task.assigneeDisplayName || task.assignee_id || '-'}</div>
            )}
          </div>

          {/* Due date */}
          <div className="sidebar-section">
            <label>期限</label>
            {editing ? (
              <input
                type="date"
                className="sidebar-input"
                value={form.due_date}
                onChange={(e) => setForm({ ...form, due_date: e.target.value })}
              />
            ) : (
              <div className={`sidebar-value ${isOverdue ? 'overdue-text' : ''}`}>
                {formatDate(task.due_date) || '未設定'}
                {isOverdue && ' (期限切れ)'}
              </div>
            )}
          </div>

          {/* Requester */}
          <div className="sidebar-section">
            <label>依頼者</label>
            <div className="sidebar-value">{task.requesterDisplayName || task.requester_user_id || '-'}</div>
          </div>

          {/* Type */}
          <div className="sidebar-section">
            <label>種別</label>
            <div className="sidebar-value">{task.task_type === 'broadcast' ? '一斉タスク' : '個人タスク'}</div>
          </div>

          {/* Meta */}
          <div className="sidebar-section">
            <label>作成日</label>
            <div className="sidebar-value small">{formatDateTime(task.created_at)}</div>
          </div>

          {task.source_permalink && (
            <div className="sidebar-section">
              <a href={task.source_permalink} target="_blank" rel="noopener noreferrer" className="slack-link">
                Slackで表示
              </a>
            </div>
          )}

          {/* Edit / Save buttons */}
          <div className="sidebar-actions">
            {editing ? (
              <>
                <button className="btn-primary" onClick={handleSave} disabled={saving}>
                  {saving ? '保存中...' : '保存'}
                </button>
                <button className="btn-ghost" onClick={() => setEditing(false)}>キャンセル</button>
              </>
            ) : (
              <button className="btn-outline" onClick={() => setEditing(true)}>編集</button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
