// タスク完了状態 ⇄ 元Slackメッセージのリアクション同期
// - タスク完了 → 元メッセージに :white_check_mark: 付与
// - 完了解除 → リアクション削除
// - 元メッセージに :white_check_mark: が付いた → タスク完了に同期

const DONE_REACTION = 'white_check_mark';

// status に応じてリアクションを add/remove
async function syncTaskDoneReaction(client, task) {
  if (!client || !task || !task.channel_id || !task.message_ts) return;
  const method = task.status === 'done' ? 'add' : 'remove';
  try {
    await client.reactions[method]({
      channel: task.channel_id,
      timestamp: task.message_ts,
      name: DONE_REACTION,
    });
  } catch (e) {
    const err = e?.data?.error || '';
    if (!/already_reacted|no_reaction|message_not_found|channel_not_found|cant_react/.test(err)) {
      console.warn(`[task-source-reaction] ${method} fail:`, err || e.message);
    }
  }
}

// Slack側で :white_check_mark: → タスクを完了に
function registerSourceDoneListener({ app, dbGetTaskBySource, dbUpdateStatus, notifyTaskSimpleDM }) {
  app.event('reaction_added', async ({ event, body }) => {
    try {
      if (event.reaction !== DONE_REACTION) return;
      if (event.item?.type !== 'message') return;
      const teamId = body?.team_id;
      if (!teamId) return;
      const task = await dbGetTaskBySource(teamId, event.item.channel, event.item.ts);
      if (!task || task.status === 'done') return;

      const updated = await dbUpdateStatus(teamId, task.id, 'done');
      if (!updated) return;
      console.log(`[task-source-reaction] task ${task.id} → done via ✅ by ${event.user}`);

      if (typeof notifyTaskSimpleDM === 'function') {
        const targets = new Set([updated.requester_user_id, updated.assignee_id].filter(Boolean));
        for (const uid of targets) {
          await notifyTaskSimpleDM(uid, updated, 'タスクが完了しました（Slack ✅ リアクション）').catch(() => {});
        }
      }
    } catch (e) { console.warn('[task-source-reaction] listener fail:', e.message); }
  });

  // 完了済みのタスクのリアクションが外れた場合は in_progress に戻す
  app.event('reaction_removed', async ({ event, body }) => {
    try {
      if (event.reaction !== DONE_REACTION) return;
      if (event.item?.type !== 'message') return;
      const teamId = body?.team_id;
      if (!teamId) return;
      const task = await dbGetTaskBySource(teamId, event.item.channel, event.item.ts);
      if (!task || task.status !== 'done') return;

      const updated = await dbUpdateStatus(teamId, task.id, 'in_progress');
      if (!updated) return;
      console.log(`[task-source-reaction] task ${task.id} → in_progress (✅ removed) by ${event.user}`);
    } catch (e) { console.warn('[task-source-reaction] remove listener fail:', e.message); }
  });
}

module.exports = { syncTaskDoneReaction, registerSourceDoneListener, DONE_REACTION };
