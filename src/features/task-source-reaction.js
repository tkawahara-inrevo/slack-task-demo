// タスク完了状態 ⇄ 元Slackメッセージのリアクション同期
//
// personal タスク:
//   - 完了 → 元メッセージに :white_check_mark: 付与
//   - 再オープン → 削除
//   - 元メッセージに ✅（依頼者/担当者）→ タスク完了
//   - 元メッセージから ✅ → in_progress に戻す
//
// broadcast タスク:
//   - 全員完了 → 元メッセージに ✅ 付与
//   - 元メッセージに ✅（target本人）→ その人の完了として記録（全員揃えば全体done）
//   - 元メッセージから ✅（target本人）→ その人の完了を取り消し（doneだった場合 in_progress に戻る）

const DONE_REACTION = '済';

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

function registerSourceDoneListener({
  app,
  dbGetTaskBySource,
  dbUpdateStatus,
  dbUpsertCompletion,
  dbCountCompletions,
  dbCountTargets,
  dbListTargetUserIds,
  dbUpdateBroadcastCounts,
  dbQuery,
  notifyTaskSimpleDM,
}) {
  app.event('reaction_added', async ({ event, body, client, context }) => {
    try {
      if (event.reaction !== DONE_REACTION) return;
      if (event.item?.type !== 'message') return;
      if (event.user && context?.botUserId && event.user === context.botUserId) return; // 自己ループ防止
      const teamId = body?.team_id;
      if (!teamId) return;
      const task = await dbGetTaskBySource(teamId, event.item.channel, event.item.ts);
      if (!task) return;

      const reactor = event.user;

      if (task.task_type === 'broadcast') {
        // 対象者でなければ無視
        let targets = [];
        try { targets = await dbListTargetUserIds(teamId, task.id); } catch (_) {}
        if (targets.length && !targets.includes(reactor)) return;

        await dbUpsertCompletion(teamId, task.id, reactor);
        const done = await dbCountCompletions(teamId, task.id);
        const total = task.total_count || await dbCountTargets(teamId, task.id);
        try { await dbUpdateBroadcastCounts(teamId, task.id, done, total); } catch (_) {}
        console.log(`[task-source-reaction] broadcast ${task.id}: ${reactor} ✅ (${done}/${total})`);

        if (done >= total && total > 0 && task.status !== 'done') {
          const upd = await dbUpdateStatus(teamId, task.id, 'done');
          if (upd) {
            await syncTaskDoneReaction(client, upd);
            if (typeof notifyTaskSimpleDM === 'function' && upd.requester_user_id) {
              await notifyTaskSimpleDM(upd.requester_user_id, upd, '一斉タスクが全員完了しました').catch(() => {});
            }
          }
        }
        return;
      }

      // personal
      if (task.status === 'done') return;
      if (reactor !== task.assignee_id && reactor !== task.requester_user_id) return;
      const updated = await dbUpdateStatus(teamId, task.id, 'done');
      if (!updated) return;
      await syncTaskDoneReaction(client, updated); // ✅は既にあるので no-op
      console.log(`[task-source-reaction] personal ${task.id} → done by ${reactor}`);
      if (typeof notifyTaskSimpleDM === 'function') {
        const ids = new Set([updated.requester_user_id, updated.assignee_id].filter(x => x && x !== reactor));
        for (const uid of ids) {
          await notifyTaskSimpleDM(uid, updated, 'タスクが完了しました（Slack ✅）').catch(() => {});
        }
      }
    } catch (e) { console.warn('[task-source-reaction] add listener fail:', e.message); }
  });

  app.event('reaction_removed', async ({ event, body, client, context }) => {
    try {
      if (event.reaction !== DONE_REACTION) return;
      if (event.item?.type !== 'message') return;
      if (event.user && context?.botUserId && event.user === context.botUserId) return;
      const teamId = body?.team_id;
      if (!teamId) return;
      const task = await dbGetTaskBySource(teamId, event.item.channel, event.item.ts);
      if (!task) return;

      const reactor = event.user;

      if (task.task_type === 'broadcast') {
        let targets = [];
        try { targets = await dbListTargetUserIds(teamId, task.id); } catch (_) {}
        if (targets.length && !targets.includes(reactor)) return;

        // その人の完了レコード削除
        await dbQuery(
          `DELETE FROM task_completions WHERE team_id=$1 AND task_id=$2 AND user_id=$3`,
          [teamId, task.id, reactor]
        ).catch(() => {});
        const done = await dbCountCompletions(teamId, task.id);
        const total = task.total_count || await dbCountTargets(teamId, task.id);
        try { await dbUpdateBroadcastCounts(teamId, task.id, done, total); } catch (_) {}
        console.log(`[task-source-reaction] broadcast ${task.id}: ${reactor} 取消 (${done}/${total})`);

        if (task.status === 'done' && done < total) {
          const upd = await dbUpdateStatus(teamId, task.id, 'in_progress');
          if (upd) await syncTaskDoneReaction(client, upd);
        }
        return;
      }

      // personal
      if (task.status !== 'done') return;
      if (reactor !== task.assignee_id && reactor !== task.requester_user_id) return;
      const updated = await dbUpdateStatus(teamId, task.id, 'in_progress');
      if (!updated) return;
      console.log(`[task-source-reaction] personal ${task.id} → in_progress (✅ removed) by ${reactor}`);
    } catch (e) { console.warn('[task-source-reaction] remove listener fail:', e.message); }
  });
}

module.exports = { syncTaskDoneReaction, registerSourceDoneListener, DONE_REACTION };
