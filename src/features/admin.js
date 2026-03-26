function registerAdminFeature(deps) {
  const {
    app,
    canManageTeamSettings,
    dbHasUserCompleted,
    dbQuery,
    formatDueDateOnly,
    getTeamIdFromBody,
    getTeamSettings,
    getUserDisplayName,
    getUserIdFromBody,
    openDetailModal,
    openTeamSettingsModal,
    safeJsonParse,
    statusLabel,
  } = deps;

  const ADMIN_COMMAND = process.env.ADMIN_COMMAND || "/task-admin";
  const MAX_ROWS_PER_SECTION = 8;

  function normalizeScope(value) {
    return ["active", "done", "all"].includes(value) ? value : "active";
  }

  function summarizeTeamSettings(settings = {}) {
    const homeRangeMap = {
      to_me: "自分に関係あるもの",
      requested_by_me: "自分が依頼したもの",
      all: "すべて",
    };
    const homeStateMap = {
      active: "進行中",
      done: "完了済み",
    };
    const displayModeMap = {
      standard: "標準表示",
      compact: "コンパクト表示",
    };
    const boolText = (value) => (value ? "有効" : "無効");

    return [
      `Home表示: ${displayModeMap[settings.defaultHomeDisplayMode] || "標準表示"}`,
      `既定範囲: ${homeRangeMap[settings.defaultHomeRange] || "自分に関係あるもの"}`,
      `既定ステータス: ${homeStateMap[settings.defaultHomeState] || "進行中"}`,
      `リアクション起点: ${boolText(settings.reactionTaskifyEnabled)}`,
      `期限DM: ${boolText(settings.dueDmNotificationsEnabled)}`,
      `完了DM: ${boolText(settings.completionDmNotificationsEnabled)}`,
      `コメントDM: ${boolText(settings.commentDmNotificationsEnabled)}`,
      `期限切れチャンネル通知: ${boolText(settings.overdueChannelNotificationsEnabled)}`,
    ].join("\n");
  }

  async function listRequestedTasks(teamId, userId, scopeKey) {
    const params = [teamId, userId];
    let whereScope = "";
    if (scopeKey === "active") {
      whereScope = "AND t.status NOT IN ('done', 'cancelled')";
    } else if (scopeKey === "done") {
      whereScope = "AND t.status = 'done'";
    }

    const res = await dbQuery(
      `
        SELECT t.*
        FROM tasks t
        WHERE t.team_id = $1
          AND t.requester_user_id = $2
          ${whereScope}
        ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
        LIMIT ${MAX_ROWS_PER_SECTION};
      `,
      params,
    );
    return res.rows || [];
  }

  async function listPersonalAssignedTasks(teamId, userId, scopeKey) {
    const params = [teamId, userId];
    let whereScope = "";
    if (scopeKey === "active") {
      whereScope = "AND t.status NOT IN ('done', 'cancelled')";
    } else if (scopeKey === "done") {
      whereScope = "AND t.status = 'done'";
    }

    const res = await dbQuery(
      `
        SELECT t.*
        FROM tasks t
        WHERE t.team_id = $1
          AND (t.task_type IS NULL OR t.task_type = 'personal')
          AND t.assignee_id = $2
          ${whereScope}
        ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
        LIMIT ${MAX_ROWS_PER_SECTION};
      `,
      params,
    );
    return res.rows || [];
  }

  async function listBroadcastTargetTasks(teamId, userId, scopeKey) {
    const params = [teamId, userId];
    let whereScope = "";
    if (scopeKey === "active") {
      whereScope = "AND t.status NOT IN ('done', 'cancelled')";
    } else if (scopeKey === "done") {
      whereScope = "AND t.status = 'done'";
    }

    const res = await dbQuery(
      `
        SELECT DISTINCT ON (t.id) t.*
        FROM tasks t
        JOIN task_targets tt
          ON tt.team_id = t.team_id
         AND tt.task_id::text = t.id
        WHERE t.team_id = $1
          AND t.task_type = 'broadcast'
          AND tt.user_id = $2
          ${whereScope}
        ORDER BY t.id, (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC;
      `,
      params,
    );
    return (res.rows || []).slice(0, MAX_ROWS_PER_SECTION);
  }

  function buildTaskSummary(task, extraLine = null) {
    const due = formatDueDateOnly(task.due_date);
    const typeLabel = task.task_type === "broadcast" ? "broadcast" : "personal";
    const lines = [
      `*${task.title || "（タイトルなし）"}*`,
      `ID: \`${task.id}\``,
      `種別: ${typeLabel} / ステータス: ${statusLabel(task.status)}`,
      `期限: ${due}`,
    ];
    if (extraLine) lines.push(extraLine);
    return lines.join("\n");
  }

  async function buildTaskSection({
    teamId,
    title,
    tasks,
    selectedUserId,
    includeBroadcastCompletion = false,
  }) {
    const blocks = [
      { type: "divider" },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*${title}* (${tasks.length}件表示)`,
        },
      },
    ];

    if (!tasks.length) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "該当タスクはありません。" }],
      });
      return blocks;
    }

    for (const task of tasks) {
      let extraLine = null;
      if (includeBroadcastCompletion) {
        const completed = await dbHasUserCompleted(teamId, task.id, selectedUserId);
        extraLine = completed ? "本人完了: 済み" : "本人完了: 未完了";
      }
      blocks.push({
        type: "section",
        text: { type: "mrkdwn", text: buildTaskSummary(task, extraLine) },
        accessory: {
          type: "button",
          action_id: "admin_open_task_detail",
          text: { type: "plain_text", text: "詳細" },
          value: JSON.stringify({ teamId, taskId: task.id }),
        },
      });
    }

    return blocks;
  }

  async function buildAdminModalView({
    teamId,
    viewerUserId,
    selectedUserId = "",
    scopeKey = "active",
  }) {
    const normalizedScope = normalizeScope(scopeKey);
    const teamSettings = await getTeamSettings(teamId);

    const blocks = [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: "*管理者調査*\nユーザーを選ぶと、発行タスク・自分あて personal・対象 broadcast をまとめて確認できます。",
        },
      },
      {
        type: "input",
        block_id: "admin_target_user",
        label: { type: "plain_text", text: "対象ユーザー" },
        element: {
          type: "users_select",
          action_id: "admin_user_select",
          placeholder: { type: "plain_text", text: "ユーザーを選択" },
          ...(selectedUserId ? { initial_user: selectedUserId } : {}),
        },
      },
      {
        type: "input",
        block_id: "admin_scope",
        label: { type: "plain_text", text: "表示ステータス" },
        element: {
          type: "static_select",
          action_id: "admin_scope_select",
          options: [
            {
              text: { type: "plain_text", text: "進行中" },
              value: "active",
            },
            {
              text: { type: "plain_text", text: "完了済み" },
              value: "done",
            },
            {
              text: { type: "plain_text", text: "すべて" },
              value: "all",
            },
          ],
          initial_option: {
            text: {
              type: "plain_text",
              text:
                normalizedScope === "done"
                  ? "完了済み"
                  : normalizedScope === "all"
                    ? "すべて"
                    : "進行中",
            },
            value: normalizedScope,
          },
        },
      },
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*チーム設定*\n${summarizeTeamSettings(teamSettings)}`,
        },
        accessory: {
          type: "button",
          action_id: "admin_open_team_settings",
          text: { type: "plain_text", text: "チーム設定を開く" },
          value: JSON.stringify({ teamId, viewerUserId }),
        },
      },
    ];

    if (!selectedUserId) {
      blocks.push({ type: "divider" });
      blocks.push({
        type: "context",
        elements: [
          {
            type: "mrkdwn",
            text: "まず対象ユーザーを選択してください。",
          },
        ],
      });
    } else {
      const [selectedUserName, requested, assignedPersonal, assignedBroadcast] =
        await Promise.all([
          getUserDisplayName(teamId, selectedUserId),
          listRequestedTasks(teamId, selectedUserId, normalizedScope),
          listPersonalAssignedTasks(teamId, selectedUserId, normalizedScope),
          listBroadcastTargetTasks(teamId, selectedUserId, normalizedScope),
        ]);

      blocks.push({
        type: "section",
        text: {
          type: "mrkdwn",
          text: `*調査対象*: <@${selectedUserId}> (${selectedUserName})`,
        },
      });

      blocks.push(
        ...(await buildTaskSection({
          teamId,
          title: "発行したタスク",
          tasks: requested,
          selectedUserId,
        })),
      );
      blocks.push(
        ...(await buildTaskSection({
          teamId,
          title: "自分あて personal",
          tasks: assignedPersonal,
          selectedUserId,
        })),
      );
      blocks.push(
        ...(await buildTaskSection({
          teamId,
          title: "対象 broadcast",
          tasks: assignedBroadcast,
          selectedUserId,
          includeBroadcastCompletion: true,
        })),
      );
    }

    return {
      type: "modal",
      callback_id: "admin_modal",
      private_metadata: JSON.stringify({
        teamId,
        viewerUserId,
        selectedUserId,
        scopeKey: normalizedScope,
      }),
      title: { type: "plain_text", text: "管理" },
      close: { type: "plain_text", text: "閉じる" },
      blocks,
    };
  }

  async function refreshAdminModal({ client, body, view, patch = {} }) {
    const meta = safeJsonParse(view?.private_metadata || "{}") || {};
    const teamId = patch.teamId || meta.teamId || getTeamIdFromBody(body);
    const viewerUserId =
      patch.viewerUserId || meta.viewerUserId || getUserIdFromBody(body);
    const selectedUserId = Object.prototype.hasOwnProperty.call(
      patch,
      "selectedUserId",
    )
      ? patch.selectedUserId
      : meta.selectedUserId || "";
    const scopeKey = patch.scopeKey || meta.scopeKey || "active";

    const nextView = await buildAdminModalView({
      teamId,
      viewerUserId,
      selectedUserId,
      scopeKey,
    });
    await client.views.update({
      view_id: view.id,
      hash: view.hash,
      view: nextView,
    });
  }

  app.command(ADMIN_COMMAND, async ({ ack, body, client }) => {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    if (!teamId || !userId) {
      await ack();
      return;
    }

    const allowed = await canManageTeamSettings(client, userId);
    if (!allowed) {
      await ack({
        response_type: "ephemeral",
        text: "管理モーダルはワークスペース管理者のみ利用できます。",
      });
      return;
    }

    await ack();
    await client.views.open({
      trigger_id: body.trigger_id,
      view: await buildAdminModalView({
        teamId,
        viewerUserId: userId,
      }),
    });
  });

  app.action("admin_user_select", async ({ ack, body, client }) => {
    await ack();
    try {
      const selectedUserId = body.actions?.[0]?.selected_user || "";
      await refreshAdminModal({
        client,
        body,
        view: body.view,
        patch: { selectedUserId },
      });
    } catch (e) {
      console.error("admin_user_select error:", e?.data || e);
    }
  });

  app.action("admin_scope_select", async ({ ack, body, client }) => {
    await ack();
    try {
      const scopeKey = body.actions?.[0]?.selected_option?.value || "active";
      await refreshAdminModal({
        client,
        body,
        view: body.view,
        patch: { scopeKey },
      });
    } catch (e) {
      console.error("admin_scope_select error:", e?.data || e);
    }
  });

  app.action("admin_open_task_detail", async ({ ack, body, client }) => {
    await ack();
    try {
      const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
      const teamId = payload.teamId || getTeamIdFromBody(body);
      const taskId = payload.taskId;
      const viewerUserId = getUserIdFromBody(body);
      if (!teamId || !taskId || !viewerUserId) return;

      await openDetailModal(client, {
        trigger_id: body.trigger_id,
        teamId,
        taskId,
        viewerUserId,
        origin: "admin",
      });
    } catch (e) {
      console.error("admin_open_task_detail error:", e?.data || e);
    }
  });

  app.action("admin_open_team_settings", async ({ ack, body, client }) => {
    await ack();
    try {
      const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
      const teamId = payload.teamId || getTeamIdFromBody(body);
      const userId = payload.viewerUserId || getUserIdFromBody(body);
      if (!teamId || !userId) return;
      await openTeamSettingsModal({
        client,
        triggerId: body.trigger_id,
        teamId,
        userId,
      });
    } catch (e) {
      console.error("admin_open_team_settings error:", e?.data || e);
    }
  });

  app.view("admin_modal", async ({ ack }) => {
    await ack();
  });

  return {
    ADMIN_COMMAND,
    buildAdminModalView,
  };
}

module.exports = {
  registerAdminFeature,
};
