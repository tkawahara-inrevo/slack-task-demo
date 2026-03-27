function registerReactionFeature(deps) {
  const {
    __cacheKey,
    __cachePut,
    app,
    buildThreadCardBlocks,
    dbCreateTask,
    dbGetTaskBySource,
    dbGetThreadCard,
    getTeamIdFromBody,
    isReactionTaskifyEnabled = async () => true,
    noMention,
    openTaskCreateModal,
    prettifySlackText,
    prettifyUserMentions,
    randomUUID,
    resolveDeptForUser,
    safeEphemeral,
    safeJsonParse,
    slackDateYmd,
    publishHomeBurst = () => {},
    upsertThreadCard,
  } = deps;

const TASK_REACTION_NAME = "task";

// キーワードタスク化：全角・半角両対応
const TASK_KEYWORD_PATTERNS = [
  /＜タスク化＞/,
  /<タスク化>/,
];

// blocks から user_id を拾う（rich_text の user mention を拾う）
function extractUserIdsFromBlocks(blocks) {
  const out = [];
  const walk = (node) => {
    if (!node) return;
    if (Array.isArray(node)) return node.forEach(walk);
    if (typeof node !== "object") return;

    if (node.type === "user" && node.user_id) {
      if (!out.includes(node.user_id)) out.push(node.user_id);
    }
    for (const v of Object.values(node)) walk(v);
  };
  walk(blocks);
  return out;
}

// ★追加：blocks から usergroup_id を拾う（rich_text の usergroup）
function extractUserGroupIdsFromBlocks(blocks) {
  const out = [];
  const walk = (node) => {
    if (!node) return;
    if (Array.isArray(node)) return node.forEach(walk);
    if (typeof node !== "object") return;

    // Slackのrich_textで usergroup は type: "usergroup" が来ることがある
    if (node.type === "usergroup" && node.usergroup_id) {
      if (!out.includes(node.usergroup_id)) out.push(node.usergroup_id);
    }
    for (const v of Object.values(node)) walk(v);
  };
  walk(blocks);
  return out;
}

// ★追加：text から usergroup token を拾う（<!subteam^ID|@handle> / <!subteam^ID>）
function extractUserGroupIdsFromText(rawText) {
  const text = String(rawText || "");
  const out = [];
  const re = /<!subteam\^([A-Z0-9]+)(?:\|[^>]+)?>/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const gid = m[1];
    if (gid && !out.includes(gid)) out.push(gid);
  }
  return out;
}

function inferTargetsFromMessage(rawText, fallbackUserId, blocks = null) {
  const users = [];
  const groups = [];

  // ① blocks 優先（textにIDが出ない投稿を救う）
  for (const u of extractUserIdsFromBlocks(blocks)) users.push(u);
  for (const g of extractUserGroupIdsFromBlocks(blocks)) groups.push(g);

  // ② text の <@Uxxx> と <!subteam^...> を拾う（保険）
  {
    const text = String(rawText || "");
    const ure = /<@([A-Z0-9]+)(?:\|[^>]+)?>/g;
    let m;
    while ((m = ure.exec(text)) !== null) {
      const uid = m[1];
      if (uid && !users.includes(uid)) users.push(uid);
    }
  }
  for (const g of extractUserGroupIdsFromText(rawText)) {
    if (!groups.includes(g)) groups.push(g);
  }

  // ③ 何も無ければ fallback（リアクションした人）
  if (!users.length) users.push(fallbackUserId);

  return { userIds: users.filter(Boolean), groupIds: groups.filter(Boolean) };
}

function buildReactionPromptBlocks({
  previewText,
  assigneeId,
  dueYmd,
  payloadCreate,
  payloadEdit,
}) {
  const safePreview = noMention((previewText || "").trim()) || "（本文なし）";
  const short =
    safePreview.length > 300 ? safePreview.slice(0, 300) + "…" : safePreview;

  return [
    { type: "header", text: { type: "plain_text", text: "✅ タスク化の確認" } },
    {
      type: "section",
      text: { type: "mrkdwn", text: `*内容*\n>${short.replace(/\n/g, "\n>")}` },
    },
    {
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: `👤 *対応者*：<@${assigneeId}>　　📅 *期限*：${dueYmd}（今日）`,
        },
      ],
    },
    { type: "divider" },
    {
      type: "actions",
      elements: [
        {
          type: "button",
          text: { type: "plain_text", text: "タスク化" },
          style: "primary",
          action_id: "reaction_task_confirm_create",
          value: payloadCreate,
        },
        {
          type: "button",
          text: { type: "plain_text", text: "内容編集" },
          action_id: "reaction_task_open_edit_modal",
          value: payloadEdit,
        },
      ],
    },
  ];
}

app.event("reaction_added", async ({ event, client, body }) => {
  try {
    if ((event?.reaction || "") !== TASK_REACTION_NAME) return;

    const teamId = body?.team_id || body?.team?.id || event?.team;
    if (!(await isReactionTaskifyEnabled(teamId))) return;
    const channelId = event?.item?.channel;
    const msgTs = event?.item?.ts;
    const actorUserId = event?.user; // リアクションした人
    if (!teamId || !channelId || !msgTs || !actorUserId) return;

    // すでに「確認UI（スレッドカード）」を出していたら何もしない（1メッセージ1回）
    const existingCard = await dbGetThreadCard(teamId, channelId, msgTs);
    if (existingCard?.card_ts) return;

    // すでにタスク化済みなら案内だけ（ここは現行踏襲）
    const existingTask = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existingTask?.id) {
      await safeEphemeral(client, channelId, actorUserId, "✅ タスク化済み");
      return;
    }

    // 元メッセージ取得（本文＋発言者）
    // - thread返信でも安定して取れるように、reactions.get(full:true) を優先する
    let rawText = "";
    let requesterUserId = "";
    let mm = null;

    try {
      const rg = await client.reactions.get({
        channel: channelId,
        timestamp: msgTs,
        full: true,
      });
      mm = rg?.message || null;
      rawText = mm?.text || "";
      requesterUserId = mm?.user || "";
    } catch (e) {
      console.error("reaction_added reactions.get error:", e?.data || e);
    }

    // フォールバック（必要なら）
    if (!mm) {
      try {
        const hist = await client.conversations.history({
          channel: channelId,
          latest: msgTs,
          inclusive: true,
          limit: 1,
        });
        mm = (hist.messages || [])[0] || null;
        rawText = mm?.text || "";
        requesterUserId = mm?.user || "";
      } catch (e) {
        console.error(
          "reaction_added conversations.history error:",
          e?.data || e,
        );
      }
    }

    // ✅ 対応者推定（blocks優先 → text → fallback）
    const { userIds: initialUsers } = inferTargetsFromMessage(
      rawText,
      actorUserId,
      mm?.blocks || null,
    );

    // 代表1名（既存ロジック互換用）
    const assigneeId = initialUsers[0] || actorUserId;

    // 期限は今日固定
    const dueYmd = slackDateYmd(new Date());

    // スレッド親（そのスレッドに出す）
    const threadRootTs = mm?.thread_ts || mm?.ts || msgTs;

    const effectiveRequester = requesterUserId || actorUserId;

    // permalink取得・テキスト整形・dept解決を並列実行
    const [permalinkResult, prettyTextResult, requesterDept, assigneeDept] =
      await Promise.all([
        client.chat.getPermalink({ channel: channelId, message_ts: msgTs })
          .then(r => r?.permalink || "").catch(() => ""),
        (async () => {
          try {
            let t = await prettifySlackText(rawText, teamId);
            t = await prettifyUserMentions(t, teamId);
            return t;
          } catch (_) { return rawText; }
        })(),
        resolveDeptForUser(teamId, effectiveRequester),
        resolveDeptForUser(teamId, assigneeId),
      ]);

    const permalink = permalinkResult;
    const prettyText = prettyTextResult;
    const description = String(prettyText || rawText || "").trim();
    const title = description || "（本文なし）";

    const taskId = randomUUID();

    const created = await dbCreateTask({
      id: taskId,
      team_id: teamId,
      channel_id: channelId,
      message_ts: msgTs,
      source_permalink: permalink || null,
      title,
      description,
      requester_user_id: effectiveRequester,
      created_by_user_id: actorUserId,
      assignee_id: assigneeId,
      assignee_label: null,
      status: "in_progress",
      due_date: dueYmd,
      requester_dept: requesterDept,
      assignee_dept: assigneeDept,
      task_type: "personal",
      broadcast_group_handle: null,
      broadcast_group_id: null,
      total_count: null,
      completed_count: 0,
      notified_at: null,
    });

    // タスク化完了カードをスレッドに表示（「詳細を開く」ボタン付き）
    const doneBlocks = await buildThreadCardBlocks({ teamId, task: created });

    if (!String(channelId || "").startsWith("D")) {
      await upsertThreadCard(client, {
        teamId,
        channelId,
        parentTs: msgTs,
        threadTs: threadRootTs,
        blocks: doneBlocks,
      });
    }
    try {
      await publishHomeBurst(
        client,
        teamId,
        [effectiveRequester, actorUserId, assigneeId].filter(Boolean),
        200,
      );
    } catch (_) {}
  } catch (e) {
    if (e?.data?.error !== "not_in_channel")
      console.error("reaction_added error:", e?.data || e);
  }
});

// ================================
// キーワードタスク化: メッセージに ＜タスク化＞ or <タスク化> が含まれたら即タスク作成
// ================================
app.event("message", async ({ event, client, body }) => {
  try {
    // bot メッセージ・編集・削除は無視
    if (event?.subtype) return;

    const rawText = event?.text || "";
    const matched = TASK_KEYWORD_PATTERNS.some((re) => re.test(rawText));
    if (!matched) return;

    const teamId = body?.team_id || body?.team?.id || event?.team;
    if (!teamId) return;
    if (!(await isReactionTaskifyEnabled(teamId))) return;

    const channelId = event?.channel;
    const msgTs = event?.ts;
    const actorUserId = event?.user;
    if (!channelId || !msgTs || !actorUserId) return;

    // すでにタスク化済みなら無視
    const existingTask = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existingTask?.id) return;

    // キーワードを除去したテキストをタイトルにする
    let cleanText = rawText;
    for (const re of TASK_KEYWORD_PATTERNS) {
      cleanText = cleanText.replace(re, "");
    }
    cleanText = cleanText.trim();

    // 対応者推定（メンション先 or 投稿者自身）
    const { userIds: initialUsers } = inferTargetsFromMessage(
      rawText,
      actorUserId,
      event?.blocks || null,
    );
    const assigneeId = initialUsers[0] || actorUserId;

    const dueYmd = slackDateYmd(new Date());
    const threadRootTs = event?.thread_ts || msgTs;

    const [permalinkResult, prettyTextResult, requesterDept, assigneeDept] =
      await Promise.all([
        client.chat.getPermalink({ channel: channelId, message_ts: msgTs })
          .then((r) => r?.permalink || "").catch(() => ""),
        (async () => {
          try {
            let t = await prettifySlackText(cleanText, teamId);
            t = await prettifyUserMentions(t, teamId);
            return t;
          } catch (_) { return cleanText; }
        })(),
        resolveDeptForUser(teamId, actorUserId),
        resolveDeptForUser(teamId, assigneeId),
      ]);

    const description = String(prettyTextResult || cleanText || "").trim();
    const title = description || "（本文なし）";
    const taskId = randomUUID();

    const created = await dbCreateTask({
      id: taskId,
      team_id: teamId,
      channel_id: channelId,
      message_ts: msgTs,
      source_permalink: permalinkResult || null,
      title,
      description,
      requester_user_id: actorUserId,
      created_by_user_id: actorUserId,
      assignee_id: assigneeId,
      assignee_label: null,
      status: "in_progress",
      due_date: dueYmd,
      requester_dept: requesterDept,
      assignee_dept: assigneeDept,
      task_type: "personal",
      broadcast_group_handle: null,
      broadcast_group_id: null,
      total_count: null,
      completed_count: 0,
      notified_at: null,
    });

    // スレッドにタスク化完了カードを表示
    const doneBlocks = await buildThreadCardBlocks({ teamId, task: created });

    if (!String(channelId || "").startsWith("D")) {
      await upsertThreadCard(client, {
        teamId,
        channelId,
        parentTs: msgTs,
        threadTs: threadRootTs,
        blocks: doneBlocks,
      });
    } else {
      // DMの場合はスレッドに直接投稿
      await client.chat.postMessage({
        channel: channelId,
        thread_ts: threadRootTs,
        text: `✅ タスク化しました: ${noMention(title)}`,
        blocks: doneBlocks,
      });
    }

    try {
      await publishHomeBurst(
        client,
        teamId,
        [actorUserId, assigneeId].filter(Boolean),
        200,
      );
    } catch (_) {}
  } catch (e) {
    if (e?.data?.error !== "not_in_channel")
      console.error("keyword_taskify error:", e?.data || e);
  }
});

app.action("reaction_task_confirm_create", async ({ ack, body, client }) => {
  await ack();

  try {
    const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const teamId = payload.teamId || getTeamIdFromBody(body);
    const channelId = payload.channelId;
    const msgTs = payload.msgTs;
    const actorUserId = body.user?.id;

    const requesterUserId = payload.requesterUserId || actorUserId;
    const assigneeId = payload.assigneeId || actorUserId;
    const dueYmd = payload.dueYmd || slackDateYmd(new Date());
    const rawText = payload.messageText || "";

    if (!teamId || !channelId || !msgTs || !actorUserId) return;

    // すでにタスク化済みなら何もしない（痕跡は残ってる想定）
    const existing = await dbGetTaskBySource(teamId, channelId, msgTs);
    if (existing?.id) return;

    // permalink取得・テキスト整形・dept解決を並列実行
    const [permalinkResult, prettyTextResult, requesterDept, assigneeDept] =
      await Promise.all([
        client.chat.getPermalink({ channel: channelId, message_ts: msgTs })
          .then(r => r?.permalink || "").catch(() => ""),
        (async () => {
          let t = await prettifySlackText(rawText, teamId);
          t = await prettifyUserMentions(t, teamId);
          return t;
        })(),
        resolveDeptForUser(teamId, requesterUserId),
        resolveDeptForUser(teamId, assigneeId),
      ]);

    const permalink = permalinkResult;
    const prettyText = prettyTextResult;
    const description = String(prettyText || rawText || "").trim();
    const title = description || "（本文なし）";

    const taskId = randomUUID();

    // この導線は「personalタスクを即作成」だけに絞る（リアクション→確定ボタン）
    const taskType = "personal";
    const status = "in_progress"; // 初期は進行中で固定
    const due = dueYmd; // "YYYY-MM-DD"

    const created = await dbCreateTask({
      id: taskId,
      team_id: teamId,
      channel_id: channelId,
      message_ts: msgTs, // ← parentTs ではなく msgTs
      source_permalink: permalink || null,
      title,
      description,
      requester_user_id: requesterUserId,
      created_by_user_id: actorUserId,
      assignee_id: assigneeId, // ← personalAssigneeId ではなく assigneeId
      assignee_label: null,
      status,
      due_date: due,
      requester_dept: requesterDept,
      assignee_dept: assigneeDept,
      task_type: taskType,
      broadcast_group_handle: null,
      broadcast_group_id: null,
      total_count: null,
      completed_count: 0,
      notified_at: null,
    });

    // タスク詳細カードに差し替え（スレッドに出せるチャンネルだけ）
    const doneBlocks = await buildThreadCardBlocks({ teamId, task: created });

    // DM（Dxxxx）は thread card を作らない（仕様）
    if (!String(channelId || "").startsWith("D")) {
      await upsertThreadCard(client, {
        teamId,
        channelId,
        parentTs: msgTs, // 一意キー（リアクション対象）
        threadTs: payload.threadTs || msgTs, // 投稿先スレッド親（threadRootTs）
        blocks: doneBlocks,
      });
    }
  } catch (e) {
    console.error("reaction_task_confirm_create error:", e?.data || e);
  }
});

app.action("reaction_task_open_edit_modal", async ({ ack, body, client }) => {
  await ack();

  try {
    const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const teamId = payload.teamId || getTeamIdFromBody(body);
    const channelId = payload.channelId;
    const msgTs = payload.msgTs;
    const actorUserId = body.user?.id;

    if (!teamId || !channelId || !msgTs || !actorUserId) return;

    const rawText = payload.messageText || "";

    let prettyText = await prettifySlackText(rawText, teamId);
    prettyText = await prettifyUserMentions(prettyText, teamId);

    const cacheKey = __cacheKey(teamId, channelId, msgTs);
    __cachePut(cacheKey, prettyText || "");

    await openTaskCreateModal(client, {
      trigger_id: body.trigger_id,
      teamId,
      channelId,
      msgTs,
      actorUserId,
      includeContentInput: false,
    });
  } catch (e) {
    console.error("reaction_task_open_edit_modal error:", e?.data || e);
  }
});

  return {
    extractUserGroupIdsFromBlocks,
    extractUserIdsFromBlocks,
    inferTargetsFromMessage,
  };
}

module.exports = {
  registerReactionFeature,
};
