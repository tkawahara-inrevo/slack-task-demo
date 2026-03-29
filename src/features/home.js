function registerHomeFeature(deps) {
  const {
    app,
    assigneeDisplay,
    buildTaskListModalView,
    canUserSeeChannel,
    dbHasUserCompleted,
    dbGetUserCompletedTaskIds = async () => new Set(),
    dbListBroadcastTasksByStatuses,
    dbListBroadcastTasksByStatusesWithScope,
    dbListPersonalTasksByStatusesWithScope,
    formatDueDateOnly,
    getSubteamIdMap,
    getTeamIdFromBody,
    getUserDisplayName,
    getUserIconUrl,
    getUserIdFromBody,
    getUsergroupMembers,
    resolveHomeDefaults = async () => ({
      displayMode: "standard",
      rangeKey: "to_me",
      scopeKey: "active",
    }),
    handleCompleteTask,
    noMention,
    openDetailModal,
    openTaskCreateModal,
    safeJsonParse,
    searchUsergroups,
    slackDateYmd,
    toAtShortName,
    todayJstYmd,
    openUserSettingsModal,
    dbQuery = async () => ({ rows: [] }),
    dbListDashTeamMembers = async () => [],
    getUserSettings = async () => ({}),
    dbCreatePersonalFilter = async () => {},
    dbListPersonalFilters = async () => [],
    dbDeletePersonalFilter = async () => {},
    dbSetPersonalFilterMembers = async () => {},
    dbGetPersonalFilterMemberIds = async () => [],
    randomUUID = () => require("crypto").randomUUID(),
  } = deps;

  async function isBroadcastAssignedToUser(task, teamId, userId) {
    if (!task || task.task_type !== "broadcast" || !userId) return false;

    if (String(task.assignee_label || "").includes(`<@${userId}>`)) {
      return true;
    }

    if (task.broadcast_group_id) {
      try {
        const members = await getUsergroupMembers(teamId, task.broadcast_group_id);
        if ((members || []).includes(userId)) return true;
      } catch (_) {}
    }

    return false;
  }

async function publishHomeForUsers(client, teamId, userIds, intervalMs = 200) {
  const uniq = Array.from(new Set((userIds || []).filter(Boolean)));
  // 1人目は即座に await で確実に反映
  if (uniq.length > 0) {
    await publishHome({ client, teamId, userId: uniq[0] }).catch(() => {});
  }
  // 2人目以降は間隔を空けて
  for (let i = 1; i < uniq.length; i++) {
    const u = uniq[i];
    setTimeout(() => {
      publishHome({ client, teamId, userId: u }).catch(() => {});
    }, i * intervalMs);
  }
}

const homeState = new Map();
const hydratedHomeState = new Set();

// 未完了（= 進行中）
const ACTIVE_STATUSES = ["in_progress"];
const DONE_STATUSES = ["done"];

function getHomeState(teamId, userId) {
  const k = `${teamId}:${userId}`;
  const s = homeState.get(k) || {
    viewKey: "all",
    scopeKey: "active",
    personalScopeKey: "to_me",
    assigneeUserId: userId,
    deptKey: "all",
    broadcastScopeKey: "to_me",
    displayMode: "standard", // "standard" | "compact"
    homeMore: { overdue: false, today: false },
    homeFold: { overdue: false, today: false, later: false },
  };

  // 後方互換：昔のstateに homeMore が無い場合に備える
  const homeMore = {
    overdue: !!s?.homeMore?.overdue,
    today: !!s?.homeMore?.today,
  };

  // 後方互換：昔のstateに homeFold が無い場合に備える
  const homeFold = {
    overdue: !!s?.homeFold?.overdue,
    today: !!s?.homeFold?.today,
    later: !!s?.homeFold?.later,
  };

  // ★表示は常に「すべて」に固定（personal/broadcastの切替を使わない）
  // ★範囲は broadcastScopeKey を共通キーとして使う
  return {
    ...s,
    viewKey: "all",
    broadcastScopeKey: s.broadcastScopeKey || "to_me",
    personalScopeKey: s.broadcastScopeKey || s.personalScopeKey || "to_me",
    homeMore,
    homeFold,
  };
}

function setHomeState(teamId, userId, next) {
  const k = `${teamId}:${userId}`;

  // ★viewKey は固定、範囲は broadcastScopeKey に統一
  const prev = getHomeState(teamId, userId);

  const merged = {
    ...prev,
    ...next,
    viewKey: "all",
  };

  // homeMore はネストなので shallow merge だと壊れる → 明示的にmerge
  merged.homeMore = {
    ...(prev.homeMore || { overdue: false, today: false }),
    ...(next.homeMore || {}),
  };

  // homeFold もネストなので明示的にmerge
  merged.homeFold = {
    ...(prev.homeFold || { overdue: false, today: false, later: false }),
    ...(next.homeFold || {}),
  };

  if (merged.broadcastScopeKey) {
    merged.personalScopeKey = merged.broadcastScopeKey;
  }

  homeState.set(k, merged);
  hydratedHomeState.add(k);
}

async function ensureHomeStateLoaded(teamId, userId) {
  const k = `${teamId}:${userId}`;
  if (hydratedHomeState.has(k)) return;

  const defaults = await resolveHomeDefaults(teamId, userId);
  setHomeState(teamId, userId, {
    displayMode: defaults.displayMode,
    broadcastScopeKey: defaults.rangeKey,
    personalScopeKey: defaults.rangeKey,
    scopeKey: defaults.scopeKey,
  });
}

function stripMentions(text) {
  return text
    .replace(/<@[^>]+>/g, " ")
    .replace(/<!subteam\^[^>]+>/g, " ")
    .replace(/<!channel>/g, " ")
    .replace(/<!here>/g, " ")
    .replace(/<!everyone>/g, " ")
    // @ハンドル（英数字）
    .replace(/[@＠][\w][\w.-]*/g, " ")
    // @日本語名（+姓）(+/英名）— 例: @土井 燎/Kagari Doi
    .replace(/[@＠][^\x00-\x7F]+(?:\s+[^\x00-\x7F]+)*(?:\s*\/\s*[A-Za-z\s.]+)?/g, " ")
    .replace(/https?:\/\/\S+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function taskLineForHome(task) {
  const rawTitle = String(task.title || "").trim();
  const rawDesc = String(task.description || "").trim();

  // タイトル・本文どちらもメンションを除去して表示
  let preview = stripMentions(rawTitle) || stripMentions(rawDesc) || "（本文なし）";

  // @表記を抑止（通知防止）
  preview = preview.replace(/@/g, "＠");

  // ★ 最大5行（読み取りのための split のみ。文字列自体は置換しない）
  const MAX_LINES = 5;
  const lines = preview.split(/\r?\n/);
  if (lines.length > MAX_LINES) {
    preview = lines.slice(0, MAX_LINES).join("\n") + "\n…";
  }

  // ★ 最大200文字程度
  const MAX_PREVIEW_CHARS = 200;
  if (preview.length > MAX_PREVIEW_CHARS) {
    preview = preview.slice(0, MAX_PREVIEW_CHARS) + "…";
  }

  if (!preview) preview = "（本文なし）";
  return preview;
}

function buildHomeFiltersModalView({ teamId, userId, st, deptText, groups = [], personalFilters = [] }) {
  const rangeKey = st.broadcastScopeKey || "to_me";
  const scopeKey = st.scopeKey || "active";
  const deptKey = st.deptKey || "all";
  const deptOptions = [
    { text: { type: "plain_text", text: "すべて" }, value: "all" },
    { text: { type: "plain_text", text: "未設定" }, value: "__none__" },
    ...groups.map((g) => ({ text: { type: "plain_text", text: `@${g.handle}` }, value: g.id })),
    ...personalFilters.map((f) => ({ text: { type: "plain_text", text: `★ ${f.name}` }, value: `pf:${f.id}` })),
  ];

  const rangeOptions = [
    { text: { type: "plain_text", text: "範囲：自分あて" }, value: "to_me" },
    {
      text: { type: "plain_text", text: "範囲：自分が発行" },
      value: "requested_by_me",
    },
    { text: { type: "plain_text", text: "範囲：すべて" }, value: "all" },
  ];
  const stateOptions = [
    { text: { type: "plain_text", text: "状態：未完了" }, value: "active" },
    { text: { type: "plain_text", text: "状態：完了" }, value: "done" },
  ];

  return {
    type: "modal",
    callback_id: "home_filters_modal",
    private_metadata: JSON.stringify({ teamId, userId }),
    title: { type: "plain_text", text: "絞り込み" },
    submit: { type: "plain_text", text: "適用" },
    close: { type: "plain_text", text: "閉じる" },
    blocks: [
      {
        type: "input",
        block_id: "range",
        label: { type: "plain_text", text: "範囲" },
        element: {
          type: "static_select",
          action_id: "home_filters_range",
          options: rangeOptions,
          initial_option:
            rangeOptions.find((o) => o.value === rangeKey) || rangeOptions[0],
        },
      },
      {
        type: "input",
        block_id: "state",
        label: { type: "plain_text", text: "状態" },
        element: {
          type: "static_select",
          action_id: "home_filters_state",
          options: stateOptions,
          initial_option:
            stateOptions.find((o) => o.value === scopeKey) || stateOptions[0],
        },
      },
      {
        type: "input",
        block_id: "dept",
        optional: true,
        label: {
          type: "plain_text",
          text: "部署（範囲=すべて のときのみ有効）",
        },
        element: {
          type: "static_select",
          action_id: "home_dept_select",
          placeholder: { type: "plain_text", text: "部署（@グループ）を選択" },
          options: deptOptions,
          initial_option: deptOptions.find((o) => o.value === deptKey) || deptOptions[0],
        },
      },
    ],
  };
}

app.action("open_home_filters_modal", async ({ ack, body, client }) => {
  await ack();

  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  await ensureHomeStateLoaded(teamId, userId);
  const st = getHomeState(teamId, userId);

  const [groups, personalFilters] = await Promise.all([
    searchUsergroups(""),
    dbListPersonalFilters(teamId, userId).catch(() => []),
  ]);

  await client.views.open({
    trigger_id: body.trigger_id,
    view: buildHomeFiltersModalView({ teamId, userId, st, groups, personalFilters }),
  });
});

app.view("home_filters_modal", async ({ ack, body, view, client }) => {
  await ack();

  const meta = safeJsonParse(view.private_metadata) || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const userId = meta.userId || getUserIdFromBody(body);
  if (!teamId || !userId) return;

  await ensureHomeStateLoaded(teamId, userId);
  const range =
    view?.state?.values?.range?.home_filters_range?.selected_option?.value ||
    "to_me";
  const scope =
    view?.state?.values?.state?.home_filters_state?.selected_option?.value ||
    "active";

  const deptOpt =
    view?.state?.values?.dept?.home_dept_select?.selected_option || null;
  const dept = deptOpt?.value || "all";

  // ✅ 部署は「範囲=すべて」のときだけ反映（それ以外は保持しても使わない）
  setHomeState(teamId, userId, {
    broadcastScopeKey: range,
    scopeKey: scope,
    ...(range === "all" ? { deptKey: dept } : {}),
  });

  await publishHome({ client, teamId, userId });
});

async function publishHome({ client, teamId, userId }) {
  await ensureHomeStateLoaded(teamId, userId);
  const st = getHomeState(teamId, userId);
  const statuses = st.scopeKey === "done" ? DONE_STATUSES : ACTIVE_STATUSES;

  const blocks = [];

  // ✅ フィルタは Home 上のプルダウンで直接変更する（モーダル遷移を挟まない）
  const rangeKey0 = st.broadcastScopeKey || "to_me";
  const stateKey0 = st.scopeKey || "active";
  const deptKey0 = st.deptKey || "all";

  const rangeOptions0 = [
    { text: { type: "plain_text", text: "範囲：自分あて" }, value: "to_me" },
    { text: { type: "plain_text", text: "範囲：自分が発行" }, value: "requested_by_me" },
    { text: { type: "plain_text", text: "範囲：すべて" }, value: "all" },
  ];
  const stateOptions0 = [
    { text: { type: "plain_text", text: "状態：未完了" }, value: "active" },
    { text: { type: "plain_text", text: "状態：完了" }, value: "done" },
  ];

  // 範囲=すべての時だけ「部署」フィルタを出す
  const [deptGroups, personalFilters] = await Promise.all([
    searchUsergroups(""),
    dbListPersonalFilters(teamId, userId).catch(() => []),
  ]);
  const deptOptions = [
    { text: { type: "plain_text", text: "部署：すべて" }, value: "all" },
    { text: { type: "plain_text", text: "部署：未設定" }, value: "__none__" },
    ...deptGroups.map((g) => ({ text: { type: "plain_text", text: `@${g.handle}` }, value: g.id })),
    ...personalFilters.map((f) => ({ text: { type: "plain_text", text: `★ ${f.name}` }, value: `pf:${f.id}` })),
  ];

  const actionElements = [
    {
      type: "static_select",
      action_id: "home_broadcast_scope_select",
      options: rangeOptions0,
      initial_option:
        rangeOptions0.find((o) => o.value === rangeKey0) || rangeOptions0[0],
    },
  ];

  if (rangeKey0 === "all") {
    actionElements.push({
      type: "static_select",
      action_id: "home_dept_select",
      options: deptOptions,
      initial_option: deptOptions.find((o) => o.value === deptKey0) || deptOptions[0],
    });
  }

  actionElements.push({
    type: "static_select",
    action_id: "home_scope_select",
    options: stateOptions0,
    initial_option:
      stateOptions0.find((o) => o.value === stateKey0) || stateOptions0[0],
  });

  actionElements.push({
    type: "button",
    action_id: "open_home_task_create_modal",
    text: { type: "plain_text", text: "＋ タスク作成" },
    style: "primary",
    value: JSON.stringify({ teamId, userId }),
  });

  actionElements.push({
    type: "button",
    action_id: "open_user_settings_from_home",
    text: { type: "plain_text", text: "設定" },
    value: JSON.stringify({ teamId, userId }),
  });

  blocks.push({ type: "actions", elements: actionElements });

  blocks.push({ type: "divider" });

  // データ取得
  let tasks = [];

  // 混在ソート（due_date昇順 → created_at降順、due無しは最後）
  const toTime = (d) => {
    if (!d) return null;
    const dt = d instanceof Date ? d : new Date(d);
    return Number.isNaN(dt.getTime()) ? null : dt.getTime();
  };

  const cmp = (a, b) => {
    const at = toTime(a.due_date);
    const bt = toTime(b.due_date);
    if (at === null && bt !== null) return 1;
    if (at !== null && bt === null) return -1;
    if (at !== null && bt !== null && at !== bt) return at - bt;

    const ac = toTime(a.created_at);
    const bc = toTime(b.created_at);
    if (ac !== null && bc !== null && ac !== bc) return bc - ac;

    return String(b.id || "").localeCompare(String(a.id || ""));
  };

  // ★新：表示は常に「すべて」（personal + broadcast 混在）
  const rangeKey = st.broadcastScopeKey || "to_me";
  const deptKey = st.deptKey || "all";

  // personal は範囲で絞る（to_me / requested_by_me / all）
  const personalScope =
    rangeKey === "to_me" || rangeKey === "requested_by_me" ? rangeKey : "all";
  const listFetchLimit = rangeKey === "requested_by_me" ? 200 : 60;
  let personalTasks = await dbListPersonalTasksByStatusesWithScope(
    teamId,
    statuses,
    personalScope,
    userId,
    listFetchLimit,
  );

  // ✅ 方針：依頼者=対応者 の personal タスクは「範囲=すべて」では出さない
  // - to_me / requested_by_me では今まで通り見える（自分の整理には必要）
  if (rangeKey === "all") {
    personalTasks = (personalTasks || []).filter((t) => {
      const r = t?.requester_user_id;
      const a = t?.assignee_id;
      return !(r && a && r === a);
    });
  }

  // broadcast は範囲で絞る（to_me は JOIN、requested_by_me は requester、all は JOINなし）
  let broadcastTasks =
    rangeKey === "to_me" || rangeKey === "requested_by_me"
      ? await dbListBroadcastTasksByStatusesWithScope(
          teamId,
          statuses,
          rangeKey,
          userId,
          listFetchLimit,
        )
      : await dbListBroadcastTasksByStatuses(
          teamId,
          statuses,
          "all",
          listFetchLimit,
        );

  if (rangeKey === "to_me") {
    const fallbackBroadcastTasks = await dbListBroadcastTasksByStatuses(
      teamId,
      statuses,
      "all",
      Math.max(120, listFetchLimit * 2),
    );
    const existingIds = new Set((broadcastTasks || []).map((t) => String(t.id)));

    const candidates = (fallbackBroadcastTasks || []).filter(
      (t) => !existingIds.has(String(t.id)),
    );
    if (candidates.length > 0) {
      const checks = await Promise.all(
        candidates.map((task) =>
          Promise.all([
            isBroadcastAssignedToUser(task, teamId, userId),
            dbHasUserCompleted(teamId, task.id, userId),
          ]).then(([isAssigned, alreadyCompleted]) => ({
            task,
            isAssigned,
            alreadyCompleted,
          })),
        ),
      );
      for (const { task, isAssigned, alreadyCompleted } of checks) {
        const shouldInclude =
          isAssigned &&
          (st.scopeKey === "done"
            ? task.status === "done" || alreadyCompleted
            : !alreadyCompleted);
        if (shouldInclude) {
          broadcastTasks.push(task);
          existingIds.add(String(task.id));
        }
      }
    }
  }

  // ★範囲=すべて かつ 部署指定 のときだけ「@mkに関わる全て」に絞る（JS側）
  if (rangeKey === "all" && deptKey && deptKey !== "all") {
    const members = deptKey.startsWith("pf:")
      ? await dbGetPersonalFilterMemberIds(teamId, deptKey.slice(3))
      : await getUsergroupMembers(teamId, deptKey);
    const memberSet = new Set((members || []).filter(Boolean));

    // personal: 担当者 or 依頼者 が部署メンバーに含まれるもの
    personalTasks = (personalTasks || []).filter((t) => {
      const a = t?.assignee_id;
      const r = t?.requester_user_id;
      return (a && memberSet.has(a)) || (r && memberSet.has(r));
    });

    // broadcast: (対象ユーザーに部署メンバーが含まれる) OR (依頼者が部署メンバー) OR (対象グループが一致)
    broadcastTasks = (broadcastTasks || []).filter((t) => {
      const r = t?.requester_user_id;
      if (r && memberSet.has(r)) return true;

      const gid = t?.broadcast_group_id;
      if (gid && String(gid) === String(deptKey)) return true;

      return false;
    });
  }

  const merged = [...personalTasks, ...broadcastTasks].sort(cmp);

  // ★保険：同一IDは必ず1つにする（重複完全排除）
  const seen = new Set();
  tasks = [];
  for (const t of merged) {
    const key = `${t.task_type || "personal"}:${t.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    tasks.push(t);
  }

  // public は参加していなくても表示する / private は表示しない
  // DM（Dxxxx）は基本表示しないが、「範囲=自分あて(to_me)」かつ personal で自分担当のものだけ表示する
  {
    const requesterVisibleIds = new Set();
    if (rangeKey === "requested_by_me") {
      for (const task of tasks || []) {
        if (!task) continue;
        if (String(task.requester_user_id || "") === String(userId)) {
          requesterVisibleIds.add(String(task.id));
        }
      }
    }

    const uniqChannels = Array.from(
      new Set((tasks || []).map((t) => t.channel_id).filter(Boolean)),
    );
    const okMap = new Map(
      await Promise.all(
        uniqChannels.map(async (ch) => {
          const ok = await canUserSeeChannel({
            client,
            teamId,
            channelId: ch,
            userId,
          });
          return [ch, ok];
        }),
      ),
    );

    tasks = (tasks || []).filter((t) => {
      const ch = String(t.channel_id || "");

      // チャンネル情報が無いものは落とさない
      if (!ch) return true;

      // ✅ DM起点タスクは Home の範囲絞り込み結果をそのまま通す
      //    （ここでは追加の可視性制限をかけない）
      if (ch.startsWith("D")) {
        return true;
      }

      if (
        rangeKey === "requested_by_me" &&
        requesterVisibleIds.has(String(t.id))
      ) {
        return true;
      }

      // 通常ルール：publicのみOK（private/その他はNG）
      return okMap.get(ch) === true;
    });
  }

  // ✅ ダッシュボード連携フィルター（個人設定で指定されたプロジェクトorチームで絞り込み）
  {
    const userSettings = await getUserSettings(teamId, userId);
    const projectFilterId = userSettings.homeProjectFilter || "";
    const teamFilterId = userSettings.homeDashTeamFilter || "";

    if (projectFilterId) {
      // プロジェクトに紐づくタスクIDだけ表示
      const ptRes = await dbQuery(
        `SELECT task_id FROM project_tasks WHERE team_id=$1 AND project_id=$2`,
        [teamId, projectFilterId],
      );
      const projectTaskIds = new Set(ptRes.rows.map((r) => String(r.task_id)));
      tasks = tasks.filter((t) => projectTaskIds.has(String(t.id)));
    } else if (teamFilterId) {
      // チームメンバーのタスクだけ表示
      const members = await dbListDashTeamMembers(teamId, teamFilterId);
      const memberIds = new Set(members.map((m) => m.user_id));
      tasks = tasks.filter((t) =>
        memberIds.has(t.assignee_id) || memberIds.has(t.requester_user_id),
      );
    }
  }

  // 表示：未完了はステータス別に分ける（完了/取り下げはまとめ）
  if (rangeKey === "to_me") {
    const hiddenAssignedTasks = [];
    const visibleKeys = new Set(
      (tasks || []).map((t) => `${t.task_type || "personal"}:${t.id}`),
    );
    const allCandidates = [...personalTasks, ...broadcastTasks].filter((task) => {
      if (!task) return false;
      const key = `${task.task_type || "personal"}:${task.id}`;
      return !visibleKeys.has(key);
    });

    // broadcastタスクの割り当て・完了チェックを並列実行
    const broadcastCandidates = allCandidates.filter((t) => t.task_type === "broadcast");
    const personalCandidates = allCandidates.filter((t) => t.task_type !== "broadcast");

    if (broadcastCandidates.length > 0) {
      const bcChecks = await Promise.all(
        broadcastCandidates.map((task) =>
          Promise.all([
            isBroadcastAssignedToUser(task, teamId, userId),
            dbHasUserCompleted(teamId, task.id, userId),
          ]).then(([isAssigned, hasCompleted]) => ({ task, isAssigned, hasCompleted })),
        ),
      );
      for (const { task, isAssigned, hasCompleted } of bcChecks) {
        if (!isAssigned) continue;
        if (st.scopeKey === "done") {
          if (task.status !== "done" && !hasCompleted) continue;
        } else if (hasCompleted) {
          continue;
        }
        hiddenAssignedTasks.push(task);
      }
    }

    for (const task of personalCandidates) {
      if (String(task.assignee_id || "") === String(userId)) {
        hiddenAssignedTasks.push(task);
      }
    }

    if (hiddenAssignedTasks.length) {
      tasks = [...tasks, ...hiddenAssignedTasks].sort(cmp);
    }
  }

  if (st.scopeKey === "done") {
    // ★完了は直近N時間だけ表示（ここはあなたが2週間=336時間に変更する想定でOK）
    const DONE_VISIBLE_HOURS = 168; // ←あなたの方針でここを336にしてね
    const cutoffMs = Date.now() - DONE_VISIBLE_HOURS * 60 * 60 * 1000;

    const recentDoneTasks = (tasks || []).filter((t) => {
      const ts = t?.completed_at || t?.updated_at || t?.created_at;
      if (!ts) return false;
      const d = new Date(ts);
      if (Number.isNaN(d.getTime())) return false;
      return d.getTime() >= cutoffMs;
    });

    blocks.push({
      type: "section",
      text: {
        type: "mrkdwn",
        text: `*✅ 完了（直近${DONE_VISIBLE_HOURS}時間）*`,
      },
    });

    if (!recentDoneTasks.length) {
      blocks.push({
        type: "context",
        elements: [{ type: "mrkdwn", text: "（直近の完了なし）" }],
      });
    } else {
      for (const t of recentDoneTasks) {
        const isBroadcast = t.task_type === "broadcast";

        // ✅ broadcast は Home で「未完了に戻す」を表示しない
        const canReopen =
          !isBroadcast &&
          (userId === t.requester_user_id || userId === t.assignee_id);

        const rawDesc = String(t.description || "")
          .replace(/\r\n/g, "\n")
          .trim();

        // プレビュー：最大2行 + 最大160文字（軽くて読みやすい）
        const MAX_LINES = 2;
        const MAX_CHARS = 160;

        let preview = rawDesc || "（本文なし）";
        const lines = preview.split("\n");
        if (lines.length > MAX_LINES)
          preview = lines.slice(0, MAX_LINES).join("\n") + "\n…";
        if (preview.length > MAX_CHARS)
          preview = preview.slice(0, MAX_CHARS) + "…";
        preview = noMention(preview);

        // タイトル + プレビュー
        blocks.push({
          type: "section",
          text: { type: "mrkdwn", text: `${preview}` },
        });

        // 補助情報（小さく）
        blocks.push({
          type: "context",
          elements: [
            {
              type: "mrkdwn",
              text: `依頼者：<@${t.requester_user_id}>　/　対応者：${assigneeDisplay(t)}　/　期限：${formatDueDateOnly(t.due_date)}`,
            },
          ],
        });

        // ボタン行：未完了に戻す（personalのみ） / 詳細
        const elems = [];

        if (canReopen) {
          elems.push({
            type: "button",
            text: { type: "plain_text", text: "未完了に戻す ↩️" },
            action_id: "reopen_task",
            value: JSON.stringify({ teamId, taskId: t.id }),
            confirm: {
              title: { type: "plain_text", text: "確認" },
              text: {
                type: "mrkdwn",
                text: "このタスクを*未完了*に戻します。",
              },
              confirm: { type: "plain_text", text: "戻す" },
              deny: { type: "plain_text", text: "やめる" },
            },
          });
        }

        elems.push({
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_modal",
          value: JSON.stringify({ teamId, taskId: t.id, origin: "home" }),
        });

        blocks.push({ type: "actions", elements: elems });
        blocks.push({ type: "divider" });
      }
    }
  } else {
    // ================================
    // ②：未完了は「期限切れ / 期限内」でグルーピング（JST 기준）
    // ================================
    const today = todayJstYmd(); // 既存関数（JSTのYYYY-MM-DD）を使う :contentReference[oaicite:2]{index=2}

    const dueYmdOf = (t) =>
      slackDateYmd(t?.due_date) ||
      (typeof t?.due_date === "string" ? t.due_date.slice(0, 10) : "");

    const isOverdue = (t) => {
      const due = dueYmdOf(t);
      if (!due) return false; // dueなしは「期限内」扱い（仕様確定後に変えられる）
      return due < today;
    };

    const overdue = tasks.filter((t) => isOverdue(t));

    const todayTasks = tasks.filter((t) => {
      const due = dueYmdOf(t);
      return due && !isOverdue(t) && due === today;
    });

    const laterTasks = tasks.filter((t) => {
      const due = dueYmdOf(t);
      return !isOverdue(t) && (!due || due > today);
    });

    const requesterIconMap = new Map();
    const assigneeIconMap = new Map();
    const shortNameMap = new Map();
    const viewerCompletedMap = new Map();

    // requester（全タスク）
    const requesterIds = Array.from(
      new Set((tasks || []).map((t) => t?.requester_user_id).filter(Boolean)),
    );

    // assignee（broadcastは複数対象なので除外）
    const assigneeIds = Array.from(
      new Set(
        (tasks || [])
          .filter((t) => t?.task_type !== "broadcast")
          .map((t) => t?.assignee_id)
          .filter(Boolean),
      ),
    );
    const displayNameIds = Array.from(
      new Set([...requesterIds, ...assigneeIds].filter(Boolean)),
    );
    const broadcastTaskIdsForViewer = Array.from(
      new Set(
        (tasks || [])
          .filter((t) => rangeKey === "to_me" && t?.task_type === "broadcast")
          .map((t) => t?.id)
          .filter(Boolean),
      ),
    );

    // ★範囲=すべて(all) の時はアイコン取得しない（大量呼び出しで固まりやすいので）
    const preloads = [
      Promise.all(
        displayNameIds.map(async (uid) => {
          const full = await getUserDisplayName(teamId, uid);
          shortNameMap.set(uid, toAtShortName(full));
        }),
      ),
      // broadcastの完了済みタスクを一括取得（N回→1回のDBクエリ）
      dbGetUserCompletedTaskIds(teamId, broadcastTaskIdsForViewer, userId).then(
        (completedSet) => {
          for (const taskId of broadcastTaskIdsForViewer) {
            viewerCompletedMap.set(taskId, completedSet.has(taskId));
          }
        },
      ),
    ];

    if (rangeKey !== "all") {
      preloads.push(
        Promise.all(
          requesterIds.map(async (uid) => {
            const url = await getUserIconUrl(teamId, uid);
            if (url) requesterIconMap.set(uid, url);
          }),
        ),
      );
      preloads.push(
        Promise.all(
          assigneeIds.map(async (uid) => {
            const url = await getUserIconUrl(teamId, uid);
            if (url) assigneeIconMap.set(uid, url);
          }),
        ),
      );
    }

    await Promise.all(preloads);

    const pushTaskList = async (
      title,
      list,
      totalCount = null,
      opts = null,
    ) => {
      // Slack Home view は blocks <= 100 制限がある
      const MAX_BLOCKS = 100;
      const SAFETY = 8; // 見出しや末尾の余裕

      const canAdd = (n) => blocks.length + n <= MAX_BLOCKS - SAFETY;

      const titlePlain = String(title || "")
        .replace(/\*/g, "")
        .trim();

      const count = totalCount ?? list.length;

      // ✅ 見出しは「ボタン1個」の行として表示（疑似：見出しクリック）
      if (opts?.toggleAction) {
        const label =
          `${titlePlain}（${count}件） ${opts.toggleLabel || ""}`.trim();

        blocks.push({
          type: "actions",
          elements: [
            {
              type: "button",
              text: { type: "plain_text", text: label },
              action_id: opts.toggleAction.action_id,
              value: opts.toggleAction.value,
            },
          ],
        });
      } else {
        blocks.push({
          type: "header",
          text: {
            type: "plain_text",
            text: `${titlePlain}（${count}件）`,
          },
        });
      }
      blocks.push({ type: "divider" });

      // ✅ 畳み状態：一覧を出さない（もっと見るとは別）
      if (opts?.folded) {
        return;
      }

      // ✅ 0件のときは何も出さない（UIノイズ削減）
      if (!list.length) {
        return;
      }

      let shown = 0;

      // ★範囲=すべて のときだけ、詳細を右に出す（下には出さない）
      const showDetailOnRight = rangeKey === "all";
      for (const t of list) {
        const viewKey = t.task_type === "broadcast" ? "broadcast" : "personal";

        // ★ broadcastで「自分が完了済みか？」を判定（範囲=自分あて の時だけ）
        const viewerCompleted =
          rangeKey === "to_me" && t.task_type === "broadcast"
            ? viewerCompletedMap.get(t.id) === true
            : false;

        // 詳細ボタン（共通）
        const detailBtn = {
          type: "button",
          text: { type: "plain_text", text: "詳細" },
          action_id: "open_detail_modal",
          value: JSON.stringify({ teamId, taskId: t.id, origin: "home" }),
        };

        const needsActions =
          !showDetailOnRight && rangeKey !== "to_me"
            ? true // (詳細を下に出す)
            : rangeKey === "to_me"; // (完了/詳細が必要)

        const needsCompletedHint =
          rangeKey === "to_me" &&
          t.task_type === "broadcast" &&
          viewerCompleted;

        const estimated =
          1 + // section
          1 + // people context
          1 + // meta context
          (needsCompletedHint ? 1 : 0) + // "あなたは完了済み" context
          (needsActions ? 1 : 0) + // actions
          1; // separator context

        if (!canAdd(estimated)) break;
        // ✅ 主：タスク内容（本文）
        const compactOptions = [
          // ✅ コンパクト時：完了もここに入れる（to_me かつ 未完了のときだけ）
          ...(rangeKey === "to_me" &&
          !(t.task_type === "broadcast" && viewerCompleted)
            ? [
                {
                  text: {
                    type: "plain_text",
                    text:
                      t.task_type === "broadcast"
                        ? "自分だけ完了 ✅"
                        : "完了 ✅",
                  },
                  value: `c:${t.id}`,
                },
              ]
            : []),

          // ✅ 詳細
          {
            text: { type: "plain_text", text: "詳細を開く" },
            value: `d:${t.id}`,
          },
        ].slice(0, 5);
        const compactOverflow =
          st.displayMode === "compact"
            ? {
                accessory: {
                  type: "overflow",
                  action_id: "home_task_overflow",
                  options: compactOptions,
                },
              }
            : null;

        blocks.push({
          type: "section",
          text: {
            type: "mrkdwn",
            text: taskLineForHome(t, viewKey),
          },

          // 🖥 標準：従来どおり（範囲=all のときだけ右に詳細）
          ...(st.displayMode !== "compact" && showDetailOnRight
            ? { accessory: detailBtn }
            : {}),

          // 📱 コンパクト：右端に小さい「…」
          ...(compactOverflow || {}),
        });
        // ✅ 小：アイコン + 依頼者 ⇒ アイコン + 対応者（既存のアイコンMapを利用）
        const requesterId = t?.requester_user_id;
        const assigneeId = t?.assignee_id;

        const requesterIcon = requesterId
          ? requesterIconMap.get(requesterId)
          : null;
        const assigneeIcon =
          t?.task_type !== "broadcast" && assigneeId
            ? assigneeIconMap.get(assigneeId)
            : null;

        // 名前取得を軽くするためのローカルキャッシュ
        function getShortAtName(userId) {
          if (!userId) return "-";
          return shortNameMap.get(userId) || "-";
        }

        // broadcast の対応者表示（assignee_label が "@田中/John" みたいな形式でも短縮する）
        const assigneeText =
          viewKey === "broadcast"
            ? assigneeDisplay(t) // ← broadcastはここに統一（shortenAssigneeLabelもここで効く）
            : assigneeId
              ? getShortAtName(assigneeId)
              : "-";

        const requesterText = requesterId
          ? getShortAtName(requesterId)
          : "-";

        const peopleElements = [];
        if (requesterIcon)
          peopleElements.push({
            type: "image",
            image_url: requesterIcon,
            alt_text: "requester",
          });
        peopleElements.push({ type: "mrkdwn", text: requesterText });
        peopleElements.push({ type: "mrkdwn", text: "⇒" });
        if (assigneeIcon)
          peopleElements.push({
            type: "image",
            image_url: assigneeIcon,
            alt_text: "assignee",
          });
        peopleElements.push({ type: "mrkdwn", text: assigneeText });

        blocks.push({ type: "context", elements: peopleElements });

        // ✅ 小：期限 + 元メッセージへリンク
        const dueText = t?.due_date
          ? `（${formatDueDateOnly(t.due_date)}）まで`
          : "";
        const sourceHintText = !t?.message_ts
          ? "Home \u304b\u3089\u4f5c\u6210"
          : "";
        const hasSourceMessage = !!(t?.source_permalink && t?.message_ts);
        const linkText = hasSourceMessage
          ? `🔗 <${t.source_permalink}|元メッセージへ>`
          : "";

        const metaElems = [];
        if (dueText) metaElems.push({ type: "mrkdwn", text: dueText });
        if (linkText) metaElems.push({ type: "mrkdwn", text: linkText });
        if (sourceHintText)
          metaElems.push({ type: "mrkdwn", text: sourceHintText });

        blocks.push({
          type: "context",
          elements: metaElems.length
            ? metaElems
            : [{ type: "mrkdwn", text: " " }],
        });

        // ✅ Homeの完了ボタンは「範囲=自分あて（to_me）」の時だけ
        if (rangeKey !== "to_me") {
          // 📱コンパクトは右端「…」に詳細が入ってるので actions を出さない
          if (st.displayMode !== "compact" && !showDetailOnRight) {
            blocks.push({
              type: "actions",
              elements: [detailBtn],
            });
          }
        } else {
          // rangeKey === "to_me"
          if (t.task_type === "broadcast" && viewerCompleted) {
            // 「完了済み」表示（グレー相当）
            blocks.push({
              type: "context",
              elements: [{ type: "mrkdwn", text: "✅ あなたは完了済み" }],
            });

            // 詳細だけ（※showDetailOnRight の場合は下に出さない）
            if (st.displayMode !== "compact" && !showDetailOnRight) {
              blocks.push({
                type: "actions",
                elements: [detailBtn],
              });
            }
          } else {
            // 完了 +（必要なら）詳細（自分あての時だけ）
            if (st.displayMode === "standard") {
              // 🖥 標準：今まで通り（PC快適性維持）
              const elems = [
                {
                  type: "button",
                  text: {
                    type: "plain_text",
                    text:
                      t.task_type === "broadcast"
                        ? "自分だけ完了 ✅"
                        : "完了 ✅",
                  },
                  style: "primary",
                  action_id: "complete_task",
                  value: JSON.stringify({ teamId, taskId: t.id }),
                },
                {
                  type: "button",
                  text: { type: "plain_text", text: "詳細" },
                  action_id: "open_detail_modal",
                  value: JSON.stringify({
                    teamId,
                    taskId: t.id,
                    origin: "home",
                  }),
                },
              ];
              blocks.push({ type: "actions", elements: elems });
            } else {
            }
          }
        }

        blocks.push({ type: "divider" });
        shown++;
      }

      if (shown < list.length && canAdd(2)) {
        blocks.push({
          type: "context",
          elements: [{ type: "mrkdwn", text: `…他 ${list.length - shown}件` }],
        });
        blocks.push({ type: "divider" });
      }
    };

    // スマホ優先：期限切れ → 今日 → 明日以降
    const MORE_LIMIT = 10; // ★範囲=すべて(all) のときだけ「もっと見る」で段階表示

    const isAllRange = rangeKey === "all";

    const overdueTotal = overdue.length;
    const todayTotal = todayTasks.length;

    const overdueExpanded = !!st.homeMore?.overdue;
    const todayExpanded = !!st.homeMore?.today;

    // ✅ 追加：畳み状態（もっと見るとは別）
    const overdueFolded = !!st.homeFold?.overdue;
    const todayFolded = !!st.homeFold?.today;
    const laterFolded = !!st.homeFold?.later;

    const overdueVisible =
      isAllRange && !overdueExpanded ? overdue.slice(0, MORE_LIMIT) : overdue;

    const todayVisible =
      isAllRange && !todayExpanded
        ? todayTasks.slice(0, MORE_LIMIT)
        : todayTasks;

    await pushTaskList("*🚨 期限切れ*", overdueVisible, overdueTotal, {
      toggleAction: {
        action_id: "home_toggle_fold",
        value: JSON.stringify({ section: "overdue" }),
      },
      toggleLabel: overdueFolded ? "▽" : "△",
      folded: overdueFolded,
    });

    if (!overdueFolded && isAllRange && overdueTotal > MORE_LIMIT) {
      const hidden = Math.max(0, overdueTotal - overdueVisible.length);
      blocks.push({
        type: "actions",
        elements: [
          {
            type: "button",
            text: {
              type: "plain_text",
              text: overdueExpanded
                ? "閉じる"
                : `もっと見る（残り${hidden}件）`,
            },
            action_id: "home_toggle_more",
            value: JSON.stringify({ section: "overdue" }),
          },
        ],
      });
      blocks.push({ type: "divider" });
    }

    await pushTaskList("*🟨 今日*", todayVisible, todayTotal, {
      toggleAction: {
        action_id: "home_toggle_fold",
        value: JSON.stringify({ section: "today" }),
      },
      toggleLabel: todayFolded ? "▽" : "△",
      folded: todayFolded,
    });

    // ★既存：もっと見る（今日）
    if (!todayFolded && isAllRange && todayTotal > MORE_LIMIT) {
      const hidden = Math.max(0, todayTotal - todayVisible.length);
      blocks.push({
        type: "actions",
        elements: [
          {
            type: "button",
            text: {
              type: "plain_text",
              text: todayExpanded ? "閉じる" : `もっと見る（残り${hidden}件）`,
            },
            action_id: "home_toggle_more",
            value: JSON.stringify({ section: "today" }),
          },
        ],
      });
      blocks.push({ type: "divider" });
    }

    await pushTaskList("*🟩 明日以降*", laterTasks, null, {
      toggleAction: {
        action_id: "home_toggle_fold",
        value: JSON.stringify({ section: "later" }),
      },
      toggleLabel: laterFolded ? "▽" : "△",
      folded: laterFolded,
    });
  }
  const FOOTER_BLOCKS = [
    {
      type: "context",
      elements: [{ type: "mrkdwn", text: "\u200b" }], // ゼロ幅スペース（見えない）
    },
    {
      type: "context",
      elements: [{ type: "mrkdwn", text: "\u200b" }],
    },
    {
      type: "context",
      elements: [{ type: "mrkdwn", text: "\u200b" }],
    },
  ];

  // Slack blocks 上限は 100。末尾の余白ブロック分を先に確保する
  const SLACK_BLOCK_LIMIT = 100;
  const RESERVE = FOOTER_BLOCKS.length;

  // ✅ blocks が多すぎる場合、末尾が切られて「スクロールできない」原因になるので削る
  if (blocks.length > SLACK_BLOCK_LIMIT - RESERVE) {
    // 末尾は中途半端に切るとUIが崩れやすいので、
    // 「安全に収まるところまで」ガツッと切って注意文を入れる
    const keep = SLACK_BLOCK_LIMIT - RESERVE - 1; // 注意文1個分も確保
    blocks.splice(keep);

    blocks.push({
      type: "context",
      elements: [
        {
          type: "mrkdwn",
          text: "⚠️ 表示件数が多いため一部を省略しました。フィルタ/折り畳みで絞ると下まで見やすくなるよ。",
        },
      ],
    });
    blocks.push({
      type: "actions",
      elements: [
        {
          type: "button",
          action_id: "open_task_list_modal",
          text: { type: "plain_text", text: "一覧を開く" },
          value: JSON.stringify({
            teamId,
            userId,
            rangeKey,
            scopeKey: st.scopeKey || "active",
          }),
        },
      ],
    });
  }

  // ✅ 最後に“必ず残る”余白を付与
  blocks.push(...FOOTER_BLOCKS);

  await client.views.publish({
    user_id: userId,
    view: {
      type: "home",
      callback_id: "home",
      blocks,
    },
  });
}

app.options("assignee_groups_select", async ({ ack, payload }) => {
  try {
    const q = payload?.value || "";
    const groups = await searchUsergroups(q);
    await ack({
      options: groups.map((g) => ({
        text: { type: "plain_text", text: `@${g.handle}` },
        value: g.id,
      })),
    });
  } catch (e) {
    console.error("options error:", e?.data || e);
    await ack({ options: [] });
  }
});

// ================================
// Home personal assignee (external_select) options
// ================================
const HOME_USERLIST_CACHE_MS = 5 * 60 * 1000;
const homeUserListCache = new Map(); // teamId -> { at, users: [{id, name}] }

async function listUsersCached(teamId) {
  const now = Date.now();
  const cached = homeUserListCache.get(teamId);
  if (cached && now - cached.at < HOME_USERLIST_CACHE_MS) return cached.users;

  const res = await app.client.users.list();
  const users = (res.members || [])
    .filter((u) => u && !u.deleted && !u.is_bot)
    .map((u) => {
      const name =
        (u.profile?.display_name && u.profile.display_name.trim()) ||
        (u.real_name && u.real_name.trim()) ||
        (u.name && String(u.name).trim()) ||
        u.id;
      return { id: u.id, name };
    });

  homeUserListCache.set(teamId, { at: now, users });
  return users;
}

app.options("home_person_assignee_select", async ({ ack, body, payload }) => {
  try {
    const teamId = body.team?.id || body.team_id;
    const userId = body.user?.id;
    const st = getHomeState(teamId, userId);
    const deptKey = st?.deptKey || "all";

    const q = String(payload?.value || "")
      .trim()
      .toLowerCase();
    // 初期候補：未入力でも上位5件を返す（担当部署があればその所属から、なければ全員から）
    const allUsers = await listUsersCached(teamId);

    // dept 絞り込み用の許可集合（null=絞り込みなし）
    let allowed = null;
    if (deptKey && deptKey !== "all" && deptKey !== "__none__") {
      const members = deptKey.startsWith("pf:")
        ? await dbGetPersonalFilterMemberIds(teamId, deptKey.slice(3))
        : await getUsergroupMembers(teamId, deptKey);
      allowed = new Set(members || []);
    } else if (deptKey === "__none__") {
      // 未設定を実用にしていないため候補なし
      await ack({ options: [] });
      return;
    }

    const filtered = allUsers
      .filter((u) => {
        if (allowed && !allowed.has(u.id)) return false;
        if (!q) return true; // dept指定時は空検索でも候補を出す
        return u.name.toLowerCase().includes(q);
      })
      .sort((a, b) => {
        if (a.id === userId) return -1;
        if (b.id === userId) return 1;
        return a.name.localeCompare(b.name);
      })
      .slice(0, 100)
      .map((u) => ({
        text: { type: "plain_text", text: u.name },
        value: u.id,
      }));

    await ack({ options: filtered });
  } catch (e) {
    console.error("home_person_assignee_select options error:", e?.data || e);
    await ack({ options: [] });
  }
});

app.event("app_home_opened", async ({ event, client, body }) => {
  try {
    const teamId = body.team_id || body.team?.id || event.team;
    const userId = event.user;
    // Phase8-4: Homeの検索条件を保持（初回のみ初期化）
    const k = `${teamId}:${userId}`;
    if (!homeState.has(k)) {
      setHomeState(teamId, userId, {
        viewKey: "all",
        scopeKey: "active",
        personalScopeKey: "to_me",
        assigneeUserId: userId,
        deptKey: "all",
        broadcastScopeKey: "to_me",
      });
    }

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("app_home_opened error:", e?.data || e);
  }
});

app.action("my_tasks_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "to_me";

    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const scopeKey = meta.scopeKey || "active";

    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: selected,
      scopeKey,
    });

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view,
    });
  } catch (e) {
    console.error("my_tasks_scope_select error:", e?.data || e);
  }
});

app.action("my_tasks_status_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "active";

    const meta = safeJsonParse(body.view?.private_metadata || "{}") || {};
    const rangeKey = meta.rangeKey || "to_me";

    // 📱スマホ対策：まずローディング表示（ここが体感に効く！）
    const loadingView = {
      type: "modal",
      callback_id: "task_list_modal",
      title: { type: "plain_text", text: "タスク一覧" },
      close: { type: "plain_text", text: "閉じる" },
      private_metadata: body.view?.private_metadata || "{}",
      blocks: [
        {
          type: "section",
          text: { type: "mrkdwn", text: "読み込み中…⏳" },
        },
      ],
    };

    try {
      await client.views.update({
        view_id: body.view.id,
        hash: body.view.hash,
        view: loadingView,
      });
    } catch (_) {}

    // 本描画（DB/整形が重くても、ユーザーは「反応した」って分かる）
    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey,
      scopeKey: selected,
    });

    await client.views.update({
      view_id: body.view.id,
      hash: body.view.hash,
      view,
    });
  } catch (e) {
    console.error("my_tasks_status_select error:", e?.data || e);
  }
});

// 一覧を開く（作成完了エフェメラル等から）
app.action("open_task_list_modal", async ({ ack, body, client }) => {
  await ack();
  try {
    const payload = safeJsonParse(body.actions?.[0]?.value || "{}") || {};
    const teamId = payload.teamId || getTeamIdFromBody(body);
    const userId = payload.userId || getUserIdFromBody(body);
    if (!teamId || !userId) return;

    const trigger_id = body.trigger_id;
    if (!trigger_id) return;

    const view = await buildTaskListModalView({
      teamId,
      userId,
      rangeKey: payload.rangeKey || "to_me",
      scopeKey: payload.scopeKey || "active",
    });
    await client.views.open({ trigger_id, view });
  } catch (e) {
    console.error("open_task_list_modal error:", e?.data || e);
  }
});

// Home: mode change
app.action("home_view_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    // ★表示は固定（保険：過去UIのイベントが飛んでも all に寄せる）
    setHomeState(teamId, userId, { viewKey: "all" });

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_view_select error:", e?.data || e);
  }
});

app.action("home_person_assignee_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selectedUser = body.actions?.[0]?.selected_option?.value || userId;

    setHomeState(teamId, userId, { assigneeUserId: selectedUser });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_person_assignee_select error:", e?.data || e);
  }
});

// personal: 担当者クリア（空欄=全員対象）
app.action("home_person_assignee_clear", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    setHomeState(teamId, userId, { assigneeUserId: null });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_person_assignee_clear error:", e?.data || e);
  }
});

app.action("home_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "active";

    setHomeState(teamId, userId, { scopeKey: selected });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_scope_select error:", e?.data || e);
  }
});

app.action("home_dept_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "all";

    setHomeState(teamId, userId, { deptKey: selected, assigneeUserId: null });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_dept_select error:", e?.data || e);
  }
});

app.action("home_broadcast_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const scopeKey = body.actions?.[0]?.selected_option?.value;

    if (!teamId || !userId || !scopeKey) return;

    setHomeState(teamId, userId, { broadcastScopeKey: scopeKey });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_broadcast_scope_select error:", e?.data || e);
  }
});

// Home: 「もっと見る / 閉じる」トグル（範囲=すべて(all) の時だけUI上に出す想定）
app.action("home_toggle_more", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    const payload = safeJsonParse(body.actions?.[0]?.value || "");
    const section = payload?.section; // "overdue" | "today"
    if (!teamId || !userId) return;
    if (section !== "overdue" && section !== "today") return;

    const st = getHomeState(teamId, userId);
    const next = {
      homeMore: {
        ...(st.homeMore || { overdue: false, today: false }),
        [section]: !st.homeMore?.[section],
      },
    };

    setHomeState(teamId, userId, next);
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_toggle_more error:", e?.data || e);
  }
});

// Home: 「開く / 閉じる」トグル（畳む機能。もっと見るとは別）
app.action("home_toggle_fold", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    const payload = safeJsonParse(body.actions?.[0]?.value || "");
    const section = payload?.section; // "overdue" | "today" | "later"
    if (!teamId || !userId) return;
    if (section !== "overdue" && section !== "today" && section !== "later")
      return;

    const st = getHomeState(teamId, userId);
    const next = {
      homeFold: {
        ...(st.homeFold || { overdue: false, today: false, later: false }),
        [section]: !st.homeFold?.[section],
      },
    };

    setHomeState(teamId, userId, next);
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_toggle_fold error:", e?.data || e);
  }
});

// Home: フィルタをリセット（Phase8-4）
app.action("home_reset_filters", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);

    setHomeState(teamId, userId, {
      viewKey: "all",
      scopeKey: "active",
      personalScopeKey: "to_me",
      assigneeUserId: userId,
      deptKey: "all",
      broadcastScopeKey: "to_me",
    });

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_reset_filters error:", e?.data || e);
  }
});

// personal: 範囲（自分が対応/自分が発行/すべて）
app.action("home_personal_scope_select", async ({ ack, body, client }) => {
  await ack();
  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const selected = body.actions?.[0]?.selected_option?.value || "to_me";

    if (selected === "all") {
      // すべて：検索UIが有効（dept/assignee）
      setHomeState(teamId, userId, { personalScopeKey: "all" });
    } else {
      // すべて以外：隠れフィルタ事故を防ぐため検索条件をリセット
      setHomeState(teamId, userId, {
        personalScopeKey: selected,
        deptKey: "all",
        assigneeUserId: userId,
      });
    }

    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("home_personal_scope_select error:", e?.data || e);
  }
});

app.action("toggle_home_display_mode", async ({ ack, body, client }) => {
  await ack();
  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const st = getHomeState(teamId, userId);
  const nextMode = st.displayMode === "compact" ? "standard" : "compact";
  setHomeState(teamId, userId, { displayMode: nextMode });

  await publishHome({ client, teamId, userId });
});

app.action("home_task_overflow", async ({ ack, body, client, action }) => {
  await ack();

  try {
    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    const v = action?.selected_option?.value || "";
    if (!teamId || !userId || !v) return;

    // value: "c:<taskId>" | "d:<taskId>"
    const [kind, taskId] = v.split(":");
    if (!taskId) return;

    if (kind === "d") {
      await openDetailModal(client, {
        trigger_id: body.trigger_id,
        teamId,
        taskId,
        viewerUserId: userId,
        origin: "home",
        isFromModal: false,
      });
      return;
    }

    if (kind === "c") {
      // ✅ body をそのまま渡す（handleCompleteTask 側が body を期待している）
      await handleCompleteTask({ client, body, teamId, taskId });
      return;
    }
  } catch (e) {
    console.error("home_task_overflow error:", e?.data || e);
  }
});

// ================================
// Personal filter create
// ================================
app.action("open_personal_filter_modal", async ({ ack, body, client }) => {
  await ack();
  const teamId = getTeamIdFromBody(body);
  const userId = getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const isFromModal = body.view?.type === "modal";
  try {
    const openFn = isFromModal ? client.views.push : client.views.open;
    await openFn.call(client.views, {
      trigger_id: body.trigger_id,
      view: {
        type: "modal",
        callback_id: "personal_filter_create_modal",
        private_metadata: JSON.stringify({ teamId, userId }),
        title: { type: "plain_text", text: "フィルタ作成" },
        submit: { type: "plain_text", text: "作成" },
        close: { type: "plain_text", text: "キャンセル" },
        blocks: [
          {
            type: "input",
            block_id: "filter_name",
            label: { type: "plain_text", text: "チーム名" },
            element: {
              type: "plain_text_input",
              action_id: "value",
              placeholder: { type: "plain_text", text: "例：営業チーム" },
            },
          },
          {
            type: "input",
            block_id: "filter_members",
            label: { type: "plain_text", text: "メンバー" },
            element: {
              type: "multi_users_select",
              action_id: "value",
              placeholder: { type: "plain_text", text: "メンバーを選択（複数可）" },
            },
          },
        ],
      },
    });
  } catch (e) {
    console.error("open_personal_filter_modal error:", e?.data || e);
  }
});

app.view("personal_filter_create_modal", async ({ ack, body, view, client }) => {
  await ack();
  const meta = safeJsonParse(view.private_metadata) || {};
  const teamId = meta.teamId || getTeamIdFromBody(body);
  const userId = meta.userId || getUserIdFromBody(body);
  if (!teamId || !userId) return;

  const name = view.state.values.filter_name?.value?.value?.trim();
  const memberIds = view.state.values.filter_members?.value?.selected_users || [];
  if (!name) return;

  try {
    const id = randomUUID();
    await dbCreatePersonalFilter(teamId, userId, id, name);
    await dbSetPersonalFilterMembers(teamId, id, memberIds);

    // 作成したフィルタを即座にアクティブにして Home を更新
    setHomeState(teamId, userId, { deptKey: `pf:${id}`, broadcastScopeKey: "all" });
    await publishHome({ client, teamId, userId });
  } catch (e) {
    console.error("personal_filter_create_modal error:", e?.data || e);
  }
});

app.action("open_home_task_create_modal", async ({ ack, body, client }) => {
  await ack();

  try {
    const teamId = getTeamIdFromBody(body);
    const actorUserId = getUserIdFromBody(body);
    if (!teamId || !actorUserId) return;

    await openTaskCreateModal(client, {
      trigger_id: body.trigger_id,
      teamId,
      channelId: "",
      msgTs: "",
      actorUserId,
      includeContentInput: true,
      initialContent: "",
    });
  } catch (e) {
    console.error("open_home_task_create_modal error:", e?.data || e);
  }
});

app.action("open_user_settings_from_home", async ({ ack, body, action, client }) => {
  await ack();
  try {
    const p = safeJsonParse(action?.value || "{}") || {};
    const teamId = p.teamId || getTeamIdFromBody(body);
    const userId = p.userId || getUserIdFromBody(body);
    if (!teamId || !userId) return;
    await openUserSettingsModal({ client, triggerId: body.trigger_id, teamId, userId });
  } catch (e) {
    console.error("open_user_settings_from_home error:", e?.data || e);
  }
});

  return {
    getHomeState,
    publishHome,
    publishHomeForUsers,
    setHomeState,
  };
}

module.exports = {
  registerHomeFeature,
};
