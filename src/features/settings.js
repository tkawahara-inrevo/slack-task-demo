function registerSettingsFeature(deps) {
  const {
    app,
    dbGetTeamSettings,
    dbGetUserSettings,
    dbUpsertTeamSettings,
    dbUpsertUserSettings,
    dbListProjects = async () => [],
    dbListDashTeams: _dbListDashTeams = async () => [],
    dbQuery = async () => ({ rows: [] }),
    getTeamIdFromBody,
    getUserIdFromBody,
    publishHome,
    safeJsonParse,
    setHomeState,
  } = deps;

  const USER_SETTINGS_COMMAND =
    process.env.USER_SETTINGS_COMMAND || "/task-user-settings";
  const TEAM_SETTINGS_COMMAND =
    process.env.TEAM_SETTINGS_COMMAND || "/task-team-settings";

  const DEFAULT_TEAM_SETTINGS = Object.freeze({
    defaultHomeDisplayMode: "standard",
    defaultHomeRange: "to_me",
    defaultHomeState: "active",
    reactionTaskifyEnabled: true,
    dueDmNotificationsEnabled: true,
    completionDmNotificationsEnabled: true,
    commentDmNotificationsEnabled: true,
    overdueChannelNotificationsEnabled: true,
  });

  const DEFAULT_USER_SETTINGS = Object.freeze({
    homeDisplayMode: "inherit",
    homeRange: "inherit",
    homeState: "inherit",
    dueDmNotificationsEnabled: "inherit",
    dueRequesterDmNotificationsEnabled: "inherit",
    dueNotificationSchedule: "morning_only", // "morning_only" = 朝9時のみ, "morning_and_afternoon" = 朝9時＋16時
    completionDmNotificationsEnabled: "inherit",
    commentDmNotificationsEnabled: "inherit",
    homeProjectFilter: "",  // プロジェクトIDでホームタブをフィルター（空=フィルターなし）
    homeDashTeamFilter: "", // ダッシュボードチームIDでフィルター（空=フィルターなし）
  });

  const teamSettingsCache = new Map();
  const userSettingsCache = new Map();

  function userCacheKey(teamId, userId) {
    return `${teamId}:${userId}`;
  }

  function asObject(value) {
    return value && typeof value === "object" && !Array.isArray(value)
      ? value
      : {};
  }

  function normalizeBooleanChoice(value, fallback = true) {
    if (value === "true" || value === true) return true;
    if (value === "false" || value === false) return false;
    return fallback;
  }

  function normalizeTeamSettings(settings = {}) {
    const src = asObject(settings);
    return {
      defaultHomeDisplayMode:
        src.defaultHomeDisplayMode === "compact" ? "compact" : "standard",
      defaultHomeRange: ["to_me", "requested_by_me", "all"].includes(
        src.defaultHomeRange,
      )
        ? src.defaultHomeRange
        : DEFAULT_TEAM_SETTINGS.defaultHomeRange,
      defaultHomeState:
        src.defaultHomeState === "done" ? "done" : "active",
      reactionTaskifyEnabled: normalizeBooleanChoice(
        src.reactionTaskifyEnabled,
        DEFAULT_TEAM_SETTINGS.reactionTaskifyEnabled,
      ),
      dueDmNotificationsEnabled: normalizeBooleanChoice(
        src.dueDmNotificationsEnabled,
        DEFAULT_TEAM_SETTINGS.dueDmNotificationsEnabled,
      ),
      completionDmNotificationsEnabled: normalizeBooleanChoice(
        src.completionDmNotificationsEnabled,
        DEFAULT_TEAM_SETTINGS.completionDmNotificationsEnabled,
      ),
      commentDmNotificationsEnabled: normalizeBooleanChoice(
        src.commentDmNotificationsEnabled,
        DEFAULT_TEAM_SETTINGS.commentDmNotificationsEnabled,
      ),
      overdueChannelNotificationsEnabled: normalizeBooleanChoice(
        src.overdueChannelNotificationsEnabled,
        DEFAULT_TEAM_SETTINGS.overdueChannelNotificationsEnabled,
      ),
    };
  }

  function normalizeUserSettings(settings = {}) {
    const src = asObject(settings);
    const pickChoice = (value, allowed, fallback) =>
      allowed.includes(value) ? value : fallback;

    return {
      homeDisplayMode: pickChoice(
        src.homeDisplayMode,
        ["inherit", "standard", "compact"],
        DEFAULT_USER_SETTINGS.homeDisplayMode,
      ),
      homeRange: pickChoice(
        src.homeRange,
        ["inherit", "to_me", "requested_by_me", "all"],
        DEFAULT_USER_SETTINGS.homeRange,
      ),
      homeState: pickChoice(
        src.homeState,
        ["inherit", "active", "done"],
        DEFAULT_USER_SETTINGS.homeState,
      ),
      dueDmNotificationsEnabled: pickChoice(
        src.dueDmNotificationsEnabled,
        ["inherit", "true"],
        src.dueDmNotificationsEnabled === "false" ? "inherit" : DEFAULT_USER_SETTINGS.dueDmNotificationsEnabled,
      ),
      dueRequesterDmNotificationsEnabled: pickChoice(
        src.dueRequesterDmNotificationsEnabled,
        ["inherit", "true", "false"],
        DEFAULT_USER_SETTINGS.dueRequesterDmNotificationsEnabled,
      ),
      dueNotificationSchedule: pickChoice(
        src.dueNotificationSchedule,
        ["morning_only", "morning_and_afternoon"],
        DEFAULT_USER_SETTINGS.dueNotificationSchedule,
      ),
      completionDmNotificationsEnabled: pickChoice(
        src.completionDmNotificationsEnabled,
        ["inherit", "true", "false"],
        DEFAULT_USER_SETTINGS.completionDmNotificationsEnabled,
      ),
      commentDmNotificationsEnabled: pickChoice(
        src.commentDmNotificationsEnabled,
        ["inherit", "true", "false"],
        DEFAULT_USER_SETTINGS.commentDmNotificationsEnabled,
      ),
      homeProjectFilter: String(src.homeProjectFilter || ""),
      homeDashTeamFilter: String(src.homeDashTeamFilter || ""),
    };
  }

  async function getTeamSettings(teamId) {
    if (!teamId) return { ...DEFAULT_TEAM_SETTINGS };
    if (teamSettingsCache.has(teamId)) return teamSettingsCache.get(teamId);

    const row = await dbGetTeamSettings(teamId);
    const next = normalizeTeamSettings(row?.settings || {});
    teamSettingsCache.set(teamId, next);
    return next;
  }

  async function getUserSettings(teamId, userId) {
    if (!teamId || !userId) return { ...DEFAULT_USER_SETTINGS };
    const key = userCacheKey(teamId, userId);
    if (userSettingsCache.has(key)) return userSettingsCache.get(key);

    const row = await dbGetUserSettings(teamId, userId);
    const next = normalizeUserSettings(row?.settings || {});
    userSettingsCache.set(key, next);
    return next;
  }

  function invalidateSettingsCache(teamId, userId = null) {
    if (teamId) teamSettingsCache.delete(teamId);
    if (teamId && userId) userSettingsCache.delete(userCacheKey(teamId, userId));
  }

  async function resolveHomeDefaults(teamId, userId) {
    const [teamSettings, userSettings] = await Promise.all([
      getTeamSettings(teamId),
      getUserSettings(teamId, userId),
    ]);

    return {
      displayMode:
        userSettings.homeDisplayMode === "inherit"
          ? teamSettings.defaultHomeDisplayMode
          : userSettings.homeDisplayMode,
      rangeKey:
        userSettings.homeRange === "inherit"
          ? teamSettings.defaultHomeRange
          : userSettings.homeRange,
      scopeKey:
        userSettings.homeState === "inherit"
          ? teamSettings.defaultHomeState
          : userSettings.homeState,
    };
  }

  async function isReactionTaskifyEnabled(teamId) {
    const teamSettings = await getTeamSettings(teamId);
    return !!teamSettings.reactionTaskifyEnabled;
  }

  async function isOverdueChannelNotificationEnabled(teamId) {
    const teamSettings = await getTeamSettings(teamId);
    return !!teamSettings.overdueChannelNotificationsEnabled;
  }

  async function isUserDmEnabled(teamId, userId, kind) {
    if (!teamId || !userId || !kind) return true;

    const [teamSettings, userSettings] = await Promise.all([
      getTeamSettings(teamId),
      getUserSettings(teamId, userId),
    ]);

    const teamMap = {
      due: teamSettings.dueDmNotificationsEnabled,
      completion: teamSettings.completionDmNotificationsEnabled,
      comment: teamSettings.commentDmNotificationsEnabled,
    };
    const userMap = {
      due: userSettings.dueDmNotificationsEnabled,
      due_requester: userSettings.dueRequesterDmNotificationsEnabled,
      completion: userSettings.completionDmNotificationsEnabled,
      comment: userSettings.commentDmNotificationsEnabled,
    };

    const userChoice = userMap[kind];
    if (userChoice === "true") return true;
    if (userChoice === "false") return false;
    if (Object.prototype.hasOwnProperty.call(teamMap, kind)) {
      return !!teamMap[kind];
    }
    return true;
  }

  async function getUserDueSchedule(teamId, userId) {
    if (!teamId || !userId) return "morning_only";
    const us = await getUserSettings(teamId, userId);
    return us.dueNotificationSchedule || "morning_only";
  }

  async function canManageTeamSettings(client, userId) {
    if (!userId) return false;
    try {
      const res = await client.users.info({ user: userId });
      const user = res?.user;
      return !!(
        user?.is_admin ||
        user?.is_owner ||
        user?.is_primary_owner
      );
    } catch (_) {
      return false;
    }
  }

  function buildUserSettingsModalView(teamId, userId, settings, opts = {}) {
    const { projects = [], taskTriggers = [] } = opts;
    const triggerInitial = taskTriggers.filter(t => t.enabled).map(t => t.keyword).join('\n');
    const dueScheduleOptions = [
      { text: { type: "plain_text", text: "朝 9:00 のみ" }, value: "morning_only" },
      { text: { type: "plain_text", text: "朝 9:00 + 16:00" }, value: "morning_and_afternoon" },
    ];
    const dmOnOffOptions = [
      { text: { type: "plain_text", text: "受け取る" }, value: "true" },
      { text: { type: "plain_text", text: "受け取らない" }, value: "false" },
    ];

    const currentProject = projects.find((p) => p.id === settings.homeProjectFilter);

    return {
      type: "modal",
      callback_id: "user_settings_modal",
      private_metadata: JSON.stringify({ teamId, userId }),
      title: { type: "plain_text", text: "個人設定" },
      submit: { type: "plain_text", text: "保存" },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: "*通知設定*",
          },
        },
        {
          type: "input",
          block_id: "due_notification_schedule",
          label: { type: "plain_text", text: "期限通知のタイミング" },
          element: {
            type: "static_select",
            action_id: "value",
            options: dueScheduleOptions,
            initial_option:
              dueScheduleOptions.find(
                (option) => option.value === settings.dueNotificationSchedule,
              ) || dueScheduleOptions[0],
          },
        },
        {
          type: "input",
          block_id: "completion_dm_notifications",
          label: { type: "plain_text", text: "完了通知 DM" },
          element: {
            type: "static_select",
            action_id: "value",
            options: dmOnOffOptions,
            initial_option:
              dmOnOffOptions.find(
                (option) =>
                  option.value ===
                  (settings.completionDmNotificationsEnabled === "inherit"
                    ? "true"
                    : settings.completionDmNotificationsEnabled),
              ) || dmOnOffOptions[0],
          },
        },
        {
          type: "input",
          block_id: "due_requester_dm_notifications",
          label: { type: "plain_text", text: "依頼者としての今日期限通知 DM" },
          element: {
            type: "static_select",
            action_id: "value",
            options: dmOnOffOptions,
            initial_option:
              dmOnOffOptions.find(
                (option) =>
                  option.value ===
                  (settings.dueRequesterDmNotificationsEnabled === "inherit"
                    ? "true"
                    : settings.dueRequesterDmNotificationsEnabled),
              ) || dmOnOffOptions[0],
          },
        },
        {
          type: "input",
          block_id: "comment_dm_notifications",
          label: { type: "plain_text", text: "コメント通知 DM" },
          element: {
            type: "static_select",
            action_id: "value",
            options: dmOnOffOptions,
            initial_option:
              dmOnOffOptions.find(
                (option) =>
                  option.value ===
                  (settings.commentDmNotificationsEnabled === "inherit"
                    ? "true"
                    : settings.commentDmNotificationsEnabled),
              ) || dmOnOffOptions[0],
          },
        },
        { type: "divider" },
        {
          type: "section",
          text: {
            type: "mrkdwn",
            text: "*🐶 自動タスク化キーワード*\nここに登録した単語が *あなた自身の発言* に含まれると自動でタスク化されます。1行に1つ書いてください。全社共通の `<タスク化>` `＜タスク化＞` は常に有効です。",
          },
        },
        {
          type: "input",
          block_id: "task_trigger_keywords",
          optional: true,
          label: { type: "plain_text", text: "自分用キーワード（1行に1つ）" },
          element: {
            type: "plain_text_input",
            action_id: "value",
            multiline: true,
            initial_value: triggerInitial,
            placeholder: { type: "plain_text", text: "例:\nTODO:\nメモ\n📝" },
          },
        },
        ...(projects.length > 0
          ? [
              { type: "divider" },
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text:
                    "*Homeタブ フィルター設定*\nダッシュボードのプロジェクト・チームでホームタブの表示を絞り込めます。",
                },
              },
              {
                type: "input",
                block_id: "home_project_filter",
                optional: true,
                label: { type: "plain_text", text: "プロジェクトで絞り込み" },
                element: {
                  type: "static_select",
                  action_id: "value",
                  placeholder: { type: "plain_text", text: "フィルターなし" },
                  options: [
                    {
                      text: { type: "plain_text", text: "フィルターなし" },
                      value: "__none__",
                    },
                    ...projects.map((p) => ({
                      text: { type: "plain_text", text: p.name },
                      value: p.id,
                    })),
                  ],
                  ...(settings.homeProjectFilter
                    ? {
                        initial_option: currentProject
                          ? {
                              text: { type: "plain_text", text: currentProject.name },
                              value: settings.homeProjectFilter,
                            }
                          : {
                              text: { type: "plain_text", text: "フィルターなし" },
                              value: "__none__",
                            },
                      }
                    : {}),
                },
              },
              { type: "divider" },
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text:
                    "*パーソナルフィルター*\nメンバーを選んで「チーム」として保存し、部署フィルターで使えます。",
                },
                accessory: {
                  type: "button",
                  action_id: "open_personal_filter_manage_modal",
                  text: { type: "plain_text", text: "フィルター管理" },
                  value: JSON.stringify({ teamId, userId }),
                },
              },
            ]
          : [
              { type: "divider" },
              {
                type: "section",
                text: {
                  type: "mrkdwn",
                  text:
                    "*パーソナルフィルター*\nメンバーを選んで「チーム」として保存し、部署フィルターで使えます。",
                },
                accessory: {
                  type: "button",
                  action_id: "open_personal_filter_manage_modal",
                  text: { type: "plain_text", text: "フィルター管理" },
                  value: JSON.stringify({ teamId, userId }),
                },
              },
            ]),
      ],
    };
  }

  function buildTeamSettingsModalView(teamId, userId, settings) {
    const displayModeOptions = [
      { text: { type: "plain_text", text: "標準表示" }, value: "standard" },
      { text: { type: "plain_text", text: "コンパクト表示" }, value: "compact" },
    ];
    const homeRangeOptions = [
      { text: { type: "plain_text", text: "自分に関係あるもの" }, value: "to_me" },
      { text: { type: "plain_text", text: "自分が依頼したもの" }, value: "requested_by_me" },
      { text: { type: "plain_text", text: "すべて" }, value: "all" },
    ];
    const homeStateOptions = [
      { text: { type: "plain_text", text: "進行中" }, value: "active" },
      { text: { type: "plain_text", text: "完了済み" }, value: "done" },
    ];
    const boolOptions = [
      { text: { type: "plain_text", text: "有効" }, value: "true" },
      { text: { type: "plain_text", text: "無効" }, value: "false" },
    ];

    const initialBoolOption = (value) =>
      boolOptions.find((option) => option.value === String(!!value)) ||
      boolOptions[0];

    return {
      type: "modal",
      callback_id: "team_settings_modal",
      private_metadata: JSON.stringify({ teamId, userId }),
      title: { type: "plain_text", text: "チーム設定" },
      submit: { type: "plain_text", text: "保存" },
      close: { type: "plain_text", text: "閉じる" },
      blocks: [
        {
          type: "input",
          block_id: "default_home_display_mode",
          label: { type: "plain_text", text: "Home の既定表示モード" },
          element: {
            type: "static_select",
            action_id: "value",
            options: displayModeOptions,
            initial_option:
              displayModeOptions.find(
                (option) => option.value === settings.defaultHomeDisplayMode,
              ) || displayModeOptions[0],
          },
        },
        {
          type: "input",
          block_id: "default_home_range",
          label: { type: "plain_text", text: "Home の既定表示範囲" },
          element: {
            type: "static_select",
            action_id: "value",
            options: homeRangeOptions,
            initial_option:
              homeRangeOptions.find(
                (option) => option.value === settings.defaultHomeRange,
              ) || homeRangeOptions[0],
          },
        },
        {
          type: "input",
          block_id: "default_home_state",
          label: { type: "plain_text", text: "Home の既定ステータス" },
          element: {
            type: "static_select",
            action_id: "value",
            options: homeStateOptions,
            initial_option:
              homeStateOptions.find(
                (option) => option.value === settings.defaultHomeState,
              ) || homeStateOptions[0],
          },
        },
        {
          type: "input",
          block_id: "reaction_taskify_enabled",
          label: { type: "plain_text", text: "リアクション起点タスク化" },
          element: {
            type: "static_select",
            action_id: "value",
            options: boolOptions,
            initial_option: initialBoolOption(settings.reactionTaskifyEnabled),
          },
        },
        {
          type: "input",
          block_id: "due_dm_notifications_enabled",
          label: { type: "plain_text", text: "期限通知 DM の既定値" },
          element: {
            type: "static_select",
            action_id: "value",
            options: boolOptions,
            initial_option: initialBoolOption(
              settings.dueDmNotificationsEnabled,
            ),
          },
        },
        {
          type: "input",
          block_id: "completion_dm_notifications_enabled",
          label: { type: "plain_text", text: "完了通知 DM の既定値" },
          element: {
            type: "static_select",
            action_id: "value",
            options: boolOptions,
            initial_option: initialBoolOption(
              settings.completionDmNotificationsEnabled,
            ),
          },
        },
        {
          type: "input",
          block_id: "comment_dm_notifications_enabled",
          label: { type: "plain_text", text: "コメント通知 DM の既定値" },
          element: {
            type: "static_select",
            action_id: "value",
            options: boolOptions,
            initial_option: initialBoolOption(
              settings.commentDmNotificationsEnabled,
            ),
          },
        },
        {
          type: "input",
          block_id: "overdue_channel_notifications_enabled",
          label: { type: "plain_text", text: "期限切れ一覧のチャンネル通知" },
          element: {
            type: "static_select",
            action_id: "value",
            options: boolOptions,
            initial_option: initialBoolOption(
              settings.overdueChannelNotificationsEnabled,
            ),
          },
        },
      ],
    };
  }

  // 自動タスク化キーワード（user_task_triggers）読み書き
  async function fetchTaskTriggers(teamId, userId) {
    try {
      const r = await dbQuery(
        `SELECT id, keyword, enabled FROM user_task_triggers WHERE team_id=$1 AND user_id=$2 ORDER BY created_at`,
        [teamId, userId]
      );
      return r.rows || [];
    } catch (e) { console.error('[settings] fetchTaskTriggers:', e.message); return []; }
  }
  async function saveTaskTriggers(teamId, userId, newKeywords) {
    try {
      const existing = await fetchTaskTriggers(teamId, userId);
      const existingMap = new Map(existing.map(t => [t.keyword, t]));
      const newSet = new Set(newKeywords);
      // 削除（既存にあるが新リストにないもの）
      const toDelete = existing.filter(t => !newSet.has(t.keyword)).map(t => t.id);
      if (toDelete.length > 0) {
        await dbQuery(`DELETE FROM user_task_triggers WHERE id = ANY($1::text[])`, [toDelete]);
      }
      // 追加（新リストにあるが既存にないもの）or 再有効化
      for (const kw of newKeywords) {
        const ex = existingMap.get(kw);
        if (!ex) {
          await dbQuery(
            `INSERT INTO user_task_triggers (team_id, user_id, keyword) VALUES ($1,$2,$3)
             ON CONFLICT (team_id, user_id, keyword) DO UPDATE SET enabled=true`,
            [teamId, userId, kw]
          );
        } else if (!ex.enabled) {
          await dbQuery(`UPDATE user_task_triggers SET enabled=true WHERE id=$1`, [ex.id]);
        }
      }
    } catch (e) { console.error('[settings] saveTaskTriggers:', e.message); }
  }

  async function openUserSettingsModal({ client, triggerId, teamId, userId }) {
    if (!client || !triggerId || !teamId || !userId) return;
    const [settings, projects, taskTriggers] = await Promise.all([
      getUserSettings(teamId, userId),
      dbListProjects(teamId),
      fetchTaskTriggers(teamId, userId),
    ]);
    await client.views.open({
      trigger_id: triggerId,
      view: buildUserSettingsModalView(teamId, userId, settings, { projects, taskTriggers }),
    });
  }

  async function openTeamSettingsModal({ client, triggerId, teamId, userId }) {
    if (!client || !triggerId || !teamId || !userId) return false;
    const allowed = await canManageTeamSettings(client, userId);
    if (!allowed) return false;

    const settings = await getTeamSettings(teamId);
    await client.views.open({
      trigger_id: triggerId,
      view: buildTeamSettingsModalView(teamId, userId, settings),
    });
    return true;
  }

  app.command(USER_SETTINGS_COMMAND, async ({ ack, body, client }) => {
    await ack();

    const teamId = getTeamIdFromBody(body);
    const userId = getUserIdFromBody(body);
    if (!teamId || !userId) return;

    const [settings, projects, taskTriggers] = await Promise.all([
      getUserSettings(teamId, userId),
      dbListProjects(teamId),
      fetchTaskTriggers(teamId, userId),
    ]);
    await client.views.open({
      trigger_id: body.trigger_id,
      view: buildUserSettingsModalView(teamId, userId, settings, { projects, taskTriggers }),
    });
  });

  app.command(TEAM_SETTINGS_COMMAND, async ({ ack, body, client }) => {
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
        text: "チーム設定はワークスペース管理者のみ変更できます。",
      });
      return;
    }

    await ack();
    const settings = await getTeamSettings(teamId);
    await client.views.open({
      trigger_id: body.trigger_id,
      view: buildTeamSettingsModalView(teamId, userId, settings),
    });
  });

  app.view("user_settings_modal", async ({ ack, body, view, client }) => {
    const meta = safeJsonParse(view.private_metadata || "{}") || {};
    const teamId = meta.teamId || getTeamIdFromBody(body);
    const userId = meta.userId || getUserIdFromBody(body);
    if (!teamId || !userId) {
      await ack();
      return;
    }

    const nextSettings = normalizeUserSettings({
      homeDisplayMode: "inherit",
      homeRange: "inherit",
      homeState: "inherit",
      dueDmNotificationsEnabled: "true",
      dueRequesterDmNotificationsEnabled:
        view.state.values.due_requester_dm_notifications?.value?.selected_option
          ?.value || "true",
      dueNotificationSchedule:
        view.state.values.due_notification_schedule?.value?.selected_option?.value || "morning_only",
      completionDmNotificationsEnabled:
        view.state.values.completion_dm_notifications?.value?.selected_option
          ?.value || "true",
      commentDmNotificationsEnabled:
        view.state.values.comment_dm_notifications?.value?.selected_option
          ?.value || "true",
      homeProjectFilter: (() => {
        const v = view.state.values.home_project_filter?.value?.selected_option?.value;
        return v === "__none__" ? "" : (v || "");
      })(),
      homeDashTeamFilter: (() => {
        const v = view.state.values.home_dash_team_filter?.value?.selected_option?.value;
        return v === "__none__" ? "" : (v || "");
      })(),
    });

    await dbUpsertUserSettings(teamId, userId, nextSettings);
    invalidateSettingsCache(teamId, userId);

    // 自動タスク化キーワードを差分更新
    try {
      const raw = view.state.values.task_trigger_keywords?.value?.value || '';
      const keywords = raw.split(/\r?\n/)
        .map(s => s.trim())
        .filter(s => s.length > 0 && s.length <= 50);
      // 重複除去
      const unique = [...new Set(keywords)];
      await saveTaskTriggers(teamId, userId, unique);
    } catch (e) { console.error('[settings] task trigger save error:', e.message); }

    const resolved = await resolveHomeDefaults(teamId, userId);
    setHomeState(teamId, userId, {
      displayMode: resolved.displayMode,
      broadcastScopeKey: resolved.rangeKey,
      personalScopeKey: resolved.rangeKey,
      scopeKey: resolved.scopeKey,
    });

    await ack({
      response_action: "update",
      view: {
        type: "modal",
        callback_id: "user_settings_saved",
        title: { type: "plain_text", text: "個人設定" },
        close: { type: "plain_text", text: "閉じる" },
        blocks: [
          {
            type: "section",
            text: { type: "mrkdwn", text: "個人設定を保存しました。" },
          },
        ],
      },
    });

    try {
      await publishHome({ client, teamId, userId });
    } catch (_) {}
  });

  app.view("team_settings_modal", async ({ ack, body, view, client }) => {
    const meta = safeJsonParse(view.private_metadata || "{}") || {};
    const teamId = meta.teamId || getTeamIdFromBody(body);
    const userId = meta.userId || getUserIdFromBody(body);
    if (!teamId || !userId) {
      await ack();
      return;
    }

    if (!(await canManageTeamSettings(client, userId))) {
      await ack({
        response_action: "errors",
        errors: {
          default_home_display_mode: "チーム設定を変更できる権限がありません。",
        },
      });
      return;
    }

    const nextSettings = normalizeTeamSettings({
      defaultHomeDisplayMode:
        view.state.values.default_home_display_mode?.value?.selected_option
          ?.value,
      defaultHomeRange:
        view.state.values.default_home_range?.value?.selected_option?.value,
      defaultHomeState:
        view.state.values.default_home_state?.value?.selected_option?.value,
      reactionTaskifyEnabled:
        view.state.values.reaction_taskify_enabled?.value?.selected_option
          ?.value,
      dueDmNotificationsEnabled:
        view.state.values.due_dm_notifications_enabled?.value?.selected_option
          ?.value,
      completionDmNotificationsEnabled:
        view.state.values.completion_dm_notifications_enabled?.value
          ?.selected_option?.value,
      commentDmNotificationsEnabled:
        view.state.values.comment_dm_notifications_enabled?.value
          ?.selected_option?.value,
      overdueChannelNotificationsEnabled:
        view.state.values.overdue_channel_notifications_enabled?.value
          ?.selected_option?.value,
    });

    await dbUpsertTeamSettings(teamId, nextSettings, userId);
    invalidateSettingsCache(teamId);

    await ack({
      response_action: "update",
      view: {
        type: "modal",
        callback_id: "team_settings_saved",
        title: { type: "plain_text", text: "チーム設定" },
        close: { type: "plain_text", text: "閉じる" },
        blocks: [
          {
            type: "section",
            text: { type: "mrkdwn", text: "チーム設定を保存しました。" },
          },
        ],
      },
    });
  });

  return {
    canManageTeamSettings,
    getTeamSettings,
    getUserSettings,
    getUserDueSchedule,
    isOverdueChannelNotificationEnabled,
    isReactionTaskifyEnabled,
    isUserDmEnabled,
    openTeamSettingsModal,
    openUserSettingsModal,
    resolveHomeDefaults,
  };
}

module.exports = {
  registerSettingsFeature,
};
