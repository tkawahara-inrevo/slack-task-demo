const { Pool } = require("pg");
const { randomUUID } = require("crypto");
const { uniqIds } = require("../utils/common");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: parseInt(process.env.DB_POOL_SIZE, 10) || 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 3000,
  ssl: process.env.PGSSL === "true" ? { rejectUnauthorized: false } : undefined,
});

async function dbQuery(text, params) {
  const isTransientPgError = (e) => {
    const code = e?.code;
    return (
      code === "ECONNRESET" ||
      code === "ETIMEDOUT" ||
      code === "ECONNREFUSED" ||
      code === "EPIPE"
    );
  };

  let lastErr = null;

  for (let attempt = 0; attempt < 2; attempt++) {
    const client = await pool.connect();
    try {
      return await client.query(text, params);
    } catch (e) {
      lastErr = e;
      if (!isTransientPgError(e) || attempt === 1) {
        throw e;
      }
      await new Promise((r) => setTimeout(r, 150));
    } finally {
      client.release();
    }
  }

  throw lastErr;
}

async function dbGetUserDept(teamId, userId) {
  const q = `
    SELECT team_id, user_id, dept_key, dept_handle, updated_at
    FROM user_departments
    WHERE user_id=$1
    ORDER BY (team_id=$2) DESC, updated_at DESC
    LIMIT 1;
  `;
  const res = await dbQuery(q, [userId, teamId]);
  return res.rows[0] || null;
}

async function dbUpsertUserDept(teamId, userId, dept_key, dept_handle) {
  const q = `
    INSERT INTO user_departments (team_id, user_id, dept_key, dept_handle, updated_at)
    VALUES ($1,$2,$3,$4, now())
    ON CONFLICT (user_id)
    DO UPDATE SET team_id=EXCLUDED.team_id, dept_key=EXCLUDED.dept_key, dept_handle=EXCLUDED.dept_handle, updated_at=now()
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, userId, dept_key, dept_handle]);
  return res.rows[0] || null;
}

async function dbEnsureSettingsSchema() {
  await Promise.all([
    dbQuery(`
      CREATE TABLE IF NOT EXISTS team_settings (
        team_id TEXT PRIMARY KEY,
        settings JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_by_user_id TEXT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS user_settings (
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        settings JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (team_id, user_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS notification_threads (
        team_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        parent_ts TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (team_id, channel_id, kind)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS dashboard_roles (
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'member',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (team_id, user_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS dash_teams (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        name TEXT NOT NULL,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS dash_team_members (
        dash_team_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (dash_team_id, user_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS projects (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        name TEXT NOT NULL,
        dash_team_id TEXT,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS project_tasks (
        project_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (project_id, task_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS personal_filters (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        owner_user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS personal_filter_members (
        filter_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        PRIMARY KEY (filter_id, user_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS integrations (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        service_type TEXT NOT NULL,
        name TEXT NOT NULL,
        config JSONB NOT NULL DEFAULT '{}'::jsonb,
        enabled BOOLEAN NOT NULL DEFAULT false,
        last_synced_at TIMESTAMPTZ,
        last_sync_status TEXT,
        last_sync_message TEXT,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS integration_field_mappings (
        id TEXT PRIMARY KEY,
        integration_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        local_field TEXT NOT NULL,
        remote_field TEXT NOT NULL,
        direction TEXT NOT NULL DEFAULT 'both',
        transform JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS integration_sync_log (
        id TEXT PRIMARY KEY,
        integration_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        direction TEXT NOT NULL,
        status TEXT NOT NULL,
        records_processed INT DEFAULT 0,
        records_created INT DEFAULT 0,
        records_updated INT DEFAULT 0,
        records_failed INT DEFAULT 0,
        error_detail TEXT,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at TIMESTAMPTZ
      );
    `),
  ]);

  // 初期admin を設定（存在しなければ）
  const INITIAL_ADMIN_ID = process.env.DASHBOARD_ADMIN_USER_ID || "U0A6JPMKVRR";
  if (INITIAL_ADMIN_ID) {
    const teamsRes = await dbQuery(
      `SELECT DISTINCT team_id FROM tasks WHERE team_id IS NOT NULL LIMIT 1;`,
    );
    const firstTeam = teamsRes.rows[0]?.team_id;
    if (firstTeam) {
      await dbQuery(
        `INSERT INTO dashboard_roles (team_id, user_id, role) VALUES ($1, $2, 'admin')
         ON CONFLICT (team_id, user_id) DO NOTHING;`,
        [firstTeam, INITIAL_ADMIN_ID],
      );
    }
  }
}

function normalizeSettingsObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

async function dbGetTeamSettings(teamId) {
  const q = `
    SELECT team_id, settings, updated_by_user_id, updated_at
    FROM team_settings
    WHERE team_id=$1
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId]);
  const row = res.rows[0] || null;
  if (!row) return null;
  return {
    ...row,
    settings: normalizeSettingsObject(row.settings),
  };
}

async function dbUpsertTeamSettings(teamId, settings, updatedByUserId = null) {
  const q = `
    INSERT INTO team_settings (team_id, settings, updated_by_user_id, updated_at)
    VALUES ($1, $2::jsonb, $3, now())
    ON CONFLICT (team_id)
    DO UPDATE SET
      settings = EXCLUDED.settings,
      updated_by_user_id = EXCLUDED.updated_by_user_id,
      updated_at = now()
    RETURNING team_id, settings, updated_by_user_id, updated_at;
  `;
  const res = await dbQuery(q, [
    teamId,
    JSON.stringify(normalizeSettingsObject(settings)),
    updatedByUserId,
  ]);
  const row = res.rows[0] || null;
  if (!row) return null;
  return {
    ...row,
    settings: normalizeSettingsObject(row.settings),
  };
}

async function dbGetUserSettings(teamId, userId) {
  const q = `
    SELECT team_id, user_id, settings, updated_at
    FROM user_settings
    WHERE team_id=$1 AND user_id=$2
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId, userId]);
  const row = res.rows[0] || null;
  if (!row) return null;
  return {
    ...row,
    settings: normalizeSettingsObject(row.settings),
  };
}

async function dbUpsertUserSettings(teamId, userId, settings) {
  const q = `
    INSERT INTO user_settings (team_id, user_id, settings, updated_at)
    VALUES ($1, $2, $3::jsonb, now())
    ON CONFLICT (team_id, user_id)
    DO UPDATE SET
      settings = EXCLUDED.settings,
      updated_at = now()
    RETURNING team_id, user_id, settings, updated_at;
  `;
  const res = await dbQuery(q, [
    teamId,
    userId,
    JSON.stringify(normalizeSettingsObject(settings)),
  ]);
  const row = res.rows[0] || null;
  if (!row) return null;
  return {
    ...row,
    settings: normalizeSettingsObject(row.settings),
  };
}

async function dbGetNotificationThread(teamId, channelId, kind) {
  const q = `
    SELECT team_id, channel_id, kind, parent_ts, updated_at
    FROM notification_threads
    WHERE team_id=$1 AND channel_id=$2 AND kind=$3
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId, channelId, kind]);
  return res.rows[0] || null;
}

async function dbUpsertNotificationThread(teamId, channelId, kind, parentTs) {
  const q = `
    INSERT INTO notification_threads (team_id, channel_id, kind, parent_ts, updated_at)
    VALUES ($1, $2, $3, $4, now())
    ON CONFLICT (team_id, channel_id, kind)
    DO UPDATE SET
      parent_ts = EXCLUDED.parent_ts,
      updated_at = now()
    RETURNING team_id, channel_id, kind, parent_ts, updated_at;
  `;
  const res = await dbQuery(q, [teamId, channelId, kind, parentTs]);
  return res.rows[0] || null;
}

async function dbCreateTask(task) {
  const q = `
    INSERT INTO tasks (
      id, team_id, channel_id, message_ts, source_permalink,
      title, description,
      requester_user_id, created_by_user_id,
      assignee_id, assignee_label,
      status, due_date,
      requester_dept, assignee_dept,
      task_type, broadcast_group_handle, broadcast_group_id,
      total_count, completed_count,
      notified_at,
      created_at, updated_at
    ) VALUES (
      $1,$2,$3,$4,$5,
      $6,$7,
      $8,$9,
      $10,$11,
      $12,$13,
      $14,$15,
      $16,$17,$18,
      $19,$20,
      $21,
      now(), now()
    )
    RETURNING *;
  `;
  const params = [
    task.id,
    task.team_id,
    task.channel_id,
    task.message_ts,
    task.source_permalink,
    task.title,
    task.description,
    task.requester_user_id,
    task.created_by_user_id,
    task.assignee_id ?? null,
    task.assignee_label ?? null,
    task.status,
    task.due_date,
    task.requester_dept ?? null,
    task.assignee_dept ?? null,
    task.task_type ?? "personal",
    task.broadcast_group_handle ?? null,
    task.broadcast_group_id ?? null,
    task.total_count ?? null,
    task.completed_count ?? 0,
    task.notified_at ?? null,
  ];
  const res = await dbQuery(q, params);
  return res.rows[0];
}

async function dbGetTaskById(teamId, taskId) {
  const q = `SELECT * FROM tasks WHERE team_id=$1 AND id=$2 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0] || null;
}

async function dbGetTaskBySource(teamId, channelId, messageTs) {
  const q = `
    SELECT *
    FROM tasks
    WHERE team_id=$1
      AND channel_id=$2
      AND message_ts=$3
    ORDER BY created_at DESC
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId, channelId, messageTs]);
  return res.rows[0] || null;
}

async function dbUpdateStatus(teamId, taskId, status) {
  const q = `
    UPDATE tasks
    SET status=$3,
        completed_at = CASE WHEN $3='done' THEN now() ELSE completed_at END,
        updated_at = now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, taskId, status]);
  return res.rows[0] || null;
}

async function dbUpdateTaskContent(teamId, taskId, patch) {
  const q = `
    UPDATE tasks
    SET
      assignee_id = COALESCE($3, assignee_id),
      assignee_dept = COALESCE($4, assignee_dept),
      due_date = $5,
      description = COALESCE($6, description),
      updated_at = now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [
    teamId,
    taskId,
    patch?.assignee_id ?? null,
    patch?.assignee_dept ?? null,
    patch?.due_date ?? null,
    patch?.description ?? null,
  ]);
  return res.rows[0] || null;
}

async function dbUpdateBroadcastCounts(
  teamId,
  taskId,
  completedCount,
  totalCount,
) {
  const q = `
    UPDATE tasks
    SET completed_count=$3,
        total_count = COALESCE(total_count, $4),
        updated_at=now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [
    teamId,
    taskId,
    completedCount,
    totalCount ?? null,
  ]);
  return res.rows[0] || null;
}

async function dbInsertTaskTargets(teamId, taskId, userIds) {
  if (!userIds?.length) return;
  const values = [];
  const params = [];
  let i = 1;
  for (const uid of userIds) {
    params.push(taskId, teamId, uid);
    values.push(`($${i++},$${i++},$${i++})`);
  }
  const q = `
    INSERT INTO task_targets (task_id, team_id, user_id)
    VALUES ${values.join(",")}
    ON CONFLICT (task_id, user_id) DO NOTHING;
  `;
  await dbQuery(q, params);
}

async function dbIsUserTarget(teamId, taskId, userId) {
  const q = `SELECT 1 FROM task_targets WHERE team_id=$1 AND task_id=$2 AND user_id=$3 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId, userId]);
  return !!res.rows[0];
}

async function dbHasUserCompleted(teamId, taskId, userId) {
  const q = `SELECT 1 FROM task_completions WHERE team_id=$1 AND task_id=$2 AND user_id=$3 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, taskId, userId]);
  return !!res.rows[0];
}

// taskIds[] に対して一括で「このユーザーが完了済みか」を返す → Set<taskId>
async function dbGetUserCompletedTaskIds(teamId, taskIds, userId) {
  if (!taskIds || !taskIds.length) return new Set();
  const q = `SELECT task_id FROM task_completions WHERE team_id=$1 AND user_id=$2 AND task_id = ANY($3)`;
  const res = await dbQuery(q, [teamId, userId, taskIds]);
  return new Set(res.rows.map((r) => r.task_id));
}

async function dbUpsertCompletion(teamId, taskId, userId) {
  const q = `
    INSERT INTO task_completions (task_id, team_id, user_id)
    VALUES ($1,$2,$3)
    ON CONFLICT (task_id, user_id) DO NOTHING;
  `;
  await dbQuery(q, [taskId, teamId, userId]);
}

async function dbCountTargets(teamId, taskId) {
  const q = `SELECT COUNT(*)::int AS c FROM task_targets WHERE team_id=$1 AND task_id=$2;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0]?.c ?? 0;
}

async function dbCountCompletions(teamId, taskId) {
  const q = `SELECT COUNT(*)::int AS c FROM task_completions WHERE team_id=$1 AND task_id=$2;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return res.rows[0]?.c ?? 0;
}

async function dbListTargetUserIds(teamId, taskId) {
  const q = `SELECT user_id FROM task_targets WHERE team_id=$1 AND task_id=$2;`;
  const res = await dbQuery(q, [teamId, taskId]);
  return (res.rows || []).map((r) => r.user_id).filter(Boolean);
}

async function dbDeleteTaskTargets(teamId, taskId) {
  const q = `DELETE FROM task_targets WHERE team_id=$1 AND task_id=$2;`;
  await dbQuery(q, [teamId, taskId]);
}

async function dbPruneTaskCompletionsByTargets(teamId, taskId) {
  const q = `
    DELETE FROM task_completions tc
    WHERE tc.team_id = $1
      AND tc.task_id = $2
      AND NOT EXISTS (
        SELECT 1
        FROM task_targets tt
        WHERE tt.team_id = tc.team_id
          AND tt.task_id = tc.task_id
          AND tt.user_id = tc.user_id
      );
  `;
  await dbQuery(q, [teamId, taskId]);
}

async function dbReplaceTaskTargets(teamId, taskId, userIds) {
  await dbDeleteTaskTargets(teamId, taskId);
  await dbInsertTaskTargets(teamId, taskId, uniqIds(userIds));
  await dbPruneTaskCompletionsByTargets(teamId, taskId);
}

async function dbUpdateTaskEditableFields(teamId, taskId, patch) {
  const q = `
    UPDATE tasks
    SET
      task_type = COALESCE($3, task_type),
      assignee_id = $4,
      assignee_label = $5,
      assignee_dept = $6,
      due_date = $7,
      description = $8,
      broadcast_group_handle = $9,
      broadcast_group_id = $10,
      total_count = $11,
      completed_count = $12,
      updated_at = now()
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [
    teamId,
    taskId,
    patch?.task_type ?? null,
    patch?.assignee_id ?? null,
    patch?.assignee_label ?? null,
    patch?.assignee_dept ?? null,
    patch?.due_date ?? null,
    patch?.description ?? null,
    patch?.broadcast_group_handle ?? null,
    patch?.broadcast_group_id ?? null,
    patch?.total_count ?? null,
    patch?.completed_count ?? 0,
  ]);
  return res.rows[0] || null;
}

async function dbGetThreadCard(teamId, channelId, parentTs) {
  const q = `
    SELECT * FROM thread_cards
    WHERE team_id=$1 AND channel_id=$2 AND parent_ts=$3
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId, channelId, parentTs]);
  return res.rows[0] || null;
}

async function dbUpsertThreadCard(teamId, channelId, parentTs, cardTs) {
  const existing = await dbGetThreadCard(teamId, channelId, parentTs);
  if (existing) {
    const q = `
      UPDATE thread_cards
      SET card_ts=$4, updated_at=now()
      WHERE team_id=$1 AND channel_id=$2 AND parent_ts=$3
      RETURNING *;
    `;
    const res = await dbQuery(q, [teamId, channelId, parentTs, cardTs]);
    return res.rows[0];
  }

  const q = `
    INSERT INTO thread_cards (id, team_id, channel_id, parent_ts, card_ts, updated_at)
    VALUES ($1,$2,$3,$4,$5, now())
    RETURNING *;
  `;
  const res = await dbQuery(q, [
    randomUUID(),
    teamId,
    channelId,
    parentTs,
    cardTs,
  ]);
  return res.rows[0];
}

async function dbListBroadcastTasksByStatuses(
  teamId,
  statuses,
  deptKey = "all",
  limit = 30,
) {
  const params = [teamId, statuses, limit];
  let whereDept = "";
  if (deptKey && deptKey !== "all") {
    if (deptKey === "__none__") {
      whereDept = "AND t.requester_dept IS NULL";
    } else {
      whereDept = "AND t.requester_dept = $4";
      params.push(deptKey);
    }
  }
  const q = `
    SELECT t.*
    FROM tasks t
    WHERE t.team_id=$1
      AND t.task_type='broadcast'
      AND t.status = ANY($2::text[])
      ${whereDept}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbListBroadcastTasksByStatusesWithScope(
  teamId,
  statuses,
  scopeKey,
  viewerUserId,
  limit = 30,
) {
  const params = [teamId, statuses, limit];
  let joinTargets = "";
  let whereScope = "";

  const wantsDoneView = (statuses || []).includes("done");
  const wantsNotCompleted = !wantsDoneView;

  let joinCompletions = "";
  let whereNotCompleted = "";
  let whereStatus = "AND t.status = ANY($2::text[])";

  if (scopeKey === "to_me") {
    joinTargets =
      "JOIN task_targets tt ON tt.task_id::text = t.id AND tt.team_id=t.team_id";
    whereScope = "AND tt.user_id = $4";
    params.push(viewerUserId);

    if (wantsNotCompleted) {
      joinCompletions =
        "LEFT JOIN task_completions tc ON tc.task_id::text = t.id AND tc.team_id=t.team_id AND tc.user_id = $4";
      whereNotCompleted = "AND tc.user_id IS NULL";
    }

    if (wantsDoneView) {
      joinCompletions =
        "LEFT JOIN task_completions tc ON tc.task_id::text = t.id AND tc.team_id=t.team_id AND tc.user_id = $4";
      whereStatus =
        "AND (t.status = ANY($2::text[]) OR tc.user_id IS NOT NULL)";
    }
  } else if (scopeKey === "requested_by_me") {
    whereScope = "AND t.requester_user_id = $4";
    params.push(viewerUserId);
  }

  const q = `
    SELECT x.*
    FROM (
      SELECT DISTINCT ON (t.id) t.*
      FROM tasks t
      ${joinTargets}
      ${joinCompletions}
      WHERE t.team_id=$1
        AND t.task_type='broadcast'
        ${whereStatus}
        ${whereScope}
        ${whereNotCompleted}
      ORDER BY
        t.id,
        (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    ) x
    ORDER BY (x.due_date IS NULL) ASC, x.due_date ASC, x.created_at DESC
    LIMIT $3;
  `;

  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbListPersonalTasksByStatusesWithScope(
  teamId,
  statuses,
  scopeKey,
  viewerUserId,
  limit = 60,
) {
  const params = [teamId, statuses, limit];
  let whereScope = "";

  if (scopeKey === "to_me") {
    whereScope = "AND t.assignee_id = $4";
    params.push(viewerUserId);
  } else if (scopeKey === "requested_by_me") {
    whereScope = "AND t.requester_user_id = $4";
    params.push(viewerUserId);
  }

  const q = `
    SELECT t.*
    FROM tasks t
    WHERE t.team_id=$1
      AND (t.task_type IS NULL OR t.task_type='personal')
      AND t.status = ANY($2::text[])
      ${whereScope}
    ORDER BY (t.due_date IS NULL) ASC, t.due_date ASC, t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbListTaskComments(teamId, taskId, limit = 10) {
  const q = `
    SELECT user_id, comment, created_at
    FROM task_comments
    WHERE team_id=$1 AND task_id=$2
    ORDER BY created_at ASC
    LIMIT $3;
  `;
  const res = await dbQuery(q, [teamId, taskId, limit]);
  return res.rows || [];
}

async function dbInsertTaskComment(teamId, taskId, userId, comment) {
  const q = `
    INSERT INTO task_comments (id, team_id, task_id, user_id, comment)
    VALUES ($1,$2,$3,$4,$5);
  `;
  await dbQuery(q, [
    randomUUID(),
    teamId,
    taskId,
    userId,
    String(comment || "").trim(),
  ]);
}

// ================================
// Dashboard roles
// ================================
async function dbGetDashboardRole(teamId, userId) {
  const q = `SELECT role FROM dashboard_roles WHERE team_id=$1 AND user_id=$2 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, userId]);
  return res.rows[0]?.role || "member";
}

async function dbSetDashboardRole(teamId, userId, role) {
  const q = `
    INSERT INTO dashboard_roles (team_id, user_id, role, updated_at)
    VALUES ($1, $2, $3, now())
    ON CONFLICT (team_id, user_id)
    DO UPDATE SET role = EXCLUDED.role, updated_at = now()
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, userId, role]);
  return res.rows[0] || null;
}

async function dbListDashboardAdmins(teamId) {
  const q = `SELECT user_id, role, updated_at FROM dashboard_roles WHERE team_id=$1 AND role='admin';`;
  const res = await dbQuery(q, [teamId]);
  return res.rows;
}

// ================================
// Dash teams
// ================================
async function dbCreateDashTeam(id, teamId, name, createdBy) {
  const q = `
    INSERT INTO dash_teams (id, team_id, name, created_by, created_at)
    VALUES ($1, $2, $3, $4, now())
    RETURNING *;
  `;
  const res = await dbQuery(q, [id, teamId, name, createdBy]);
  return res.rows[0];
}

async function dbListDashTeams(teamId) {
  const q = `SELECT * FROM dash_teams WHERE team_id=$1 ORDER BY created_at ASC;`;
  const res = await dbQuery(q, [teamId]);
  return res.rows;
}

async function dbGetDashTeam(teamId, dashTeamId) {
  const q = `SELECT * FROM dash_teams WHERE team_id=$1 AND id=$2 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, dashTeamId]);
  return res.rows[0] || null;
}

async function dbDeleteDashTeam(teamId, dashTeamId) {
  await dbQuery(`DELETE FROM dash_team_members WHERE team_id=$1 AND dash_team_id=$2;`, [teamId, dashTeamId]);
  await dbQuery(`DELETE FROM dash_teams WHERE team_id=$1 AND id=$2;`, [teamId, dashTeamId]);
}

async function dbUpdateDashTeam(teamId, dashTeamId, name) {
  const q = `UPDATE dash_teams SET name=$3 WHERE team_id=$1 AND id=$2 RETURNING *;`;
  const res = await dbQuery(q, [teamId, dashTeamId, name]);
  return res.rows[0] || null;
}

// ================================
// Dash team members
// ================================
async function dbAddDashTeamMember(teamId, dashTeamId, userId) {
  const q = `
    INSERT INTO dash_team_members (dash_team_id, team_id, user_id, added_at)
    VALUES ($1, $2, $3, now())
    ON CONFLICT (dash_team_id, user_id) DO NOTHING
    RETURNING *;
  `;
  const res = await dbQuery(q, [dashTeamId, teamId, userId]);
  return res.rows[0] || null;
}

async function dbRemoveDashTeamMember(teamId, dashTeamId, userId) {
  await dbQuery(
    `DELETE FROM dash_team_members WHERE team_id=$1 AND dash_team_id=$2 AND user_id=$3;`,
    [teamId, dashTeamId, userId],
  );
}

async function dbListDashTeamMembers(teamId, dashTeamId) {
  const q = `SELECT user_id, added_at FROM dash_team_members WHERE team_id=$1 AND dash_team_id=$2 ORDER BY added_at ASC;`;
  const res = await dbQuery(q, [teamId, dashTeamId]);
  return res.rows;
}

async function dbGetUserDashTeams(teamId, userId) {
  const q = `
    SELECT dt.*
    FROM dash_teams dt
    JOIN dash_team_members dtm ON dtm.dash_team_id = dt.id AND dtm.team_id = dt.team_id
    WHERE dt.team_id = $1 AND dtm.user_id = $2
    ORDER BY dt.name ASC;
  `;
  const res = await dbQuery(q, [teamId, userId]);
  return res.rows;
}

// ================================
// Projects
// ================================
async function dbCreateProject(id, teamId, name, dashTeamId, createdBy) {
  const q = `
    INSERT INTO projects (id, team_id, name, dash_team_id, created_by, created_at)
    VALUES ($1, $2, $3, $4, $5, now())
    RETURNING *;
  `;
  const res = await dbQuery(q, [id, teamId, name, dashTeamId || null, createdBy]);
  return res.rows[0];
}

async function dbListProjects(teamId) {
  const q = `SELECT * FROM projects WHERE team_id=$1 ORDER BY created_at ASC;`;
  const res = await dbQuery(q, [teamId]);
  return res.rows;
}

async function dbGetProject(teamId, projectId) {
  const q = `SELECT * FROM projects WHERE team_id=$1 AND id=$2 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, projectId]);
  return res.rows[0] || null;
}

async function dbDeleteProject(teamId, projectId) {
  await dbQuery(`DELETE FROM project_tasks WHERE team_id=$1 AND project_id=$2;`, [teamId, projectId]);
  await dbQuery(`DELETE FROM projects WHERE team_id=$1 AND id=$2;`, [teamId, projectId]);
}

async function dbUpdateProject(teamId, projectId, patch) {
  const q = `
    UPDATE projects SET name=COALESCE($3, name), dash_team_id=$4
    WHERE team_id=$1 AND id=$2 RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, projectId, patch.name, patch.dash_team_id ?? null]);
  return res.rows[0] || null;
}

// ================================
// Project tasks
// ================================
async function dbAddProjectTask(teamId, projectId, taskId) {
  const q = `
    INSERT INTO project_tasks (project_id, task_id, team_id, added_at)
    VALUES ($1, $2, $3, now())
    ON CONFLICT (project_id, task_id) DO NOTHING;
  `;
  await dbQuery(q, [projectId, taskId, teamId]);
}

async function dbRemoveProjectTask(teamId, projectId, taskId) {
  await dbQuery(
    `DELETE FROM project_tasks WHERE team_id=$1 AND project_id=$2 AND task_id=$3;`,
    [teamId, projectId, taskId],
  );
}

async function dbListProjectTasks(teamId, projectId, limit = 200) {
  const q = `
    SELECT t.*
    FROM tasks t
    JOIN project_tasks pt ON pt.task_id = t.id AND pt.team_id = t.team_id
    WHERE pt.team_id=$1 AND pt.project_id=$2
    ORDER BY t.created_at DESC
    LIMIT $3;
  `;
  const res = await dbQuery(q, [teamId, projectId, limit]);
  return res.rows;
}

// ================================
// Integrations
// ================================
async function dbCreateIntegration(teamId, { service_type, name, config, created_by }) {
  const id = randomUUID();
  const q = `
    INSERT INTO integrations (id, team_id, service_type, name, config, created_by)
    VALUES ($1, $2, $3, $4, $5::jsonb, $6)
    RETURNING *;
  `;
  const res = await dbQuery(q, [id, teamId, service_type, name, JSON.stringify(config || {}), created_by]);
  return res.rows[0];
}

async function dbListIntegrations(teamId) {
  const q = `SELECT * FROM integrations WHERE team_id=$1 ORDER BY created_at DESC;`;
  const res = await dbQuery(q, [teamId]);
  return res.rows;
}

async function dbGetIntegration(teamId, integrationId) {
  const q = `SELECT * FROM integrations WHERE team_id=$1 AND id=$2 LIMIT 1;`;
  const res = await dbQuery(q, [teamId, integrationId]);
  return res.rows[0] || null;
}

async function dbUpdateIntegration(teamId, integrationId, patch) {
  const sets = [];
  const params = [teamId, integrationId];
  let idx = 3;
  if (patch.name !== undefined) { sets.push(`name=$${idx++}`); params.push(patch.name); }
  if (patch.config !== undefined) { sets.push(`config=$${idx++}::jsonb`); params.push(JSON.stringify(patch.config)); }
  if (patch.enabled !== undefined) { sets.push(`enabled=$${idx++}`); params.push(patch.enabled); }
  if (patch.last_synced_at !== undefined) { sets.push(`last_synced_at=$${idx++}`); params.push(patch.last_synced_at); }
  if (patch.last_sync_status !== undefined) { sets.push(`last_sync_status=$${idx++}`); params.push(patch.last_sync_status); }
  if (patch.last_sync_message !== undefined) { sets.push(`last_sync_message=$${idx++}`); params.push(patch.last_sync_message); }
  if (sets.length === 0) return dbGetIntegration(teamId, integrationId);
  sets.push("updated_at=now()");
  const q = `UPDATE integrations SET ${sets.join(",")} WHERE team_id=$1 AND id=$2 RETURNING *;`;
  const res = await dbQuery(q, params);
  return res.rows[0] || null;
}

async function dbDeleteIntegration(teamId, integrationId) {
  await dbQuery(`DELETE FROM integration_field_mappings WHERE team_id=$1 AND integration_id=$2;`, [teamId, integrationId]);
  await dbQuery(`DELETE FROM integration_sync_log WHERE team_id=$1 AND integration_id=$2;`, [teamId, integrationId]);
  await dbQuery(`DELETE FROM integrations WHERE team_id=$1 AND id=$2;`, [teamId, integrationId]);
}

// Field mappings
async function dbListFieldMappings(teamId, integrationId) {
  const q = `SELECT * FROM integration_field_mappings WHERE team_id=$1 AND integration_id=$2 ORDER BY created_at ASC;`;
  const res = await dbQuery(q, [teamId, integrationId]);
  return res.rows;
}

async function dbSetFieldMappings(teamId, integrationId, mappings) {
  await dbQuery(`DELETE FROM integration_field_mappings WHERE team_id=$1 AND integration_id=$2;`, [teamId, integrationId]);
  for (const m of mappings) {
    const id = randomUUID();
    await dbQuery(
      `INSERT INTO integration_field_mappings (id, integration_id, team_id, local_field, remote_field, direction, transform)
       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb);`,
      [id, integrationId, teamId, m.local_field, m.remote_field, m.direction || "both", JSON.stringify(m.transform || null)],
    );
  }
  return dbListFieldMappings(teamId, integrationId);
}

// Sync log
async function dbCreateSyncLog(teamId, integrationId, direction) {
  const id = randomUUID();
  await dbQuery(
    `INSERT INTO integration_sync_log (id, integration_id, team_id, direction, status) VALUES ($1, $2, $3, $4, 'running');`,
    [id, integrationId, teamId, direction],
  );
  return id;
}

async function dbUpdateSyncLog(logId, patch) {
  const sets = [];
  const params = [logId];
  let idx = 2;
  if (patch.status !== undefined) { sets.push(`status=$${idx++}`); params.push(patch.status); }
  if (patch.records_processed !== undefined) { sets.push(`records_processed=$${idx++}`); params.push(patch.records_processed); }
  if (patch.records_created !== undefined) { sets.push(`records_created=$${idx++}`); params.push(patch.records_created); }
  if (patch.records_updated !== undefined) { sets.push(`records_updated=$${idx++}`); params.push(patch.records_updated); }
  if (patch.records_failed !== undefined) { sets.push(`records_failed=$${idx++}`); params.push(patch.records_failed); }
  if (patch.error_detail !== undefined) { sets.push(`error_detail=$${idx++}`); params.push(patch.error_detail); }
  if (patch.finished_at !== undefined) { sets.push(`finished_at=$${idx++}`); params.push(patch.finished_at); }
  if (sets.length === 0) return;
  await dbQuery(`UPDATE integration_sync_log SET ${sets.join(",")} WHERE id=$1;`, params);
}

async function dbListSyncLogs(teamId, integrationId, limit = 20) {
  const q = `SELECT * FROM integration_sync_log WHERE team_id=$1 AND integration_id=$2 ORDER BY started_at DESC LIMIT $3;`;
  const res = await dbQuery(q, [teamId, integrationId, limit]);
  return res.rows;
}

// ================================
// Personal filters (Slack Home個人フィルタ)
// ================================
async function dbCreatePersonalFilter(teamId, ownerUserId, id, name) {
  await dbQuery(
    `INSERT INTO personal_filters (id, team_id, owner_user_id, name) VALUES ($1, $2, $3, $4)`,
    [id, teamId, ownerUserId, name],
  );
  return { id, name };
}

async function dbListPersonalFilters(teamId, ownerUserId) {
  const res = await dbQuery(
    `SELECT id, name, created_at FROM personal_filters WHERE team_id=$1 AND owner_user_id=$2 ORDER BY created_at ASC`,
    [teamId, ownerUserId],
  );
  return res.rows || [];
}

async function dbDeletePersonalFilter(teamId, ownerUserId, id) {
  await dbQuery(`DELETE FROM personal_filter_members WHERE team_id=$1 AND filter_id=$2`, [teamId, id]);
  await dbQuery(`DELETE FROM personal_filters WHERE team_id=$1 AND owner_user_id=$2 AND id=$3`, [teamId, ownerUserId, id]);
}

async function dbSetPersonalFilterMembers(teamId, filterId, userIds) {
  await dbQuery(`DELETE FROM personal_filter_members WHERE team_id=$1 AND filter_id=$2`, [teamId, filterId]);
  for (const userId of userIds) {
    await dbQuery(
      `INSERT INTO personal_filter_members (filter_id, team_id, user_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
      [filterId, teamId, userId],
    );
  }
}

async function dbGetPersonalFilterMemberIds(teamId, filterId) {
  const res = await dbQuery(
    `SELECT user_id FROM personal_filter_members WHERE team_id=$1 AND filter_id=$2`,
    [teamId, filterId],
  );
  return (res.rows || []).map((r) => r.user_id);
}

module.exports = {
  dbEnsureSettingsSchema,
  dbCountCompletions,
  dbCountTargets,
  dbCreateTask,
  dbDeleteTaskTargets,
  dbGetTaskById,
  dbGetNotificationThread,
  dbGetTaskBySource,
  dbGetThreadCard,
  dbGetTeamSettings,
  dbGetUserDept,
  dbGetUserSettings,
  dbHasUserCompleted,
  dbGetUserCompletedTaskIds,
  dbInsertTaskComment,
  dbInsertTaskTargets,
  dbIsUserTarget,
  dbListBroadcastTasksByStatuses,
  dbListBroadcastTasksByStatusesWithScope,
  dbListPersonalTasksByStatusesWithScope,
  dbListTargetUserIds,
  dbListTaskComments,
  dbPruneTaskCompletionsByTargets,
  dbQuery,
  dbReplaceTaskTargets,
  dbUpdateBroadcastCounts,
  dbUpdateStatus,
  dbUpdateTaskContent,
  dbUpdateTaskEditableFields,
  dbUpsertCompletion,
  dbUpsertNotificationThread,
  dbUpsertTeamSettings,
  dbUpsertThreadCard,
  dbUpsertUserDept,
  dbUpsertUserSettings,
  pool,
  // Dashboard
  dbGetDashboardRole,
  dbSetDashboardRole,
  dbListDashboardAdmins,
  dbCreateDashTeam,
  dbListDashTeams,
  dbGetDashTeam,
  dbDeleteDashTeam,
  dbUpdateDashTeam,
  dbAddDashTeamMember,
  dbRemoveDashTeamMember,
  dbListDashTeamMembers,
  dbGetUserDashTeams,
  dbCreateProject,
  dbListProjects,
  dbGetProject,
  dbDeleteProject,
  dbUpdateProject,
  dbAddProjectTask,
  dbRemoveProjectTask,
  dbListProjectTasks,
  // Personal filters
  dbCreatePersonalFilter,
  dbListPersonalFilters,
  dbDeletePersonalFilter,
  dbSetPersonalFilterMembers,
  dbGetPersonalFilterMemberIds,
  // Integrations
  dbCreateIntegration,
  dbListIntegrations,
  dbGetIntegration,
  dbUpdateIntegration,
  dbDeleteIntegration,
  dbListFieldMappings,
  dbSetFieldMappings,
  dbCreateSyncLog,
  dbUpdateSyncLog,
  dbListSyncLogs,
};
