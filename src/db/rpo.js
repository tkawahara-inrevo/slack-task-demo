// RPO案件管理 DB操作
const { dbQuery } = require('./index');
const { randomUUID } = require('crypto');

// ─────────────────────────────────────────
// テーブル初期化・スキーマ拡張
// ─────────────────────────────────────────
async function dbEnsureRpoSchema() {
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS rpo_clients (
      id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id      TEXT NOT NULL,
      name         TEXT NOT NULL,
      color        TEXT NOT NULL DEFAULT 'Ocean',
      plan         TEXT NOT NULL DEFAULT 'monthly',
      status       TEXT NOT NULL DEFAULT 'active',
      data         JSONB NOT NULL DEFAULT '{}',
      created_by   TEXT,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  // 案件担当チーム（dash_team_id を後付け可能に）
  await dbQuery(`ALTER TABLE rpo_clients ADD COLUMN IF NOT EXISTS dash_team_id TEXT;`);
  // HR部署フラグ（dash_teams に後付け）
  await dbQuery(`ALTER TABLE dash_teams ADD COLUMN IF NOT EXISTS is_hr_dept BOOLEAN NOT NULL DEFAULT false;`);
  // workload_items と案件の紐付け
  await dbQuery(`ALTER TABLE workload_items ADD COLUMN IF NOT EXISTS rpo_client_id TEXT;`);

  // タスクテンプレート
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS rpo_task_templates (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id     TEXT NOT NULL,
      title       TEXT NOT NULL,
      notes       TEXT,
      sort_order  INT NOT NULL DEFAULT 0,
      is_active   BOOLEAN NOT NULL DEFAULT true,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  // 応募者
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS rpo_applicants (
      id                  TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id             TEXT NOT NULL,
      rpo_client_id       TEXT NOT NULL,
      name                TEXT NOT NULL,
      status              TEXT NOT NULL DEFAULT '応募',
      source              TEXT,
      source_key          TEXT,
      assigned_cs_user_id TEXT,
      notes               TEXT,
      applied_at          DATE,
      external_id         TEXT,
      created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  // 応募者アクション履歴
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS rpo_applicant_actions (
      id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id      TEXT NOT NULL,
      applicant_id TEXT NOT NULL,
      type         TEXT NOT NULL,
      content      TEXT,
      created_by   TEXT,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  // 求人媒体連携設定
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS rpo_media_sources (
      id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id     TEXT NOT NULL,
      name        TEXT NOT NULL,
      webhook_key TEXT UNIQUE,
      config      JSONB NOT NULL DEFAULT '{}',
      is_active   BOOLEAN NOT NULL DEFAULT true,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  // 媒体マスタ
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS rpo_media_masters (
      id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id    TEXT NOT NULL,
      name       TEXT NOT NULL,
      sort_order INT NOT NULL DEFAULT 0,
      is_active  BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  await dbQuery(`ALTER TABLE rpo_clients ADD COLUMN IF NOT EXISTS phase TEXT NOT NULL DEFAULT 'cr';`).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_clients_team           ON rpo_clients(team_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_clients_dash_team      ON rpo_clients(dash_team_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_clients_status         ON rpo_clients(team_id, status);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_workload_items_rpo_client  ON workload_items(rpo_client_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_applicants_client      ON rpo_applicants(rpo_client_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_applicants_team        ON rpo_applicants(team_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_applicants_status      ON rpo_applicants(rpo_client_id, status);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_actions_applicant      ON rpo_applicant_actions(applicant_id);`);
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_media_masters_team     ON rpo_media_masters(team_id);`);
  // JSONB data フィールド: 全体GINインデックス（任意キー検索を高速化）
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_rpo_clients_data_gin       ON rpo_clients USING GIN (data);`);

  // RPO設定テーブル（Apps Script URL など）
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS rpo_settings (
      team_id TEXT NOT NULL,
      key     TEXT NOT NULL,
      value   TEXT,
      PRIMARY KEY (team_id, key)
    );
  `);
}

// ─────────────────────────────────────────
// RPO設定
// ─────────────────────────────────────────
async function dbGetRpoSettings(teamId) {
  const { rows } = await dbQuery(
    `SELECT key, value FROM rpo_settings WHERE team_id = $1`, [teamId]
  );
  return rows.reduce((acc, r) => { acc[r.key] = r.value; return acc; }, {});
}

async function dbSetRpoSetting(teamId, key, value) {
  await dbQuery(
    `INSERT INTO rpo_settings (team_id, key, value)
     VALUES ($1, $2, $3)
     ON CONFLICT (team_id, key) DO UPDATE SET value = EXCLUDED.value`,
    [teamId, key, value ?? null]
  );
}

const DEFAULT_MEDIA_NAMES = [
  'indeed', 'エアワーク', 'マイナビ転職', 'キャリアドラフト', 'dodaダイレクト',
  '求人ボックス', 'offerbox', 'ミイダス', 'engage', 'ビズリーチ', 'ビルメン',
  'ヤギオファー', 'オープンワーク（成果報酬）', 'キャリアドラフト（初期費用）',
  'キャリアドラフト（着座課金）', 'OfferBox（成果報酬）', '不動産キャリア',
  'ビルメン転職', 'ミドルの転職', '建職バンク', 'イレーションズ', 'Re就活30',
  'Withtalk', 'キミスカ', 'クックビズ', 'グルスタ', 'engageプレミアム', 'イーキャリア',
];

async function dbSeedMediaMasters(teamId) {
  const { rows } = await dbQuery(
    `SELECT COUNT(*)::int AS cnt FROM rpo_media_masters WHERE team_id = $1`,
    [teamId]
  );
  if (rows[0].cnt > 0) return; // already seeded
  for (let i = 0; i < DEFAULT_MEDIA_NAMES.length; i++) {
    await dbQuery(
      `INSERT INTO rpo_media_masters (team_id, name, sort_order) VALUES ($1,$2,$3)`,
      [teamId, DEFAULT_MEDIA_NAMES[i], i]
    );
  }
}

// ─────────────────────────────────────────
// 権限チェック
// ─────────────────────────────────────────
// 戻り値: { canAccess, fullAccess, myTeamIds? }
async function dbGetUserRpoAccess(teamId, userId, globalRole) {
  if (globalRole === 'admin') {
    return { canAccess: true, fullAccess: true };
  }

  // HR部署チームへの所属を確認
  const { rows: hrRows } = await dbQuery(
    `SELECT dtm.dash_team_id, dtm.role
     FROM dash_team_members dtm
     JOIN dash_teams dt ON dt.id = dtm.dash_team_id
     WHERE dt.team_id = $1 AND dtm.user_id = $2 AND dt.is_hr_dept = true`,
    [teamId, userId]
  );

  if (hrRows.length === 0) return { canAccess: false };

  // HR部署でマネージャーなら全件
  if (hrRows.some(r => r.role === 'manager')) {
    return { canAccess: true, fullAccess: true };
  }

  // 非マネージャーは自分が所属する全チームの案件のみ
  const { rows: teamRows } = await dbQuery(
    `SELECT dash_team_id FROM dash_team_members WHERE team_id = $1 AND user_id = $2`,
    [teamId, userId]
  );
  return {
    canAccess: true,
    fullAccess: false,
    myTeamIds: teamRows.map(r => r.dash_team_id),
  };
}

// ─────────────────────────────────────────
// 案件 CRUD
// ─────────────────────────────────────────
async function dbGetUserDashTeamIds(teamId, userId) {
  const { rows } = await dbQuery(
    `SELECT dash_team_id FROM dash_team_members WHERE team_id = $1 AND user_id = $2`,
    [teamId, userId]
  );
  return rows.map(r => r.dash_team_id);
}

async function dbListRpoClients(teamId, { fullAccess = true, myTeamIds = null, filterTeamId = null } = {}) {
  const params = [teamId];
  let extra = '';

  if (filterTeamId) {
    params.push(filterTeamId);
    extra = ` AND dash_team_id = $${params.length}`;
  } else if (!fullAccess) {
    if (!myTeamIds?.length) return [];
    params.push(myTeamIds);
    extra = ` AND dash_team_id = ANY($${params.length}::text[])`;
  }

  const { rows } = await dbQuery(
    `SELECT id, team_id, dash_team_id, name, color, plan, status, phase,
            data, created_by, created_at, updated_at
     FROM rpo_clients
     WHERE team_id = $1${extra}
     ORDER BY data->>'hrAssigneeName' ASC NULLS LAST, updated_at DESC`,
    params
  );
  return rows;
}

async function dbGetRpoClient(teamId, clientId) {
  const { rows } = await dbQuery(
    `SELECT * FROM rpo_clients WHERE id = $1 AND team_id = $2`,
    [clientId, teamId]
  );
  return rows[0] || null;
}

async function dbCreateRpoClient(teamId, { name, color = 'Ocean', plan = 'monthly', dashTeamId = null, data = {}, createdBy }) {
  const { rows } = await dbQuery(
    `INSERT INTO rpo_clients (team_id, name, color, plan, dash_team_id, data, created_by)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING *`,
    [teamId, name, color, plan, dashTeamId || null, JSON.stringify(data), createdBy || null]
  );
  return rows[0];
}

async function dbUpdateRpoClient(teamId, clientId, { name, color, plan, status, phase, dashTeamId, data }) {
  const fields = [];
  const vals   = [];
  let i = 1;

  if (name        !== undefined) { fields.push(`name=$${i++}`);         vals.push(name); }
  if (color       !== undefined) { fields.push(`color=$${i++}`);        vals.push(color); }
  if (plan        !== undefined) { fields.push(`plan=$${i++}`);         vals.push(plan); }
  if (status      !== undefined) { fields.push(`status=$${i++}`);       vals.push(status); }
  if (phase       !== undefined) { fields.push(`phase=$${i++}`);        vals.push(phase); }
  if (dashTeamId  !== undefined) { fields.push(`dash_team_id=$${i++}`); vals.push(dashTeamId); }
  if (data        !== undefined) { fields.push(`data=$${i++}`);         vals.push(JSON.stringify(data)); }

  if (fields.length === 0) return null;

  fields.push(`updated_at=now()`);
  vals.push(clientId, teamId);

  const { rows } = await dbQuery(
    `UPDATE rpo_clients SET ${fields.join(', ')}
     WHERE id=$${i++} AND team_id=$${i++}
     RETURNING *`,
    vals
  );
  return rows[0] || null;
}

async function dbDeleteRpoClient(teamId, clientId) {
  await dbQuery(
    `DELETE FROM rpo_clients WHERE id = $1 AND team_id = $2`,
    [clientId, teamId]
  );
}

// ─────────────────────────────────────────
// チーム一覧（案件管理用）
// ─────────────────────────────────────────
async function dbListDashTeamsForRpo(teamId) {
  const { rows } = await dbQuery(
    `SELECT id, name, is_hr_dept FROM dash_teams WHERE team_id = $1 AND name LIKE 'チーム%' ORDER BY name`,
    [teamId]
  );
  return rows;
}

async function dbSetHrDept(teamId, dashTeamId, isHrDept) {
  await dbQuery(
    `UPDATE dash_teams SET is_hr_dept = $3 WHERE team_id = $1 AND id = $2`,
    [teamId, dashTeamId, isHrDept]
  );
}

// ─────────────────────────────────────────
// 案件に紐づく workload_items
// ─────────────────────────────────────────
async function dbListRpoWorkloadItems(teamId, rpoClientId) {
  const { rows } = await dbQuery(
    `SELECT wi.*,
            dt.name  AS dash_team_name,
            ud.display_name AS owner_display_name
     FROM workload_items wi
     LEFT JOIN dash_teams dt
           ON dt.id = wi.dash_team_id AND dt.team_id = wi.team_id
     LEFT JOIN dashboard_user_directory ud
           ON ud.user_id = wi.owner_user_id AND ud.team_id = wi.team_id
     WHERE wi.team_id = $1 AND wi.rpo_client_id = $2 AND wi.is_archived = false
     ORDER BY wi.created_at ASC`,
    [teamId, rpoClientId]
  );
  return rows;
}

async function dbCreateRpoWorkloadItem(teamId, { rpoClientId, dashTeamId, ownerUserId, title, notes = null, color = null, dueDate = null, statusMemo = null, createdBy }) {
  const { rows } = await dbQuery(
    `INSERT INTO workload_items
       (id, team_id, dash_team_id, owner_user_id, title, notes, color,
        rpo_client_id, recurrence_type, sort_order, due_date, status_memo, created_by, created_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'other',0,$9,$10,$11,now(),now())
     RETURNING *`,
    [randomUUID(), teamId, dashTeamId, ownerUserId, title, notes, color, rpoClientId, dueDate || null, statusMemo || null, createdBy || null]
  );
  return rows[0];
}

async function dbDeleteRpoWorkloadItem(teamId, itemId) {
  await dbQuery(`DELETE FROM workload_cells WHERE team_id=$1 AND item_id=$2`, [teamId, itemId]);
  await dbQuery(`DELETE FROM workload_items WHERE team_id=$1 AND id=$2 AND rpo_client_id IS NOT NULL`, [teamId, itemId]);
}

async function dbListMyRpoTasks(teamId, userId) {
  const { rows } = await dbQuery(`
    SELECT wi.id, wi.rpo_client_id, wi.title, wi.notes, wi.status_memo,
           wi.due_date, wi.task_status, wi.is_done, wi.is_archived,
           wi.created_at, wi.updated_at,
           rc.name AS client_name, rc.color AS client_color
    FROM workload_items wi
    LEFT JOIN rpo_clients rc ON rc.id = wi.rpo_client_id AND rc.team_id = wi.team_id
    WHERE wi.team_id = $1 AND wi.owner_user_id = $2 AND wi.is_archived = false
      AND wi.rpo_client_id IS NOT NULL
    ORDER BY
      CASE WHEN wi.due_date IS NULL THEN 1 ELSE 0 END,
      wi.due_date ASC, wi.created_at ASC
  `, [teamId, userId]);
  return rows;
}

async function dbUpdateRpoWorkloadItem(teamId, itemId, patch) {
  const allowed = ['title', 'notes', 'status_memo', 'due_date', 'task_status', 'is_done', 'rpo_client_id'];
  const fields = [];
  const vals = [teamId, itemId];
  let i = 3;
  for (const [key, value] of Object.entries(patch || {})) {
    if (!allowed.includes(key)) continue;
    fields.push(`${key}=$${i++}`);
    vals.push(value);
  }
  if (!fields.length) return null;
  fields.push('updated_at=now()');
  const { rows } = await dbQuery(
    `UPDATE workload_items SET ${fields.join(', ')} WHERE team_id=$1 AND id=$2 AND rpo_client_id IS NOT NULL RETURNING *`,
    vals
  );
  return rows[0] || null;
}

// ─────────────────────────────────────────
// 媒体マスタ CRUD
// ─────────────────────────────────────────
async function dbListMediaMasters(teamId) {
  const { rows } = await dbQuery(
    `SELECT * FROM rpo_media_masters WHERE team_id=$1 AND is_active=true ORDER BY sort_order, name`,
    [teamId]
  );
  return rows;
}

async function dbCreateMediaMaster(teamId, name) {
  const { rows } = await dbQuery(
    `INSERT INTO rpo_media_masters (team_id, name, sort_order)
     VALUES ($1, $2, (SELECT COALESCE(MAX(sort_order)+1, 0) FROM rpo_media_masters WHERE team_id=$1))
     RETURNING *`,
    [teamId, name.trim()]
  );
  return rows[0];
}

async function dbDeleteMediaMaster(teamId, id) {
  await dbQuery(`DELETE FROM rpo_media_masters WHERE id=$1 AND team_id=$2`, [id, teamId]);
}

// ─────────────────────────────────────────
// タスクテンプレート CRUD
// ─────────────────────────────────────────
async function dbListRpoTaskTemplates(teamId) {
  const { rows } = await dbQuery(
    `SELECT * FROM rpo_task_templates WHERE team_id=$1 AND is_active=true ORDER BY sort_order, created_at`,
    [teamId]
  );
  return rows;
}

async function dbCreateRpoTaskTemplate(teamId, { title, notes = null, sortOrder = 0 }) {
  const { rows } = await dbQuery(
    `INSERT INTO rpo_task_templates (team_id, title, notes, sort_order)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [teamId, title, notes, sortOrder]
  );
  return rows[0];
}

async function dbUpdateRpoTaskTemplate(teamId, templateId, { title, notes, sortOrder, isActive }) {
  const fields = [];
  const vals = [];
  let i = 1;
  if (title     !== undefined) { fields.push(`title=$${i++}`);      vals.push(title); }
  if (notes     !== undefined) { fields.push(`notes=$${i++}`);      vals.push(notes); }
  if (sortOrder !== undefined) { fields.push(`sort_order=$${i++}`); vals.push(sortOrder); }
  if (isActive  !== undefined) { fields.push(`is_active=$${i++}`);  vals.push(isActive); }
  if (fields.length === 0) return null;
  vals.push(templateId, teamId);
  const { rows } = await dbQuery(
    `UPDATE rpo_task_templates SET ${fields.join(', ')} WHERE id=$${i++} AND team_id=$${i++} RETURNING *`,
    vals
  );
  return rows[0] || null;
}

async function dbDeleteRpoTaskTemplate(teamId, templateId) {
  await dbQuery(`DELETE FROM rpo_task_templates WHERE id=$1 AND team_id=$2`, [templateId, teamId]);
}

// テンプレートから workload_items を一括生成
async function dbApplyTemplates(teamId, rpoClientId, dashTeamId, createdBy) {
  const templates = await dbListRpoTaskTemplates(teamId);
  if (!templates.length) return [];
  const created = [];
  for (const t of templates) {
    const { rows } = await dbQuery(
      `INSERT INTO workload_items
         (id, team_id, dash_team_id, title, notes, rpo_client_id, recurrence_type, sort_order, created_by, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,'other',$7,$8,now(),now()) RETURNING *`,
      [randomUUID(), teamId, dashTeamId, t.title, t.notes, rpoClientId, t.sort_order, createdBy || null]
    );
    created.push(rows[0]);
  }
  return created;
}

// ─────────────────────────────────────────
// 応募者 CRUD
// ─────────────────────────────────────────
async function dbListRpoApplicants(teamId, rpoClientId, { status = null } = {}) {
  const params = [teamId, rpoClientId];
  let extra = '';
  if (status) { params.push(status); extra = ` AND status=$${params.length}`; }
  const { rows } = await dbQuery(
    `SELECT a.*,
            u.display_name AS assigned_cs_name,
            (SELECT content FROM rpo_applicant_actions
             WHERE applicant_id = a.id ORDER BY created_at DESC LIMIT 1) AS latest_action_content,
            (SELECT type    FROM rpo_applicant_actions
             WHERE applicant_id = a.id ORDER BY created_at DESC LIMIT 1) AS latest_action_type
     FROM rpo_applicants a
     LEFT JOIN dashboard_user_directory u
           ON u.user_id = a.assigned_cs_user_id AND u.team_id = a.team_id
     WHERE a.team_id=$1 AND a.rpo_client_id=$2${extra}
     ORDER BY a.applied_at DESC NULLS LAST, a.created_at DESC`,
    params
  );
  return rows;
}

async function dbGetRpoApplicant(teamId, applicantId) {
  const { rows } = await dbQuery(
    `SELECT a.*, u.display_name AS assigned_cs_name
     FROM rpo_applicants a
     LEFT JOIN dashboard_user_directory u
           ON u.user_id = a.assigned_cs_user_id AND u.team_id = a.team_id
     WHERE a.id=$1 AND a.team_id=$2`,
    [applicantId, teamId]
  );
  return rows[0] || null;
}

async function dbCreateRpoApplicant(teamId, {
  rpoClientId, name, status = '応募', source = null, sourceKey = null,
  assignedCsUserId = null, notes = null, appliedAt = null, externalId = null,
}) {
  const { rows } = await dbQuery(
    `INSERT INTO rpo_applicants
       (team_id, rpo_client_id, name, status, source, source_key,
        assigned_cs_user_id, notes, applied_at, external_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
    [teamId, rpoClientId, name, status, source, sourceKey,
     assignedCsUserId, notes, appliedAt || null, externalId || null]
  );
  return rows[0];
}

async function dbUpdateRpoApplicant(teamId, applicantId, {
  name, status, source, sourceKey, assignedCsUserId, notes, appliedAt,
}) {
  const fields = [];
  const vals = [];
  let i = 1;
  if (name             !== undefined) { fields.push(`name=$${i++}`);               vals.push(name); }
  if (status           !== undefined) { fields.push(`status=$${i++}`);             vals.push(status); }
  if (source           !== undefined) { fields.push(`source=$${i++}`);             vals.push(source); }
  if (sourceKey        !== undefined) { fields.push(`source_key=$${i++}`);         vals.push(sourceKey); }
  if (assignedCsUserId !== undefined) { fields.push(`assigned_cs_user_id=$${i++}`);vals.push(assignedCsUserId); }
  if (notes            !== undefined) { fields.push(`notes=$${i++}`);              vals.push(notes); }
  if (appliedAt        !== undefined) { fields.push(`applied_at=$${i++}`);         vals.push(appliedAt); }
  if (fields.length === 0) return null;
  fields.push(`updated_at=now()`);
  vals.push(applicantId, teamId);
  const { rows } = await dbQuery(
    `UPDATE rpo_applicants SET ${fields.join(', ')} WHERE id=$${i++} AND team_id=$${i++} RETURNING *`,
    vals
  );
  return rows[0] || null;
}

async function dbDeleteRpoApplicant(teamId, applicantId) {
  await dbQuery(`DELETE FROM rpo_applicant_actions WHERE team_id=$1 AND applicant_id=$2`, [teamId, applicantId]);
  await dbQuery(`DELETE FROM rpo_applicants WHERE id=$1 AND team_id=$2`, [applicantId, teamId]);
}

// ─────────────────────────────────────────
// 応募者アクション履歴
// ─────────────────────────────────────────
async function dbListApplicantActions(teamId, applicantId) {
  const { rows } = await dbQuery(
    `SELECT a.*, u.display_name AS created_by_name
     FROM rpo_applicant_actions a
     LEFT JOIN dashboard_user_directory u
           ON u.user_id = a.created_by AND u.team_id = a.team_id
     WHERE a.team_id=$1 AND a.applicant_id=$2
     ORDER BY a.created_at DESC`,
    [teamId, applicantId]
  );
  return rows;
}

async function dbAddApplicantAction(teamId, applicantId, { type, content = null, createdBy = null }) {
  const { rows } = await dbQuery(
    `INSERT INTO rpo_applicant_actions (team_id, applicant_id, type, content, created_by)
     VALUES ($1,$2,$3,$4,$5) RETURNING *`,
    [teamId, applicantId, type, content, createdBy]
  );
  return rows[0];
}

// ─────────────────────────────────────────
// 求人媒体連携
// ─────────────────────────────────────────
async function dbListRpoMediaSources(teamId) {
  const { rows } = await dbQuery(
    `SELECT * FROM rpo_media_sources WHERE team_id=$1 ORDER BY name`,
    [teamId]
  );
  return rows;
}

async function dbCreateRpoMediaSource(teamId, { name, config = {} }) {
  const webhookKey = randomUUID().replace(/-/g, '');
  const { rows } = await dbQuery(
    `INSERT INTO rpo_media_sources (team_id, name, webhook_key, config)
     VALUES ($1,$2,$3,$4) RETURNING *`,
    [teamId, name, webhookKey, JSON.stringify(config)]
  );
  return rows[0];
}

async function dbUpdateRpoMediaSource(teamId, sourceId, { name, config, isActive }) {
  const fields = [];
  const vals = [];
  let i = 1;
  if (name     !== undefined) { fields.push(`name=$${i++}`);      vals.push(name); }
  if (config   !== undefined) { fields.push(`config=$${i++}`);    vals.push(JSON.stringify(config)); }
  if (isActive !== undefined) { fields.push(`is_active=$${i++}`); vals.push(isActive); }
  if (fields.length === 0) return null;
  vals.push(sourceId, teamId);
  const { rows } = await dbQuery(
    `UPDATE rpo_media_sources SET ${fields.join(', ')} WHERE id=$${i++} AND team_id=$${i++} RETURNING *`,
    vals
  );
  return rows[0] || null;
}

async function dbDeleteRpoMediaSource(teamId, sourceId) {
  await dbQuery(`DELETE FROM rpo_media_sources WHERE id=$1 AND team_id=$2`, [sourceId, teamId]);
}

async function dbGetRpoMediaSourceByKey(webhookKey) {
  const { rows } = await dbQuery(
    `SELECT * FROM rpo_media_sources WHERE webhook_key=$1 AND is_active=true`,
    [webhookKey]
  );
  return rows[0] || null;
}

// ─────────────────────────────────────────
// 月次タスク自動生成（月額プラン）
// ─────────────────────────────────────────
async function dbGenerateMonthlyTasksForClient(teamId, rpoClientId, yearMonth, createdBy) {
  // 既存チェック: 同月の月次タスクがあればスキップ
  const label = `${yearMonth}月次`;
  const { rows: existing } = await dbQuery(
    `SELECT id FROM workload_items
     WHERE team_id=$1 AND rpo_client_id=$2 AND title LIKE $3 AND is_archived=false`,
    [teamId, rpoClientId, `%${label}%`]
  );
  if (existing.length > 0) return { skipped: true };

  const client = await dbGetRpoClient(teamId, rpoClientId);
  if (!client || client.plan !== 'monthly' || client.status !== 'active') return { skipped: true };

  const tasks = [
    '月次レポート作成',
    '月次MTG実施',
    '月次請求依頼（経理）',
  ];
  const created = [];
  for (const title of tasks) {
    const { rows } = await dbQuery(
      `INSERT INTO workload_items
         (id, team_id, dash_team_id, title, notes, rpo_client_id, recurrence_type, sort_order, created_by, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,'monthly',0,$7,now(),now()) RETURNING *`,
      [randomUUID(), teamId, client.dash_team_id, `[${label}] ${title}`, null, rpoClientId, createdBy || null]
    );
    created.push(rows[0]);
  }
  return { created };
}

// アクティブな月額案件すべてに月次タスクを生成
async function dbGenerateMonthlyTasksAll(teamId, yearMonth, createdBy) {
  const { rows: clients } = await dbQuery(
    `SELECT id FROM rpo_clients WHERE team_id=$1 AND plan='monthly' AND status='active'`,
    [teamId]
  );
  const results = [];
  for (const c of clients) {
    const r = await dbGenerateMonthlyTasksForClient(teamId, c.id, yearMonth, createdBy);
    results.push({ clientId: c.id, ...r });
  }
  return results;
}

// ─────────────────────────────────────────
// 応募者集計（ステータス別件数）
// ─────────────────────────────────────────
async function dbCountApplicantsByStatus(teamId, rpoClientId) {
  const { rows } = await dbQuery(
    `SELECT status, COUNT(*)::int AS count
     FROM rpo_applicants
     WHERE team_id=$1 AND rpo_client_id=$2
     GROUP BY status`,
    [teamId, rpoClientId]
  );
  return rows;
}

// ─────────────────────────────────────────
// 全案件サマリー（売上・予算・採用数）
// ─────────────────────────────────────────
async function dbGetRpoSummary(teamId, { fullAccess = true, myTeamIds = null, filterDashTeamId = null } = {}) {
  const params = [teamId];
  let extra = '';
  if (!fullAccess) {
    if (!myTeamIds?.length) return [];
    params.push(myTeamIds);
    extra = ` AND c.dash_team_id = ANY($${params.length}::text[])`;
  }
  if (filterDashTeamId) {
    params.push(filterDashTeamId);
    extra += ` AND c.dash_team_id = $${params.length}`;
  }

  const { rows } = await dbQuery(`
    SELECT
      c.id, c.name, c.color, c.plan, c.status, c.dash_team_id,
      dt.name AS dash_team_name,
      (c.data->'projectInfo'->>'contractAmount')::numeric AS contract_amount,
      (c.data->'projectInfo'->>'totalBudget')::numeric    AS total_budget,
      (c.data->'projectInfo'->>'hiringTarget')::int        AS hiring_target,
      COALESCE((
        SELECT SUM((m->>'mediaCost')::numeric)
        FROM jsonb_array_elements(
          CASE WHEN c.data ? 'mediaStatus' THEN c.data->'mediaStatus' ELSE '[]'::jsonb END
        ) AS m
        WHERE (m->>'mediaCost') IS NOT NULL AND (m->>'mediaCost') != ''
      ), 0) AS media_spent,
      COUNT(a.id)::int                                                                      AS total_applicants,
      COUNT(CASE WHEN a.status = '内定承諾' THEN 1 END)::int                               AS accepted_count,
      COUNT(CASE WHEN a.status = '内定'     THEN 1 END)::int                               AS offer_count,
      COUNT(CASE WHEN a.status NOT IN ('不合格','辞退') THEN 1 END)::int                   AS active_count
    FROM rpo_clients c
    LEFT JOIN dash_teams dt ON dt.id = c.dash_team_id AND dt.team_id = c.team_id
    LEFT JOIN rpo_applicants a ON a.rpo_client_id = c.id AND a.team_id = c.team_id
    WHERE c.team_id = $1${extra}
    GROUP BY c.id, dt.name
    ORDER BY c.status ASC, c.updated_at DESC
  `, params);
  return rows;
}

module.exports = {
  dbEnsureRpoSchema,
  dbGetUserRpoAccess,
  dbGetUserDashTeamIds,
  dbListRpoClients,
  dbGetRpoClient,
  dbCreateRpoClient,
  dbUpdateRpoClient,
  dbDeleteRpoClient,
  dbListDashTeamsForRpo,
  dbSetHrDept,
  dbListRpoWorkloadItems,
  dbCreateRpoWorkloadItem,
  dbDeleteRpoWorkloadItem,
  dbListMyRpoTasks,
  dbUpdateRpoWorkloadItem,
  // media masters
  dbSeedMediaMasters,
  dbListMediaMasters,
  dbCreateMediaMaster,
  dbDeleteMediaMaster,
  // templates
  dbListRpoTaskTemplates,
  dbCreateRpoTaskTemplate,
  dbUpdateRpoTaskTemplate,
  dbDeleteRpoTaskTemplate,
  dbApplyTemplates,
  // applicants
  dbListRpoApplicants,
  dbGetRpoApplicant,
  dbCreateRpoApplicant,
  dbUpdateRpoApplicant,
  dbDeleteRpoApplicant,
  // applicant actions
  dbListApplicantActions,
  dbAddApplicantAction,
  // media sources
  dbListRpoMediaSources,
  dbCreateRpoMediaSource,
  dbUpdateRpoMediaSource,
  dbDeleteRpoMediaSource,
  dbGetRpoMediaSourceByKey,
  // monthly tasks
  dbGenerateMonthlyTasksForClient,
  dbGenerateMonthlyTasksAll,
  // stats
  dbCountApplicantsByStatus,
  // summary
  dbGetRpoSummary,
  // settings
  dbGetRpoSettings,
  dbSetRpoSetting,
};
