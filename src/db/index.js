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

// トランザクション: callback(client) 内でclient.query()を使うことでアトミック実行
async function dbTransaction(callback) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    client.release();
  }
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
      CREATE TABLE IF NOT EXISTS dashboard_user_directory (
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        display_name TEXT,
        real_name TEXT,
        normalized_name TEXT,
        is_active BOOLEAN NOT NULL DEFAULT true,
        profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
        last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (team_id, user_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS dashboard_user_visibility (
        team_id TEXT NOT NULL,
        viewer_user_id TEXT NOT NULL,
        visible_user_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (team_id, viewer_user_id, visible_user_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS dashboard_team_visibility (
        team_id TEXT NOT NULL,
        viewer_user_id TEXT NOT NULL,
        visible_dash_team_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (team_id, viewer_user_id, visible_dash_team_id)
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
      CREATE TABLE IF NOT EXISTS workload_items (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        dash_team_id TEXT NOT NULL,
        owner_user_id TEXT NOT NULL,
        title TEXT NOT NULL,
        category TEXT,
        notes TEXT,
        sort_order INT NOT NULL DEFAULT 0,
        is_archived BOOLEAN NOT NULL DEFAULT false,
        created_by TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS workload_cells (
        item_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        month_key TEXT NOT NULL,
        day_num INT NOT NULL,
        intensity INT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (item_id, month_key, day_num)
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
    dbQuery(`
      CREATE TABLE IF NOT EXISTS clients (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        name TEXT NOT NULL,
        contact_name TEXT,
        contact_email TEXT,
        contact_phone TEXT,
        source TEXT,
        notes TEXT,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS deals (
        id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        client_id TEXT NOT NULL,
        name TEXT NOT NULL,
        stage TEXT NOT NULL DEFAULT 'mk',
        budget INTEGER,
        notes TEXT,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS deal_members (
        deal_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'member',
        added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (deal_id, user_id)
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS deal_activities (
        id TEXT PRIMARY KEY,
        deal_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        activity_type TEXT NOT NULL,
        content TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS deal_payments (
        id TEXT PRIMARY KEY,
        deal_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        label TEXT NOT NULL,
        amount INTEGER NOT NULL,
        due_date DATE,
        paid_date DATE,
        status TEXT NOT NULL DEFAULT 'pending',
        notes TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `),
    dbQuery(`
      CREATE TABLE IF NOT EXISTS deal_tasks (
        deal_id TEXT NOT NULL,
        task_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (deal_id, task_id)
      );
    `),
  ]);

  // Migrations: add columns that may not exist in older schemas
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'all'`);
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS deal_deliverables (
      id TEXT PRIMARY KEY,
      deal_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      title TEXT NOT NULL,
      description TEXT,
      status TEXT NOT NULL DEFAULT 'pending',
      due_date DATE,
      completed_at TIMESTAMPTZ,
      created_by TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  // ─── CRM Phase 4: kintone full parity migrations ───────────────────────────

  // clients: new columns
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS industry TEXT`);
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS prefecture TEXT`);
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS employee_range TEXT`);
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS inrevo_person TEXT`);
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS competition JSONB DEFAULT '[]'::jsonb`);
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS corporate_url TEXT`);
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS service_url1 TEXT`);
  await dbQuery(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS service_url2 TEXT`);

  // client contacts (担当者情報 subtable)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS client_contacts (
      id TEXT PRIMARY KEY,
      client_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      last_name TEXT,
      first_name TEXT,
      furigana TEXT,
      title TEXT,
      department TEXT,
      email TEXT,
      phone TEXT,
      notes TEXT,
      do_not_contact BOOLEAN NOT NULL DEFAULT FALSE,
      sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  // deals: new columns
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS yomi TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS inrevo_person TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS sales_person TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS acquisition_person TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS appointment_type TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_type TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS payment_method TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS hire_type JSONB DEFAULT '[]'::jsonb`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS first_meeting_date DATE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS acquisition_date DATE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_approval_date DATE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_send_date DATE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS order_date DATE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS conclusion_date DATE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS next_action_date DATE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS next_action_content TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS next_action_detail TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS loss_reason TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS loss_reason_detail TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_budget TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_budget_memo TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_authority TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_authority_memo TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_needs TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_needs_memo TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_timeframe TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS bant_timeframe_memo TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS initial_cost INTEGER`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS monthly_cost INTEGER`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS unit_price INTEGER`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_months INTEGER`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS guarantee_count INTEGER`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS guarantee_salary INTEGER`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS rate NUMERIC(6,2)`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS advance_payment TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS legal_check BOOLEAN DEFAULT FALSE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS antisocial_check BOOLEAN DEFAULT FALSE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS hearing_collected BOOLEAN DEFAULT FALSE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_approval BOOLEAN DEFAULT FALSE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_sent BOOLEAN DEFAULT FALSE`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS sales_memo TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS hearing_challenges JSONB DEFAULT '{}'::jsonb`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS invoice_to_name TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS invoice_to_email TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS invoice_cc_email TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_to_name TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_to_email TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS contract_cc_email TEXT`);
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS preliminary_info JSONB DEFAULT '{}'::jsonb`);

  // deal_activities: add result and yomi_at_time
  await dbQuery(`ALTER TABLE deal_activities ADD COLUMN IF NOT EXISTS result TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE deal_activities ADD COLUMN IF NOT EXISTS yomi_at_time TEXT`).catch(() => {});

  // crm_activity_settings: customizable activity types and result options per team
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS crm_activity_settings (
      team_id TEXT PRIMARY KEY,
      activity_types JSONB NOT NULL DEFAULT '["架電","商談","メール","受電","その他"]'::jsonb,
      result_options JSONB NOT NULL DEFAULT '["アポ獲得","有効会話","不通","折り返し","NG","その他"]'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});

  // deal_payments: add direction, invoice_sent, incentive_amount
  await dbQuery(`ALTER TABLE deal_payments ADD COLUMN IF NOT EXISTS direction TEXT NOT NULL DEFAULT '入金'`);
  await dbQuery(`ALTER TABLE deal_payments ADD COLUMN IF NOT EXISTS invoice_sent BOOLEAN DEFAULT FALSE`);
  await dbQuery(`ALTER TABLE deal_payments ADD COLUMN IF NOT EXISTS incentive_amount INTEGER`);

  // deal positions (募集職種別進捗)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS deal_positions (
      id TEXT PRIMARY KEY,
      deal_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      position_name TEXT NOT NULL,
      target_applications INTEGER DEFAULT 0,
      actual_applications INTEGER DEFAULT 0,
      target_hires INTEGER DEFAULT 0,
      actual_hires INTEGER DEFAULT 0,
      status TEXT DEFAULT 'active',
      sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  // deal media plans (媒体選定)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS deal_media_plans (
      id TEXT PRIMARY KEY,
      deal_id TEXT NOT NULL,
      team_id TEXT NOT NULL,
      media_name TEXT NOT NULL,
      position TEXT,
      hire_count INTEGER DEFAULT 0,
      listing_cost INTEGER DEFAULT 0,
      performance_cost INTEGER DEFAULT 0,
      margin NUMERIC(5,2) DEFAULT 0,
      notes TEXT,
      sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  // deal calc defs (計算フィールド定義)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS deal_calc_defs (
      id TEXT PRIMARY KEY,
      team_id TEXT NOT NULL,
      name TEXT NOT NULL,
      expression TEXT NOT NULL,
      description TEXT,
      sort_order INTEGER DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  // workload_items: 色・繰り返し設定カラムを追加（既存環境向け）
  await dbQuery(`ALTER TABLE workload_items ADD COLUMN IF NOT EXISTS color TEXT`);
  await dbQuery(`ALTER TABLE workload_items ADD COLUMN IF NOT EXISTS recurrence_type TEXT NOT NULL DEFAULT 'other'`);
  await dbQuery(`ALTER TABLE workload_items ADD COLUMN IF NOT EXISTS recurrence_config JSONB`);

  // 親子チーム構造
  await dbQuery(`ALTER TABLE dash_teams ADD COLUMN IF NOT EXISTS parent_id TEXT REFERENCES dash_teams(id) ON DELETE SET NULL`).catch(() => {});
  await dbQuery(`ALTER TABLE dash_team_members ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'member'`).catch(() => {});

  // チーム共有カテゴリ
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS workload_categories (
      id TEXT PRIMARY KEY,
      team_id TEXT NOT NULL,
      dash_team_id TEXT NOT NULL,
      name TEXT NOT NULL,
      color TEXT NOT NULL DEFAULT '#f97316',
      sort_order INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(team_id, dash_team_id, name)
    )
  `);

  // dashboard sessions (永続化セッション)
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS dashboard_sessions (
      session_id TEXT PRIMARY KEY,
      team_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  // recruitment_settings: 送信元メール・メールテンプレート・配点
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS from_email TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS email_subject TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS email_body TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS total_score INTEGER`).catch(() => {});

  // 適性診断・選考ステージ管理
  await dbQuery(`ALTER TABLE recruitment_candidates ADD COLUMN IF NOT EXISTS stage TEXT NOT NULL DEFAULT 'casual_talk'`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_candidates ADD COLUMN IF NOT EXISTS personality_status TEXT NOT NULL DEFAULT 'pending'`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_candidates ADD COLUMN IF NOT EXISTS personality_sent_at TIMESTAMPTZ`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_candidates ADD COLUMN IF NOT EXISTS personality_completed_at TIMESTAMPTZ`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_candidates ADD COLUMN IF NOT EXISTS department TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE crm_period_settings ADD COLUMN IF NOT EXISTS term_target BIGINT`).catch(() => {});

  // kintone App103 (活動履歴) キャッシュ
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS kintone_activities (
      record_id          TEXT PRIMARY KEY,
      team_id            TEXT NOT NULL DEFAULT 'T086C06L5V0',
      deal_record_id     TEXT,
      activity_date      DATE,
      activity_type      TEXT,
      assignee           TEXT,
      content            TEXT,
      next_action_date   DATE,
      next_action_content TEXT,
      next_action_detail TEXT,
      next_assignee      TEXT,
      yomi_at_time       TEXT,
      is_done            BOOLEAN DEFAULT FALSE,
      created_at         TIMESTAMPTZ,
      updated_at         TIMESTAMPTZ,
      synced_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_kintone_activities_deal ON kintone_activities(deal_record_id)`).catch(() => {});

  // AN依頼: ANが返す構造化見積もり項目（04費用テーブルの代替）
  await dbQuery(`ALTER TABLE an_requests ADD COLUMN IF NOT EXISTS est_media_cost BIGINT`).catch(() => {});
  await dbQuery(`ALTER TABLE an_requests ADD COLUMN IF NOT EXISTS est_unit_price BIGINT`).catch(() => {});
  await dbQuery(`ALTER TABLE an_requests ADD COLUMN IF NOT EXISTS est_budget BIGINT`).catch(() => {});
  await dbQuery(`ALTER TABLE an_requests ADD COLUMN IF NOT EXISTS est_hire_count INTEGER`).catch(() => {});
  await dbQuery(`ALTER TABLE an_requests ADD COLUMN IF NOT EXISTS recommended_media TEXT`).catch(() => {});

  // deals: 締結見込み（ヨミS/A向けの営業の手動見込み）
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS settlement_forecast TEXT`).catch(() => {});  // '今月可能性あり' | '来月締結見込み'
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS forecast_confidence TEXT`).catch(() => {});  // '高' | '中' | '低'

  // 案件から自動生成される入金予定（kintone⑥の代替）
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS deal_expected_payments (
      id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id TEXT NOT NULL DEFAULT 'T086C06L5V0',
      deal_id TEXT NOT NULL REFERENCES deals(id) ON DELETE CASCADE,
      expected_date DATE NOT NULL,
      expected_amount BIGINT NOT NULL DEFAULT 0,
      kind TEXT NOT NULL,
      month_seq INTEGER,
      status TEXT NOT NULL DEFAULT 'planned',
      actual_payment_record_id TEXT,
      note TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_dep_deal ON deal_expected_payments(deal_id)`).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_dep_status ON deal_expected_payments(team_id, status, expected_date)`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS personality_gas_url TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS personality_sheet_url TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS personality_email_subject TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS personality_email_body TEXT`).catch(() => {});
  await dbQuery(`ALTER TABLE recruitment_settings ADD COLUMN IF NOT EXISTS personality_webhook_secret TEXT`).catch(() => {});

  // 採用テスト予約送信
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS recruitment_scheduled_sends (
      id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id      TEXT NOT NULL,
      candidate_id TEXT NOT NULL,
      scheduled_at TIMESTAMPTZ NOT NULL,
      status       TEXT NOT NULL DEFAULT 'pending',
      error_message TEXT,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});

  // 誤ってSub ManagerにリネームされたSub Chiefを元に戻す
  await dbQuery(`UPDATE crm_role_targets SET role_name='Sub Chief' WHERE role_name='Sub Manager'`).catch(() => {});
  await dbQuery(`UPDATE crm_rep_roles SET role_name='Sub Chief' WHERE role_name='Sub Manager'`).catch(() => {});

  // 前期役職カラム追加
  await dbQuery(`ALTER TABLE crm_rep_roles ADD COLUMN IF NOT EXISTS prev_role_name TEXT NOT NULL DEFAULT ''`).catch(() => {});
  await dbQuery(`ALTER TABLE crm_rep_roles ADD COLUMN IF NOT EXISTS prev_monthly_target_override BIGINT`).catch(() => {});

  // KPI振り分けフラグ
  // is_retired           : 退職者。実績はすべて添田/リファラルへ集約
  // exclude_from_kpi     : 完全除外（KPIにも添田にも入れない／アライアンス扱い）
  // monthly_to_adda_ref  : 月額プランの入金のみ添田/リファラルへ集約（月額以外はBC計上）
  await dbQuery(`ALTER TABLE crm_rep_roles ADD COLUMN IF NOT EXISTS is_retired BOOLEAN NOT NULL DEFAULT false`).catch(() => {});
  await dbQuery(`ALTER TABLE crm_rep_roles ADD COLUMN IF NOT EXISTS exclude_from_kpi BOOLEAN NOT NULL DEFAULT false`).catch(() => {});
  await dbQuery(`ALTER TABLE crm_rep_roles ADD COLUMN IF NOT EXISTS monthly_to_adda_ref BOOLEAN NOT NULL DEFAULT false`).catch(() => {});

  // kintone_payments に流入経路カラム（App102 から会社名で join した値）
  // 「丸山さん × グラハム = アライアンス扱い」など、流入経路を考慮した振り分け用
  await dbQuery(`ALTER TABLE kintone_payments ADD COLUMN IF NOT EXISTS inflow_source TEXT`).catch(() => {});

  // フィールド選択肢設定
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS crm_field_options (
      team_id    TEXT NOT NULL,
      field_name TEXT NOT NULL,
      options    JSONB NOT NULL DEFAULT '[]',
      PRIMARY KEY (team_id, field_name)
    )
  `).catch(() => {});

  // deals: dormant ステータス追加（見送り）
  await dbQuery(`ALTER TABLE deals ADD COLUMN IF NOT EXISTS dormant_reason TEXT`).catch(() => {});

  // CRM権限設定
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS crm_permissions (
      team_id     TEXT PRIMARY KEY,
      bc_team_name TEXT NOT NULL DEFAULT 'Business Consulting',
      config      JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});

  // カスタムフィールド設定
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS crm_custom_fields (
      id          TEXT NOT NULL DEFAULT gen_random_uuid()::text,
      team_id     TEXT NOT NULL,
      entity_type TEXT NOT NULL,  -- 'customer' | 'deal'
      field_key   TEXT NOT NULL,
      field_label TEXT NOT NULL,
      field_type  TEXT NOT NULL DEFAULT 'text', -- text|number|date|select|textarea|checkbox
      options     JSONB NOT NULL DEFAULT '[]',
      sort_order  INTEGER NOT NULL DEFAULT 0,
      is_required BOOLEAN NOT NULL DEFAULT false,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (id),
      UNIQUE (team_id, entity_type, field_key)
    )
  `).catch(() => {});

  // 担当者別役職・目標上書き設定
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS crm_rep_roles (
      team_id TEXT NOT NULL,
      rep_name TEXT NOT NULL,
      role_name TEXT NOT NULL DEFAULT '',
      monthly_target_override BIGINT,
      PRIMARY KEY (team_id, rep_name)
    )
  `).catch(() => {});

  // HRMOS勤怠打刻ログ
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS hrmos_stamps (
      id          TEXT PRIMARY KEY,
      team_id     TEXT NOT NULL,
      slack_user_id TEXT NOT NULL,
      stamp_type  INT NOT NULL,
      stamped_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      hrmos_user_id INT,
      ok          BOOLEAN NOT NULL DEFAULT false,
      error_reason TEXT
    )
  `).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS hrmos_stamps_user_date ON hrmos_stamps(team_id, slack_user_id, stamped_at DESC)`).catch(() => {});

  // タスク通知遅延（キーワード/リアクション経由タスクは10分後に通知）
  await dbQuery(`ALTER TABLE tasks ADD COLUMN IF NOT EXISTS notify_scheduled_at TIMESTAMPTZ`).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS tasks_notify_scheduled ON tasks(notify_scheduled_at) WHERE notify_scheduled_at IS NOT NULL AND notified_at IS NULL`).catch(() => {});

  // メンション追跡（未確認メンションをホームに表示するため）
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS user_mentions (
      id               TEXT PRIMARY KEY,
      team_id          TEXT NOT NULL,
      mentioned_user_id TEXT NOT NULL,
      channel_id       TEXT NOT NULL,
      message_ts       TEXT NOT NULL,
      sender_user_id   TEXT,
      text_preview     TEXT,
      dismissed_at     TIMESTAMPTZ,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE(team_id, mentioned_user_id, channel_id, message_ts)
    )
  `).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS user_mentions_active ON user_mentions(team_id, mentioned_user_id, created_at) WHERE dismissed_at IS NULL`).catch(() => {});
  await dbQuery(`ALTER TABLE user_mentions ADD COLUMN IF NOT EXISTS mention_type TEXT NOT NULL DEFAULT 'direct'`).catch(() => {});

  // Googleカレンダー連携トークン（admin1アカウントで全員分を取得）
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS google_oauth_tokens (
      team_id       TEXT PRIMARY KEY,
      access_token  TEXT,
      refresh_token TEXT,
      expiry_date   BIGINT,
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});

  // AN依頼管理
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS an_requests (
      id            TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id       TEXT NOT NULL,
      channel_id    TEXT,
      message_ts    TEXT,
      company_name  TEXT,
      crm_deal_id   TEXT,
      sales_person  TEXT,
      request_type  TEXT,
      priority      TEXT,
      detail        TEXT,
      raw_text      TEXT,
      answer        TEXT,
      answer_by     TEXT,
      answer_at     TIMESTAMPTZ,
      status        TEXT NOT NULL DEFAULT 'pending',
      created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_an_requests_team ON an_requests(team_id, created_at DESC)`).catch(() => {});

  // 法務案件管理
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS legal_cases (
      id               TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
      team_id          TEXT NOT NULL,
      case_name        TEXT NOT NULL DEFAULT '新規案件',
      na_date          DATE,
      start_date       DATE,
      priority         TEXT NOT NULL DEFAULT '中',
      chief            TEXT,
      ball             TEXT,
      na_ledger        TEXT,
      issue_summary    TEXT,
      issue_details    TEXT,
      contract_details TEXT,
      direction        TEXT,
      thread_url       TEXT,
      result           TEXT NOT NULL DEFAULT '対応中',
      history          JSONB NOT NULL DEFAULT '[]',
      minutes          JSONB NOT NULL DEFAULT '[]',
      created_by       TEXT,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS idx_legal_cases_team ON legal_cases(team_id, updated_at DESC)`).catch(() => {});

  // HRMOS採用設定（スプシURLなど）
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS hrmos_recruitment_settings (
      team_id   TEXT PRIMARY KEY,
      sheet_url TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});

  // HRMOS採用 応募者データ
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS hrmos_applicants (
      id          TEXT PRIMARY KEY,
      team_id     TEXT NOT NULL,
      app_id      TEXT,
      job_id      TEXT,
      job_name    TEXT,
      position_name TEXT,
      applied_date DATE,
      applicant_name TEXT,
      source      TEXT,
      source_detail TEXT,
      label       TEXT,
      status      TEXT,
      offer_date  DATE,
      join_date   DATE,
      decline_date DATE,
      imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `).catch(() => {});
  await dbQuery(`CREATE INDEX IF NOT EXISTS hrmos_applicants_team_date ON hrmos_applicants(team_id, applied_date)`).catch(() => {});
  await dbQuery(`CREATE UNIQUE INDEX IF NOT EXISTS hrmos_applicants_team_appid ON hrmos_applicants(team_id, app_id) WHERE app_id IS NOT NULL`).catch(() => {});

  // チャンネル別目標設定（リード管理ダッシュボード）
  await dbQuery(`
    CREATE TABLE IF NOT EXISTS crm_channel_targets (
      team_id              TEXT NOT NULL,
      source               TEXT NOT NULL,
      lead_unit_price      BIGINT   NOT NULL DEFAULT 0,
      cost_per_month       BIGINT   NOT NULL DEFAULT 0,
      vendor_note          TEXT     NOT NULL DEFAULT '',
      expected_leads       INTEGER  NOT NULL DEFAULT 0,
      expected_appo_count  INTEGER  NOT NULL DEFAULT 0,
      expected_appo_rate   NUMERIC(5,2) NOT NULL DEFAULT 0,
      expected_order_rate  NUMERIC(5,2) NOT NULL DEFAULT 0,
      expected_unit_price  BIGINT   NOT NULL DEFAULT 0,
      updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (team_id, source)
    )
  `).catch(() => {});
  await dbQuery(`ALTER TABLE crm_channel_targets ADD COLUMN IF NOT EXISTS lead_unit_price BIGINT NOT NULL DEFAULT 0`).catch(() => {});
  await dbQuery(`ALTER TABLE crm_channel_targets ADD COLUMN IF NOT EXISTS vendor_note TEXT NOT NULL DEFAULT ''`).catch(() => {});
  await dbQuery(`ALTER TABLE crm_channel_targets ADD COLUMN IF NOT EXISTS expected_appo_count INTEGER NOT NULL DEFAULT 0`).catch(() => {});

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
async function dbCreateDashTeam(id, teamId, name, createdBy, parentId = null, showInOrgchart = true) {
  const q = `
    INSERT INTO dash_teams (id, team_id, name, created_by, parent_id, show_in_orgchart, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, now())
    RETURNING *;
  `;
  const res = await dbQuery(q, [id, teamId, name, createdBy, parentId || null, showInOrgchart]);
  return res.rows[0];
}

async function dbListDashTeams(teamId) {
  const q = `SELECT *, (SELECT COUNT(*) FROM dash_team_members m WHERE m.dash_team_id=dt.id AND m.team_id=dt.team_id) AS member_count FROM dash_teams dt WHERE team_id=$1 ORDER BY parent_id NULLS FIRST, COALESCE(sort_order, 99), created_at ASC;`;
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

async function dbListDashTeamMembersWithProfile(teamId, dashTeamId) {
  const ROLE_ORDER = `CASE m.role
    WHEN 'manager'   THEN 1
    WHEN 'chief'     THEN 2
    WHEN 'sub_chief' THEN 3
    WHEN 'lead'      THEN 4
    ELSE 5 END`;
  const q = `
    SELECT m.user_id, m.added_at, m.role,
      d.display_name, d.real_name,
      d.profile_json->>'title' AS title,
      d.profile_json->>'image_72' AS avatar_url
    FROM dash_team_members m
    LEFT JOIN dashboard_user_directory d ON d.team_id = m.team_id AND d.user_id = m.user_id
    WHERE m.team_id=$1 AND m.dash_team_id=$2
    ORDER BY ${ROLE_ORDER} ASC, m.added_at ASC;
  `;
  const res = await dbQuery(q, [teamId, dashTeamId]);
  return res.rows;
}

async function dbUpdateDashTeamMemberRole(teamId, dashTeamId, userId, role) {
  const valid = ['admin', 'manager', 'chief', 'sub_chief', 'lead', 'member'];
  if (!valid.includes(role)) throw new Error(`Invalid role: ${role}`);
  await dbQuery(
    `UPDATE dash_team_members SET role=$4 WHERE team_id=$1 AND dash_team_id=$2 AND user_id=$3`,
    [teamId, dashTeamId, userId, role],
  );
}

// Returns the user's Slack profile title (from dashboard_user_directory)
async function dbGetUserSlackTitle(teamId, userId) {
  const res = await dbQuery(
    `SELECT profile_json->>'title' AS title FROM dashboard_user_directory WHERE team_id=$1 AND user_id=$2 LIMIT 1`,
    [teamId, userId],
  );
  return res.rows[0]?.title || '';
}

// Returns true if user has 'admin' role in any dash_team
async function dbUserHasAdminTeamRole(teamId, userId) {
  const res = await dbQuery(
    `SELECT 1 FROM dash_team_members WHERE team_id=$1 AND user_id=$2 AND role='admin' LIMIT 1`,
    [teamId, userId],
  );
  return res.rows.length > 0;
}

// Returns the user's team memberships with their role in each team
async function dbGetUserDashTeamRoles(teamId, userId) {
  const q = `
    SELECT dt.id, dt.name, dt.parent_id, m.role
    FROM dash_team_members m
    JOIN dash_teams dt ON dt.id = m.dash_team_id AND dt.team_id = m.team_id
    WHERE m.team_id = $1 AND m.user_id = $2;
  `;
  const res = await dbQuery(q, [teamId, userId]);
  return res.rows;
}

// Returns a team + all its descendant teams (for dept_leader scope)
async function dbGetDashTeamSubtree(teamId, rootId) {
  const q = `
    WITH RECURSIVE subtree AS (
      SELECT id, name, parent_id FROM dash_teams WHERE team_id=$1 AND id=$2
      UNION ALL
      SELECT dt.id, dt.name, dt.parent_id
      FROM dash_teams dt
      JOIN subtree s ON dt.parent_id = s.id AND dt.team_id = $1
    )
    SELECT id FROM subtree;
  `;
  const res = await dbQuery(q, [teamId, rootId]);
  return res.rows.map(r => r.id);
}

// Update dash team (name and/or parent_id)
async function dbUpdateDashTeamFull(teamId, dashTeamId, { name, parentId }) {
  const sets = [];
  const params = [teamId, dashTeamId];
  let idx = 3;
  if (name !== undefined) { sets.push(`name=$${idx++}`); params.push(name); }
  if (parentId !== undefined) { sets.push(`parent_id=$${idx++}`); params.push(parentId || null); }
  if (!sets.length) return;
  await dbQuery(
    `UPDATE dash_teams SET ${sets.join(', ')} WHERE team_id=$1 AND id=$2`,
    params,
  );
}

async function dbGetUserDashTeams(teamId, userId) {
  // Returns teams where user is a direct member, OR where user is member of parent team
  const q = `
    SELECT dt.*
    FROM dash_teams dt
    WHERE dt.team_id = $1
      AND (
        EXISTS (
          SELECT 1 FROM dash_team_members dtm
          WHERE dtm.dash_team_id = dt.id AND dtm.team_id = dt.team_id AND dtm.user_id = $2
        )
        OR (
          dt.parent_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM dash_team_members dtm
            WHERE dtm.dash_team_id = dt.parent_id AND dtm.team_id = dt.team_id AND dtm.user_id = $2
          )
        )
      )
    ORDER BY dt.name ASC;
  `;
  const res = await dbQuery(q, [teamId, userId]);
  return res.rows;
}

async function dbListDashboardVisibleUsers(teamId, viewerUserId) {
  const q = `
    SELECT visible_user_id, added_at
    FROM dashboard_user_visibility
    WHERE team_id=$1 AND viewer_user_id=$2
    ORDER BY added_at ASC;
  `;
  const res = await dbQuery(q, [teamId, viewerUserId]);
  return res.rows;
}

async function dbReplaceDashboardVisibleUsers(teamId, viewerUserId, visibleUserIds) {
  await dbQuery(
    `DELETE FROM dashboard_user_visibility WHERE team_id=$1 AND viewer_user_id=$2`,
    [teamId, viewerUserId],
  );

  const uniqueIds = Array.from(
    new Set(
      (Array.isArray(visibleUserIds) ? visibleUserIds : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean)
        .filter((visibleUserId) => visibleUserId !== viewerUserId),
    ),
  );

  if (!uniqueIds.length) return;

  const values = [];
  const params = [];
  let i = 1;
  for (const visibleUserId of uniqueIds) {
    values.push(`($${i++}, $${i++}, $${i++}, now())`);
    params.push(teamId, viewerUserId, visibleUserId);
  }

  await dbQuery(
    `
      INSERT INTO dashboard_user_visibility (team_id, viewer_user_id, visible_user_id, added_at)
      VALUES ${values.join(", ")}
    `,
    params,
  );
}

async function dbListDashboardVisibleTeams(teamId, viewerUserId) {
  const q = `
    SELECT visible_dash_team_id, added_at
    FROM dashboard_team_visibility
    WHERE team_id=$1 AND viewer_user_id=$2
    ORDER BY added_at ASC;
  `;
  const res = await dbQuery(q, [teamId, viewerUserId]);
  return res.rows;
}

async function dbReplaceDashboardVisibleTeams(teamId, viewerUserId, visibleDashTeamIds) {
  await dbQuery(
    `DELETE FROM dashboard_team_visibility WHERE team_id=$1 AND viewer_user_id=$2`,
    [teamId, viewerUserId],
  );

  const uniqueIds = Array.from(
    new Set(
      (Array.isArray(visibleDashTeamIds) ? visibleDashTeamIds : [])
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  );

  if (!uniqueIds.length) return;

  const values = [];
  const params = [];
  let i = 1;
  for (const visibleDashTeamId of uniqueIds) {
    values.push(`($${i++}, $${i++}, $${i++}, now())`);
    params.push(teamId, viewerUserId, visibleDashTeamId);
  }

  await dbQuery(
    `
      INSERT INTO dashboard_team_visibility (team_id, viewer_user_id, visible_dash_team_id, added_at)
      VALUES ${values.join(", ")}
    `,
    params,
  );
}

// ================================
// Dashboard user directory
// ================================
function normalizeDirectoryName(...parts) {
  return parts
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

async function dbUpsertDashboardUserDirectoryMember(teamId, user) {
  const profile = user?.profile || {};
  const displayName = profile.display_name_normalized || profile.display_name || user?.name || null;
  const realName = profile.real_name_normalized || profile.real_name || user?.real_name || null;
  const normalizedName = normalizeDirectoryName(displayName, realName, user?.id);
  const q = `
    INSERT INTO dashboard_user_directory
      (team_id, user_id, display_name, real_name, normalized_name, is_active, profile_json, last_synced_at)
    VALUES
      ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
    ON CONFLICT (team_id, user_id)
    DO UPDATE SET
      display_name = EXCLUDED.display_name,
      real_name = EXCLUDED.real_name,
      normalized_name = EXCLUDED.normalized_name,
      is_active = EXCLUDED.is_active,
      profile_json = EXCLUDED.profile_json,
      last_synced_at = now()
    RETURNING *;
  `;
  const res = await dbQuery(q, [
    teamId,
    user?.id,
    displayName,
    realName,
    normalizedName,
    !user?.deleted,
    JSON.stringify(profile || {}),
  ]);
  return res.rows[0] || null;
}

async function dbListDashboardUserDirectory(teamId, { query = "", limit = 200 } = {}) {
  const trimmed = String(query || "").trim().toLowerCase();
  const params = [teamId];
  let where = `WHERE team_id = $1`;
  if (trimmed) {
    params.push(`%${trimmed}%`);
    where += ` AND (
      lower(coalesce(display_name, '')) LIKE $2
      OR lower(coalesce(real_name, '')) LIKE $2
      OR lower(coalesce(user_id, '')) LIKE $2
      OR lower(coalesce(normalized_name, '')) LIKE $2
    )`;
  }
  params.push(limit);
  const q = `
    SELECT team_id, user_id, display_name, real_name, normalized_name, is_active, profile_json, last_synced_at
    FROM dashboard_user_directory
    ${where}
    ORDER BY is_active DESC, coalesce(display_name, real_name, user_id) ASC
    LIMIT $${params.length};
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbGetDashboardDirectoryMember(teamId, userId) {
  const q = `
    SELECT team_id, user_id, display_name, real_name, normalized_name, is_active, profile_json, last_synced_at
    FROM dashboard_user_directory
    WHERE team_id=$1 AND user_id=$2
    LIMIT 1;
  `;
  const res = await dbQuery(q, [teamId, userId]);
  return res.rows[0] || null;
}

async function dbSetUserDirectoryActive(teamId, userId, isActive) {
  await dbQuery(
    `UPDATE dashboard_user_directory SET is_active=$3 WHERE team_id=$1 AND user_id=$2`,
    [teamId, userId, isActive],
  );
}

// ================================
// Workload gantt
// ================================
async function dbListWorkloadItems(teamId, dashTeamId, ownerUserId = null) {
  const params = [teamId, dashTeamId];
  let where = `WHERE wi.team_id=$1 AND wi.dash_team_id=$2 AND wi.is_archived=false`;
  if (ownerUserId) {
    params.push(ownerUserId);
    where += ` AND wi.owner_user_id=$3`;
  }
  const q = `
    SELECT wi.*, rc.name AS rpo_client_name
    FROM workload_items wi
    LEFT JOIN rpo_clients rc ON rc.id = wi.rpo_client_id AND rc.team_id = wi.team_id
    ${where}
    ORDER BY wi.owner_user_id ASC, wi.sort_order ASC, wi.created_at ASC;
  `;
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbGetWorkloadItem(teamId, itemId) {
  const res = await dbQuery(
    `SELECT * FROM workload_items WHERE team_id=$1 AND id=$2 LIMIT 1`,
    [teamId, itemId],
  );
  return res.rows[0] || null;
}

async function dbCreateWorkloadItem(teamId, {
  id,
  dashTeamId,
  ownerUserId,
  title,
  category = null,
  notes = null,
  sortOrder = 0,
  createdBy = null,
  color = null,
  recurrenceType = 'other',
  recurrenceConfig = null,
  estimatedHours = null,
}) {
  const q = `
    INSERT INTO workload_items
      (id, team_id, dash_team_id, owner_user_id, title, category, notes, sort_order, created_by, color, recurrence_type, recurrence_config, estimated_hours, created_at, updated_at)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13, now(), now())
    RETURNING *;
  `;
  const res = await dbQuery(q, [
    id, teamId, dashTeamId, ownerUserId, title, category, notes, sortOrder, createdBy,
    color, recurrenceType, recurrenceConfig ? JSON.stringify(recurrenceConfig) : null,
    estimatedHours != null ? Number(estimatedHours) : null,
  ]);
  return res.rows[0] || null;
}

async function dbUpdateWorkloadItem(teamId, itemId, patch) {
  const allowed = ["owner_user_id", "title", "category", "notes", "sort_order", "dash_team_id", "is_archived", "color", "recurrence_type", "recurrence_config", "due_date", "status_memo", "is_done", "estimated_hours"];
  const sets = [];
  const vals = [];
  let i = 3;
  for (const [key, value] of Object.entries(patch || {})) {
    if (!allowed.includes(key)) continue;
    sets.push(`${key}=$${i++}`);
    vals.push(value);
  }
  if (!sets.length) {
    const existing = await dbQuery(`SELECT * FROM workload_items WHERE team_id=$1 AND id=$2 LIMIT 1`, [teamId, itemId]);
    return existing.rows[0] || null;
  }
  sets.push(`updated_at=now()`);
  const q = `
    UPDATE workload_items
    SET ${sets.join(", ")}
    WHERE team_id=$1 AND id=$2
    RETURNING *;
  `;
  const res = await dbQuery(q, [teamId, itemId, ...vals]);
  return res.rows[0] || null;
}

async function dbDeleteWorkloadItem(teamId, itemId) {
  await dbQuery(`DELETE FROM workload_cells WHERE team_id=$1 AND item_id=$2`, [teamId, itemId]);
  await dbQuery(`DELETE FROM workload_items WHERE team_id=$1 AND id=$2`, [teamId, itemId]);
}

async function dbListWorkloadCells(teamId, dashTeamId, monthKey) {
  const q = `
    SELECT wc.item_id, wc.month_key, wc.day_num, wc.intensity
    FROM workload_cells wc
    JOIN workload_items wi ON wi.id = wc.item_id AND wi.team_id = wc.team_id
    WHERE wc.team_id=$1 AND wi.dash_team_id=$2 AND wc.month_key=$3 AND wi.is_archived=false
    ORDER BY wc.item_id ASC, wc.day_num ASC;
  `;
  const res = await dbQuery(q, [teamId, dashTeamId, monthKey]);
  return res.rows;
}

async function dbSetWorkloadCells(teamId, itemId, monthKey, cells) {
  await dbQuery(`DELETE FROM workload_cells WHERE team_id=$1 AND item_id=$2 AND month_key=$3`, [teamId, itemId, monthKey]);
  const normalized = Array.isArray(cells)
    ? cells
      .map((cell) => ({
        dayNum: Number(cell?.dayNum),
        intensity: Number(cell?.intensity),
      }))
      .filter((cell) => Number.isInteger(cell.dayNum) && cell.dayNum >= 1 && cell.dayNum <= 31 && [1, 2].includes(cell.intensity))
    : [];
  if (!normalized.length) return;
  const values = [];
  const params = [];
  let i = 1;
  for (const cell of normalized) {
    values.push(`($${i++},$${i++},$${i++},$${i++},$${i++}, now())`);
    params.push(itemId, teamId, monthKey, cell.dayNum, cell.intensity);
  }
  await dbQuery(`
    INSERT INTO workload_cells (item_id, team_id, month_key, day_num, intensity, updated_at)
    VALUES ${values.join(", ")}
  `, params);
}

async function dbCopyWorkloadMonth(teamId, dashTeamId, fromMonthKey, toMonthKey) {
  const q = `
    INSERT INTO workload_cells (item_id, team_id, month_key, day_num, intensity, updated_at)
    SELECT wc.item_id, wc.team_id, $4 AS month_key, wc.day_num, wc.intensity, now()
    FROM workload_cells wc
    JOIN workload_items wi ON wi.id = wc.item_id AND wi.team_id = wc.team_id
    WHERE wc.team_id=$1 AND wi.dash_team_id=$2 AND wc.month_key=$3 AND wi.is_archived=false
    ON CONFLICT (item_id, month_key, day_num)
    DO UPDATE SET intensity = EXCLUDED.intensity, updated_at = now();
  `;
  await dbQuery(q, [teamId, dashTeamId, fromMonthKey, toMonthKey]);
}

// ================================
// ================================
// Workload categories
// ================================
async function dbListWorkloadCategories(teamId, dashTeamId) {
  const res = await dbQuery(
    `SELECT * FROM workload_categories WHERE team_id=$1 AND dash_team_id=$2 ORDER BY sort_order ASC, created_at ASC`,
    [teamId, dashTeamId],
  );
  return res.rows;
}

async function dbUpsertWorkloadCategory(teamId, { id, dashTeamId, name, color, sortOrder = 0 }) {
  const res = await dbQuery(
    `INSERT INTO workload_categories (id, team_id, dash_team_id, name, color, sort_order, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,now())
     ON CONFLICT (team_id, dash_team_id, name) DO UPDATE
       SET color=$5, sort_order=$6
     RETURNING *`,
    [id, teamId, dashTeamId, name, color, sortOrder],
  );
  return res.rows[0] || null;
}

async function dbUpdateWorkloadCategory(teamId, id, { name, color }) {
  const res = await dbQuery(
    `UPDATE workload_categories SET name=$3, color=$4 WHERE team_id=$1 AND id=$2 RETURNING *`,
    [teamId, id, name, color],
  );
  return res.rows[0] || null;
}

async function dbDeleteWorkloadCategory(teamId, id) {
  await dbQuery(`DELETE FROM workload_categories WHERE team_id=$1 AND id=$2`, [teamId, id]);
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

async function dbUpdatePersonalFilter(teamId, ownerUserId, id, name) {
  await dbQuery(
    `UPDATE personal_filters SET name=$4 WHERE team_id=$1 AND owner_user_id=$2 AND id=$3`,
    [teamId, ownerUserId, id, name],
  );
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

// ================================
// CRM: Clients
// ================================
async function dbCreateClient(teamId, id, { name, contactName, contactEmail, contactPhone, source, notes, createdBy }) {
  await dbQuery(
    `INSERT INTO clients (id, team_id, name, contact_name, contact_email, contact_phone, source, notes, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [id, teamId, name, contactName || null, contactEmail || null, contactPhone || null, source || null, notes || null, createdBy],
  );
  return dbGetClient(teamId, id);
}

async function dbListClients(teamId, { search = '', limit = 50, offset = 0 } = {}) {
  const q = search
    ? `SELECT * FROM clients WHERE team_id=$1 AND name ILIKE $2 ORDER BY created_at DESC LIMIT $3 OFFSET $4`
    : `SELECT * FROM clients WHERE team_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`;
  const params = search ? [teamId, `%${search}%`, limit, offset] : [teamId, limit, offset];
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbGetClient(teamId, id) {
  const res = await dbQuery(`SELECT * FROM clients WHERE team_id=$1 AND id=$2`, [teamId, id]);
  return res.rows[0] || null;
}

async function dbUpdateClient(teamId, id, fields) {
  const allowed = [
    'name', 'contact_name', 'contact_email', 'contact_phone', 'source', 'notes',
    'industry', 'prefecture', 'employee_range', 'inrevo_person', 'competition',
    'corporate_url', 'service_url1', 'service_url2',
  ];
  const sets = [];
  const vals = [];
  let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) {
      sets.push(`${k}=$${i++}`);
      vals.push(Array.isArray(v) ? JSON.stringify(v) : v);
    }
  }
  if (!sets.length) return dbGetClient(teamId, id);
  sets.push(`updated_at=now()`);
  await dbQuery(`UPDATE clients SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
  return dbGetClient(teamId, id);
}

// ================================
// CRM: Client Contacts (担当者情報)
// ================================
async function dbCreateClientContact(teamId, id, { clientId, lastName, firstName, furigana, title, department, email, phone, notes, doNotContact, sortOrder }) {
  await dbQuery(
    `INSERT INTO client_contacts (id, client_id, team_id, last_name, first_name, furigana, title, department, email, phone, notes, do_not_contact, sort_order)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
    [id, clientId, teamId, lastName||null, firstName||null, furigana||null, title||null, department||null, email||null, phone||null, notes||null, doNotContact||false, sortOrder||0],
  );
  const res = await dbQuery(`SELECT * FROM client_contacts WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbListClientContacts(teamId, clientId) {
  const res = await dbQuery(
    `SELECT * FROM client_contacts WHERE team_id=$1 AND client_id=$2 ORDER BY sort_order, created_at`,
    [teamId, clientId],
  );
  return res.rows;
}

async function dbUpdateClientContact(teamId, id, fields) {
  const allowed = ['last_name','first_name','furigana','title','department','email','phone','notes','do_not_contact','sort_order'];
  const sets = []; const vals = []; let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) { sets.push(`${k}=$${i++}`); vals.push(v); }
  }
  if (!sets.length) return;
  await dbQuery(`UPDATE client_contacts SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
  const res = await dbQuery(`SELECT * FROM client_contacts WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbDeleteClientContact(teamId, id) {
  await dbQuery(`DELETE FROM client_contacts WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

async function dbDeleteClient(teamId, id) {
  await dbQuery(`DELETE FROM deal_members WHERE team_id=$1 AND deal_id IN (SELECT id FROM deals WHERE team_id=$1 AND client_id=$2)`, [teamId, id]);
  await dbQuery(`DELETE FROM deals WHERE team_id=$1 AND client_id=$2`, [teamId, id]);
  await dbQuery(`DELETE FROM clients WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

// ================================
// CRM: Deals
// ================================
async function dbCreateDeal(teamId, id, { clientId, name, stage, budget, notes, createdBy }) {
  await dbQuery(
    `INSERT INTO deals (id, team_id, client_id, name, stage, budget, notes, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [id, teamId, clientId, name, stage || 'mk', budget || null, notes || null, createdBy],
  );
  return dbGetDeal(teamId, id);
}

async function dbListDeals(teamId, { clientId, stage, userId, limit = 100 } = {}) {
  let q = `SELECT d.*, c.name AS client_name FROM deals d
           JOIN clients c ON c.id=d.client_id AND c.team_id=d.team_id
           WHERE d.team_id=$1`;
  const params = [teamId];
  let i = 2;
  if (clientId) { q += ` AND d.client_id=$${i++}`; params.push(clientId); }
  if (stage) { q += ` AND d.stage=$${i++}`; params.push(stage); }
  // Visibility: if userId provided, show deals that are either 'all' visibility or user is a member
  if (userId) {
    q += ` AND (d.visibility='all' OR EXISTS (SELECT 1 FROM deal_members dm WHERE dm.deal_id=d.id AND dm.user_id=$${i++}))`;
    params.push(userId);
  }
  q += ` ORDER BY d.updated_at DESC LIMIT $${i}`; params.push(limit);
  const res = await dbQuery(q, params);
  return res.rows;
}

async function dbGetDeal(teamId, id) {
  const res = await dbQuery(
    `SELECT d.*, c.name AS client_name FROM deals d
     JOIN clients c ON c.id=d.client_id AND c.team_id=d.team_id
     WHERE d.team_id=$1 AND d.id=$2`,
    [teamId, id],
  );
  return res.rows[0] || null;
}

const DEAL_JSONB_FIELDS = new Set(['hire_type', 'hearing_challenges', 'preliminary_info', 'competition']);

async function dbUpdateDeal(teamId, id, fields) {
  const allowed = [
    'name', 'stage', 'budget', 'notes', 'client_id', 'visibility',
    'yomi', 'inrevo_person', 'sales_person', 'acquisition_person',
    'appointment_type', 'contract_type', 'payment_method', 'hire_type',
    'first_meeting_date', 'acquisition_date', 'contract_approval_date',
    'contract_send_date', 'order_date', 'conclusion_date',
    'next_action_date', 'next_action_content', 'next_action_detail',
    'loss_reason', 'loss_reason_detail',
    'bant_budget', 'bant_budget_memo', 'bant_authority', 'bant_authority_memo',
    'bant_needs', 'bant_needs_memo', 'bant_timeframe', 'bant_timeframe_memo',
    'initial_cost', 'monthly_cost', 'unit_price', 'contract_months',
    'guarantee_count', 'guarantee_salary', 'rate', 'advance_payment',
    'legal_check', 'antisocial_check', 'hearing_collected',
    'contract_approval', 'contract_sent', 'sales_memo',
    'hearing_challenges', 'preliminary_info',
    'invoice_to_name', 'invoice_to_email', 'invoice_cc_email',
    'contract_to_name', 'contract_to_email', 'contract_cc_email',
    'settlement_forecast', 'forecast_confidence',
  ];
  const sets = [];
  const vals = [];
  let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) {
      sets.push(`${k}=$${i++}`);
      vals.push(DEAL_JSONB_FIELDS.has(k) ? JSON.stringify(v) : v);
    }
  }
  if (!sets.length) return dbGetDeal(teamId, id);
  sets.push(`updated_at=now()`);
  await dbQuery(`UPDATE deals SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
  return dbGetDeal(teamId, id);
}

async function dbDeleteDeal(teamId, id) {
  await dbQuery(`DELETE FROM deal_members WHERE deal_id=$1 AND team_id=$2`, [id, teamId]);
  await dbQuery(`DELETE FROM deals WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

async function dbAddDealMember(teamId, dealId, userId, role = 'member') {
  await dbQuery(
    `INSERT INTO deal_members (deal_id, team_id, user_id, role) VALUES ($1,$2,$3,$4)
     ON CONFLICT (deal_id, user_id) DO UPDATE SET role=$4`,
    [dealId, teamId, userId, role],
  );
}

async function dbRemoveDealMember(teamId, dealId, userId) {
  await dbQuery(`DELETE FROM deal_members WHERE deal_id=$1 AND team_id=$2 AND user_id=$3`, [dealId, teamId, userId]);
}

async function dbListDealMembers(teamId, dealId) {
  const res = await dbQuery(
    `SELECT user_id, role, added_at FROM deal_members WHERE deal_id=$1 AND team_id=$2 ORDER BY added_at`,
    [dealId, teamId],
  );
  return res.rows;
}

async function dbIsDealMember(teamId, dealId, userId) {
  const res = await dbQuery(
    `SELECT 1 FROM deal_members WHERE deal_id=$1 AND team_id=$2 AND user_id=$3`,
    [dealId, teamId, userId],
  );
  return res.rows.length > 0;
}

// ================================
// CRM: Deal Activities
// ================================
async function dbCreateDealActivity(teamId, id, { dealId, userId, activityType, content, metadata }) {
  await dbQuery(
    `INSERT INTO deal_activities (id, deal_id, team_id, user_id, activity_type, content, metadata)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [id, dealId, teamId, userId, activityType, content || null, JSON.stringify(metadata || {})],
  );
  const res = await dbQuery(`SELECT * FROM deal_activities WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbListDealActivities(teamId, dealId) {
  const res = await dbQuery(
    `SELECT * FROM deal_activities WHERE team_id=$1 AND deal_id=$2 ORDER BY created_at DESC`,
    [teamId, dealId],
  );
  return res.rows;
}

async function dbDeleteDealActivity(teamId, id) {
  await dbQuery(`DELETE FROM deal_activities WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

// ================================
// CRM: Deal Payments
// ================================
async function dbCreateDealPayment(teamId, id, { dealId, label, amount, dueDate, notes, direction, incentiveAmount }) {
  await dbQuery(
    `INSERT INTO deal_payments (id, deal_id, team_id, label, amount, due_date, notes, direction, incentive_amount)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [id, dealId, teamId, label, amount, dueDate || null, notes || null, direction || '入金', incentiveAmount || null],
  );
  const res = await dbQuery(`SELECT * FROM deal_payments WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbListDealPayments(teamId, dealId) {
  const res = await dbQuery(
    `SELECT * FROM deal_payments WHERE team_id=$1 AND deal_id=$2 ORDER BY created_at`,
    [teamId, dealId],
  );
  return res.rows;
}

async function dbUpdateDealPayment(teamId, id, fields) {
  const allowed = ['label', 'amount', 'due_date', 'paid_date', 'status', 'notes', 'direction', 'invoice_sent', 'incentive_amount'];
  const sets = [];
  const vals = [];
  let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) { sets.push(`${k}=$${i++}`); vals.push(v); }
  }
  if (!sets.length) return;
  await dbQuery(`UPDATE deal_payments SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
}

async function dbDeleteDealPayment(teamId, id) {
  await dbQuery(`DELETE FROM deal_payments WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

// ================================
// CRM: Deal Tasks
// ================================
async function dbAddDealTask(teamId, dealId, taskId) {
  await dbQuery(
    `INSERT INTO deal_tasks (deal_id, task_id, team_id) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
    [dealId, taskId, teamId],
  );
}

async function dbRemoveDealTask(teamId, dealId, taskId) {
  await dbQuery(`DELETE FROM deal_tasks WHERE team_id=$1 AND deal_id=$2 AND task_id=$3`, [teamId, dealId, taskId]);
}

async function dbListDealTasks(teamId, dealId) {
  const res = await dbQuery(
    `SELECT t.* FROM tasks t
     JOIN deal_tasks dt ON dt.task_id=t.id AND dt.team_id=t.team_id
     WHERE dt.team_id=$1 AND dt.deal_id=$2
     ORDER BY t.created_at DESC`,
    [teamId, dealId],
  );
  return res.rows;
}

// ================================
// CRM: Deal Deliverables
// ================================
async function dbCreateDeliverable(teamId, id, { dealId, title, description, dueDate, createdBy }) {
  await dbQuery(
    `INSERT INTO deal_deliverables (id, deal_id, team_id, title, description, due_date, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [id, dealId, teamId, title, description || null, dueDate || null, createdBy],
  );
  const res = await dbQuery(`SELECT * FROM deal_deliverables WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbListDeliverables(teamId, dealId) {
  const res = await dbQuery(
    `SELECT * FROM deal_deliverables WHERE team_id=$1 AND deal_id=$2 ORDER BY created_at`,
    [teamId, dealId],
  );
  return res.rows;
}

async function dbUpdateDeliverable(teamId, id, fields) {
  const allowed = ['title', 'description', 'status', 'due_date', 'completed_at'];
  const sets = [];
  const vals = [];
  let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) { sets.push(`${k}=$${i++}`); vals.push(v); }
  }
  if (!sets.length) return;
  sets.push(`updated_at=now()`);
  await dbQuery(`UPDATE deal_deliverables SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
  const res = await dbQuery(`SELECT * FROM deal_deliverables WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbDeleteDeliverable(teamId, id) {
  await dbQuery(`DELETE FROM deal_deliverables WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

// ================================
// CRM: Deal Positions (募集職種別進捗)
// ================================
async function dbCreateDealPosition(teamId, id, { dealId, positionName, targetApplications, targetHires, sortOrder }) {
  await dbQuery(
    `INSERT INTO deal_positions (id, deal_id, team_id, position_name, target_applications, target_hires, sort_order)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [id, dealId, teamId, positionName, targetApplications||0, targetHires||0, sortOrder||0],
  );
  const res = await dbQuery(`SELECT * FROM deal_positions WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbListDealPositions(teamId, dealId) {
  const res = await dbQuery(
    `SELECT * FROM deal_positions WHERE team_id=$1 AND deal_id=$2 ORDER BY sort_order, created_at`,
    [teamId, dealId],
  );
  return res.rows;
}

async function dbUpdateDealPosition(teamId, id, fields) {
  const allowed = ['position_name','target_applications','actual_applications','target_hires','actual_hires','status','sort_order'];
  const sets = []; const vals = []; let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) { sets.push(`${k}=$${i++}`); vals.push(v); }
  }
  if (!sets.length) return;
  sets.push(`updated_at=now()`);
  await dbQuery(`UPDATE deal_positions SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
  const res = await dbQuery(`SELECT * FROM deal_positions WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbDeleteDealPosition(teamId, id) {
  await dbQuery(`DELETE FROM deal_positions WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

// ================================
// CRM: Deal Media Plans (媒体選定)
// ================================
async function dbCreateDealMediaPlan(teamId, id, { dealId, mediaName, position, hireCount, listingCost, performanceCost, margin, notes, sortOrder }) {
  await dbQuery(
    `INSERT INTO deal_media_plans (id, deal_id, team_id, media_name, position, hire_count, listing_cost, performance_cost, margin, notes, sort_order)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
    [id, dealId, teamId, mediaName, position||null, hireCount||0, listingCost||0, performanceCost||0, margin||0, notes||null, sortOrder||0],
  );
  const res = await dbQuery(`SELECT * FROM deal_media_plans WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbListDealMediaPlans(teamId, dealId) {
  const res = await dbQuery(
    `SELECT * FROM deal_media_plans WHERE team_id=$1 AND deal_id=$2 ORDER BY sort_order, created_at`,
    [teamId, dealId],
  );
  return res.rows;
}

async function dbUpdateDealMediaPlan(teamId, id, fields) {
  const allowed = ['media_name','position','hire_count','listing_cost','performance_cost','margin','notes','sort_order'];
  const sets = []; const vals = []; let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) { sets.push(`${k}=$${i++}`); vals.push(v); }
  }
  if (!sets.length) return;
  sets.push(`updated_at=now()`);
  await dbQuery(`UPDATE deal_media_plans SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
  const res = await dbQuery(`SELECT * FROM deal_media_plans WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbDeleteDealMediaPlan(teamId, id) {
  await dbQuery(`DELETE FROM deal_media_plans WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

// ================================
// CRM: Deal Calc Defs (計算フィールド定義)
// ================================
async function dbCreateCalcDef(teamId, id, { name, expression, description, sortOrder }) {
  await dbQuery(
    `INSERT INTO deal_calc_defs (id, team_id, name, expression, description, sort_order)
     VALUES ($1,$2,$3,$4,$5,$6)`,
    [id, teamId, name, expression, description||null, sortOrder||0],
  );
  const res = await dbQuery(`SELECT * FROM deal_calc_defs WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbListCalcDefs(teamId) {
  const res = await dbQuery(
    `SELECT * FROM deal_calc_defs WHERE team_id=$1 ORDER BY sort_order, created_at`,
    [teamId],
  );
  return res.rows;
}

async function dbUpdateCalcDef(teamId, id, fields) {
  const allowed = ['name','expression','description','sort_order'];
  const sets = []; const vals = []; let i = 3;
  for (const [k, v] of Object.entries(fields)) {
    if (allowed.includes(k)) { sets.push(`${k}=$${i++}`); vals.push(v); }
  }
  if (!sets.length) return;
  sets.push(`updated_at=now()`);
  await dbQuery(`UPDATE deal_calc_defs SET ${sets.join(',')} WHERE team_id=$1 AND id=$2`, [teamId, id, ...vals]);
  const res = await dbQuery(`SELECT * FROM deal_calc_defs WHERE id=$1`, [id]);
  return res.rows[0];
}

async function dbDeleteCalcDef(teamId, id) {
  await dbQuery(`DELETE FROM deal_calc_defs WHERE team_id=$1 AND id=$2`, [teamId, id]);
}

async function dbPipelineSummary(teamId) {
  const res = await dbQuery(
    `SELECT stage,
            COUNT(*) AS count,
            COALESCE(SUM(budget), 0) AS total_budget
     FROM deals
     WHERE team_id=$1
     GROUP BY stage`,
    [teamId],
  );
  return res.rows;
}

module.exports = {
  dbTransaction,
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
  dbListDashTeamMembersWithProfile,
  dbUpdateDashTeamMemberRole,
  dbGetUserSlackTitle,
  dbUserHasAdminTeamRole,
  dbGetUserDashTeamRoles,
  dbGetDashTeamSubtree,
  dbUpdateDashTeamFull,
  dbGetUserDashTeams,
  dbListDashboardVisibleUsers,
  dbReplaceDashboardVisibleUsers,
  dbListDashboardVisibleTeams,
  dbReplaceDashboardVisibleTeams,
  dbUpsertDashboardUserDirectoryMember,
  dbSetUserDirectoryActive,
  dbListDashboardUserDirectory,
  dbGetDashboardDirectoryMember,
  dbListWorkloadItems,
  dbGetWorkloadItem,
  dbCreateWorkloadItem,
  dbUpdateWorkloadItem,
  dbDeleteWorkloadItem,
  dbListWorkloadCells,
  dbSetWorkloadCells,
  dbListWorkloadCategories,
  dbUpsertWorkloadCategory,
  dbUpdateWorkloadCategory,
  dbDeleteWorkloadCategory,
  dbCopyWorkloadMonth,
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
  dbUpdatePersonalFilter,
  dbDeletePersonalFilter,
  dbSetPersonalFilterMembers,
  dbGetPersonalFilterMemberIds,
  // CRM: Clients
  dbCreateClient,
  dbListClients,
  dbGetClient,
  dbUpdateClient,
  dbDeleteClient,
  // CRM: Client Contacts
  dbCreateClientContact,
  dbListClientContacts,
  dbUpdateClientContact,
  dbDeleteClientContact,
  // CRM: Deals
  dbCreateDeal,
  dbListDeals,
  dbGetDeal,
  dbUpdateDeal,
  dbDeleteDeal,
  dbAddDealMember,
  dbRemoveDealMember,
  dbListDealMembers,
  dbIsDealMember,
  // CRM: Activities
  dbCreateDealActivity,
  dbListDealActivities,
  dbDeleteDealActivity,
  // CRM: Payments
  dbCreateDealPayment,
  dbListDealPayments,
  dbUpdateDealPayment,
  dbDeleteDealPayment,
  // CRM: Deliverables
  dbCreateDeliverable,
  dbListDeliverables,
  dbUpdateDeliverable,
  dbDeleteDeliverable,
  // CRM: Deal Positions
  dbCreateDealPosition,
  dbListDealPositions,
  dbUpdateDealPosition,
  dbDeleteDealPosition,
  // CRM: Deal Media Plans
  dbCreateDealMediaPlan,
  dbListDealMediaPlans,
  dbUpdateDealMediaPlan,
  dbDeleteDealMediaPlan,
  // CRM: Calc Defs
  dbCreateCalcDef,
  dbListCalcDefs,
  dbUpdateCalcDef,
  dbDeleteCalcDef,
  // CRM: Pipeline
  dbPipelineSummary,
  // CRM: Deal-Task linkage
  dbAddDealTask,
  dbRemoveDealTask,
  dbListDealTasks,
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
