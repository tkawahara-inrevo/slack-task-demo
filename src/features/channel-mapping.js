// Slackチャンネル・グループ メンバーシップ管理
const { dbQuery } = require('../db/index');

// ジョブ管理（メモリ内）
const jobs = new Map();

async function syncChannelMapping(slackClient, teamId, jobId) {
  const setStep = (step, detail = '') => {
    if (jobId && jobs.has(jobId)) {
      Object.assign(jobs.get(jobId), { step, detail, updatedAt: Date.now() });
    }
  };

  // 1. メンバー取得（@inrevo.jpフィルター、取れなければ全非botアクティブユーザー）
  setStep('メンバー情報を取得中...', '');
  let allUsers = [], cursor;
  do {
    const res = await slackClient.users.list({ limit: 200, cursor: cursor || undefined });
    allUsers.push(...(res.members || []).filter(m => !m.is_bot && !m.deleted && m.id !== 'USLACKBOT'));
    cursor = res.response_metadata?.next_cursor;
  } while (cursor);

  // emailフィルター（取れなければ全員対象）
  const withEmail = allUsers.filter(m => (m.profile?.email || '').endsWith('@inrevo.jp'));
  const members = withEmail.length > 0 ? withEmail : allUsers;
  setStep('チャンネル一覧を取得中...', `${members.length}名取得完了`);

  // 2. チャンネル取得
  let channels = [];
  cursor = undefined;
  do {
    const res = await slackClient.conversations.list({ types: 'public_channel', exclude_archived: true, limit: 200, cursor: cursor || undefined });
    channels.push(...(res.channels || []));
    cursor = res.response_metadata?.next_cursor;
  } while (cursor);
  setStep('ユーザーグループを取得中...', `${channels.length}チャンネル取得完了`);

  // 3. ユーザーグループ取得
  const ugRes = await slackClient.usergroups.list({ include_disabled: false });
  const usergroups = ugRes.usergroups || [];
  setStep('チャンネルメンバーを取得中...', `${usergroups.length}グループ取得完了`);

  const memberIdSet = new Set(members.map(m => m.id));

  // 4. チャンネルメンバー取得（並列5件ずつ）
  const chMemberships = {};
  const BATCH = 5;
  for (let i = 0; i < channels.length; i += BATCH) {
    const batch = channels.slice(i, i + BATCH);
    setStep('チャンネルメンバーを取得中...', `${i}/${channels.length} チャンネル処理済`);
    await Promise.all(batch.map(async ch => {
      const ids = new Set();
      let cur;
      try {
        do {
          const r = await slackClient.conversations.members({ channel: ch.id, limit: 200, cursor: cur || undefined });
          (r.members || []).forEach(id => { if (memberIdSet.has(id)) ids.add(id); });
          cur = r.response_metadata?.next_cursor;
        } while (cur);
      } catch (e) {
        console.warn(`[channel-mapping] skip ${ch.name}: ${e.message}`);
      }
      chMemberships[ch.id] = ids;
    }));
    if (i + BATCH < channels.length) await new Promise(r => setTimeout(r, 200));
  }
  setStep('ユーザーグループメンバーを取得中...', `全${channels.length}チャンネル完了`);

  // 5. ユーザーグループメンバー取得
  const ugMemberships = {};
  for (let i = 0; i < usergroups.length; i++) {
    const ug = usergroups[i];
    setStep('ユーザーグループメンバーを取得中...', `${i}/${usergroups.length} グループ処理済`);
    try {
      const r = await slackClient.usergroups.users.list({ usergroup: ug.id });
      ugMemberships[ug.id] = new Set((r.users || []).filter(id => memberIdSet.has(id)));
    } catch (e) {
      ugMemberships[ug.id] = new Set();
    }
    if (i < usergroups.length - 1) await new Promise(r => setTimeout(r, 100));
  }
  setStep('データベースに保存中...', 'もうすぐ完了します');

  // 6. DB保存
  await dbQuery('DELETE FROM slack_membership_cache WHERE team_id=$1', [teamId]);
  await dbQuery('DELETE FROM slack_channel_cache WHERE team_id=$1', [teamId]);
  await dbQuery('DELETE FROM slack_member_cache WHERE team_id=$1', [teamId]);

  for (const m of members) {
    await dbQuery(
      `INSERT INTO slack_member_cache (team_id, user_id, display_name, title, email)
       VALUES ($1,$2,$3,$4,$5) ON CONFLICT (team_id, user_id) DO UPDATE SET display_name=$3, title=$4, email=$5`,
      [teamId, m.id, m.profile?.display_name || m.profile?.real_name || m.name || m.id,
       m.profile?.title || '', m.profile?.email || '']
    );
  }
  for (const ch of channels) {
    await dbQuery(
      `INSERT INTO slack_channel_cache (team_id, channel_id, name, type) VALUES ($1,$2,$3,'channel')
       ON CONFLICT (team_id, channel_id) DO UPDATE SET name=$3`,
      [teamId, ch.id, ch.name]
    );
    for (const uid of chMemberships[ch.id] || []) {
      await dbQuery(`INSERT INTO slack_membership_cache (team_id, user_id, channel_id) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, [teamId, uid, ch.id]);
    }
  }
  for (const ug of usergroups) {
    await dbQuery(
      `INSERT INTO slack_channel_cache (team_id, channel_id, name, type) VALUES ($1,$2,$3,'group')
       ON CONFLICT (team_id, channel_id) DO UPDATE SET name=$3`,
      [teamId, ug.id, ug.handle]
    );
    for (const uid of ugMemberships[ug.id] || []) {
      await dbQuery(`INSERT INTO slack_membership_cache (team_id, user_id, channel_id) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, [teamId, uid, ug.id]);
    }
  }
  await dbQuery(
    `INSERT INTO slack_cache_meta (team_id, synced_at) VALUES ($1, now()) ON CONFLICT (team_id) DO UPDATE SET synced_at=now()`,
    [teamId]
  );

  return { members: members.length, channels: channels.length, usergroups: usergroups.length };
}

function registerChannelMappingApi({ expressApp, authWithRole, adminOnly, slackClient }) {

  // POST /sync — 同期開始（ジョブID返却）
  expressApp.post('/api/dashboard/admin/channel-mapping/sync', authWithRole, adminOnly, async (req, res) => {
    const { teamId } = req.dashboardUser;
    const jobId = `cm_${Date.now()}_${Math.random().toString(36).slice(2,8)}`;
    jobs.set(jobId, { status: 'running', step: '準備中...', detail: '', updatedAt: Date.now() });
    res.json({ ok: true, jobId });

    (async () => {
      try {
        const result = await syncChannelMapping(slackClient, teamId, jobId);
        const job = jobs.get(jobId);
        if (job) Object.assign(job, { status: 'done', step: '同期完了', detail: `${result.members}名 / チャンネル${result.channels}件 / グループ${result.usergroups}件`, result });
        console.log('[channel-mapping] done:', result);
      } catch (e) {
        const job = jobs.get(jobId);
        if (job) Object.assign(job, { status: 'error', step: 'エラーが発生しました', detail: e.message });
        console.error('[channel-mapping] error:', e.message);
      }
    })();
  });

  // GET /sync/status/:jobId — 進捗ポーリング
  expressApp.get('/api/dashboard/admin/channel-mapping/sync/status/:jobId', authWithRole, async (req, res) => {
    const job = jobs.get(req.params.jobId);
    if (!job) {
      // ジョブがメモリにない → 完了済みの可能性があるため DB の synced_at を確認
      const { teamId } = req.dashboardUser;
      const meta = await require('../db/index').dbQuery(
        `SELECT synced_at FROM slack_cache_meta WHERE team_id=$1`, [teamId]
      ).catch(() => ({ rows: [] }));
      return res.json({ status: 'done', step: '同期完了（再読み込み済み）', detail: '', syncedAt: meta.rows[0]?.synced_at || null });
    }
    res.json(job);
  });

  // 完了ジョブを10分後に自動削除
  setInterval(() => {
    const now = Date.now();
    for (const [id, job] of jobs.entries()) {
      if ((job.status === 'done' || job.status === 'error') && now - job.updatedAt > 10 * 60 * 1000) {
        jobs.delete(id);
      }
    }
  }, 60 * 1000).unref();

  // GET /hidden — 非表示チャンネル一覧
  expressApp.get('/api/dashboard/admin/channel-mapping/hidden', authWithRole, async (req, res) => {
    const { teamId } = req.dashboardUser;
    const { rows } = await dbQuery(`SELECT channel_id FROM channel_mapping_hidden WHERE team_id=$1`, [teamId]);
    res.json({ hidden: rows.map(r => r.channel_id) });
  });

  // POST /hidden/:id — 非表示に追加
  expressApp.post('/api/dashboard/admin/channel-mapping/hidden/:id', authWithRole, async (req, res) => {
    const { teamId } = req.dashboardUser;
    await dbQuery(`INSERT INTO channel_mapping_hidden (team_id, channel_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [teamId, req.params.id]);
    res.json({ ok: true });
  });

  // DELETE /hidden/:id — 個別復元
  expressApp.delete('/api/dashboard/admin/channel-mapping/hidden/:id', authWithRole, async (req, res) => {
    const { teamId } = req.dashboardUser;
    await dbQuery(`DELETE FROM channel_mapping_hidden WHERE team_id=$1 AND channel_id=$2`, [teamId, req.params.id]);
    res.json({ ok: true });
  });

  // DELETE /hidden — 全復元
  expressApp.delete('/api/dashboard/admin/channel-mapping/hidden', authWithRole, async (req, res) => {
    const { teamId } = req.dashboardUser;
    await dbQuery(`DELETE FROM channel_mapping_hidden WHERE team_id=$1`, [teamId]);
    res.json({ ok: true });
  });

  // GET /status — 最終同期情報
  expressApp.get('/api/dashboard/admin/channel-mapping/status', authWithRole, async (req, res) => {
    const { teamId } = req.dashboardUser;
    const { rows: [meta] } = await dbQuery(`SELECT synced_at FROM slack_cache_meta WHERE team_id=$1`, [teamId]);
    const { rows: [counts] } = await dbQuery(`
      SELECT
        (SELECT COUNT(*) FROM slack_member_cache WHERE team_id=$1)::int AS members,
        (SELECT COUNT(*) FROM slack_channel_cache WHERE team_id=$1 AND type='channel')::int AS channels,
        (SELECT COUNT(*) FROM slack_channel_cache WHERE team_id=$1 AND type='group')::int AS usergroups
    `, [teamId]);
    res.json({ synced_at: meta?.synced_at || null, ...counts });
  });

  // GET /data — メンバーシップデータ
  expressApp.get('/api/dashboard/admin/channel-mapping/data', authWithRole, async (req, res) => {
    try {
      const { teamId } = req.dashboardUser;
      const { type } = req.query;
      const { rows: members } = await dbQuery(`SELECT user_id, display_name, title FROM slack_member_cache WHERE team_id=$1 ORDER BY display_name`, [teamId]);
      const { rows: channels } = await dbQuery(`SELECT channel_id, name, type FROM slack_channel_cache WHERE team_id=$1 ${type ? `AND type=$2` : ''} ORDER BY type DESC, name`, type ? [teamId, type] : [teamId]);
      const { rows: memberships } = await dbQuery(`SELECT user_id, channel_id FROM slack_membership_cache WHERE team_id=$1`, [teamId]);
      const memberMap = {};
      for (const m of memberships) {
        if (!memberMap[m.user_id]) memberMap[m.user_id] = [];
        memberMap[m.user_id].push(m.channel_id);
      }
      res.json({ members, channels, memberMap });
    } catch (e) {
      console.error('[channel-mapping] data error:', e);
      res.status(500).json({ error: 'internal' });
    }
  });
}

module.exports = { registerChannelMappingApi };
