// AI秘書: Claude Haiku 4.5 でタスクを要約・分類
// 入力: タスクタイトル + メッセージ本文 + 前後文脈（スレッド）
// 出力: { summary, type, key_points }

let _client = null;
function getClient() {
  if (_client) return _client;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY not set');
  const Anthropic = require('@anthropic-ai/sdk').default;
  _client = new Anthropic({ apiKey });
  return _client;
}

// JSON Schema: 構造化出力で確実にこの形式を返させる
const ANALYSIS_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['summary', 'type', 'key_points'],
  properties: {
    summary: {
      type: 'string',
      description: '依頼内容を1-2文で簡潔に要約。「●●を求められています」の形で。',
    },
    type: {
      type: 'string',
      enum: ['確認', '対応', '思考', '作業', 'その他'],
      description: '確認=何かを確認してほしい、対応=実作業の依頼、思考=判断や意見が欲しい、作業=単純作業、その他=分類不能',
    },
    key_points: {
      type: 'array',
      maxItems: 4,
      items: { type: 'string' },
      description: '対応する上で重要なポイント・固有名詞・期限など、最大4つ',
    },
  },
};

const SYSTEM_PROMPT = `あなたはタスク管理アシスタント「Pochi」のAI秘書機能です。
Slack由来のタスクを受け取り、依頼内容を簡潔に整理して担当者の負担を減らします。

判定ルール:
- summary: 「○○の対応依頼」「△△の確認依頼」のような自然な日本語1-2文
- type: 必ず enum のいずれか1つを選ぶ
- key_points: 3-4個。期日・対象・依頼者の意図・関連リンク等、対応者がパッと分かるべき要素

口調は事務的・簡潔に。敬語は不要。`;

// メイン関数: タスク本文＋スレッドコンテキストを分析
// task: { title, description?, requester_name?, due_date? }
// threadContext: [{author, text}] - Slackスレッドの前後メッセージ（任意）
async function analyzeTask({ title, description, requesterName, dueDate, threadContext }) {
  const client = getClient();

  const lines = [];
  if (title)         lines.push(`【タスクタイトル】\n${title}`);
  if (description)   lines.push(`【詳細】\n${description}`);
  if (requesterName) lines.push(`【依頼者】${requesterName}`);
  if (dueDate)       lines.push(`【期限】${dueDate}`);

  if (Array.isArray(threadContext) && threadContext.length > 0) {
    lines.push('【Slackスレッドの前後文脈】');
    for (const m of threadContext.slice(0, 10)) {
      const author = m.author || '不明';
      const text = (m.text || '').slice(0, 500);
      lines.push(`- ${author}: ${text}`);
    }
  }

  const userContent = lines.join('\n\n');

  const response = await client.messages.create({
    model: 'claude-haiku-4-5',
    max_tokens: 1024,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: userContent }],
    output_config: {
      format: { type: 'json_schema', schema: ANALYSIS_SCHEMA },
    },
  });

  // 構造化出力: 最初のtextブロックにJSON文字列が入る
  const textBlock = (response.content || []).find(b => b.type === 'text');
  if (!textBlock?.text) throw new Error('AI応答が空');

  let parsed;
  try { parsed = JSON.parse(textBlock.text); }
  catch (e) { throw new Error(`AI応答のJSONパース失敗: ${e.message}`); }

  return {
    summary: String(parsed.summary || '').trim(),
    type: String(parsed.type || 'その他'),
    key_points: Array.isArray(parsed.key_points) ? parsed.key_points.map(String).slice(0, 4) : [],
    usage: response.usage, // { input_tokens, output_tokens }
  };
}

module.exports = { analyzeTask };
