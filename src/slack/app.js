const { App, ExpressReceiver } = require("@slack/bolt");

const receiver = new ExpressReceiver({
  signingSecret: process.env.SLACK_SIGNING_SECRET,
});

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  receiver,
});

app.error(async (error) => {
  const payload = error.original?.payload || error.payload;
  const actionId = payload?.actions?.[0]?.action_id
    || payload?.callback_id
    || payload?.type
    || '(unknown)';
  console.error("[app.error] unhandled event:", actionId, error.message || error);
});

module.exports = {
  app,
  receiver,
};
