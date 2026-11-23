# Connect Mattermost to an agent

This is a manual Linux user-service walkthrough for a **new, dedicated Mattermost
bot**. Use it with the [shared installation and service guide](MANUAL-SERVICE.md).
The connector uses the Mattermost API and WebSocket connection; it does not need
a separate TypeScript/Bun process or a public inbound bridge endpoint. The
Telegram setup helper does not configure Mattermost.

## Prepare the bot and channel

1. Use your server's supported bot-account management flow to create a dedicated
   bot and generate its token. Bot creation may require administrator assistance
   or a server setting; do not broaden server permissions to bypass that policy.
2. Add the bot to the intended team/channel and grant it the permissions required
   to read and post there. Private channels require membership too. Avoid
   administrator privileges for a messaging bot.
3. Record the server's base HTTPS URL, your authorized user's **user ID**, and the
   intended **channel ID**. Use the UI's ID information where available or the
   server's documented API. Usernames, channel display names, and team names do
   not substitute for IDs. Keep API authorization headers out of shared output.
4. Confirm the service host can reach both the server API and WebSocket endpoint.
   If a reverse proxy sits in front of Mattermost, it must support WebSocket
   upgrades. Do not disable TLS verification to bypass certificate errors.

## Create private configuration

Follow steps 1–3 of the [service guide](MANUAL-SERVICE.md), with
`INSTANCE=mattermost`. In its private `credentials.env`, save:

```dotenv
MATTERMOST_URL=https://mattermost.example.org
MATTERMOST_TOKEN=REPLACE_WITH_BOT_TOKEN
```

Replace both values for the actual installation. Keep the file outside the
checkout with mode `0600`; do not paste its real contents into chat or Git.

In the private `bridge.json`, save this complete example and replace the IDs:

```json
{
  "botName": "agent",
  "trigger": "!agent",
  "sessionStorePath": "../state/chat-sessions.json",
  "acp": {
    "defaultBackend": "opencode",
    "command": "opencode",
    "args": ["acp"]
  },
  "mattermost": {
    "enabled": true,
    "url": "{env:MATTERMOST_URL}",
    "token": "{env:MATTERMOST_TOKEN}",
    "allowedUsers": ["REPLACE_WITH_USER_ID"],
    "allowedChannels": ["REPLACE_WITH_CHANNEL_ID"],
    "threadIsolation": true,
    "respondToMentions": true,
    "respondToThreadReplies": true,
    "respondToAllMessages": false
  }
}
```

All other connectors remain disabled by default. Set both authorization lists.
A missing, empty, invalid, or nonmatching channel list prevents admission of
messages. The service rejects an empty sender list unless you explicitly opt in
with `allowAllUsers: true`. Leave that off for a restricted bot. The bridge also
ignores its own posts and system posts. Access uses IDs, not the
`MATTERMOST_TEAM` setting.

For another harness, use the [documented backend choices](MANUAL-SERVICE.md#2-choose-a-harness-and-understand-sessions).
Review its native permissions: **Mattermost does not provide interactive approval
prompts. Protected operations are denied and reported.** Do not remove harness
restrictions to make a protected operation succeed.

## Build, install, and start

Continue with steps 4–6 of the [service guide](MANUAL-SERVICE.md). For a
Mattermost-only service, use its smaller `--no-default-features` build and
`mattermost` unit instance. For a [combined service](MANUAL-SERVICE.md#multiple-connectors-one-agent)
that includes Matrix or WhatsApp, use the SDK-enabled build. Do not reuse the
Telegram helper's service name or assume its commands manage this manually
installed service.

`--check-config` only loads configuration. It does not test the token, channel
membership, WebSocket connection, allowlists, or harness login.

## Optional durable turn receipts

To enable receipts on a Unix gateway, create a dedicated, owner-only directory
(mode `0700`) on persistent local storage and set `mattermost.turnReceiptDir`
to its absolute, non-symlink path. Keep `threadIsolation: true`. Use one gateway
process and one stable Mattermost server URL and bot identity per receipt directory.
Keep the directory and its parents stable and trusted while the bridge runs.
The bridge takes an exclusive lock and refuses to start with invalid receipt data.

Address the bot with these commands in the **original thread**:

- `!agent /status` shows the session status and your receipt status. Unsent or
  uncertain replies take precedence over the most recent turn's status.
- `!agent /recover` sends your oldest saved, known-unsent final text reply in the
  thread. It does not reopen the session, call the model, or rerun tools. A
  different sender, thread, server, or bot cannot recover it. Replaying a recovery
  post that already attempted delivery does not send another saved reply.
- If a reply is **uncertain**, look in the thread. The bridge will not retry it:
  Mattermost might have accepted all or part of the text before the connection
  failed. This also covers a restart after the send was recorded but before the
  POST could leave the gateway. There is no automatic remote reconciliation.
- If the turn is **interrupted or unknown**, the original Mattermost post will
  not start another turn after a restart. Tool effects may already have happened.
  Inspect the session before deciding whether to send a new request.

Receipts cover final text only. Tool updates, file/image uploads, failed-turn
partial output, and control notices are not recovered. Saving starts before the
prompt, not when the WebSocket receives a post; this is not a durable input queue.
`/reset` and `/resume` affect session history, not reply receipts. The private
journal `turns.json` retains undelivered reply text and acknowledged post IDs
until the associated state changes; protect it as chat data. Keep it across
restarts. Deleting or replacing it loses recovery and replay protection.
The journal does not auto-prune. The limits are 8 MiB (including reserved space
for concurrent turns), 8,192 turns, and 256 KiB UTF-8 / 384 KiB JSON per final
reply. At capacity, new prompts are refused before prompt dispatch; oversized
final replies are not sent. Resolve pending replies and monitor capacity before
using this opt-in feature for sustained traffic.

## Verify a real conversation

From the allowed account in the allowed channel:

1. Send `!agent Reply with mattermost-ready only. Do not use tools.` Alternatively,
   mention the bot using its actual Mattermost username. Check that a
   harness-generated reply arrives in the expected thread.
2. Reply plainly in that thread: `What exact marker did you just reply with?`
   Check that it continues the active conversation.
3. Restart the exact bridge service when idle. In the existing thread, send
   `!agent /status` to check for a saved conversation, then `!agent /resume` to
   reopen it without sending a prompt or replaying old work. Once the bot
   confirms, a plain threaded reply such as `What marker did we use?` continues
   that conversation when `respondToThreadReplies` is enabled. An addressed
   question still works without `/resume`; a plain reply alone does not reopen
   a saved conversation after restart.
4. With a separately consented test account, check that an unauthorized sender
   does not start harness work. If testing a disallowed channel too, explicitly
   authorize its bot membership/test without adding it to `allowedChannels`, and
   verify that no work starts. Do not impersonate users or send unsolicited tests.

Check both the chat and local service diagnostics; a missing reply alone is not
proof of denied harness access. Record unperformed checks rather than claiming
complete verification.

## Reply behavior and limits

- `respondToMentions` allows a bot mention to start work.
- `respondToThreadReplies` allows plain replies to continue an **active** isolated
  thread session.
- `threadIsolation: false` sends replies and notices to the channel and disables
  implicit plain-thread continuation and `/resume`.
- `/resume` uses only the current thread's saved backend and session. It does
  not accept a session ID, create a missing session, or reset history. It does
  not recover queued messages or work interrupted by a shutdown.
- To interrupt an active prompt, address the bot with `/cancel` (no arguments).
  With thread isolation enabled, this targets only the current thread; otherwise
  it targets the shared channel conversation. Without an open session, it does
  not start or reopen one. `Cancellation requested.` acknowledges the request,
  not that a prompt was still running, that the backend has stopped, or that
  tools have been rolled back.
- `respondToAllMessages: true` admits eligible unaddressed posts, including slash
  commands. It does not bypass sender/channel restrictions. Leave it off unless
  the allowed channel is intentionally meant for that behavior.
- The connector sends text, commands, tool updates, and generated images. It does
  not read incoming attachment bodies or show typing indicators.
- Interactive Matrix approvals do not apply to this connector.

For operation, backup, upgrade, and uninstall, return to the
[service guide](MANUAL-SERVICE.md#operate-upgrade-and-remove).
