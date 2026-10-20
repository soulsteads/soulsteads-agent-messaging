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
messages; it is not necessarily a startup error. An empty sender list can permit
all senders, so do not remove it while troubleshooting. Access uses IDs, not the
`MATTERMOST_TEAM` setting.

For another harness, use the [documented backend choices](MANUAL-SERVICE.md#2-choose-a-harness-and-understand-sessions).
Review its native permissions: **Mattermost does not provide interactive approval
prompts. Protected operations are denied and reported.** Do not remove harness
restrictions to make a protected operation succeed.

## Build, install, and start

Continue with steps 4–6 of the [service guide](MANUAL-SERVICE.md). Use its
Mattermost build (`--no-default-features`), dedicated binary, and `mattermost`
unit instance. Do not reuse the Telegram helper's service name or assume its
commands manage this manually installed service.

`--check-config` only loads configuration. It does not test the token, channel
membership, WebSocket connection, allowlists, or harness login.

## Verify a real conversation

From the allowed account in the allowed channel:

1. Send `!agent Reply with mattermost-ready only. Do not use tools.` Alternatively,
   mention the bot using its actual Mattermost username. Check that a
   harness-generated reply arrives in the expected thread.
2. Reply plainly in that thread: `What exact marker did you just reply with?`
   Check that it continues the active conversation.
3. Restart the exact bridge service when idle. Explicitly address the bot once
   in the existing thread, for example `!agent What marker did we use?`, to
   resume saved history. Plain thread replies alone do not reactivate an idle
   persisted binding after restart.
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
  implicit plain-thread continuation.
- `respondToAllMessages: true` admits eligible unaddressed posts, including slash
  commands. It does not bypass sender/channel restrictions. Leave it off unless
  the allowed channel is intentionally meant for that behavior.
- The connector sends text, commands, tool updates, and generated images. It does
  not read incoming attachment bodies or show typing indicators.
- Interactive Matrix approvals do not apply to this connector.

The [existing Mattermost reference](SETUP.md#mattermost) describes these settings
in more detail. For operation, backup, upgrade, and uninstall, return to the
[service guide](MANUAL-SERVICE.md#operate-upgrade-and-remove).
