# Connect Matrix to an agent

This is a manual Linux user-service walkthrough for a **new, dedicated Matrix
bot**, using an unencrypted room. It works alongside the
[shared installation and service guide](MANUAL-SERVICE.md). The Telegram setup
helper does not configure Matrix.

## Prepare the account and room

1. Create a dedicated bot account on your chosen homeserver using its supported
   registration process. Do not reuse a human account or an account connected to
   another bridge.
2. Obtain an access token through a supported client or the homeserver's approved
   login flow. Use a device-backed token: the connector resolves the token's
   identity and requires a device ID. Keep the token private; do not copy it from
   another application's credential store.
3. Record the bot's full Matrix user ID and your authorized user's full ID, such
   as `@agent:example.org` and `@operator:example.org`. Display names are not IDs.
4. Create a private **unencrypted** test room and invite the bot. Copy its room ID
   from your client's room settings, such as `!roomid:example.org`, not its display
   name or `#alias:example.org`. Ensure the bot has permission to join and send.
5. Confirm the service host can reach the homeserver over HTTPS. Homeserver URLs
   and user-ID domains need not be identical; use the actual server endpoint.

Unencrypted rooms expose message content to the homeserver and are not an
end-to-end-encrypted channel. Use nonsensitive test messages. Encrypted replies
are not yet qualified for interoperability; do not disable encryption on an
existing sensitive room just to follow this guide.

## Create private configuration

Follow steps 1–3 of the [service guide](MANUAL-SERVICE.md), with `INSTANCE=matrix`.
In its private `credentials.env`, save:

```dotenv
MATRIX_ACCESS_TOKEN=REPLACE_WITH_BOT_ACCESS_TOKEN
```

Do not leave a placeholder token in a real installation. A password is an
alternative (`MATRIX_PASSWORD`), but this walkthrough uses a token. Do not
configure both unless you understand which login path is used.

In the private `bridge.json`, save this complete example and replace the IDs and
homeserver URL:

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
  "matrix": {
    "enabled": true,
    "homeserver": "https://matrix.example.org",
    "userId": "@agent:example.org",
    "accessToken": "{env:MATRIX_ACCESS_TOKEN}",
    "allowedUsers": ["@operator:example.org"],
    "allowedRooms": ["!roomid:example.org"],
    "strictAllowedRooms": true,
    "autoJoin": true,
    "encryption": {"enabled": false},
    "triggerPatterns": ["!agent "],
    "threadIsolation": true,
    "respondToThreadReplies": true,
    "respondToAllMessages": false,
    "interactivePermissions": false
  }
}
```

All other connectors remain disabled by default. Keep both allowlists explicit;
`strictAllowedRooms: true` ignores all rooms if the room list is empty. `autoJoin`
enables invite handling; keep the room restriction, and verify membership before
testing. Never widen the room list merely to make an invitation work.

Use a token belonging to the intended bot and verify its actual connected
identity. Writing `userId` is not proof that the token belongs to that account.

For another harness, use the [documented backend choices](MANUAL-SERVICE.md#2-choose-a-harness-and-understand-sessions).
A `triggerPatterns` entry includes the space after `!agent`; setting the global
`trigger` alone does not replace Matrix's patterns. An explicit mention of the
bot's user ID is another way to address it.

The example leaves interactive approvals off. Matrix's `interactivePermissions`
option supports approval of supported operations, not every tool. Review the
selected harness's policy before enabling it; do not grant blanket access to work
around an unsupported approval flow.

## Build, install, and start

Continue with steps 4–6 of the [service guide](MANUAL-SERVICE.md). Use its Matrix
build (`--no-default-features --features platform-sdks`), dedicated binary,
persistent `MATRIX_STORAGE_PATH`, and `matrix` unit instance. Keep the SQLite
store and saved access token private; do not delete them during routine upgrades.

`--check-config` is only a local load check. It does not authenticate to Matrix
or prove that the selected binary includes the connector.

## Verify a real conversation

From the allowed account in the allowed room:

1. Send `!agent Reply with matrix-ready only. Do not use tools.` Check that a
   harness-generated reply arrives in the expected thread.
2. Reply in that thread with `What exact marker did you just reply with?` Check
   that it continues the same conversation.
3. Restart the exact bridge service when it is idle. In the same thread,
   explicitly address the bot again: `!agent What marker did we use?` Check that
   saved native history resumes. Do not assume any terminal chat was imported.
4. With a separately consented test account, check that an unauthorized sender
   does not start harness work. If you also test another room, explicitly authorize
   the bot's membership/test there without adding it to `allowedRooms`, and check
   that no work starts. Do not impersonate users or send unsolicited probes.

Check both the chat and local service diagnostics; silence alone does not prove
that a denied message never reached a harness. Record unperformed checks honestly.

## Display and conversation choices

This example uses isolated threads. For a dedicated room with unthreaded replies,
set `threadIsolation` to `false` and explicitly address the bot for each request
unless you deliberately enable broader reply behavior. Do not enable
`respondToAllMessages` in a shared room just for convenience.

The [Matrix display options](SETUP.md#matrix-display-options) explain tool notices,
basename labels, and optional terminal activity threads. They change presentation,
not harness permissions or authorization. For operation, backup, upgrade, and
uninstall, return to the [service guide](MANUAL-SERVICE.md#operate-upgrade-and-remove).
