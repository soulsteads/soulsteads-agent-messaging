# Review Skillstore proposals in Telegram

Agent Messaging can check a configured Skillstore registry for open proposals
and announce them in one private Telegram chat. The reviewer can inspect the
diff and approve or reject the proposal from that chat.

## Configure the inbox

Use a running Telegram connector and a Skillstore registry served by
`skillstore serve`. For shared registries use private reads so open
proposals and their diffs require authentication. The agent uses its contributor
token for submission. Create a separate Skillstore token for a human registry
reviewer and place it in an owner-only file readable by the Messaging service.
Keep the reviewer file outside agent workspaces and away from harness
credentials. For enforcement with unrestricted harness tools, run the
Telegram review inbox under a separate OS identity and a separate bot from
regular agent chat. Set `reviewsOnly` to `true` and disable other connectors
on that Messaging instance so its Telegram transport only handles reviews.

Add `skillReviews` under the existing `telegram` object and keep its bot
configuration. Use the Telegram reviewer’s numeric ID here and their ID as a
string in the connector's existing `allowedUsers` list. Start a DM with the bot.

```json
{
  "telegram": {
    "enabled": true,
    "allowedUsers": ["123456789"],
    "skillReviews": {
      "enabled": true,
      "registryUrl": "https://skills.example.net",
      "reviewerUserId": 123456789,
      "reviewerTokenFile": "/private/messaging/skillstore-reviewer.token",
      "receiptDir": "/private/messaging/review-receipts",
      "skills": ["shipping"],
      "pollSeconds": 30,
      "allowDecisions": true,
      "reviewsOnly": false
    }
  }
}
```

The token file must be owner-only. Create the receipt directory before starting
the connector and give it mode `0700`. The inbox is opt-in. Set `enabled` to
`false` to disable it. The optional `skills` list filters proposals by exact
name. An empty or omitted list follows every skill in the configured registry.
`pollSeconds` defaults to 30 and accepts values from 5 to 3600.
`allowDecisions` defaults to `true`. Set it to `false` for notifications and
diffs only and use a registry reader credential. Registry connections support
HTTPS and loopback HTTP.

## Review a correction

1. The bot sends the skill name and proposal ID when it finds an open proposal.
2. In the same DM, use `/skillreview ID` to read the diff. For longer diffs,
   follow the bot's instruction to use `skillstore proposal-diff ID` locally.
3. Send `/skillapprove ID NEW_VERSION` to publish the reviewed version, or
   `/skillreject ID` to reject it. Skillstore applies its configured reviewer
   policy. The bot reports the recorded status if a decision already exists.

If a decision times out, check with `/skillreview ID` before trying again.
A notice is recorded after Telegram acknowledges delivery. A process
interruption after send and before the receipt write can repeat the notice.
The registry owns the proposal decision across restarts. Use Agent Home's
`skill adopt` command to choose where to adopt an approved version.
