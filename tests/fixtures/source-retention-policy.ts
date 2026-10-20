import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import fs from "node:fs"
import path from "node:path"
import { execFileSync } from "node:child_process"

const root = path.resolve(import.meta.dir, "../../../opencode-chat-bridge")
assert.equal(execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(), "ce8ca57201c607494f1c930133f3758a792f0155")
const hashes = {
  "src/connector-base.ts": "64d702cbb7adb374cc4de9777a5375b60398c218d28b08538487ca9ab5b075a0",
  "connectors/matrix.ts": "1fcf68f125d9adc73d2e907fe9a67bb610c8b0e0ab355a261a58235281f26c9a",
  "connectors/discord.ts": "f1f6a104fc0854484db907775078a342dc394165c3ecd6252c2c0143546fafd8",
  "connectors/mattermost.ts": "910f132aaa9c2e1a17a34951fef8c03f677eaf018d62418902a76ea9a2330f43",
  "connectors/telegram.ts": "ca216c571937e41782690cbc517c2bd1db1f851e8915cccbdc8868bd47241404",
  "connectors/slack.ts": "7761071856accf169649064b6c34749c22f4d4a19be0dcfc94c98970a0b6b210",
  "connectors/whatsapp.ts": "bccd8c2fd97319f622e125f09bbaf1571a59c7707b42e79d834f6971bde99a60",
  "connectors/web.ts": "d64f34c1f0edbeb5791ee389cca560e1779dcde9975723a7a7cffb8386e3fde7",
}
const sources = Object.fromEntries(Object.entries(hashes).map(([file, hash]) => {
  const text = fs.readFileSync(path.join(root, file), "utf8")
  assert.equal(createHash("sha256").update(text).digest("hex"), hash, file)
  return [file, text]
}))
function section(text: string, start: string, end: string) {
  const begin = text.indexOf(start)
  assert.ok(begin >= 0, start)
  const finish = text.indexOf(end, begin + start.length)
  assert.ok(finish > begin, end)
  return text.slice(begin, finish)
}
function compile(code: string, names: string[], values: unknown[]) {
  return new Function(...names, new Bun.Transpiler({ loader: "ts" }).transformSync(code))(...values)
}
const base = sources["src/connector-base.ts"]
const baseParser = compile(section(base, "function parseSessionRetentionMins(", "/**") + "\nreturn parseSessionRetentionMins", [], [])
const slackParser = compile(section(sources["connectors/slack.ts"], "function parseSessionRetentionMins(", "// =============================================================================") + "\nreturn parseSessionRetentionMins", [], [])
const fallback = section(base, "    if (this.config.sessionRetentionMins === undefined)", "    if (this.config.allowedUsers")
const startLoop = section(base, "  protected startSessionExpiryLoop(): void", "  /**")
const rawValues = [null, "", "invalid", "0", "-15", "15", "15minutes", "  +15tail", "\t\r\n+2.9", "0x10", "1e6", "+", "--1", "-0", "00015", "١٥", "\u008515", "\u180e15", "\u200b15", "\ufeff\u00a0\u202815tail", "18446744073709551616", "9007199254740993", "1" + "0".repeat(300), "17976931348623157" + "0".repeat(292), "1" + "0".repeat(309), "9".repeat(10000)]
const whitespace = ["\u0009", "\u000b", "\u000c", "\u0020", "\u00a0", "\u1680", "\u2000", "\u2001", "\u2002", "\u2003", "\u2004", "\u2005", "\u2006", "\u2007", "\u2008", "\u2009", "\u200a", "\u202f", "\u205f", "\u3000", "\ufeff", "\u000a", "\u000d", "\u2028", "\u2029"]
rawValues.push(...whitespace.map(prefix => prefix + "+15tail"))
const cases = rawValues.map(raw => {
  const env = raw === null ? {} : { SESSION_RETENTION_MINS: raw }
  const minutes: Record<string, number | null> = {}
  const timers: Record<string, number> = {}
  for (const platform of ["matrix", "discord", "mattermost", "telegram", "slack", "whatsapp", "web"]) {
    const source = sources[`connectors/${platform}.ts`]
    const constructor = section(source, "    super({", "    })") + "    })"
    const startup = section(source, "  async start(): Promise<void>", "  async stop(): Promise<void>")
    assert.ok(startup.includes("this.startSessionExpiryLoop()"), platform)
    let installed = 0
    const code = `class Fixture {
      expiryInterval = null
      constructor(config) { this.config = config; ${fallback} }
      ${startLoop}
    }
    class Connector extends Fixture { constructor() { ${constructor} } }
    const connector = new Connector()
    connector.startSessionExpiryLoop()
    connector.startSessionExpiryLoop()
    return connector.config.sessionRetentionMins`
    minutes[platform] = compile(code,
      ["parseSessionRetentionMins", "process", "TRIGGER", "BOT_NAME", "RATE_LIMIT_SECONDS", "SESSION_RETENTION_DAYS", "SESSION_RETENTION_MINS", "ALLOWED_USERS", "setInterval", "EXPIRY_SWEEP_INTERVAL_MS"],
      [baseParser, { env }, "!fixture", "Fixture", 5, 7, slackParser(env), [], () => { installed++; return {} }, 60000]) ?? null
    timers[platform] = installed
    assert.equal(installed, minutes[platform] === null ? 0 : 1)
  }
  const bits = Object.fromEntries(Object.entries(minutes).map(([platform, value]) => {
    if (value === null) return [platform, null]
    const bytes = Buffer.alloc(8)
    bytes.writeDoubleBE(value)
    return [platform, bytes.toString("hex")]
  }))
  return { raw, minutes, bits, timers }
})
console.log(JSON.stringify({ pin: "ce8ca57201c607494f1c930133f3758a792f0155", hashes, cases }, null, 2))
