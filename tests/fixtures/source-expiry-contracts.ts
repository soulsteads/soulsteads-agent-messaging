import { describe, expect, test } from "bun:test"
import fs from "fs"
import path from "path"
import { createHash } from "crypto"

const root = path.resolve(import.meta.dir, "../../../opencode-chat-bridge")
const source = fs.readFileSync(path.join(root, "src/connector-base.ts"), "utf8")
const cases = fs.readFileSync(path.join(root, "tests/unit/connector-base.test.ts"), "utf8")
expect(createHash("sha256").update(source).digest("hex")).toBe("64d702cbb7adb374cc4de9777a5375b60398c218d28b08538487ca9ab5b075a0")
expect(createHash("sha256").update(cases).digest("hex")).toBe("c42e98b997c69ab742fd16227d7bef9f90da2c9aa1b4f4cddb3d39d1e087ecc3")

function section(text: string, start: string, end: string): string {
  const begin = text.indexOf(start)
  expect(begin).toBeGreaterThanOrEqual(0)
  const finish = text.indexOf(end, begin + start.length)
  expect(finish).toBeGreaterThan(begin)
  return text.slice(begin, finish)
}

const methods = [
  section(source, "  protected hasPersistedACPSession(", "  /**"),
  section(source, "  protected async invalidateACPSession(", "  /**"),
  section(source, "  protected getSelectedBackend(", "  private backendConfig("),
  section(source, "  private async expireStaleSessions(", "  /**"),
  section(source, "  private async handleClearCommand(", "  private async handleHelpCommand("),
].join("\n")
const parser = section(source, "function parseSessionRetentionMins(", "/**")
const persistenceCases = section(cases, 'describe("BaseConnector ACP session persistence",', "// =============================================================================")
const code = `
${parser}
class TestConnector {
  config = { connector: "test" }
  acpConfig = {}
  activeQueries = new Map()
  sessionManager = new Map()
  acpSessionStore = {}
  constructor() { this.sessionManager.sessions = this.sessionManager }
  log() {}
  logError() {}
  deleteSessionCacheDir() { throw new Error("Disk cleanup is never allowed in this harness") }
  ${methods}
}
${persistenceCases}
return { TestConnector, parseSessionRetentionMins }
`
const transformed = new Bun.Transpiler({ loader: "ts" }).transformSync(code)
const CommandHandler = {
  formatSessionClearedMessage: () => "cleared",
  formatNoSessionMessage: () => "none",
}
const extracted = new Function("describe", "test", "expect", "CommandHandler", transformed)(describe, test, expect, CommandHandler)

test("exact Source selected-backend invalidation swallows disconnect errors and continues candidates", async () => {
  const connector = new extracted.TestConnector()
  const calls: string[] = []
  connector.config.sessionRetentionMins = 1
  connector.acpSessionStore = {
    getSelectedBackend: () => "codex",
    delete: async (_connector: string, id: string, backend: string) => {
      calls.push(`delete:${id}:${backend}`)
      if (id === "failed") throw new Error("synthetic delete failure")
    },
  }
  connector.deleteSessionCacheDir = (id: string) => { calls.push(`cache:${id}`) }
  for (const id of ["failed", "good", "active", "fresh"]) {
    connector.sessionManager.set(id, {
      lastActivity: new Date(Date.now() - (id === "fresh" ? 0 : 120_000)),
      client: { disconnect: async () => { calls.push(`disconnect:${id}`); throw new Error("synthetic disconnect failure") } },
    })
  }
  connector.activeQueries.set("active", {})
  await connector.expireStaleSessions()
  expect(calls).toEqual(["delete:failed:codex", "delete:good:codex", "disconnect:good", "cache:good"])
  expect([...connector.sessionManager.keys()]).toEqual(["failed", "active", "fresh"])
})

test("exact Source clear/reset live-session path differs from expiry invalidation", async () => {
  const connector = new extracted.TestConnector()
  const calls: string[] = []
  connector.acpSessionStore = {
    getSelectedBackend: () => "codex",
    get: () => null,
    delete: async (_connector: string, id: string, backend: string) => { calls.push(`delete:${id}:${backend}`) },
  }
  connector.sessionManager.set("clear", { client: {
    deleteSession: async () => { calls.push("backend-delete"); throw new Error("synthetic backend failure") },
    disconnect: async () => { calls.push("disconnect"); throw new Error("synthetic disconnect failure") },
  } })
  await connector.handleClearCommand("clear", async (text: string) => { calls.push(text) })
  expect(calls).toEqual(["backend-delete", "disconnect", "delete:clear:codex", "cleared"])
  expect(connector.sessionManager.has("clear")).toBe(false)
})

test("exact Source retention parser disables absent invalid and nonpositive values", () => {
  for (const value of [undefined, "", "0", "-1", "bad"]) {
    expect(extracted.parseSessionRetentionMins({ SESSION_RETENTION_MINS: value })).toBeUndefined()
  }
  for (const value of ["12", "12abc", " 12 minutes"]) {
    expect(extracted.parseSessionRetentionMins({ SESSION_RETENTION_MINS: value })).toBe(12)
  }
})
