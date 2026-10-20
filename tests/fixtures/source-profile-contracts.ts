import { expect, test } from "bun:test"
import { copyACPProfile } from "../../../opencode-chat-bridge/src/session-utils"
import fs from "fs"
import path from "path"

test("pinned Source copies contents, overwrites and retains leftovers using process cwd", () => {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR!, "source-profile-"))
  try {
    const source = path.join(root, "profile")
    const destination = path.join(root, "session")
    fs.mkdirSync(path.join(source, ".opencode/skills/demo"), { recursive: true })
    fs.mkdirSync(destination)
    fs.writeFileSync(path.join(source, "AGENTS.md"), "synthetic agents")
    fs.writeFileSync(path.join(source, ".opencode/skills/demo/SKILL.md"), "synthetic skill")
    fs.writeFileSync(path.join(destination, "unrelated"), "keep")
    copyACPProfile(destination, path.relative(process.cwd(), source))
    expect(fs.readFileSync(path.join(destination, "AGENTS.md"), "utf8")).toBe("synthetic agents")
    expect(fs.readFileSync(path.join(destination, ".opencode/skills/demo/SKILL.md"), "utf8")).toBe("synthetic skill")
    fs.writeFileSync(path.join(source, "AGENTS.md"), "replacement")
    fs.unlinkSync(path.join(source, ".opencode/skills/demo/SKILL.md"))
    copyACPProfile(destination, source)
    expect(fs.readFileSync(path.join(destination, "AGENTS.md"), "utf8")).toBe("replacement")
    expect(fs.existsSync(path.join(destination, ".opencode/skills/demo/SKILL.md"))).toBe(true)
    expect(fs.readFileSync(path.join(destination, "unrelated"), "utf8")).toBe("keep")
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test("pinned Source absent and empty options no-op; nonempty missing and nondirectory fail", () => {
  const root = fs.mkdtempSync(path.join(process.env.TMPDIR!, "source-profile-"))
  try {
    copyACPProfile(root)
    copyACPProfile(root, "")
    expect(() => copyACPProfile(root, path.join(root, "missing"))).toThrow("ACP profile directory does not exist")
    fs.writeFileSync(path.join(root, "file"), "synthetic")
    expect(() => copyACPProfile(root, path.join(root, "file"))).toThrow("ACP profile directory does not exist")
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})
