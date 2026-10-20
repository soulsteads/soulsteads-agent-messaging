import { expect, test } from "bun:test"
import fs from "fs"
import vm from "vm"

const source = fs.readFileSync(new URL("../../../opencode-chat-bridge/src/cli.ts", import.meta.url), "utf8")
const transpiler = new Bun.Transpiler({ loader: "ts" })
const display = source.slice(source.indexOf("function displayImage("), source.indexOf("// MCP servers"))
const helpers = source.slice(source.indexOf("    const findImageFile ="), source.indexOf("    // Single prompt mode"))
const listeners = source.slice(source.indexOf('  client.on("chunk"'), source.indexOf('  client.on("agent-set"'))

function harness(files: string[], available = true, status = 0) {
  const lines: string[] = [], calls: any[] = [], events: Record<string, Function> = {}
  const context = vm.createContext({
    hasChafa: available, showImages: true, responseBuffer: "",
    existsSync: (name: string) => files.includes(name),
    spawnSync: (...args: any[]) => { calls.push(args); return { status } },
    console: { log: (line = "") => lines.push(line) },
    process: { stdout: { write: (line: string) => lines.push(line) } },
    client: { on: (name: string, handler: Function) => events[name] = handler },
  })
  vm.runInContext(transpiler.transformSync(display + helpers + listeners), context)
  return { lines, calls, events, context, process: () => vm.runInContext("processImages()", context) }
}

test("actual pinned CLI helper slices: three forms, both page variants, actual-path dedup and reset", () => {
  const files = ["/tmp/doclibrary_cache/book_page1.png", "/tmp/doclibrary_cache/other_page_2.png"]
  const cli = harness(files)
  cli.events.chunk("Image file: /tmp/doclibrary_cache/book_page_1.png\n`/tmp/doclibrary_cache/book_page1.png`\n/tmp/doclibrary_cache/other_page2.png\n")
  expect(cli.calls).toHaveLength(0)
  cli.process()
  expect(cli.calls.map(call => call[1])).toEqual(files.map(name => ["--size=60", "--colors=256", "--symbols=block", name]))
  cli.process()
  expect(cli.calls).toHaveLength(2)
  cli.events.chunk("Image file: /tmp/doclibrary_cache/book_page1.png\n")
  cli.process()
  expect(cli.calls).toHaveLength(3)
})

test("actual pinned CLI helper slices: --no-images does not suppress inline notices or render bytes", () => {
  const cli = harness(["/tmp/doclibrary_cache/book.png"])
  vm.runInContext("showImages = false", cli.context)
  cli.events.chunk("Image file: /tmp/doclibrary_cache/book.png\n")
  cli.events.image({ mimeType: "image/png", alt: "synthetic" })
  cli.events.image({ mimeType: "image/png" })
  cli.process()
  expect(cli.calls).toHaveLength(0)
  expect(cli.lines).toContain("\n[Image received: synthetic]")
  expect(cli.lines).toContain("\n[Image received: image/png]")
})

test("actual pinned CLI helper slices: absent/failing renderer fallback, missing path skipped", () => {
  for (const [available, status] of [[false, 0], [true, 1]] as const) {
    const cli = harness(["/tmp/doclibrary_cache/book.png"], available, status)
    cli.events.chunk("Image file: /tmp/doclibrary_cache/book.png\nImage file: /tmp/doclibrary_cache/missing.png\n")
    cli.process()
    expect(cli.lines).toContain("[Image: /tmp/doclibrary_cache/book.png]")
    expect(cli.calls).toHaveLength(available ? 1 : 0)
  }
})
