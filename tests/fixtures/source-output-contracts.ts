import { expect, test } from "bun:test"
import { ACPClient } from "../../../opencode-chat-bridge/src/acp-client"
import { diagnoseEmptyResponse } from "../../../opencode-chat-bridge/src/acp-response-diagnostics"
import { ImageHandler } from "../../../opencode-chat-bridge/src/image-handler"
import { MatrixPermissionBroker } from "../../../opencode-chat-bridge/connectors/matrix-permissions"
import fs from "fs"
import os from "os"
import path from "path"

test("source processes buffered trailing text before nonempty collector closure without delay", async () => {
  const client = new ACPClient()
  Object.assign(client as any, { sessionId: "fixture" })
  ;(client as any).send = () => new Promise(resolve => {
    ;(client as any).pending.set(1, { resolve, reject: () => {} })
    queueMicrotask(() => (client as any).handleData(Buffer.from([
      { method: "session/update", params: { update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "hello " } } } },
      { id: 1, result: { stopReason: "end_turn" } },
      { method: "session/update", params: { update: { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "world" } } } },
    ].map(message => JSON.stringify(message)).join("\n") + "\n")))
  })
  expect(await client.prompt("fixture")).toBe("hello world")
  expect(client.acceptsFollowups).toBe(false)
})

test("source typed image shapes exclude arbitrary data and initial tool content", () => {
  const client = new ACPClient()
  const images: any[] = []
  client.on("image", image => images.push(image))
  const image = { type: "image", data: "cG5n", mimeType: "image/png", alt: "caption" }
  const handle = (update: any) => (client as any).handleSessionUpdate({ update })
  handle({ sessionUpdate: "agent_message_chunk", content: image })
  ;(client as any).handleSessionUpdate({ part: image })
  handle({ part: image })
  handle({ sessionUpdate: "tool_call", toolCallId: "one", content: [{ content: image }] })
  handle({ sessionUpdate: "tool_call_update", toolCallId: "one", status: "completed", content: [{ content: image }] })
  handle({ sessionUpdate: "tool_call_update", toolCallId: "one", status: "completed", content: [], rawOutput: { output: JSON.stringify(image) } })
  for (const output of [image, [image], { content: [image] }, { data: image.data }, { type: "text", data: image.data }]) {
    handle({ sessionUpdate: "tool_call_update", toolCallId: "one", status: "completed", rawOutput: { output: JSON.stringify(output) } })
  }
  expect(images).toHaveLength(7)
  expect(images.every(item => item.type === "image" && item.alt === "caption")).toBe(true)
})

test("source inline image events preserve surrounding text order and repeated images", () => {
  const client = new ACPClient()
  const events: string[] = []
  client.on("chunk", chunk => events.push(chunk))
  client.on("image", () => events.push("image"))
  for (const content of [{ type: "text", text: "before" }, { type: "image", data: "cG5n" }, { type: "text", text: "middle" }, { type: "image", data: "cG5n" }, { type: "text", text: "after" }]) {
    ;(client as any).handleSessionUpdate({ update: { sessionUpdate: "agent_message_chunk", content } })
  }
  expect(events).toEqual(["before", "image", "middle", "image", "after"])
})

test("source unavailable approval is generic and cancelled without tool disclosures", () => {
  const client = new ACPClient()
  const notices: any[] = []
  const writes: any[] = []
  Object.assign(client as any, { acp: { stdin: { write: (text: string) => writes.push(JSON.parse(text)) } } })
  client.on("permission_rejected", notice => notices.push(notice))
  const request = (params: any) => ({ jsonrpc: "2.0", id: 900, method: "session/request_permission", params: { sessionId: "fixture", options: [{ optionId: "allow", kind: "allow_once" }, { optionId: "reject", kind: "reject_once" }], ...params, toolCall: { toolCallId: "source-fixture-tool", ...params.toolCall } } })
  for (const toolCall of [{ title: "write", rawInput: { filePath: "notes.txt" } }, { title: "shell" }, { title: "read", locations: [{ path: "other.txt" }] }]) {
    ;(client as any).handlePermissionRequest(request({ toolCall }))
  }
  expect(notices.map(item => item.message)).toEqual(Array(3).fill("Permission denied: approval unavailable for this request"))
  expect(notices.every(item => item.permission === "tool" && item.path === null)).toBe(true)
  const rejection = { jsonrpc: "2.0", id: 900, result: { outcome: { outcome: "cancelled" } } }
  expect(writes).toEqual([rejection, rejection, rejection])
  for (const params of [
    { title: "", toolCall: { title: "" } },
    { path: "params.txt", toolCall: { title: "read", locations: [{ path: "" }] } },
    { path: "", directory: "folder", toolCall: { title: "read" } },
  ]) {
    ;(client as any).handlePermissionRequest(request(params))
  }
  expect(notices.slice(3).map(item => item.message)).toEqual(Array(3).fill("Permission denied: approval unavailable for this request"))
  expect(writes).toEqual(Array(6).fill(rejection))
})
test("source approval screening uses ASCII token boundaries after Unicode text", async () => {
  const broker = new MatrixPermissionBroker()
  const notices: string[] = []
  const outcome = await broker.request({sender:"@fixture:local", roomId:"!fixture:local", threadId:"", sessionId:"fixture", cwd:"/tmp"},
    {sessionId:"fixture", toolCall:{toolCallId:"operation", kind:"execute", rawInput:{command:"printf 'éghp_A'"}}, options:[{kind:"allow_once", optionId:"once"}]} as any,
    new AbortController().signal, async body => { notices.push(body) }, () => true)
  expect(outcome).toEqual({outcome:"cancelled"})
  expect(notices).toEqual([])
})

test("source markers tool-first dedup and whitespace diagnostic distinction", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "output-oracle-"))
  const image = path.join(root, "image.png")
  fs.writeFileSync(image, "fixture")
  const uploads: string[] = []
  expect(await ImageHandler.processResponse(`[DOCLIBRARY_IMAGE]${image}[/DOCLIBRARY_IMAGE] caption`, `Path: ${image}`, async file => { uploads.push(file) })).toBe("caption")
  expect(uploads).toEqual([image])
  expect(diagnoseEmptyResponse("   ", "   ", "").source).toBe("bridge-processing-removed")
  expect(diagnoseEmptyResponse("answer", "", "").source).toBe("bridge-capture-lost")
  expect(diagnoseEmptyResponse("", "", "").source).toBe("acp-no-text")
})

test("source nonnormal followup reason wins over original end_turn", async () => {
  const client = new ACPClient()
  Object.assign(client as any, { sessionId: "fixture" })
  let finish: any
  let requests = 0
  ;(client as any).send = async () => {
    if (requests++ === 0) return await new Promise(resolve => { finish = resolve })
    return { result: { stopReason: "refusal" } }
  }
  const prompt = client.prompt("fixture")
  await client.forwardPrompt("fixture followup")
  finish({ result: { stopReason: "end_turn" } })
  await prompt
  expect(client.lastPromptStopReason).toBe("refusal")
  expect(client.acceptsFollowups).toBe(false)
})

test("source duplicate tool identity produces one start counter", () => {
  const client = new ACPClient()
  let starts = 0
  client.on("activity", event => { if (event.type === "tool_start") starts++ })
  const update = { sessionUpdate: "tool_call", toolCallId: "same", title: "write", rawInput: { path: "notes.txt" } }
  ;(client as any).handleSessionUpdate({ update })
  ;(client as any).handleSessionUpdate({ update })
  ;(client as any).handleSessionUpdate({ update: { sessionUpdate: "tool_call_update", toolCallId: "same", status: "completed" } })
  expect(starts).toBe(1)
})
