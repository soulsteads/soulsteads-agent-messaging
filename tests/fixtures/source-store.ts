const [source, file, ready] = process.argv.slice(2)
const { ACPSessionStore } = await import(source)
const store = new ACPSessionStore(file)
await Bun.write(ready, "ready")
for (let index = 0; index < 40; index++) {
  await store.set({ connector: "matrix", threadId: `source-${index}`, sessionId: `ts-${index}`, cwd: "/fixture", backendId: "fixture", updatedAt: "2026-10-04T00:00:00Z", backend: "codex" }, true)
}
console.log("Source store writer completed")
