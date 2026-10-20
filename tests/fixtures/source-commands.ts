import { readFileSync } from 'node:fs'

const [sourceRoot, specificationFile] = process.argv.slice(2)
const { WebConnector } = await import(`${sourceRoot}/connectors/web.ts`)
const { sanitizeServerPaths, removeImageMarkers, removeDocMarkers } = await import(`${sourceRoot}/src/index.ts`)
const specification = JSON.parse(readFileSync(specificationFile, 'utf8'))
const connector: any = new WebConnector()
connector.config.rateLimitSeconds = 0
const cases: any[] = []
let events: any[] = []
connector.wsSend = (_clientId: string, event: any) => events.push(event)
try {
  for (const step of specification.steps) {
    events = []
    const existing = connector.sessionManager.get(step.id)
    const originalNow = Date.now
    const originalDates = existing ? [existing.createdAt, existing.lastActivity] : null
    if (step.ageMs !== undefined) {
      if (!existing) throw new Error('rounding requires an actual completed ACP session')
      const now = originalNow()
      Date.now = () => now
      existing.createdAt = new Date(now - step.ageMs)
      existing.lastActivity = new Date(now - step.idleMs)
    }
    try {
      await connector.onWsMessage({ data: { clientId: step.id } }, JSON.stringify({ type: 'message', text: step.text }))
    } finally {
      Date.now = originalNow
      if (originalDates) [existing.createdAt, existing.lastActivity] = originalDates
    }
    const session = connector.sessionManager.get(step.id)
    const responses = events.filter(event => event.type === 'response').map(event => event.text)
    const chunks = events.filter(event => event.type === 'chunk').map(event => event.text).join('')
    let requests: any[] = []
    try { requests = readFileSync('audit.jsonl', 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line)) } catch (error: any) {
      if (error.code !== 'ENOENT') throw error
    }
    cases.push({ ...step, output: responses.length ? responses : chunks ? [chunks] : [],
      libraryOutput: responses.length ? responses : chunks ? [sanitizeServerPaths(removeDocMarkers(removeImageMarkers(chunks)))] : [],
      states: events.filter(event => event.type === 'session_state').map(event => event.state),
      prompts: requests.filter(request => request.method === 'session/prompt').map(request => request.params.prompt[0].text),
      sessionOpens: requests.filter(request => request.method === 'session/new').length,
      sessionDeletes: requests.filter(request => request.method === 'session/delete').length,
      stats: session ? { messageCount: session.messageCount, inputChars: session.inputChars, outputChars: session.outputChars } : null })
  }
} finally {
  await connector.disconnectAllSessions()
}
console.log(JSON.stringify(cases))
