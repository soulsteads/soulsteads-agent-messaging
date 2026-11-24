import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";

if (process.argv.length !== 3) {
  console.error("Usage: node tests/settings_console_browser.mjs /path/to/soulsteads-messaging");
  process.exit(2);
}
const binary = resolve(process.argv[2]);
const chromium = process.env.CHROMIUM || "chromium";
const root = await mkdtemp(join(tmpdir(), "settings-browser-"));
const children = [];
const watchdog = setTimeout(() => {
  for (const child of children) child.kill("SIGKILL");
}, 30000);

try {
  await writeFile(join(root, "settings.json"), '{"telegram":{"token":"browser_private"},"rateLimitSeconds":5}');
  const server = spawn(binary,
    ["--settings", "serve", "--config", "settings.json"], {
      cwd: root, env: { HOME: root, WEB_PORT: "4444", MATTERMOST_TOKEN: "synthetic_fallback" }, stdio: ["ignore", "pipe", "ignore"],
    });
  children.push(server);
  const lines = createInterface({ input: server.stdout });
  const bootstrap = await new Promise((resolve, reject) => {
    server.once("error", () => reject(new Error("Could not start settings console")));
    lines.once("close", () => reject(new Error("Settings console closed before bootstrap")));
    lines.once("line", resolve);
  });
  if (!/^http:\/\/127\.0\.0\.1:\d+\/#[-\w]+$/.test(bootstrap)) {
    throw new Error("Expected a private loopback bootstrap URL");
  }
  const browser = spawn(chromium.includes("/") ? resolve(chromium) : chromium, [
    "--headless", "--no-sandbox", "--disable-dev-shm-usage", "--disable-background-networking",
    "--no-proxy-server", "--no-first-run", "--remote-debugging-pipe", "--user-data-dir=" + join(root, "browser"),
  ], { cwd: root, env: { HOME: root, PATH: process.env.PATH }, stdio: ["ignore", "ignore", "ignore", "pipe", "pipe"] });
  children.push(browser);
  const pending = new Map();
  let closed = false;
  let id = 0;
  let buffer = "";
  browser.stdio[4].setEncoding("utf8");
  browser.stdio[4].on("data", data => {
    buffer += data;
    let end;
    while ((end = buffer.indexOf("\0")) >= 0) {
      const message = JSON.parse(buffer.slice(0, end));
      buffer = buffer.slice(end + 1);
      if (message.id && pending.has(message.id)) {
        const [resolve, reject] = pending.get(message.id);
        pending.delete(message.id);
        if (message.error) reject(new Error("Browser protocol error"));
        else resolve(message.result);
      }
    }
  });
  function browserClosed() {
    closed = true;
    for (const [, reject] of pending.values()) reject(new Error("Chromium exited"));
    pending.clear();
  }
  browser.on("exit", browserClosed);
  browser.on("error", browserClosed);
  browser.stdio[3].on("error", browserClosed);
  function command(method, params, sessionId) {
    if (closed) return Promise.reject(new Error("Chromium exited"));
    const request = { id: ++id, method, params, sessionId };
    return new Promise((resolve, reject) => {
      pending.set(id, [resolve, reject]);
      browser.stdio[3].write(JSON.stringify(request) + "\0");
    });
  }
  const { targetId } = await command("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await command("Target.attachToTarget", { targetId, flatten: true });
  const navigate = url => command("Page.navigate", { url }, sessionId);
  async function evaluate(expression) {
    const result = await command("Runtime.evaluate", { expression, returnByValue: true }, sessionId);
    if (result.exceptionDetails) throw new Error("Browser evaluation failed");
    return result.result.value;
  }
  async function wait(expression, label) {
    for (let i = 0; i < 80; i++) {
      if (await evaluate(expression)) return;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    throw new Error("Timed out waiting for " + label);
  }
  async function screenshot(name) {
    const { data } = await command("Page.captureScreenshot", { format: "png" }, sessionId);
    await writeFile(join(process.env.SETTINGS_SCREENSHOTS, name), Buffer.from(data, "base64"));
  }
  await command("Emulation.setDeviceMetricsOverride", {
    width: 1366, height: 850, deviceScaleFactor: 1, mobile: false,
  }, sessionId);
  await navigate(bootstrap);
  await wait('document.querySelector("#status")?.textContent.includes("validation passed")', "validated page");
  assert.equal(await evaluate('location.hash === "" && localStorage.length === 0 && sessionStorage.length === 0 && document.cookie === ""'), true);
  assert.equal(await evaluate('document.documentElement.outerHTML.includes("browser_private")'), false);
  assert.equal(await evaluate('!!document.querySelector("#source")'), true, "source filter is present");
  async function filter(id, value, event = "input") {
    await evaluate(`(() => {
      const control = document.getElementById(${JSON.stringify(id)});
      control.value = ${JSON.stringify(value)};
      control.dispatchEvent(new Event(${JSON.stringify(event)}));
    })()`);
  }
  const click = selector => evaluate(`(() => {
    const control = document.querySelector(${JSON.stringify(selector)});
    control.focus();
    control.click();
  })()`);
  const rows = () => evaluate('Array.from(document.querySelectorAll(".setting code"), node => node.textContent)');
  await click('details[data-group="General"] summary');
  await filter("source", "configured", "change");
  assert.ok((await rows()).includes("/rateLimitSeconds"));
  assert.ok(!(await rows()).includes("/web/port"));
  await filter("search", "web/port");
  assert.deepEqual(await rows(), []);
  assert.match(await evaluate('document.querySelector("#results").textContent'), /no settings match/i);
  await filter("source", "env_override", "change");
  assert.deepEqual(await rows(), ["/web/port"]);
  assert.equal(await evaluate('document.querySelector(".badge").textContent'), "Environment");
  assert.match(await evaluate('document.querySelector("#results").textContent'), /1 setting/);
  assert.equal(await evaluate('document.querySelector("details").open'), true);
  assert.match(await evaluate('document.querySelector(".setting").textContent'), /Current environment value: 4444/);
  assert.equal(await evaluate('document.querySelector(".setting input, .setting select") === null'), true);
  if (process.env.SETTINGS_SCREENSHOTS) {
    await screenshot("settings-console-laptop.png");
  }
  await command("Emulation.setDeviceMetricsOverride", {
    width: 360, height: 800, deviceScaleFactor: 1, mobile: true,
  }, sessionId);
  await click(".setting button");
  assert.equal(await evaluate('document.querySelector("#path").value'), "/web/port");
  assert.equal(await evaluate('document.activeElement.id'), "value");
  assert.match(await evaluate('document.querySelector("#edit-context").textContent'), /environment.*takes precedence/i);
  assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth && innerWidth === 360'), true);
  if (process.env.SETTINGS_SCREENSHOTS) {
    await evaluate("window.scrollTo(0, 0)");
    await screenshot("settings-console-mobile.png");
  }
  const statusText = () => evaluate('document.querySelector("#status").textContent');
  async function assertEnvironmentSave() {
    const confirmation = await statusText();
    assert.match(confirmation, /saved to file/i);
    assert.match(confirmation, /environment.*takes precedence/i);
    assert.match(confirmation, /even after.*restart/i);
    assert.doesNotMatch(confirmation, /restart the gateway to apply/i);
    assert.equal(await evaluate('document.querySelector("#edit-context").textContent'), "");
  }
  await filter("value", "5554");
  await click('#edit button[value="set"]');
  await wait('document.querySelector("#path")?.value === ""', "environment file save");
  assert.match(await evaluate('document.querySelector(".setting").textContent'), /Current environment value: 4444/);
  await assertEnvironmentSave();
  await filter("search", "mattermost/token");
  assert.deepEqual(await rows(), ["/mattermost/token"]);
  assert.equal(await evaluate('document.querySelector(".badge").textContent'), "Environment");
  await click(".setting button");
  await filter("value", '"synthetic_file"');
  await click('#edit button[value="set"]');
  await wait('document.querySelector("#path")?.value === ""', "fallback file save");
  assert.match(await statusText(), /saved to file.*restart the gateway to apply/i);
  assert.doesNotMatch(await statusText(), /environment.*takes precedence/i);
  await filter("source", "configured", "change");
  assert.deepEqual(await rows(), ["/mattermost/token"]);
  assert.equal(await evaluate('document.querySelector(".badge").textContent'), "File");
  assert.equal(JSON.parse(await readFile(join(root, "settings.json"), "utf8")).mattermost.token, "synthetic_file");
  await filter("search", "web/port");
  await filter("source", "env_override", "change");
  await click(".setting button");
  await filter("value", "5555");
  await evaluate(`(() => {
    const originalFetch = window.fetch;
    window.fetch = (url, options) => {
      if (url === "/api/settings" && options.method === "GET") {
        window.fetch = originalFetch;
        return Promise.reject(new TypeError("Test refresh failure"));
      }
      return originalFetch(url, options);
    };
  })()`);
  await click('#edit button[value="set"]');
  await wait('document.querySelector("#path")?.value === ""', "environment save with refresh failure");
  assert.match(await statusText(), /saved to file.*reload to refresh/i);
  assert.doesNotMatch(await statusText(), /environment.*takes precedence|restart the gateway/i);
  assert.equal(JSON.parse(await readFile(join(root, "settings.json"), "utf8")).web.port, 5555);
  await click("#refresh");
  await wait('document.querySelector(".setting")?.textContent.includes("Current environment value: 4444")', "environment value after reload");
  await click("#clear-filters");
  assert.equal(await evaluate('document.querySelector("#search").value === "" && document.querySelector("#source").value === ""'), true);
  assert.equal(await evaluate('document.querySelector(\'details[data-group="General"]\').open'), true);
  assert.equal(await evaluate('document.querySelector(\'details[data-group="web"]\').open'), false);
  await filter("source", "default", "change");
  assert.ok(!(await rows()).includes("/rateLimitSeconds"));
  assert.ok((await rows()).length > 1);
  await filter("source", "configured", "change");
  await filter("search", "RATELIMITSECONDS");
  assert.deepEqual(await rows(), ["/rateLimitSeconds"]);
  const scalarValue = () => evaluate('document.querySelector(".setting input, .setting select")?.value');
  const enterScalar = value => evaluate(`(() => {
    const control = document.querySelector(".setting input, .setting select");
    control.value = ${JSON.stringify(value)};
    control.dispatchEvent(new Event("input"));
    control.dispatchEvent(new Event("change"));
  })()`);
  const waitForIdle = label => wait('document.querySelector("#workspace")?.getAttribute("aria-busy") === "false"', label);
  const readSettings = async () => JSON.parse(await readFile(join(root, "settings.json"), "utf8"));
  await enterScalar("8");
  await filter("source", "", "change");
  assert.equal(await scalarValue(), "8", "visible scalar draft survives source filtering");
  await filter("source", "default", "change");
  assert.deepEqual(await rows(), []);
  await click("#clear-filters");
  await filter("search", "rateLimitSeconds");
  assert.equal(await scalarValue(), "8", "hidden scalar draft survives clearing filters");
  await filter("search", "/toolMessages/mode");
  const originalMode = await scalarValue();
  const draftMode = originalMode === "trace" ? "off" : "trace";
  await enterScalar(draftMode);
  await filter("search", "rateLimitSeconds");
  assert.equal((await readSettings()).rateLimitSeconds, 5);
  assert.equal((await readSettings()).toolMessages, undefined);
  await evaluate(`(() => {
    const input = document.querySelector(".setting input");
    input.dataset.beforeSave = "";
  })()`);
  await click(".setting button");
  await wait('document.querySelector("#workspace")?.getAttribute("aria-busy") === "false"', "scalar request");
  assert.match(await statusText(), /restart the gateway to apply/i);
  assert.doesNotMatch(await statusText(), /environment.*takes precedence/i);
  assert.equal(await evaluate('document.querySelector(".setting input:not([data-before-save])")?.value'), "8");
  assert.equal(await evaluate('document.activeElement.textContent'), "Set");
  assert.equal(JSON.parse(await readFile(join(root, "settings.json"), "utf8")).rateLimitSeconds, 8);
  await filter("search", "/toolMessages/mode");
  assert.equal(await scalarValue(), draftMode, "saving one path preserves another draft");
  assert.equal((await readSettings()).toolMessages, undefined);
  await click("#refresh");
  await waitForIdle("explicit reload");
  assert.equal(await scalarValue(), originalMode, "explicit reload discards drafts");

  const freshMode = ["off", "events", "trace"].find(mode => mode !== originalMode && mode !== draftMode);
  await enterScalar(draftMode);
  await filter("search", "rateLimitSeconds");
  await enterScalar("7");
  await evaluate(`(() => {
    const originalFetch = window.fetch;
    window.settingsPostCount = 0;
    window.fetch = async (url, options) => {
      if (url === "/api/settings" && options.method === "POST") window.settingsPostCount++;
      if (url === "/api/settings" && options.method === "GET" && window.settingsPostCount === 1) {
        // Save has accepted the POST response. Hold the real GET until the fixture is replaced.
        await new Promise(resolve => { window.resumeSettingsRefresh = resolve; });
      }
      return originalFetch(url, options);
    };
    window.restoreSettingsFetch = () => { window.fetch = originalFetch; };
  })()`);
  await click(".setting button");
  await wait('typeof window.resumeSettingsRefresh === "function"', "post-save GET");
  const afterFirstSave = await readSettings();
  assert.equal(afterFirstSave.rateLimitSeconds, 7);
  assert.equal(afterFirstSave.toolMessages, undefined);
  const replacement = join(root, "settings-next.json");
  await writeFile(replacement, JSON.stringify({ ...afterFirstSave, toolMessages: { mode: freshMode } }));
  await rename(replacement, join(root, "settings.json"));
  await evaluate("window.resumeSettingsRefresh()");
  await waitForIdle("post-save refresh of externally changed draft");
  await filter("search", "/toolMessages/mode");
  assert.equal(await scalarValue(), freshMode, "refresh shows the fresh disk value for the second draft path");
  assert.equal(await evaluate("window.settingsPostCount"), 1, "refresh leaves the second path unsaved");
  assert.equal((await readSettings()).toolMessages.mode, freshMode);
  assert.match(await statusText(), /draft.*discarded/i);
  await evaluate("window.restoreSettingsFetch()");

  await filter("search", "rateLimitSeconds");
  await enterScalar("9");
  await click(".setting button:nth-of-type(2)");
  await waitForIdle("scalar reset");
  assert.equal((await readSettings()).rateLimitSeconds, undefined);
  assert.notEqual(await scalarValue(), "9", "successful reset discards the path's draft");
  await enterScalar("8");
  await click(".setting button");
  await waitForIdle("restore scalar fixture");

  await enterScalar("9");
  await filter("search", "/toolMessages/mode");
  await enterScalar(draftMode);
  const changed = { ...await readSettings(), rateLimitSeconds: 6 };
  await writeFile(join(root, "settings.json"), JSON.stringify(changed));
  await filter("search", "rateLimitSeconds");
  await click(".setting button");
  await waitForIdle("conflicting scalar save");
  assert.match(await evaluate('document.querySelector("#status").textContent'), /file changed/i);
  assert.equal(await scalarValue(), "8", "conflict discards the attempted draft");
  await filter("search", "/toolMessages/mode");
  assert.equal(await scalarValue(), freshMode, "conflict discards other drafts from that revision");
  assert.equal((await readSettings()).rateLimitSeconds, 6);
  await click("#refresh");
  await waitForIdle("reload after conflict");
  await click("#clear-filters");
  await filter("search", "/telegram/token");
  await click(".setting button");
  assert.equal(await evaluate('document.querySelector("#value").value'), "");
  await filter("value", '"replacement_private"');
  await click('#edit button[value="set"]');
  await wait('document.querySelector("#path")?.value === ""', "credential save");
  const saved = JSON.parse(await readFile(join(root, "settings.json"), "utf8"));
  assert.equal(saved.web.port, 5555);
  assert.equal(saved.rateLimitSeconds, 6);
  assert.equal(saved.telegram.token, "replacement_private");
  assert.equal(await evaluate(`!document.documentElement.outerHTML.includes("replacement_private")
    && document.querySelector("#value").value === ""
    && localStorage.length === 0 && sessionStorage.length === 0`), true);
  await writeFile(join(root, "settings.json"), JSON.stringify({ ...saved, streamTools: ["shell"] }));
  await click("#refresh");
  await waitForIdle("reload legacy tool output setting");
  await filter("search", "showOutputFor");
  assert.deepEqual(await rows(), []);
  await filter("search", "streamTools");
  assert.deepEqual(await rows(), ["/streamTools"]);
  assert.equal(await evaluate('document.querySelector(".badge").textContent'), "File");
  assert.match(await evaluate('document.querySelector(".setting").textContent'), /legacy tool output/i);
  assert.equal(await evaluate('document.querySelector(".setting button:nth-of-type(2)").textContent'), "Reset");
  await click(".setting button:nth-of-type(2)");
  await waitForIdle("reset legacy tool output setting");
  assert.deepEqual(await readSettings(), saved);
  await writeFile(join(root, "settings.json"), '{"agentMessaging":false}');
  await click("#refresh");
  await wait('document.querySelector("#results").textContent.includes("unavailable")', "unavailable list");
  console.log("Chromium settings console behavior passed.");
} finally {
  clearTimeout(watchdog);
  await Promise.all(children.reverse().map(async child => {
    if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
    child.kill("SIGKILL");
    await once(child, "exit");
  }));
  await rm(root, { recursive: true, force: true, maxRetries: 5 });
}
