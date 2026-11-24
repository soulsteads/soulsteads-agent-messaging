"use strict";

const token = location.hash.slice(1);
history.replaceState(null, "", location.pathname);

const workspace = document.querySelector("#workspace");
const status = document.querySelector("#status");
const settings = document.querySelector("#settings");
const search = document.querySelector("#search");
const source = document.querySelector("#source");
const clearFilters = document.querySelector("#clear-filters");
const results = document.querySelector("#results");
const editContext = document.querySelector("#edit-context");
const environmentGuidance = "The environment takes precedence. Saving or resetting here changes only the file.";
const path = document.querySelector("#path");
const value = document.querySelector("#value");
const messages = new Map([
  ["request_denied", "This request must come from the local console."],
  ["authorization_required", "Open the private URL printed by this console process."],
  ["config_read", "Check that the settings file is a safe, readable file."],
  ["duplicate_key", "Remove duplicate keys from the settings file."],
  ["invalid_json", "Correct the JSON syntax in the settings file."],
  ["expected_object", "Use a JSON object at the root of the settings file."],
  ["environment", "Check project dotenv syntax and file size."],
  ["invalid_shape", "Check the shape of connector and settings sections."],
  ["runtime_override", "Check the runtime environment settings."],
  ["web_port", "Set web.port to an integer from 0 to 65535."],
  ["connector_access", "Check the access policy for enabled remote connectors."],
  ["invalid_or_empty_operation", "Choose an existing override to reset or supply a changed value at a valid pointer."],
  ["stale_revision", "The file changed. Reload and validate before trying again."],
  ["concurrent_update", "Another edit is in progress. Reload and validate before trying again."],
  ["config_write", "Check file size and directory permissions."],
  ["durability_uncertain", "The file was published, but disk sync failed. Reload to check the result."],
]);
const enums = new Map([
  ["/toolMessages/mode", ["off", "events", "status", "trace", "hermes"]],
  ["/matrix/terminalThread/mode", ["off", "always", "auto"]],
]);
class ConsoleError extends Error {
  constructor(code) {
    super(messages.get(code) || "The settings request failed.");
    this.code = code;
  }
}

let entries = [];
let revision;
const drafts = new Map();
const openGroups = new Set();
let wasFiltering = false;
let busy = false;

async function request(method, body) {
  const response = await fetch("/api/settings", {
    method,
    headers: { Authorization: "Bearer " + token, "Content-Type": "application/json" },
    credentials: "omit",
    redirect: "error",
    body,
  });
  const result = await response.json();
  if (!response.ok) throw new ConsoleError(result.error);
  return result;
}

function hasScalarControl(entry) {
  return entry.value !== null && entry.source !== "env_override";
}

const savedReload = "Saved to file. Reload to refresh the settings list.";

async function reload(savedPath) {
  if (savedPath === undefined) drafts.clear();
  revision = undefined;
  const result = await request("GET").catch(error => {
    entries = [];
    render();
    throw error;
  });
  revision = result.revision;
  entries = result.settings;
  const editableEntries = new Map(entries.filter(hasScalarControl).map(entry => [entry.path, entry]));
  let discardedDraft = false;
  for (const [pointer, draft] of drafts) {
    const entry = editableEntries.get(pointer);
    if (entry?.source !== draft.originalSource || entry?.value !== draft.originalValue) {
      drafts.delete(pointer);
      discardedDraft = true;
    }
  }
  render();
  const validation = result.validation
    ? messages.get(result.validation) || "The current settings failed startup validation. Check with the settings CLI."
    : "Local settings validation passed.";
  const confirmation = savedPath === undefined ? ""
    : !entries.length ? savedReload
    : isEnvironmentOverridden(savedPath)
      ? "Saved to file. The environment takes precedence even after a gateway restart while the override is set."
      : "Saved to file. Please restart the gateway to apply.";
  status.textContent = confirmation ? confirmation + " " + validation : validation;
  if (discardedDraft) status.textContent += " Unsaved drafts were discarded because their settings changed or became unavailable.";
}

function element(tag, text) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  return node;
}

function button(text, action) {
  const node = element("button", text);
  node.type = "button";
  node.addEventListener("click", action);
  return node;
}

function render() {
  const query = search.value.trim().toLowerCase();
  const filtering = Boolean(query || source.value);
  if (!wasFiltering) {
    for (const section of settings.querySelectorAll("details")) {
      if (section.open) openGroups.add(section.dataset.group);
      else openGroups.delete(section.dataset.group);
    }
  }
  wasFiltering = filtering;
  workspace.classList.toggle("is-filtering", filtering);
  let matches = 0;
  const groups = new Map();
  settings.replaceChildren();
  for (const entry of entries) {
    if (!entry.path.toLowerCase().includes(query) || (source.value && entry.source !== source.value)) continue;
    matches++;
    const segments = entry.path.split("/");
    const group = segments.length > 2 ? segments[1] : "General";
    if (!groups.has(group)) {
      const section = element("details");
      section.dataset.group = group;
      section.open = filtering || openGroups.has(group);
      section.append(element("summary", group));
      settings.append(section);
      groups.set(group, section);
    }
    const row = element("div");
    row.className = "setting";
    row.dataset.path = entry.path;
    const heading = element("div");
    heading.className = "setting-heading";
    const badge = element("span", {
      default: "Default",
      configured: "File",
      env_override: "Environment",
    }[entry.source]);
    badge.className = "badge";
    badge.dataset.source = entry.source;
    heading.append(element("code", entry.path), badge);
    row.append(heading);
    if (entry.path === "/streamTools" && entry.source === "configured") {
      row.append(element("p", "Legacy tool output setting."));
    }
    const overridden = entry.source === "env_override";
    if (overridden) {
      row.append(element("p", entry.value === null
        ? "The current environment value is hidden."
        : "Current environment value: " + entry.value));
    }
    if (entry.default !== null) row.append(element("small", "Default: " + entry.default));
    const actions = element("div");
    actions.className = "actions";
    row.append(actions);
    if (hasScalarControl(entry)) {
      const options = enums.get(entry.path) || (typeof entry.value === "boolean" ? ["true", "false"] : null);
      const control = element(options ? "select" : "input");
      control.setAttribute("aria-label", entry.path);
      if (options) {
        for (const choice of options) control.append(element("option", choice));
      } else {
        control.type = "number";
        control.step = "1";
        control.min = "0";
      }
      control.value = drafts.get(entry.path)?.input ?? entry.value;
      control.addEventListener("input", () => {
        if (control.value === String(entry.value)) drafts.delete(entry.path);
        else drafts.set(entry.path, {
          input: control.value,
          originalSource: entry.source,
          originalValue: entry.value,
        });
      });
      actions.append(control, button("Set", () => {
        if (!control.reportValidity() || control.value === "") return;
        const value = enums.has(entry.path) ? control.value
          : options ? control.value === "true" : control.valueAsNumber;
        if (typeof value === "number" && !Number.isSafeInteger(value)) {
          status.textContent = "Use a whole number in range or edit the value with a JSON Pointer.";
          return;
        }
        perform(() => save({ op: "set", path: entry.path, value }));
      }));
    } else {
      actions.append(button(overridden ? "Edit file value" : "Edit write-only", () => {
        path.value = entry.path;
        value.value = "";
        updateEditContext();
        value.focus();
      }));
    }
    if (!overridden && (entry.path !== "/streamTools" || entry.source === "configured")) {
      actions.append(button("Reset", () => perform(() => save({ op: "reset", path: entry.path }))));
    }
    groups.get(group).append(row);
  }
  results.textContent = !entries.length ? "Settings list unavailable. See the status above."
    : !matches ? "No settings match the filters."
    : matches + (matches === 1 ? " setting" : " settings")
      + (filtering ? " shown · " + entries.length + " total" : "");
  updateEditContext();
  updateControls();
}

function isEnvironmentOverridden(pointer) {
  return entries.some(entry => entry.path === pointer && entry.source === "env_override");
}

function updateEditContext() {
  editContext.textContent = isEnvironmentOverridden(path.value) ? environmentGuidance : "";
}

function updateControls() {
  for (const control of workspace.querySelectorAll("button, input, select, textarea")) {
    control.disabled = busy;
  }
  clearFilters.disabled = busy || !wasFiltering;
}

async function save(operation) {
  if (!revision) {
    status.textContent = "Reload and validate before editing.";
    return;
  }
  try {
    await request("POST", JSON.stringify({ revision, ...operation }));
  } catch (error) {
    if (error instanceof ConsoleError && ["stale_revision", "concurrent_update"].includes(error.code)) {
      revision = undefined;
      drafts.clear();
      render();
    }
    throw error;
  }
  drafts.delete(operation.path);
  status.textContent = "Saved to file.";
  await reload(operation.path).catch(() => {
    status.textContent = savedReload;
  });
  return true;
}

async function perform(action) {
  if (busy) return;
  const focused = document.activeElement;
  const focusedPath = focused.closest(".setting")?.dataset.path;
  const buttonLabel = focused.textContent;
  busy = true;
  updateControls();
  workspace.setAttribute("aria-busy", "true");
  try {
    await action();
  } catch (error) {
    status.textContent = error instanceof ConsoleError ? error.message : "Could not reach the local settings console.";
  } finally {
    busy = false;
    updateControls();
    if (document.activeElement === document.body) {
      const replacement = focusedPath && Array.from(settings.querySelectorAll(".setting"))
        .find(row => row.dataset.path === focusedPath);
      if (focused.isConnected) focused.focus();
      else if (replacement) {
        Array.from(replacement.querySelectorAll("button"))
          .find(control => control.textContent === buttonLabel)?.focus();
      } else if (focusedPath) search.focus();
    }
    workspace.setAttribute("aria-busy", "false");
  }
}

search.addEventListener("input", render);
source.addEventListener("change", render);
clearFilters.addEventListener("click", () => {
  search.value = "";
  source.value = "";
  render();
  search.focus();
});
path.addEventListener("input", updateEditContext);
document.querySelector("#refresh").addEventListener("click", () => perform(reload));
document.querySelector("#edit").addEventListener("submit", event => {
  event.preventDefault();
  const op = event.submitter?.value || "set";
  // Send advanced JSON as written so the server can reject duplicate members
  // and preserve integers outside JavaScript's exact numeric range.
  const entered = value.value;
  value.value = "";
  const pointer = path.value;
  perform(async () => {
    const operation = op === "set" ? { op: "set_json", path: pointer, json: entered }
      : { op: "reset", path: pointer };
    if (await save(operation)) {
      path.value = "";
      updateEditContext();
    }
  });
});

if (token) {
  workspace.hidden = false;
  perform(reload);
}
