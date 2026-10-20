/**
 * Soulsteads Agent Messaging - Web Widget
 *
 * Self-contained embeddable chat interface. No external dependencies.
 *
 * Modes:
 *   "widget"   (default) - floating bubble + popup panel
 *   "embedded" - fills a container element, no bubble
 *
 * Configuration (set BEFORE loading this script):
 *
 *   window.OpenCodeWidget = {
 *     mode: "widget",            // "widget" | "embedded"
 *     container: "#chat",        // CSS selector (embedded mode)
 *     title: "OpenCode",         // defaults to the server botName
 *     placeholder: "Type a message...",
 *     welcome: "Hello! How can I help?",
 *     position: "right",         // bubble side: "right" | "left"
 *     connectTimeoutMs: 10000,
 *     processingTimeoutMs: 330000,
 *     theme: {
 *       primary: "#2563eb",
 *       header: "#1e293b",
 *     }
 *   };
 */
;(function () {
  "use strict"

  // Prevent double-init
  if (window.__ocWidgetLoaded) return
  window.__ocWidgetLoaded = true

  // ==========================================================================
  // Configuration
  // ==========================================================================

  var UC = window.OpenCodeWidget || {}
  var SERVER_CFG = window.OpenCodeWidgetServerConfig || {}

  // Auto-detect server from script src
  var scriptEl = document.currentScript || document.querySelector('script[src*="widget.js"]')
  var scriptUrl = scriptEl ? new URL(scriptEl.src) : null
  var SERVER = scriptUrl ? scriptUrl.origin : window.location.origin
  var WS_PROTO = scriptUrl
    ? scriptUrl.protocol === "https:" ? "wss:" : "ws:"
    : window.location.protocol === "https:" ? "wss:" : "ws:"
  var WS_URL = WS_PROTO + "//" + (scriptUrl ? scriptUrl.host : window.location.host) + "/ws"

  var MODE = UC.mode || "widget" // "widget" | "embedded"
  var CONTAINER_SEL = UC.container || null
  var WidgetState = window.OpenCodeWidgetState
  var CONNECT_TIMEOUT_MS = WidgetState.positiveTimeout(UC.connectTimeoutMs, 10000)
  var PROCESSING_TIMEOUT_MS = WidgetState.positiveTimeout(UC.processingTimeoutMs, 330000)
  var ATTACHMENTS = SERVER_CFG.attachments || { enabled: false }

  var CFG = {
    title: UC.title || SERVER_CFG.title || "OpenCode",
    placeholder: UC.placeholder || "Type a message...",
    welcome: UC.welcome || null,
    position: UC.position || "right",
    primary: (UC.theme && UC.theme.primary) || "#2563eb",
    header: (UC.theme && UC.theme.header) || "#1e293b",
    userBg: (UC.theme && UC.theme.userBg) || "#2563eb",
    userText: (UC.theme && UC.theme.userText) || "#ffffff",
    botBg: (UC.theme && UC.theme.botBg) || "#f1f5f9",
    botText: (UC.theme && UC.theme.botText) || "#1e293b",
  }

  // ==========================================================================
  // State
  // ==========================================================================

  var STORE_KEY = "oc-widget"
  var THUMBNAIL_KEY_PREFIX = "oc-thumbnail:"
  var THUMBNAIL_MAX_DIMENSION = 640
  var THUMBNAIL_MAX_CHARS = 220000
  var THUMBNAIL_TOTAL_CHARS = 2000000
  var ws = null
  var state = loadState()
  var clientId = state.clientId || uid()
  var messages = state.messages || []
  var isOpen = MODE === "embedded" // embedded starts open
  var isProcessing = false
  var reconnAttempts = 0
  var reconnTimer = null
  var connectTimer = null
  var processingTimer = null
  var curBotEl = null // DOM element currently receiving streamed chunks
  var curBotText = "" // Raw streamed text; re-rendered after every chunk
  var curBotSupplementalText = "" // File labels previously included by textContent persistence
  var pendingMessage = null // queued payload when sending while disconnected
  var selectedImages = [] // transient; image bytes are never persisted to localStorage
  var addingImages = false
  var imageSelectionGeneration = 0

  // DOM refs
  var root, bubble, panel, msgsEl, inputEl, sendBtn, statusEl, thinkingEl
  var imageInputEl, imagePreviewEl, imageErrorEl
  var activityEls = Object.create(null)

  // ==========================================================================
  // Helpers
  // ==========================================================================

  function uid() {
    return "oc_" + Math.random().toString(36).slice(2) + Date.now().toString(36)
  }

  function loadState() {
    try { return JSON.parse(localStorage.getItem(STORE_KEY) || "{}") }
    catch (e) { return {} }
  }

  function saveState() {
    try {
      localStorage.setItem(STORE_KEY, JSON.stringify({
        clientId: clientId,
        messages: messages.slice(-50),
      }))
      removeUnreferencedThumbnails()
    } catch (e) { /* quota etc */ }
  }

  function currentThumbnailPrefix() {
    return THUMBNAIL_KEY_PREFIX + clientId + ":"
  }

  function storedThumbnailEntries() {
    var entries = []
    try {
      for (var i = 0; i < sessionStorage.length; i++) {
        var key = sessionStorage.key(i)
        if (!key || key.indexOf(THUMBNAIL_KEY_PREFIX) !== 0) continue
        var raw = sessionStorage.getItem(key)
        if (!raw) continue
        try {
          var record = JSON.parse(raw)
          entries.push({
            key: key,
            size: raw.length,
            createdAt: typeof record.createdAt === "number" ? record.createdAt : 0,
          })
        } catch (e) {
          sessionStorage.removeItem(key)
          i--
        }
      }
    } catch (e) { /* storage unavailable */ }
    return entries
  }

  function pruneStoredThumbnails(maxTotalChars) {
    var entries = storedThumbnailEntries()
    var keys = WidgetState.thumbnailKeysToEvict(entries, maxTotalChars)
    for (var i = 0; i < keys.length; i++) {
      try { sessionStorage.removeItem(keys[i]) } catch (e) { /* storage unavailable */ }
    }
  }

  function removeUnreferencedThumbnails() {
    var referenced = Object.create(null)
    for (var i = 0; i < messages.length; i++) {
      var keys = messages[i].thumbnailKeys || []
      for (var j = 0; j < keys.length; j++) referenced[keys[j]] = true
    }
    var prefix = currentThumbnailPrefix()
    try {
      for (var k = sessionStorage.length - 1; k >= 0; k--) {
        var key = sessionStorage.key(k)
        if (key && key.indexOf(prefix) === 0 && !referenced[key]) {
          sessionStorage.removeItem(key)
        }
      }
    } catch (e) { /* storage unavailable */ }
  }

  function clearStoredThumbnails() {
    var prefix = currentThumbnailPrefix()
    try {
      for (var i = sessionStorage.length - 1; i >= 0; i--) {
        var key = sessionStorage.key(i)
        if (key && key.indexOf(prefix) === 0) sessionStorage.removeItem(key)
      }
    } catch (e) { /* storage unavailable */ }
  }

  function persistThumbnails(messageId, images) {
    var keys = []
    for (var i = 0; i < images.length; i++) {
      var dataUrl = images[i].thumbnailDataUrl
      if (!WidgetState.safeThumbnailDataUrl(dataUrl, THUMBNAIL_MAX_CHARS)) continue
      var key = currentThumbnailPrefix() + messageId + ":" + i
      var record = JSON.stringify({ dataUrl: dataUrl, createdAt: Date.now() })
      try {
        pruneStoredThumbnails(Math.max(0, THUMBNAIL_TOTAL_CHARS - record.length))
        sessionStorage.setItem(key, record)
        keys.push(key)
      } catch (e) { /* quota or storage unavailable */ }
    }
    pruneStoredThumbnails(THUMBNAIL_TOTAL_CHARS)
    return keys
  }

  function storedThumbnailDataUrl(key) {
    if (typeof key !== "string" || key.indexOf(currentThumbnailPrefix()) !== 0) return null
    try {
      var record = JSON.parse(sessionStorage.getItem(key) || "{}")
      return WidgetState.safeThumbnailDataUrl(record.dataUrl, THUMBNAIL_MAX_CHARS)
        ? record.dataUrl
        : null
    } catch (e) {
      return null
    }
  }

  function esc(t) {
    var d = document.createElement("div")
    d.textContent = t
    return d.innerHTML
  }

  function scrollDown() {
    if (msgsEl) requestAnimationFrame(function () { msgsEl.scrollTop = msgsEl.scrollHeight })
  }

  // ==========================================================================
  // Styles
  // ==========================================================================

  function injectCSS() {
    var s = document.createElement("style")
    s.textContent = [
      // --- Reset scoped to widget ---
      ".oc-root,.oc-root *{box-sizing:border-box;margin:0;padding:0;}",
      ".oc-root{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;font-size:14px;line-height:1.5;color:#1e293b;}",

      // --- Widget mode container (fixed) ---
      ".oc-root--widget{position:fixed;bottom:20px;" + CFG.position + ":20px;z-index:99999;}",

      // --- Embedded mode container ---
      ".oc-root--embedded{position:relative;width:100%;height:100%;min-height:300px;}",

      // --- Bubble ---
      ".oc-bubble{width:56px;height:56px;border-radius:50%;background:" + CFG.primary + ";color:#fff;border:none;cursor:pointer;" +
        "display:flex;align-items:center;justify-content:center;box-shadow:0 4px 16px rgba(0,0,0,.2);" +
        "transition:transform .2s,box-shadow .2s;position:relative;}",
      ".oc-bubble:hover{transform:scale(1.06);box-shadow:0 6px 20px rgba(0,0,0,.25);}",
      ".oc-bubble svg{width:24px;height:24px;fill:currentColor;}",
      ".oc-badge{position:absolute;top:-2px;right:-2px;width:12px;height:12px;background:#ef4444;border-radius:50%;border:2px solid #fff;display:none;}",
      ".oc-badge--on{display:block;}",

      // --- Panel (widget mode) ---
      ".oc-panel--widget{position:absolute;bottom:70px;" + CFG.position + ":0;width:380px;max-width:calc(100vw - 40px);height:520px;max-height:calc(100vh - 120px);" +
        "background:#fff;border-radius:16px;box-shadow:0 8px 32px rgba(0,0,0,.15);display:none;flex-direction:column;overflow:hidden;border:1px solid #e2e8f0;}",
      ".oc-panel--widget.oc-open{display:flex;animation:oc-up .25s ease-out;}",

      // --- Panel (embedded mode) ---
      ".oc-panel--embedded{width:100%;height:100%;background:#fff;display:flex;flex-direction:column;overflow:hidden;border-radius:inherit;}",

      "@keyframes oc-up{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}",

      // --- Header ---
      ".oc-hdr{background:" + CFG.header + ";color:#fff;padding:14px 16px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;}",
      ".oc-hdr-t{font-weight:600;font-size:15px;}",
      ".oc-hdr-s{font-size:11px;opacity:.7;margin-top:1px;}",
      ".oc-hdr-actions{display:flex;gap:4px;align-items:center;}",
      ".oc-btn-icon{background:none;border:none;color:#fff;cursor:pointer;padding:4px;opacity:.7;transition:opacity .2s;border-radius:4px;}",
      ".oc-btn-icon:hover{opacity:1;background:rgba(255,255,255,.1);}",
      ".oc-btn-icon svg{width:18px;height:18px;fill:currentColor;display:block;}",

      // --- Messages ---
      ".oc-msgs{flex:1;overflow-y:auto;padding:16px;display:flex;flex-direction:column;gap:8px;}",
      ".oc-msgs::-webkit-scrollbar{width:6px;} .oc-msgs::-webkit-scrollbar-track{background:transparent;} .oc-msgs::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:3px;}",

      // --- Bubbles ---
      ".oc-msg{max-width:85%;padding:10px 14px;border-radius:12px;word-wrap:break-word;white-space:pre-wrap;font-size:14px;}",
      ".oc-msg--user{align-self:flex-end;background:" + CFG.userBg + ";color:" + CFG.userText + ";border-bottom-right-radius:4px;}",
      ".oc-msg--bot{align-self:flex-start;background:" + CFG.botBg + ";color:" + CFG.botText + ";border-bottom-left-radius:4px;}",
      ".oc-msg-text>p+*,.oc-msg-text>pre+*,.oc-msg-text>ul+*,.oc-msg-text>ol+*,.oc-msg-text>h1+*,.oc-msg-text>h2+*,.oc-msg-text>h3+*,.oc-msg-text>h4+*,.oc-msg-text>h5+*,.oc-msg-text>h6+*{margin-top:8px;}",
      ".oc-msg-text h1{font-size:1.5em}.oc-msg-text h2{font-size:1.35em}.oc-msg-text h3{font-size:1.2em}.oc-msg-text h4,.oc-msg-text h5,.oc-msg-text h6{font-size:1.05em}",
      ".oc-msg-text ul,.oc-msg-text ol{padding-left:20px;}.oc-msg-text pre{padding:8px;background:rgba(15,23,42,.08);border-radius:6px;overflow-x:auto;white-space:pre-wrap;}.oc-msg-text code{font-family:monospace;}.oc-msg-text a{color:#2563eb;text-decoration:underline;overflow-wrap:anywhere;}",
      ".oc-msg-text{overflow-x:auto;}.oc-msg-text table{width:100%;border-collapse:collapse;font-size:13px;}.oc-msg-text th,.oc-msg-text td{padding:6px 8px;border:1px solid #cbd5e1;vertical-align:top;}.oc-msg-text th{font-weight:600;background:rgba(15,23,42,.06);}",

      // --- Activity ---
      ".oc-activity{align-self:flex-start;max-width:100%;font-size:12px;color:#6b7280;background:#f3f4f6;padding:6px 10px;border-left:3px solid #9ca3af;border-radius:4px;font-family:monospace;white-space:pre-wrap;overflow-wrap:anywhere;margin:4px 0;}",
      ".oc-tool-out{align-self:stretch;background:#1e293b;color:#e2e8f0;padding:10px 12px;border-radius:8px;font-family:monospace;font-size:12px;white-space:pre-wrap;word-break:break-all;max-height:200px;overflow-y:auto;margin:4px 0;}",
      ".oc-tool-details{align-self:stretch;margin:4px 0;}",
      ".oc-tool-details summary{font-size:12px;color:#64748b;cursor:pointer;padding:4px 0;user-select:none;}",
      ".oc-tool-details summary:hover{color:#475569;}",
      ".oc-tool-out--collapsed{margin-top:4px;max-height:300px;}",

      // --- Thinking dots ---
      ".oc-think{align-self:flex-start;padding:10px 14px;display:none;gap:5px;}",
      ".oc-think--on{display:flex;}",
      ".oc-think span{width:8px;height:8px;background:#94a3b8;border-radius:50%;animation:oc-dot 1.4s infinite ease-in-out both;}",
      ".oc-think span:nth-child(1){animation-delay:-.32s;} .oc-think span:nth-child(2){animation-delay:-.16s;}",
      "@keyframes oc-dot{0%,80%,100%{transform:scale(0)}40%{transform:scale(1)}}",

      // --- Input area ---
      ".oc-inp-area{padding:12px;border-top:1px solid #e2e8f0;display:flex;flex-direction:column;gap:8px;flex-shrink:0;background:#fff;}",
      ".oc-inp-row{display:flex;gap:8px;align-items:flex-end;}",
      ".oc-inp{flex:1;padding:10px 14px;border:1px solid #e2e8f0;border-radius:10px;font-size:14px;font-family:inherit;resize:none;" +
        "outline:none;max-height:100px;min-height:40px;line-height:1.4;transition:border-color .2s;}",
      ".oc-inp:focus{border-color:" + CFG.primary + ";}",
      ".oc-inp::placeholder{color:#94a3b8;}",
      ".oc-send{width:40px;height:40px;border-radius:10px;background:" + CFG.primary + ";color:#fff;border:none;cursor:pointer;" +
        "display:flex;align-items:center;justify-content:center;flex-shrink:0;transition:opacity .2s;align-self:flex-end;}",
      ".oc-send:disabled{opacity:.4;cursor:not-allowed;}",
      ".oc-send svg{width:18px;height:18px;fill:currentColor;}",
      ".oc-attach{width:40px;height:40px;border-radius:10px;background:#fff;color:#64748b;border:1px solid #e2e8f0;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;}",
      ".oc-attach:hover{color:" + CFG.primary + ";border-color:" + CFG.primary + ";}",
      ".oc-attach svg{width:19px;height:19px;fill:currentColor;}",
      ".oc-img-preview{display:flex;gap:8px;flex-wrap:wrap;}",
      ".oc-img-item{position:relative;width:64px;height:64px;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;background:#f8fafc;}",
      ".oc-img-item img{width:100%;height:100%;object-fit:cover;display:block;}",
      ".oc-img-remove{position:absolute;top:2px;right:2px;width:20px;height:20px;border:0;border-radius:50%;background:rgba(15,23,42,.8);color:#fff;cursor:pointer;font-size:15px;line-height:18px;}",
      ".oc-img-error{font-size:12px;color:#b91c1c;display:none;}",
      ".oc-img-error--on{display:block;}",

      // --- Welcome ---
      ".oc-welcome{text-align:center;color:#64748b;padding:20px;font-size:13px;}",

      // --- Images ---
      ".oc-msg img{max-width:100%;border-radius:8px;margin-top:6px;}",

      // --- Mobile ---
      "@media(max-width:480px){" +
        ".oc-panel--widget{position:fixed;bottom:80px;left:12px;right:12px;width:auto;height:calc(100vh - 100px);border-radius:12px;}" +
      "}",
    ].join("\n")
    document.head.appendChild(s)
  }

  // ==========================================================================
  // SVG icons
  // ==========================================================================

  var ICON_CHAT = '<svg viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/></svg>'
  var ICON_DOWN = '<svg viewBox="0 0 24 24"><path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6z"/></svg>'
  var ICON_CLOSE = '<svg viewBox="0 0 24 24"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>'
  var ICON_SEND = '<svg viewBox="0 0 24 24"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>'
  var ICON_ATTACH = '<svg viewBox="0 0 24 24"><path d="M16.5 6v11.5a4.5 4.5 0 0 1-9 0V5a3 3 0 0 1 6 0v10.5a1.5 1.5 0 0 1-3 0V6H9v9.5a3 3 0 0 0 6 0V5a4.5 4.5 0 0 0-9 0v12.5a6 6 0 0 0 12 0V6h-1.5z"/></svg>'
  var ICON_CLEAR = '<svg viewBox="0 0 24 24"><path d="M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z"/></svg>'

  // ==========================================================================
  // DOM
  // ==========================================================================

  function build() {
    root = document.createElement("div")
    root.className = "oc-root oc-root--" + MODE

    // --- Panel ---
    panel = document.createElement("div")
    panel.className = MODE === "embedded" ? "oc-panel--embedded" : "oc-panel--widget"
    if (MODE === "embedded") panel.classList.add("oc-open")

    // Header
    var hdr = document.createElement("div")
    hdr.className = "oc-hdr"

    var hdrLeft = document.createElement("div")
    hdrLeft.innerHTML = '<div class="oc-hdr-t">' + esc(CFG.title) + '</div><div class="oc-hdr-s" id="oc-status">Connecting...</div>'
    statusEl = hdrLeft.querySelector("#oc-status")

    var hdrActions = document.createElement("div")
    hdrActions.className = "oc-hdr-actions"

    // Clear button
    var clearBtn = document.createElement("button")
    clearBtn.className = "oc-btn-icon"
    clearBtn.title = "Clear chat"
    clearBtn.innerHTML = ICON_CLEAR
    clearBtn.onclick = clearChat
    hdrActions.appendChild(clearBtn)

    // Close button (widget mode only)
    if (MODE === "widget") {
      var closeBtn = document.createElement("button")
      closeBtn.className = "oc-btn-icon"
      closeBtn.title = "Close"
      closeBtn.innerHTML = ICON_CLOSE
      closeBtn.onclick = togglePanel
      hdrActions.appendChild(closeBtn)
    }

    hdr.appendChild(hdrLeft)
    hdr.appendChild(hdrActions)

    // Messages
    msgsEl = document.createElement("div")
    msgsEl.className = "oc-msgs"

    // Thinking indicator (lives inside messages, always last)
    thinkingEl = document.createElement("div")
    thinkingEl.className = "oc-think"
    thinkingEl.innerHTML = "<span></span><span></span><span></span>"
    msgsEl.appendChild(thinkingEl)

    // Input area
    var inpArea = document.createElement("div")
    inpArea.className = "oc-inp-area"

    imagePreviewEl = document.createElement("div")
    imagePreviewEl.className = "oc-img-preview"
    imageErrorEl = document.createElement("div")
    imageErrorEl.className = "oc-img-error"

    var inpRow = document.createElement("div")
    inpRow.className = "oc-inp-row"

    inputEl = document.createElement("textarea")
    inputEl.className = "oc-inp"
    inputEl.placeholder = CFG.placeholder
    inputEl.rows = 1
    inputEl.maxLength = 100000
    inputEl.onkeydown = onKey
    inputEl.oninput = autoGrow
    inputEl.onpaste = onPaste

    if (ATTACHMENTS.enabled) {
      imageInputEl = document.createElement("input")
      imageInputEl.type = "file"
      imageInputEl.accept = (ATTACHMENTS.allowedMimeTypes || []).join(",")
      imageInputEl.multiple = ATTACHMENTS.maxFilesPerMessage > 1
      imageInputEl.hidden = true
      imageInputEl.onchange = function () {
        addImageFiles(Array.from(imageInputEl.files || []))
        imageInputEl.value = ""
      }

      var attachBtn = document.createElement("button")
      attachBtn.type = "button"
      attachBtn.className = "oc-attach"
      attachBtn.title = "Attach image"
      attachBtn.setAttribute("aria-label", "Attach image")
      attachBtn.innerHTML = ICON_ATTACH
      attachBtn.onclick = function () { imageInputEl.click() }
      inpRow.appendChild(attachBtn)
      inpArea.appendChild(imageInputEl)
    }

    sendBtn = document.createElement("button")
    sendBtn.className = "oc-send"
    sendBtn.innerHTML = ICON_SEND
    sendBtn.onclick = doSend

    inpRow.appendChild(inputEl)
    inpRow.appendChild(sendBtn)
    inpArea.appendChild(imagePreviewEl)
    inpArea.appendChild(imageErrorEl)
    inpArea.appendChild(inpRow)

    panel.appendChild(hdr)
    panel.appendChild(msgsEl)
    panel.appendChild(inpArea)

    root.appendChild(panel)

    // --- Bubble (widget mode only) ---
    if (MODE === "widget") {
      bubble = document.createElement("button")
      bubble.className = "oc-bubble"
      bubble.setAttribute("aria-label", "Open chat")
      bubble.innerHTML = ICON_CHAT + '<span class="oc-badge"></span>'
      bubble.onclick = togglePanel
      root.appendChild(bubble)
    }

    // Mount
    if (MODE === "embedded" && CONTAINER_SEL) {
      var target = document.querySelector(CONTAINER_SEL)
      if (target) {
        target.appendChild(root)
      } else {
        console.warn("[OpenCode Widget] Container not found: " + CONTAINER_SEL)
        document.body.appendChild(root)
      }
    } else {
      document.body.appendChild(root)
    }

    renderHistory()
  }

  // ==========================================================================
  // Message rendering
  // ==========================================================================

  function renderHistory() {
    activityEls = Object.create(null)
    // Clear everything except the thinking indicator
    while (msgsEl.firstChild !== thinkingEl) {
      msgsEl.removeChild(msgsEl.firstChild)
    }

    if (messages.length === 0 && CFG.welcome) {
      var w = document.createElement("div")
      w.className = "oc-welcome"
      w.textContent = CFG.welcome
      msgsEl.insertBefore(w, thinkingEl)
    }

    for (var i = 0; i < messages.length; i++) {
      var bubbleElement = appendBubble(messages[i].role, messages[i].text)
      appendStoredThumbnails(bubbleElement, messages[i].thumbnailKeys || [])
    }
    scrollDown()
  }

  function appendBubble(role, text) {
    // Remove welcome
    var w = msgsEl.querySelector(".oc-welcome")
    if (w) w.remove()

    var el = document.createElement("div")
    el.className = "oc-msg oc-msg--" + role
    if (role === "bot") {
      var textEl = document.createElement("div")
      textEl.className = "oc-msg-text"
      window.OpenCodeMessageRenderer.render(textEl, text, document)
      el.appendChild(textEl)
    } else {
      el.textContent = text
    }
    msgsEl.insertBefore(el, thinkingEl)
    return el
  }

  function appendStoredThumbnails(bubbleElement, keys) {
    for (var i = 0; i < keys.length; i++) {
      var dataUrl = storedThumbnailDataUrl(keys[i])
      if (!dataUrl) continue
      var preview = document.createElement("img")
      preview.className = "oc-msg-thumbnail"
      preview.src = dataUrl
      preview.alt = "Attached image thumbnail"
      bubbleElement.appendChild(preview)
    }
  }

  function renderBotText(bubbleElement, text) {
    var textElement = bubbleElement.querySelector(".oc-msg-text")
    if (!textElement) {
      textElement = document.createElement("div")
      textElement.className = "oc-msg-text"
      bubbleElement.insertBefore(textElement, bubbleElement.firstChild)
    }
    window.OpenCodeMessageRenderer.render(textElement, text, document)
  }

  // ==========================================================================
  // UI actions
  // ==========================================================================

  function togglePanel() {
    isOpen = !isOpen
    panel.classList.toggle("oc-open", isOpen)

    if (bubble) {
      bubble.innerHTML = isOpen
        ? ICON_DOWN + '<span class="oc-badge"></span>'
        : ICON_CHAT + '<span class="oc-badge"></span>'
    }

    if (isOpen) {
      hideBadge()
      inputEl.focus()
      scrollDown()
      ensureConnected()
    }
  }

  function showBadge() {
    if (!bubble) return
    var b = bubble.querySelector(".oc-badge")
    if (b) b.classList.add("oc-badge--on")
  }

  function hideBadge() {
    if (!bubble) return
    var b = bubble.querySelector(".oc-badge")
    if (b) b.classList.remove("oc-badge--on")
  }

  function showImageError(message) {
    if (!imageErrorEl) return
    imageErrorEl.textContent = message || ""
    imageErrorEl.classList.toggle("oc-img-error--on", Boolean(message))
  }

  function onPaste(e) {
    if (!ATTACHMENTS.enabled || !e.clipboardData) return
    var files = []
    var items = Array.from(e.clipboardData.items || [])
    for (var i = 0; i < items.length; i++) {
      if (items[i].kind === "file" && items[i].type.indexOf("image/") === 0) {
        var file = items[i].getAsFile()
        if (file) files.push(file)
      }
    }
    if (files.length === 0) return
    e.preventDefault()
    addImageFiles(files)
  }

  async function addImageFiles(files) {
    if (addingImages) return
    addingImages = true
    var generation = imageSelectionGeneration
    showImageError("")
    try {
      var remaining = ATTACHMENTS.maxFilesPerMessage - selectedImages.length
      if (remaining <= 0) {
        showImageError("Only " + ATTACHMENTS.maxFilesPerMessage + " image(s) may be attached.")
        return
      }
      if (files.length > remaining) {
        showImageError("Only " + ATTACHMENTS.maxFilesPerMessage + " image(s) may be attached.")
      }
      for (var i = 0; i < files.length && i < remaining; i++) {
        try {
          var prepared = await prepareImage(files[i])
          if (generation !== imageSelectionGeneration) return
          selectedImages.push(prepared)
          renderSelectedImages()
        } catch (e) {
          showImageError(e && e.message ? e.message : "Could not attach that image.")
          break
        }
      }
    } finally {
      addingImages = false
    }
  }

  async function inspectImageDimensions(file) {
    var bytes = new Uint8Array(await file.arrayBuffer())
    return WidgetState.imageDimensions(bytes, file.type)
  }

  async function prepareImage(file) {
    var allowed = ATTACHMENTS.allowedMimeTypes || []
    if (allowed.indexOf(file.type) === -1) throw new Error("Unsupported image type.")
    if (file.size > ATTACHMENTS.maxFileBytes) {
      throw new Error("Image exceeds the " + Math.floor(ATTACHMENTS.maxFileBytes / 1048576) + " MiB limit.")
    }

    var dimensions = await inspectImageDimensions(file)
    if (!dimensions) throw new Error("The pasted file is not a valid image.")
    var width = dimensions.width
    var height = dimensions.height
    var pixels = width * height
    if (width < 1 || height < 1 || width > ATTACHMENTS.maxWidth || height > ATTACHMENTS.maxHeight || pixels > ATTACHMENTS.maxPixels) {
      throw new Error("Image dimensions are too large.")
    }

    var blob = file
    var resizeMax = ATTACHMENTS.resizeMaxDimension
    if (Math.max(width, height) > resizeMax) {
      var bitmap
      try {
        bitmap = await createImageBitmap(file)
      } catch (e) {
        throw new Error("The pasted file is not a valid image.")
      }
      width = bitmap.width
      height = bitmap.height
      var scale = resizeMax / Math.max(width, height)
      var targetWidth = Math.max(1, Math.round(width * scale))
      var targetHeight = Math.max(1, Math.round(height * scale))
      var canvas = document.createElement("canvas")
      canvas.width = targetWidth
      canvas.height = targetHeight
      var context = canvas.getContext("2d")
      if (!context) {
        bitmap.close()
        throw new Error("Image resizing is unavailable in this browser.")
      }
      context.drawImage(bitmap, 0, 0, targetWidth, targetHeight)
      try {
        blob = await new Promise(function (resolve, reject) {
          canvas.toBlob(function (result) {
            if (result) resolve(result)
            else reject(new Error("Could not resize the image."))
          }, file.type, 0.85)
        })
      } finally {
        bitmap.close()
      }
      width = targetWidth
      height = targetHeight
    }

    if (blob.size > ATTACHMENTS.maxFileBytes) {
      throw new Error("Resized image still exceeds the upload limit.")
    }
    var data = await blobToBase64(blob)
    var thumbnailDataUrl = ""
    try { thumbnailDataUrl = await createThumbnailDataUrl(blob) } catch (e) { /* optional preview */ }
    return {
      mimeType: blob.type || file.type,
      data: data,
      width: width,
      height: height,
      thumbnailDataUrl: thumbnailDataUrl,
    }
  }

  async function createThumbnailDataUrl(blob) {
    var bitmap = await createImageBitmap(blob)
    try {
      var scale = Math.min(1, THUMBNAIL_MAX_DIMENSION / Math.max(bitmap.width, bitmap.height))
      var width = Math.max(1, Math.round(bitmap.width * scale))
      var height = Math.max(1, Math.round(bitmap.height * scale))
      var canvas = document.createElement("canvas")
      canvas.width = width
      canvas.height = height
      var context = canvas.getContext("2d")
      if (!context) throw new Error("Canvas is unavailable.")
      context.drawImage(bitmap, 0, 0, width, height)
      var thumbnail = await new Promise(function (resolve, reject) {
        canvas.toBlob(function (result) {
          if (result) resolve(result)
          else reject(new Error("Could not create thumbnail."))
        }, "image/webp", 0.75)
      })
      var dataUrl = "data:image/webp;base64," + await blobToBase64(thumbnail)
      return WidgetState.safeThumbnailDataUrl(dataUrl, THUMBNAIL_MAX_CHARS) ? dataUrl : ""
    } finally {
      bitmap.close()
    }
  }

  function blobToBase64(blob) {
    return new Promise(function (resolve, reject) {
      var reader = new FileReader()
      reader.onload = function () {
        var result = String(reader.result || "")
        var comma = result.indexOf(",")
        if (comma < 0) reject(new Error("Could not read the image."))
        else resolve(result.slice(comma + 1))
      }
      reader.onerror = function () { reject(new Error("Could not read the image.")) }
      reader.readAsDataURL(blob)
    })
  }

  function renderSelectedImages() {
    if (!imagePreviewEl) return
    imagePreviewEl.textContent = ""
    selectedImages.forEach(function (image, index) {
      var item = document.createElement("div")
      item.className = "oc-img-item"
      var preview = document.createElement("img")
      preview.src = "data:" + image.mimeType + ";base64," + image.data
      preview.alt = "Attached image"
      var remove = document.createElement("button")
      remove.type = "button"
      remove.className = "oc-img-remove"
      remove.setAttribute("aria-label", "Remove image")
      remove.textContent = "x"
      remove.onclick = function () {
        selectedImages.splice(index, 1)
        showImageError("")
        renderSelectedImages()
      }
      item.appendChild(preview)
      item.appendChild(remove)
      imagePreviewEl.appendChild(item)
    })
  }

  function clearSelectedImages() {
    imageSelectionGeneration++
    selectedImages = []
    showImageError("")
    renderSelectedImages()
  }

  function onKey(e) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault()
      doSend()
    }
  }

  function autoGrow() {
    inputEl.style.height = "auto"
    inputEl.style.height = Math.min(inputEl.scrollHeight, 100) + "px"
  }

  function doSend() {
    var text = inputEl.value.trim()
    if (addingImages) {
      showImageError("Wait for the image preview before sending.")
      return
    }
    if ((!text && selectedImages.length === 0) || isProcessing) return
    if (text.charAt(0) === "/" && selectedImages.length > 0) {
      showImageError("Remove the image before sending a command.")
      return
    }

    var outgoingImages = selectedImages.slice()
    var displayText = text
    if (outgoingImages.length > 0) {
      displayText += (displayText ? "\n" : "") +
        "[Attached " + outgoingImages.length + " image" + (outgoingImages.length === 1 ? "" : "s") + "]"
    }
    var messageId = uid()
    var thumbnailKeys = persistThumbnails(messageId, outgoingImages)
    var userBubble = addMsg("user", displayText, {
      id: messageId,
      thumbnailKeys: thumbnailKeys,
    })
    outgoingImages.forEach(function (image) {
      var preview = document.createElement("img")
      preview.src = "data:" + image.mimeType + ";base64," + image.data
      preview.alt = "Attached image"
      userBubble.appendChild(preview)
    })

    var payload = {
      type: "message",
      text: text,
      images: outgoingImages.map(function (image) {
        return { mimeType: image.mimeType, data: image.data }
      }),
    }
    inputEl.value = ""
    inputEl.style.height = "auto"
    clearSelectedImages()

    showThinking()
    isProcessing = true
    sendBtn.disabled = true
    armProcessingTimer()

    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload))
    } else {
      // Queue one validated payload -- including transient image data -- until reconnect.
      pendingMessage = payload
      ensureConnected()
    }
  }

  function addMsg(role, text, details) {
    var message = { role: role, text: text, ts: Date.now() }
    if (details && details.id) message.id = details.id
    if (details && details.thumbnailKeys && details.thumbnailKeys.length > 0) {
      message.thumbnailKeys = details.thumbnailKeys
    }
    messages.push(message)
    var bubbleElement = appendBubble(role, text)
    scrollDown()
    saveState()
    return bubbleElement
  }

  function clearChat() {
    clearSelectedImages()
    clearStoredThumbnails()
    // Send /clear to server if connected
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: "message", text: "/clear" }))
    }
    messages = []
    saveState()
    renderHistory()
  }

  function showThinking() {
    thinkingEl.classList.add("oc-think--on")
    scrollDown()
  }

  function hideThinking() {
    thinkingEl.classList.remove("oc-think--on")
  }

  function showActivity(text, activityId) {
    var el = document.createElement("div")
    el.className = "oc-activity"
    el.textContent = "> " + text
    msgsEl.insertBefore(el, thinkingEl)
    if (activityId) activityEls[activityId] = el
    scrollDown()
  }

  function updateActivity(activityId, text) {
    var el = activityEls[activityId]
    if (!el) {
      showActivity(text, activityId)
      return
    }
    el.textContent = "> " + text
    scrollDown()
  }

  function clearActivity() {
    // Activities and tool output persist - nothing to clear
  }

  function appendToolOutput(text) {
    // Reuse existing tool-output block or create one
    var el = msgsEl.querySelector(".oc-tool-out:last-of-type")
    if (!el || el.nextElementSibling !== thinkingEl) {
      el = document.createElement("pre")
      el.className = "oc-tool-out"
      msgsEl.insertBefore(el, thinkingEl)
    }
    el.textContent += (el.textContent ? "\n" : "") + text
    scrollDown()
  }

  function setStatus(t) {
    if (statusEl) statusEl.textContent = t
  }

  function clearConnectTimer() {
    if (!connectTimer) return
    clearTimeout(connectTimer)
    connectTimer = null
  }

  function clearProcessingTimer() {
    if (!processingTimer) return
    clearTimeout(processingTimer)
    processingTimer = null
  }

  function armProcessingTimer() {
    clearProcessingTimer()
    processingTimer = setTimeout(function () {
      processingTimer = null
      pendingMessage = null
      hideThinking()
      curBotEl = null
      curBotText = ""
      curBotSupplementalText = ""
      isProcessing = false
      sendBtn.disabled = false
      addMsg("bot", "The request timed out. Please try again.")
    }, PROCESSING_TIMEOUT_MS)
  }

  function finishProcessing() {
    clearProcessingTimer()
    isProcessing = false
    sendBtn.disabled = false
  }

  // ==========================================================================
  // WebSocket
  // ==========================================================================

  function ensureConnected() {
    if (ws && (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN)) return
    connect()
  }

  function connect() {
    clearConnectTimer()
    if (reconnTimer) {
      clearTimeout(reconnTimer)
      reconnTimer = null
    }
    var url = WS_URL + "?clientId=" + encodeURIComponent(clientId)
    var socket
    try {
      socket = new WebSocket(url)
    } catch (e) {
      ws = null
      setStatus("Connection failed")
      scheduleReconn()
      return
    }
    ws = socket
    setStatus(reconnAttempts > 0 ? "Reconnecting..." : "Connecting...")

    connectTimer = setTimeout(function () {
      if (ws !== socket || socket.readyState !== WebSocket.CONNECTING) return
      ws = null
      setStatus("Connection timed out")
      try { socket.close() } catch (e) {}
      scheduleReconn()
    }, CONNECT_TIMEOUT_MS)

    socket.onopen = function () {
      if (ws !== socket) {
        socket.close()
        return
      }
      clearConnectTimer()
      reconnAttempts = 0
      setStatus("Online")
      sendBtn.disabled = isProcessing ? true : false

      // Flush any message queued while disconnected
      if (pendingMessage) {
        socket.send(JSON.stringify(pendingMessage))
        pendingMessage = null
        armProcessingTimer()
      }
    }

    socket.onmessage = function (ev) {
      if (ws !== socket) return
      try { handleServer(JSON.parse(ev.data)) } catch (e) {}
    }

    socket.onclose = function () {
      if (ws !== socket) return
      clearConnectTimer()
      ws = null
      setStatus("Disconnected")
      scheduleReconn()
    }

    socket.onerror = function () { /* onclose fires next */ }
  }

  function scheduleReconn() {
    if (reconnTimer) return
    reconnAttempts++
    var delay = Math.min(1000 * Math.pow(2, reconnAttempts - 1), 30000)
    setStatus("Reconnecting in " + Math.round(delay / 1000) + "s...")
    reconnTimer = setTimeout(function () {
      reconnTimer = null
      connect()
    }, delay)
  }

  // ==========================================================================
  // Server message handling
  // ==========================================================================

  function handleServer(d) {
    if (isProcessing && d.type !== "connected") armProcessingTimer()

    switch (d.type) {

      case "connected":
        clientId = d.clientId
        // Connection state is informational. A bridge restart or unavailable
        // backend must never erase browser-local conversation history.
        saveState()
        break

      case "session_state":
        if (WidgetState.shouldClearHistory(d)) {
          clearStoredThumbnails()
          messages = []
          saveState()
          renderHistory()
        }
        break

      case "chunk":
        hideThinking()
        if (!curBotEl) {
          curBotEl = appendBubble("bot", "")
          curBotText = ""
          curBotSupplementalText = ""
        }
        curBotText += d.text
        renderBotText(curBotEl, curBotText)
        scrollDown()
        break

      case "activity":
        showActivity(d.message, d.activityId)
        break

      case "activity_update":
        updateActivity(d.activityId, d.message)
        break

      case "tool_output":
        // Real-time streaming output from tools (e.g. bash)
        hideThinking()
        appendToolOutput(d.text)
        break

      case "tool_result":
        // Completed tool result (e.g. bash final output)
        hideThinking()
        appendToolOutput(d.text)
        break

      case "permission_denied":
        hideThinking()
        showActivity(d.message)
        break

      case "image":
        hideThinking()
        if (!curBotEl) {
          curBotEl = appendBubble("bot", "")
          curBotText = ""
          curBotSupplementalText = ""
        }
        var img = document.createElement("img")
        img.src = "data:" + (d.mimeType || "image/png") + ";base64," + d.data
        img.alt = d.alt || "Image"
        curBotEl.appendChild(img)
        scrollDown()
        break

      case "file":
        hideThinking()
        if (!curBotEl) {
          curBotEl = appendBubble("bot", "")
          curBotText = ""
          curBotSupplementalText = ""
        }
        var link = document.createElement("a")
        link.href = "data:" + (d.mimeType || "application/octet-stream") + ";base64," + d.data
        link.download = d.fileName || "file"
        link.textContent = d.fileName || "Download file"
        curBotSupplementalText += link.textContent
        link.style.cssText = "display:inline-block;padding:8px 12px;background:#e2e8f0;border-radius:8px;color:#1e293b;text-decoration:none;font-size:13px;margin-top:6px;"
        curBotEl.appendChild(link)
        scrollDown()
        break

      case "done":
        hideThinking()
        clearActivity()
        if (curBotEl) {
          // Move bot text bubble to the bottom (after activities) so it's visible
          msgsEl.insertBefore(curBotEl, thinkingEl)
          scrollDown()
          var persistedText = curBotText + curBotSupplementalText
          if (persistedText) {
            messages.push({ role: "bot", text: persistedText, ts: Date.now() })
            saveState()
          }
        }
        curBotEl = null
        curBotText = ""
        curBotSupplementalText = ""
        finishProcessing()
        break

      case "response": // non-streamed (commands)
        hideThinking()
        clearActivity()
        addMsg("bot", d.text)
        curBotEl = null
        curBotText = ""
        curBotSupplementalText = ""
        finishProcessing()
        break

      case "error":
        hideThinking()
        clearActivity()
        addMsg("bot", d.message || "An error occurred.")
        curBotEl = null
        curBotText = ""
        curBotSupplementalText = ""
        finishProcessing()
        break
    }

    // Show badge if panel is closed
    if (!isOpen && (d.type === "done" || d.type === "response")) {
      showBadge()
    }
  }

  // ==========================================================================
  // Init
  // ==========================================================================

  function init() {
    injectCSS()
    build()
    connect()
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init)
  } else {
    init()
  }
})()
