/**
 * Pure state decisions shared by the Web widget runtime and unit tests.
 */
;(function (global) {
  "use strict"

  function shouldClearHistory(event) {
    return Boolean(event) &&
      event.type === "session_state" &&
      event.state === "cleared"
  }

  function imageDimensions(bytes, mimeType) {
    if (!bytes || typeof bytes.length !== "number") return null
    var view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    if (mimeType === "image/png") {
      if (bytes.length < 24 || bytes[0] !== 0x89 || bytes[1] !== 0x50 || bytes[2] !== 0x4e || bytes[3] !== 0x47) return null
      return { width: view.getUint32(16), height: view.getUint32(20) }
    }
    if (mimeType === "image/jpeg") {
      if (bytes.length < 4 || bytes[0] !== 0xff || bytes[1] !== 0xd8) return null
      var sof = { 192: true, 193: true, 194: true, 195: true, 197: true, 198: true, 199: true, 201: true, 202: true, 203: true, 205: true, 206: true, 207: true }
      var offset = 2
      while (offset + 3 < bytes.length) {
        while (offset < bytes.length && bytes[offset] === 0xff) offset++
        if (offset >= bytes.length) return null
        var marker = bytes[offset++]
        if (marker === 0xd8 || marker === 0x01) continue
        if (marker === 0xd9 || marker === 0xda || offset + 2 > bytes.length) return null
        var length = view.getUint16(offset)
        if (length < 2 || offset + length > bytes.length) return null
        if (sof[marker]) {
          if (length < 7) return null
          return { width: view.getUint16(offset + 5), height: view.getUint16(offset + 3) }
        }
        offset += length
      }
      return null
    }
    if (mimeType === "image/webp") {
      var ascii = function (start, end) {
        return String.fromCharCode.apply(null, bytes.slice(start, end))
      }
      if (bytes.length < 30 || ascii(0, 4) !== "RIFF" || ascii(8, 12) !== "WEBP") return null
      var chunk = ascii(12, 16)
      if (chunk === "VP8X") {
        return {
          width: 1 + bytes[24] + (bytes[25] << 8) + (bytes[26] << 16),
          height: 1 + bytes[27] + (bytes[28] << 8) + (bytes[29] << 16),
        }
      }
      if (chunk === "VP8 " && bytes[23] === 0x9d && bytes[24] === 0x01 && bytes[25] === 0x2a) {
        return { width: view.getUint16(26, true) & 0x3fff, height: view.getUint16(28, true) & 0x3fff }
      }
      if (chunk === "VP8L" && bytes.length >= 25 && bytes[20] === 0x2f) {
        return {
          width: 1 + (((bytes[22] & 0x3f) << 8) | bytes[21]),
          height: 1 + (((bytes[24] & 0x0f) << 10) | (bytes[23] << 2) | (bytes[22] >> 6)),
        }
      }
    }
    return null
  }

  function safeThumbnailDataUrl(value, maxChars) {
    return typeof value === "string" &&
      value.length <= maxChars &&
      /^data:image\/(?:jpeg|png|webp);base64,[A-Za-z0-9+/]+={0,2}$/.test(value)
  }

  function thumbnailKeysToEvict(entries, maxTotalChars) {
    var total = entries.reduce(function (sum, entry) { return sum + entry.size }, 0)
    if (total <= maxTotalChars) return []
    var oldestFirst = entries.slice().sort(function (a, b) { return a.createdAt - b.createdAt })
    var keys = []
    for (var i = 0; i < oldestFirst.length && total > maxTotalChars; i++) {
      total -= oldestFirst[i].size
      keys.push(oldestFirst[i].key)
    }
    return keys
  }

  function positiveTimeout(value, fallback) {
    return typeof value === "number" && Number.isFinite(value) && value > 0
      ? value
      : fallback
  }

  global.OpenCodeWidgetState = {
    shouldClearHistory: shouldClearHistory,
    imageDimensions: imageDimensions,
    safeThumbnailDataUrl: safeThumbnailDataUrl,
    thumbnailKeysToEvict: thumbnailKeysToEvict,
    positiveTimeout: positiveTimeout,
  }
})(typeof window !== "undefined" ? window : globalThis)
