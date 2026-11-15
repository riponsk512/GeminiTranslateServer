// GeminiTranslateServer/index.js
require("dotenv").config();
const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const WebSocket = require("ws");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  maxHttpBufferSize: 1e7,
  pingInterval: 10000,
  pingTimeout: 20000,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 10000;
const GEMINI_API_KEY =
  process.env.GEMINI_API_KEY ||
  "AQ.Ab8RN6J18Lri4K34bssqzHMCnIAzIQBVQsxfGW31jGH5qerzJQ";
const GEMINI_LIVE_MODEL = "models/gemini-3.5-live-translate-preview";

// Pure digital 100ms zero-silence frame at 16kHz 16-bit mono (3200 zero bytes)
const SILENT_PCM_100MS_B64 = Buffer.alloc(3200).toString("base64");

const getGeminiWsUrl = (apiVersion = "v1beta") =>
  `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.${apiVersion}.GenerativeService.BidiGenerateContent?key=${encodeURIComponent(
    GEMINI_API_KEY
  )}`;

process.on("uncaughtException", (err) => {
  console.error("⚠️ [UNCAUGHT_EXCEPTION]", err?.message || err);
});
process.on("unhandledRejection", (reason) => {
  console.error("⚠️ [UNHANDLED_REJECTION]", reason?.message || reason);
});

function logTranslate(tag, msg, meta = null) {
  const ts = new Date().toISOString();
  if (meta) {
    console.log(`[${ts}] [GEMINI:${tag}] ${msg}`, JSON.stringify(meta));
  } else {
    console.log(`[${ts}] [GEMINI:${tag}] ${msg}`);
  }
}

// ============================================================================
// 🔊 NATIVE 24kHz 16-BIT MONO LITTLE-ENDIAN WAV CONTAINER
// Preserves Gemini's exact 24,000 Hz PCM16 output without resampling
// ============================================================================
function buildNative24kWavBase64(pcm24kBuffers) {
  const pcmData = Buffer.concat(pcm24kBuffers);
  const sampleRate = 24000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 1.3ms (32-sample) micro de-click at buffer boundaries
  const fadeSamples = Math.min(32, Math.floor(totalSamples / 20));
  for (let i = 0; i < fadeSamples; i++) {
    const gain = i / fadeSamples;
    const firstIdx = i * 2;
    const lastIdx = (totalSamples - 1 - i) * 2;
    pcmData.writeInt16LE(
      Math.round(pcmData.readInt16LE(firstIdx) * gain),
      firstIdx
    );
    pcmData.writeInt16LE(
      Math.round(pcmData.readInt16LE(lastIdx) * gain),
      lastIdx
    );
  }

  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + dataLen, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM = 1
  header.writeUInt16LE(1, 22); // Mono = 1
  header.writeUInt32LE(sampleRate, 24); // 24,000 Hz
  header.writeUInt32LE(sampleRate * 2, 28); // 48,000 bytes/sec
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(dataLen, 40);

  return {
    wavBase64: Buffer.concat([header, pcmData]).toString("base64"),
    durationMs: Math.round((dataLen / (sampleRate * 2)) * 1000),
  };
}

function calculatePcmBufferRms(pcmBuf) {
  if (!pcmBuf || pcmBuf.length < 4) return 0;
  const samples = Math.floor(pcmBuf.length / 2);
  const step = Math.max(1, Math.floor(samples / 120));
  let sumSq = 0;
  let count = 0;
  for (let i = 0; i < samples; i += step) {
    const val = pcmBuf.readInt16LE(i * 2) / 32768.0;
    sumSq += val * val;
    count++;
  }
  return count > 0 ? Math.sqrt(sumSq / count) : 0;
}

function hasPhraseRepetitionLoop(text) {
  const words = String(text || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]+/gu, "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (words.length < 6) return false;
  for (let len = 3; len <= 6; len++) {
    if (words.length >= len * 2) {
      const tail1 = words.slice(words.length - len).join(" ");
      const tail2 = words
        .slice(words.length - len * 2, words.length - len)
        .join(" ");
      if (tail1 === tail2) return true;
    }
  }
  return false;
}

function formatRollingLiveCaption(text, maxChars = 110) {
  let clean = String(text || "").replace(/\s+/g, " ").trim();
  if (!clean) return "";
  clean = clean.replace(/\b(\S+(?:\s+\S+){0,4})\s+\1\b/gi, "$1");
  if (clean.length <= maxChars) return clean;
  const sliced = clean.slice(-maxChars);
  const firstSpace = sliced.indexOf(" ");
  return firstSpace > 0 ? sliced.slice(firstSpace + 1).trim() : sliced;
}

// ============================================================================
// 🗂️ IN-MEMORY ROOM & BIDIRECTIONAL SESSION STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

function clearSpeakerSilenceTimers(speaker) {
  if (!speaker) return;
  if (speaker.autoSilenceWatchdog) {
    clearTimeout(speaker.autoSilenceWatchdog);
    speaker.autoSilenceWatchdog = null;
  }
  if (speaker.silenceInterval) {
    clearInterval(speaker.silenceInterval);
    speaker.silenceInterval = null;
  }
}

function closeListenerGeminiSession(participant) {
  if (!participant) return;
  participant.sessionGen = (participant.sessionGen || 0) + 1;
  if (participant.outFlushTimer) {
    clearTimeout(participant.outFlushTimer);
    participant.outFlushTimer = null;
  }
  if (participant.captionResetTimer) {
    clearTimeout(participant.captionResetTimer);
    participant.captionResetTimer = null;
  }
  participant.outPcm24kBuffers = [];
  participant.pendingInputQueue = [];
  const ws = participant.geminiWs;
  participant.geminiWs = null;
  participant.isSetupComplete = false;
  participant.sessionTargetLang = null;
  participant.isConnecting = false;
  if (ws) {
    try {
      ws.removeAllListeners();
      ws.close();
    } catch (e) {}
  }
}

// ============================================================================
// ⚡ SINGLE-DIRECTION GEMINI 3.5 LIVE TRANSLATE SESSION (WITH LATENCY TRACE)
// ============================================================================
function ensureListenerGeminiSession(
  roomId,
  listenerKey,
  apiVersion = "v1beta"
) {
  const room = rooms.get(roomId);
  if (!room) return;
  const listener = room.participants.get(listenerKey);
  if (!listener) return;

  if (!listener.translatorEnabled || !listener.hasMinutes) {
    closeListenerGeminiSession(listener);
    return;
  }

  const targetLang = String(listener.listenLang || "en")
    .trim()
    .split("-")[0]
    .toLowerCase();

  if (
    listener.geminiWs &&
    listener.geminiWs.readyState === WebSocket.OPEN &&
    listener.sessionTargetLang === targetLang &&
    !listener.isConnecting
  ) {
    return;
  }

  const now = Date.now();
  if (
    listener.isConnecting ||
    now - (listener.lastConnectAttemptAt || 0) < 2000
  ) {
    return;
  }

  listener.isConnecting = true;
  listener.lastConnectAttemptAt = now;

  closeListenerGeminiSession(listener);
  const mySessionGen = listener.sessionGen;

  const wsUrl = getGeminiWsUrl(apiVersion);
  logTranslate(
    "WS_CONNECTING",
    `Connecting ${GEMINI_LIVE_MODEL} (${apiVersion}, gen #${mySessionGen}) for listener ${listener.email}`,
    { roomId, targetLanguageCode: targetLang }
  );

  const ws = new WebSocket(wsUrl);
  listener.geminiWs = ws;
  listener.sessionTargetLang = targetLang;
  listener.isSetupComplete = false;
  listener.outPcm24kBuffers = [];
  listener.audioSeq = 0;

  let currentInputText = "";
  let currentOutputText = "";
  let isFirstBurstOfSpeech = true;
  let ignoreLoopingTurnUntilMs = 0;

  const flush24kAudioStreamToListener = (resetTextAfterFlush = false) => {
    if (listener.sessionGen !== mySessionGen) return;
    if (listener.outFlushTimer) {
      clearTimeout(listener.outFlushTimer);
      listener.outFlushTimer = null;
    }
    if (
      !listener.translatorEnabled ||
      listener.translateMode === "text_only" ||
      !listener.outPcm24kBuffers ||
      listener.outPcm24kBuffers.length === 0
    ) {
      listener.outPcm24kBuffers = [];
      if (resetTextAfterFlush) {
        currentInputText = "";
        currentOutputText = "";
      }
      return;
    }

    const cleanOrig = formatRollingLiveCaption(currentInputText, 110);
    const cleanTrans = formatRollingLiveCaption(currentOutputText, 110);

    const chunks = listener.outPcm24kBuffers.splice(
      0,
      listener.outPcm24kBuffers.length
    );
    const { wavBase64, durationMs } = buildNative24kWavBase64(chunks);
    if (durationMs < 60) return;

    const wasFirstBurst = isFirstBurstOfSpeech;
    isFirstBurstOfSpeech = false;
    const seq = ++listener.audioSeq;
    const nowMs = Date.now();

    // Echo Shield: Block listener's mic while their loudspeaker plays this phrase (+ 500ms acoustic tail)
    listener.ignoreMicUntil =
      Math.max(nowMs, listener.ignoreMicUntil || 0) + durationMs + 500;

    // ⏱️ Latency Trace Calculation
    const geminiTurnLatencyMs = listener.lastAudioSentToGeminiAt
      ? nowMs - listener.lastAudioSentToGeminiAt
      : null;

    if (wasFirstBurst && geminiTurnLatencyMs !== null) {
      logTranslate(
        "LATENCY_TRACE",
        `⚡ First translated audio burst ready for ${listener.email}`,
        {
          sessionGen: mySessionGen,
          seq,
          chunksCount: chunks.length,
          durationMs,
          geminiProcessingMs: geminiTurnLatencyMs,
          corrChunkSeq: listener.lastInChunkSeq || 0,
        }
      );
    }

    const sendSubtitles = listener.translateMode !== "voice_only";

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      sessionGen: mySessionGen,
      seq,
      corrChunkSeq: listener.lastInChunkSeq || 0,
      tCapture: listener.lastInCaptureTs || nowMs,
      tServerIn: listener.lastServerInTs || nowMs,
      tGeminiIn: listener.lastAudioSentToGeminiAt || nowMs,
      tServerOut: nowMs,
      wavBase64,
      sampleRate: 24000,
      durationMs,
      originalText: sendSubtitles ? cleanOrig || null : null,
      translatedText: sendSubtitles ? cleanTrans || null : null,
      targetLang,
      timestamp: nowMs,
    });

    if (resetTextAfterFlush) {
      currentInputText = "";
      currentOutputText = "";
    }
  };

  ws.on("open", () => {
    if (listener.sessionGen !== mySessionGen) {
      try {
        ws.close();
      } catch (e) {}
      return;
    }
    listener.isConnecting = false;

    // Official Google AI Studio Live Translate Setup:
    // - Dedicated model: models/gemini-3.5-live-translate-preview
    // - responseModalities: ["AUDIO"]
    // - translationConfig: { targetLanguageCode: targetLang, echoTargetLanguage: false }
    // - No conversational systemInstruction prompts
    const setupMessage = {
      setup: {
        model: GEMINI_LIVE_MODEL,
        inputAudioTranscription: {},
        outputAudioTranscription: {},
        generationConfig: {
          responseModalities: ["AUDIO"],
          translationConfig: {
            targetLanguageCode: targetLang,
            echoTargetLanguage: false,
          },
        },
      },
    };

    try {
      ws.send(JSON.stringify(setupMessage));
    } catch (e) {}
  });

  ws.on("message", (rawData) => {
    try {
      if (
        listener.sessionGen !== mySessionGen ||
        !listener.translatorEnabled
      ) {
        return;
      }

      const jsonStr = Buffer.isBuffer(rawData)
        ? rawData.toString("utf8")
        : String(rawData);
      const response = JSON.parse(jsonStr);

      if (response.setupComplete !== undefined) {
        listener.isSetupComplete = true;
        logTranslate(
          "SETUP_COMPLETE",
          `✅ setupComplete (${apiVersion}, gen #${mySessionGen}) for ${listener.email} (-> "${targetLang}")`
        );
        io.to(listener.socketId).emit("gemini_session_status", {
          connected: true,
          sessionGen: mySessionGen,
          mode: apiVersion,
          targetLanguageCode: targetLang,
        });

        // Only flush recent (<1.2s old) pending input chunks so stale audio is never replayed after reconnect
        if (listener.pendingInputQueue.length > 0) {
          const nowTs = Date.now();
          const freshItems = listener.pendingInputQueue
            .splice(0, listener.pendingInputQueue.length)
            .filter((item) => nowTs - item.ts < 1200);
          for (const item of freshItems) {
            sendPcm16kToListenerSession(
              roomId,
              listenerKey,
              listener,
              item.pcmBase64,
              item.chunkSeq,
              item.tCapture
            );
          }
        }
        return;
      }

      const sc = response.serverContent;
      if (!sc) return;

      if (sc.interrupted) {
        listener.outPcm24kBuffers = [];
        currentInputText = "";
        currentOutputText = "";
        isFirstBurstOfSpeech = true;
        if (listener.outFlushTimer) {
          clearTimeout(listener.outFlushTimer);
          listener.outFlushTimer = null;
        }
        io.to(listener.socketId).emit("translated_interrupted", {
          roomId,
          sessionGen: mySessionGen,
          timestamp: Date.now(),
        });
      }

      const nowMs = Date.now();
      if (nowMs < ignoreLoopingTurnUntilMs) {
        if (sc.turnComplete || sc.generationComplete) {
          ignoreLoopingTurnUntilMs = 0;
          currentInputText = "";
          currentOutputText = "";
          listener.outPcm24kBuffers = [];
        }
        return;
      }

      let hasTextUpdate = false;

      if (sc.inputTranscription?.text) {
        hasTextUpdate = true;
        currentInputText = `${currentInputText} ${sc.inputTranscription.text}`
          .replace(/\s+/g, " ")
          .trim();
      }

      if (sc.outputTranscription?.text) {
        hasTextUpdate = true;
        currentOutputText =
          `${currentOutputText} ${sc.outputTranscription.text}`
            .replace(/\s+/g, " ")
            .trim();
      }

      // Guard against model repetition loops
      if (
        hasPhraseRepetitionLoop(currentOutputText) ||
        hasPhraseRepetitionLoop(currentInputText)
      ) {
        listener.outPcm24kBuffers = [];
        if (listener.outFlushTimer) {
          clearTimeout(listener.outFlushTimer);
          listener.outFlushTimer = null;
        }
        currentInputText = "";
        currentOutputText = "";
        ignoreLoopingTurnUntilMs = nowMs + 1000;
        return;
      }

      // Progressive 24kHz PCM Audio Forwarding:
      // - First chunk of a spoken phrase (1 chunk = ~240ms) is dispatched IMMEDIATELY so playback starts while the speaker is still talking!
      // - Subsequent chunks group 2 chunks (~480ms) for smooth pre-loaded double-buffering on the phone!
      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (
          part?.inlineData?.data &&
          listener.translateMode !== "text_only"
        ) {
          const pcmBuf = Buffer.from(part.inlineData.data, "base64");
          const isBuilding = listener.outPcm24kBuffers.length > 0;
          const outRms = isBuilding ? 1.0 : calculatePcmBufferRms(pcmBuf);

          if (isBuilding || outRms >= 0.0025) {
            listener.outPcm24kBuffers.push(pcmBuf);

            const targetChunks = isFirstBurstOfSpeech ? 1 : 2;
            if (listener.outPcm24kBuffers.length >= targetChunks) {
              flush24kAudioStreamToListener(false);
            } else {
              clearTimeout(listener.outFlushTimer);
              listener.outFlushTimer = setTimeout(() => {
                flush24kAudioStreamToListener(true);
              }, 150);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          hasTextUpdate = true;
          currentOutputText = `${currentOutputText} ${part.text.trim()}`
            .replace(/\s+/g, " ")
            .trim();
        }
      }

      if (hasTextUpdate) {
        clearTimeout(listener.captionResetTimer);
        listener.captionResetTimer = setTimeout(() => {
          currentInputText = "";
          currentOutputText = "";
        }, 1200);

        if (
          listener.translateMode !== "voice_only" &&
          (currentInputText || currentOutputText) &&
          nowMs - (listener.lastSubtitleEmitAt || 0) >= 350
        ) {
          listener.lastSubtitleEmitAt = nowMs;
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            sessionGen: mySessionGen,
            originalText:
              formatRollingLiveCaption(currentInputText, 110) || null,
            translatedText:
              formatRollingLiveCaption(currentOutputText, 110) || null,
            targetLang,
            isFinal: Boolean(sc.turnComplete),
          });
        }
      }

      if (sc.turnComplete || sc.generationComplete) {
        if (
          listener.translateMode !== "voice_only" &&
          (currentInputText || currentOutputText)
        ) {
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            sessionGen: mySessionGen,
            originalText:
              formatRollingLiveCaption(currentInputText, 110) || null,
            translatedText:
              formatRollingLiveCaption(currentOutputText, 110) || null,
            targetLang,
            isFinal: true,
          });
        }
        flush24kAudioStreamToListener(true);
        isFirstBurstOfSpeech = true;
        currentInputText = "";
        currentOutputText = "";
        io.to(listener.socketId).emit("translated_turn_complete", {
          roomId,
          sessionGen: mySessionGen,
          targetLang,
          timestamp: Date.now(),
        });
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
    if (listener.sessionGen !== mySessionGen) return;
    listener.isConnecting = false;
    const msg = String(err?.message || "");
    logTranslate(
      "WS_ERROR",
      `WebSocket error (${apiVersion}) for ${listener.email}: ${msg}`
    );
    if (apiVersion === "v1beta" && msg.includes("404")) {
      listener.useV1Alpha = true;
      listener.lastConnectAttemptAt = 0;
      ensureListenerGeminiSession(roomId, listenerKey, "v1alpha");
    }
  });

  ws.on("close", () => {
    if (listener.sessionGen !== mySessionGen) return;
    listener.isConnecting = false;
    if (listener.geminiWs === ws) {
      listener.geminiWs = null;
      listener.isSetupComplete = false;
    }
    io.to(listener.socketId).emit("gemini_session_status", {
      connected: false,
      sessionGen: mySessionGen,
      targetLanguageCode: targetLang,
    });
  });
}

// ============================================================================
// 📤 STREAM 100ms 16kHz PCM CHUNK TO LISTENER'S GEMINI LIVE SESSION
// ============================================================================
function sendPcm16kToListenerSession(
  roomId,
  listenerKey,
  listener,
  pcmBase64,
  chunkSeq = 0,
  tCapture = 0
) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  const now = Date.now();
  if (chunkSeq > 0) {
    listener.lastInChunkSeq = chunkSeq;
    listener.lastInCaptureTs = tCapture || now;
    listener.lastServerInTs = now;
    listener.lastAudioSentToGeminiAt = now;
  }

  const preferredVersion = listener.useV1Alpha ? "v1alpha" : "v1beta";

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    if (pcmBase64 !== SILENT_PCM_100MS_B64 && listener.pendingInputQueue.length < 6) {
      listener.pendingInputQueue.push({
        pcmBase64,
        chunkSeq,
        tCapture,
        ts: now,
      });
    }
    ensureListenerGeminiSession(roomId, listenerKey, preferredVersion);
    return;
  }

  if (
    listener.geminiWs.readyState === WebSocket.OPEN &&
    listener.isSetupComplete
  ) {
    try {
      listener.geminiWs.send(
        JSON.stringify({
          realtimeInput: {
            audio: {
              data: pcmBase64,
              mimeType: "audio/pcm;rate=16000",
            },
          },
        })
      );
    } catch (e) {}
  } else if (
    pcmBase64 !== SILENT_PCM_100MS_B64 &&
    listener.pendingInputQueue.length < 6
  ) {
    listener.pendingInputQueue.push({
      pcmBase64,
      chunkSeq,
      tCapture,
      ts: now,
    });
  }
}

// ============================================================================
// 🔇 SERVER-SIDE AUTO-SILENCE TAIL INJECTOR
// Guarantees that if a speaker stops speaking or mutes their mic, Gemini
// immediately receives 400ms of pure 0x00 silence to finish the translation
// instead of holding pre-mute speech in memory until unmute!
// ============================================================================
function scheduleSpeakerAutoSilenceFlush(roomId, speakerKey, speaker) {
  clearSpeakerSilenceTimers(speaker);

  speaker.autoSilenceWatchdog = setTimeout(() => {
    speaker.autoSilenceWatchdog = null;
    const room = rooms.get(roomId);
    if (!room) return;

    speaker.preRollChunks = [];
    speaker.hangoverLeft = 0;
    speaker.silenceFlushLeft = 0;

    let framesSent = 0;
    speaker.silenceInterval = setInterval(() => {
      framesSent += 1;
      const curRoom = rooms.get(roomId);
      if (!curRoom || framesSent > 4) {
        clearSpeakerSilenceTimers(speaker);
        return;
      }
      for (const [listenerKey, listener] of curRoom.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          SILENT_PCM_100MS_B64,
          0,
          0
        );
      }
    }, 95);
  }, 180);
}

function broadcastRoomTranslateState(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  const list = [];
  for (const [, p] of room.participants.entries()) {
    list.push({
      email: p.email,
      speakLang: p.speakLang,
      listenLang: p.listenLang,
      translateMode: p.translateMode || "both",
      translatorEnabled: Boolean(p.translatorEnabled && p.hasMinutes),
    });
  }
  io.to(roomId).emit("room_translate_state", {
    roomId,
    participants: list,
    timestamp: Date.now(),
  });
}

// ============================================================================
// ⚡ SOCKET.IO CONNECTION HANDLER
// ============================================================================
io.on("connection", (socket) => {
  logTranslate("SOCKET_CONNECT", `Client connected: ${socket.id}`);

  // ⏱️ Client Playback Latency Telemetry Logger
  socket.on("client_latency_report", (report) => {
    logTranslate(
      "LATENCY_E2E",
      `📊 End-to-End Translation Latency for ${report?.email || socket.id}:`,
      report
    );
  });

  // 1. JOIN / SYNC TRANSLATE ROOM
  socket.on("join_translate_room", (payload) => {
    const {
      roomId,
      email,
      speakLang = "en",
      listenLang = "bn",
      translateMode = "both",
      translatorEnabled = false,
      hasMinutes = false,
    } = payload || {};

    if (!roomId || !email) return;
    const emailKey = String(email).trim().toLowerCase();

    socket.join(roomId);
    socketToMeta.set(socket.id, { roomId, emailKey });

    if (!rooms.has(roomId)) {
      rooms.set(roomId, {
        participants: new Map(),
        activeSpeakerKey: null,
        activeSpeakerRms: 0,
        floorLockUntil: 0,
      });
    }
    const room = rooms.get(roomId);

    const existing = room.participants.get(emailKey);
    const participant = existing || {
      email: String(email).trim(),
      emailKey,
      socketId: socket.id,
      speakLang: String(speakLang || "en"),
      listenLang: String(listenLang || "bn"),
      translateMode: String(translateMode || "both"),
      translatorEnabled: Boolean(translatorEnabled),
      hasMinutes: Boolean(hasMinutes),
      sessionGen: 1,
      geminiWs: null,
      isSetupComplete: false,
      sessionTargetLang: null,
      isConnecting: false,
      useV1Alpha: false,
      lastConnectAttemptAt: 0,
      lastSubtitleEmitAt: 0,
      ignoreMicUntil: 0,
      lastChunkSeq: 0,
      audioSeq: 0,
      outPcm24kBuffers: [],
      pendingInputQueue: [],
      preRollChunks: [],
      hangoverLeft: 0,
      silenceFlushLeft: 0,
      autoSilenceWatchdog: null,
      silenceInterval: null,
    };

    // Reset input chunk sequence tracker on fresh join/reconnect
    participant.lastChunkSeq = 0;
    participant.socketId = socket.id;
    participant.speakLang = String(speakLang || participant.speakLang || "en");
    participant.listenLang = String(
      listenLang || participant.listenLang || "bn"
    );
    participant.translateMode = String(
      translateMode || participant.translateMode || "both"
    );
    participant.translatorEnabled = Boolean(translatorEnabled);
    participant.hasMinutes = Boolean(hasMinutes);

    room.participants.set(emailKey, participant);

    broadcastRoomTranslateState(roomId);

    if (participant.translatorEnabled && participant.hasMinutes) {
      ensureListenerGeminiSession(
        roomId,
        emailKey,
        participant.useV1Alpha ? "v1alpha" : "v1beta"
      );
    } else {
      closeListenerGeminiSession(participant);
    }
  });

  // 2. UPDATE TRANSLATE CONFIG
  socket.on("update_translate_config", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const emailKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;

    if (!roomId || !emailKey || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const participant = room.participants.get(emailKey);
    if (!participant) return;

    const nextSpeak =
      payload.speakLang !== undefined
        ? String(payload.speakLang)
        : participant.speakLang;
    const nextListen =
      payload.listenLang !== undefined
        ? String(payload.listenLang)
        : participant.listenLang;
    const nextMode =
      payload.translateMode !== undefined
        ? String(payload.translateMode)
        : participant.translateMode || "both";
    const nextEnabled =
      payload.translatorEnabled !== undefined
        ? Boolean(payload.translatorEnabled)
        : participant.translatorEnabled;
    const nextMins =
      payload.hasMinutes !== undefined
        ? Boolean(payload.hasMinutes)
        : participant.hasMinutes;

    if (
      participant.speakLang === nextSpeak &&
      participant.listenLang === nextListen &&
      participant.translateMode === nextMode &&
      participant.translatorEnabled === nextEnabled &&
      participant.hasMinutes === nextMins
    ) {
      return;
    }

    const listenChanged = participant.listenLang !== nextListen;

    participant.speakLang = nextSpeak;
    participant.listenLang = nextListen;
    participant.translateMode = nextMode;
    participant.translatorEnabled = nextEnabled;
    participant.hasMinutes = nextMins;

    if (nextMode === "text_only") {
      participant.outPcm24kBuffers = [];
      if (participant.outFlushTimer) {
        clearTimeout(participant.outFlushTimer);
        participant.outFlushTimer = null;
      }
    }

    broadcastRoomTranslateState(roomId);

    if (participant.translatorEnabled && participant.hasMinutes) {
      if (listenChanged) {
        closeListenerGeminiSession(participant);
        participant.lastConnectAttemptAt = 0;
      }
      ensureListenerGeminiSession(
        roomId,
        emailKey,
        participant.useV1Alpha ? "v1alpha" : "v1beta"
      );
    } else {
      closeListenerGeminiSession(participant);
    }
  });

  // 3. MUTE FLUSH HANDLER: Immediately finalizes any speech spoken right before muting!
  socket.on("mic_muted_flush", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;

    if (!roomId || !speakerKey || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const speaker = room.participants.get(speakerKey);
    if (!speaker) return;

    clearSpeakerSilenceTimers(speaker);
    speaker.preRollChunks = [];
    speaker.hangoverLeft = 0;
    speaker.silenceFlushLeft = 0;

    // Immediately push 4 frames (400ms) of digital silence so Gemini translates pre-mute speech NOW
    let count = 0;
    speaker.silenceInterval = setInterval(() => {
      count += 1;
      const curRoom = rooms.get(roomId);
      if (!curRoom || count > 4) {
        clearSpeakerSilenceTimers(speaker);
        return;
      }
      for (const [listenerKey, listener] of curRoom.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          SILENT_PCM_100MS_B64,
          0,
          0
        );
      }
    }, 80);
  });

  // 4. 100ms 16kHz PCM STREAMING WITH SEQUENCE DEDUPLICATION & AUTO-SILENCE WATCHDOG
  socket.on("stream_mic_pcm_16k", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const pcmBase64 = payload?.pcmBase64;
    const chunkSeq = Number(payload?.chunkSeq) || 0;
    const tCapture = Number(payload?.tCapture) || Date.now();

    if (!roomId || !speakerKey || !pcmBase64 || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const speaker = room.participants.get(speakerKey);
    if (!speaker) return;

    // 1. Sequence Deduplication: Never process the same microphone chunk twice!
    if (chunkSeq > 0) {
      if (chunkSeq <= (speaker.lastChunkSeq || 0)) {
        return;
      }
      speaker.lastChunkSeq = chunkSeq;
    }

    const now = Date.now();

    // 2. Echo Shield: Ignore speaker's mic while their loudspeaker is playing translated audio
    if (now < (speaker.ignoreMicUntil || 0)) {
      speaker.preRollChunks = [];
      speaker.hangoverLeft = 0;
      speaker.silenceFlushLeft = 0;
      return;
    }

    let pcmBuf = null;
    let rms = 0;
    try {
      pcmBuf = Buffer.from(pcmBase64, "base64");
      rms = calculatePcmBufferRms(pcmBuf);
    } catch (e) {
      return;
    }

    // 3. Dominant-Speaker Crosstalk Lock
    if (
      room.activeSpeakerKey &&
      room.activeSpeakerKey !== speakerKey &&
      now < (room.floorLockUntil || 0)
    ) {
      if (rms < (room.activeSpeakerRms || 0.03) * 1.65) {
        speaker.preRollChunks = [];
        speaker.hangoverLeft = 0;
        speaker.silenceFlushLeft = 0;
        return;
      }
    }

    if (rms >= 0.012) {
      clearSpeakerSilenceTimers(speaker);
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 400;

      speaker.hangoverLeft = 2; // 200ms natural word ending
      speaker.silenceFlushLeft = 3; // Followed by 300ms pure digital 0x00 silence

      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        if (speaker.preRollChunks.length > 0) {
          for (const preB64 of speaker.preRollChunks) {
            sendPcm16kToListenerSession(
              roomId,
              listenerKey,
              listener,
              preB64,
              chunkSeq,
              tCapture
            );
          }
        }
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          pcmBase64,
          chunkSeq,
          tCapture
        );
      }
      speaker.preRollChunks = [];

      // Arm auto-silence watchdog in case user suddenly mutes or stops sending packets!
      scheduleSpeakerAutoSilenceFlush(roomId, speakerKey, speaker);
    } else if (speaker.hangoverLeft > 0) {
      speaker.hangoverLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          pcmBase64,
          chunkSeq,
          tCapture
        );
      }
    } else if (speaker.silenceFlushLeft > 0) {
      speaker.silenceFlushLeft -= 1;
      if (speaker.silenceFlushLeft === 0) {
        clearSpeakerSilenceTimers(speaker);
      }
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          SILENT_PCM_100MS_B64,
          0,
          0
        );
      }
    } else {
      speaker.preRollChunks = [pcmBase64];
    }
  });

  // 5. DIRECT PARTNER PCM FORWARDING
  socket.on("translate_partner_pcm_16k", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const listenerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const pcmBase64 = payload?.pcmBase64;

    if (!roomId || !listenerKey || !pcmBase64 || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const listener = room.participants.get(listenerKey);
    if (!listener || !listener.translatorEnabled || !listener.hasMinutes) {
      return;
    }

    sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64, 0, 0);
  });

  // 6. IMMEDIATE CLEANUP ON HANGUP / LEAVE / DISCONNECT
  const cleanupSocketParticipant = () => {
    const meta = socketToMeta.get(socket.id);
    socketToMeta.delete(socket.id);
    if (!meta) return;

    const { roomId, emailKey } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    const participant = room.participants.get(emailKey);
    if (participant && participant.socketId === socket.id) {
      clearSpeakerSilenceTimers(participant);
      closeListenerGeminiSession(participant);
      room.participants.delete(emailKey);
    }

    if (room.participants.size === 0) {
      rooms.delete(roomId);
    } else {
      broadcastRoomTranslateState(roomId);
    }
  };

  socket.on("leave_translate_room", cleanupSocketParticipant);
  socket.on("disconnect", cleanupSocketParticipant);
});

app.get("/", (req, res) => {
  res.status(200).json({
    server: "GeminiTranslateServer",
    model: GEMINI_LIVE_MODEL,
    status: "online",
    activeTranslateRooms: rooms.size,
    uptimeSeconds: Math.floor(process.uptime()),
  });
});

app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

server.listen(PORT, "0.0.0.0", () => {
  logTranslate(
    "SERVER_START",
    `🚀 GeminiTranslateServer (${GEMINI_LIVE_MODEL}) running on 0.0.0.0:${PORT}`
  );
});