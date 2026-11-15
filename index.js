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
// Forces Gemini's neural VAD to finalize the current phrase immediately without looping!
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
// 🔊 NATIVE 24kHz 16-BIT MONO LITTLE-ENDIAN WAV BUILDER
// ============================================================================
function buildNative24kWavBase64(pcm24kBuffers) {
  const pcmData = Buffer.concat(pcm24kBuffers);
  const sampleRate = 24000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

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

// ============================================================================
// 🛡️ REPETITION LOOP DETECTOR & ROLLING CAPTION WINDOW
// Prevents Gemini decoder loops ("Actually but tell you where live?" x10)
// and keeps captions concise (max ~110 chars of the active spoken sentence)
// ============================================================================
function hasPhraseRepetitionLoop(text) {
  const words = String(text || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]+/gu, "")
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (words.length < 6) return false;

  // Check if any 3-to-6 word sequence at the end just repeated consecutively
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

  // Collapse immediate repeated words/phrases
  clean = clean.replace(/\b(\S+(?:\s+\S+){0,4})\s+\1\b/gi, "$1");

  if (clean.length <= maxChars) return clean;
  const sliced = clean.slice(-maxChars);
  const firstSpace = sliced.indexOf(" ");
  return firstSpace > 0 ? sliced.slice(firstSpace + 1).trim() : sliced;
}

function normalizeForDedup(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

// ============================================================================
// 🗂️ IN-MEMORY ROOM & BIDIRECTIONAL SESSION STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

function closeListenerGeminiSession(participant) {
  if (!participant) return;
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
// ⚡ GOOGLE AI STUDIO LIVE TRANSLATE SESSION (ZERO REPETITION LOOP)
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

  const wsUrl = getGeminiWsUrl(apiVersion);
  logTranslate(
    "WS_CONNECTING",
    `Connecting ${GEMINI_LIVE_MODEL} (${apiVersion}) for listener ${listener.email}`,
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
  let lastFlushedNormText = "";
  let lastFlushedAt = 0;
  let isFirstBurstOfSpeech = true;
  let ignoreLoopingTurnUntilMs = 0;

  const flush24kAudioStreamToListener = (resetTextAfterFlush = false) => {
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
    const normTrans = normalizeForDedup(cleanTrans);

    // Block duplicate / looping audio phrases within 4 seconds
    if (
      normTrans &&
      normTrans === lastFlushedNormText &&
      Date.now() - lastFlushedAt < 4000
    ) {
      listener.outPcm24kBuffers = [];
      currentInputText = "";
      currentOutputText = "";
      return;
    }

    const chunks = listener.outPcm24kBuffers.splice(
      0,
      listener.outPcm24kBuffers.length
    );
    const { wavBase64, durationMs } = buildNative24kWavBase64(chunks);
    if (durationMs < 80) return;

    if (normTrans && resetTextAfterFlush) {
      lastFlushedNormText = normTrans;
      lastFlushedAt = Date.now();
    }

    isFirstBurstOfSpeech = false;
    const seq = ++listener.audioSeq;

    // Echo Shield: Block listener's mic while their loudspeaker plays this phrase (+ 550ms tail)
    listener.ignoreMicUntil =
      Math.max(Date.now(), listener.ignoreMicUntil || 0) + durationMs + 550;

    const sendSubtitles = listener.translateMode !== "voice_only";

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      seq,
      wavBase64,
      sampleRate: 24000,
      durationMs,
      originalText: sendSubtitles ? cleanOrig || null : null,
      translatedText: sendSubtitles ? cleanTrans || null : null,
      targetLang,
      timestamp: Date.now(),
    });

    if (resetTextAfterFlush) {
      currentInputText = "";
      currentOutputText = "";
    }
  };

  ws.on("open", () => {
    listener.isConnecting = false;

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
      if (!listener.translatorEnabled) return;

      const jsonStr = Buffer.isBuffer(rawData)
        ? rawData.toString("utf8")
        : String(rawData);
      const response = JSON.parse(jsonStr);

      if (response.setupComplete !== undefined) {
        listener.isSetupComplete = true;
        logTranslate(
          "SETUP_COMPLETE",
          `✅ setupComplete (${apiVersion}) for ${listener.email} (targetLanguageCode: "${targetLang}")`
        );
        io.to(listener.socketId).emit("gemini_session_status", {
          connected: true,
          mode: apiVersion,
          targetLanguageCode: targetLang,
        });

        if (listener.pendingInputQueue.length > 0) {
          const queued = listener.pendingInputQueue.splice(
            0,
            listener.pendingInputQueue.length
          );
          for (const b64 of queued) {
            sendPcm16kToListenerSession(roomId, listenerKey, listener, b64);
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

      // 🚨 LOOP BREAKER: If Gemini starts repeating the same phrase in a loop, cut it immediately!
      if (
        hasPhraseRepetitionLoop(currentOutputText) ||
        hasPhraseRepetitionLoop(currentInputText)
      ) {
        logTranslate(
          "LOOP_BROKEN",
          `🛑 Stopped decoder repetition loop for ${listener.email}`
        );
        listener.outPcm24kBuffers = [];
        if (listener.outFlushTimer) {
          clearTimeout(listener.outFlushTimer);
          listener.outFlushTimer = null;
        }
        currentInputText = "";
        currentOutputText = "";
        ignoreLoopingTurnUntilMs = nowMs + 1200;
        return;
      }

      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (
          part?.inlineData?.data &&
          listener.translateMode !== "text_only"
        ) {
          const pcmBuf = Buffer.from(part.inlineData.data, "base64");
          const isCurrentlyBuildingPhrase =
            listener.outPcm24kBuffers.length > 0;
          const outRms = isCurrentlyBuildingPhrase
            ? 1.0
            : calculatePcmBufferRms(pcmBuf);

          if (isCurrentlyBuildingPhrase || outRms >= 0.0025) {
            listener.outPcm24kBuffers.push(pcmBuf);

            // Fast first burst (2 chunks = ~450ms) so voice starts immediately;
            // continuation bursts group 3 chunks (~700ms) or flush 220ms after burst ends!
            const targetChunks = isFirstBurstOfSpeech ? 2 : 3;
            if (listener.outPcm24kBuffers.length >= targetChunks) {
              flush24kAudioStreamToListener(false);
            } else {
              clearTimeout(listener.outFlushTimer);
              listener.outFlushTimer = setTimeout(() => {
                flush24kAudioStreamToListener(true);
              }, 220);
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
        // Auto-reset server caption buffer after 1.3s of silence so old sentences never pile up!
        clearTimeout(listener.captionResetTimer);
        listener.captionResetTimer = setTimeout(() => {
          currentInputText = "";
          currentOutputText = "";
        }, 1300);

        if (
          listener.translateMode !== "voice_only" &&
          (currentInputText || currentOutputText) &&
          nowMs - (listener.lastSubtitleEmitAt || 0) >= 400
        ) {
          listener.lastSubtitleEmitAt = nowMs;
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            originalText: formatRollingLiveCaption(currentInputText, 110) || null,
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
            originalText: formatRollingLiveCaption(currentInputText, 110) || null,
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
          targetLang,
          timestamp: Date.now(),
        });
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
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
    listener.isConnecting = false;
    if (listener.geminiWs === ws) {
      listener.geminiWs = null;
      listener.isSetupComplete = false;
    }
    io.to(listener.socketId).emit("gemini_session_status", {
      connected: false,
      targetLanguageCode: targetLang,
    });
  });
}

// ============================================================================
// 📤 STREAM 16kHz PCM TO LISTENER'S GEMINI LIVE SESSION
// ============================================================================
function sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  const preferredVersion = listener.useV1Alpha ? "v1alpha" : "v1beta";

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    if (listener.pendingInputQueue.length < 10) {
      listener.pendingInputQueue.push(pcmBase64);
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
  } else if (listener.pendingInputQueue.length < 10) {
    listener.pendingInputQueue.push(pcmBase64);
  }
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
      geminiWs: null,
      isSetupComplete: false,
      sessionTargetLang: null,
      isConnecting: false,
      useV1Alpha: false,
      lastConnectAttemptAt: 0,
      lastSubtitleEmitAt: 0,
      ignoreMicUntil: 0,
      audioSeq: 0,
      outPcm24kBuffers: [],
      pendingInputQueue: [],
      preRollChunks: [],
      hangoverLeft: 0,
      silenceFlushLeft: 0,
    };

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

  // 3. SPEECH-GATED CONTINUOUS STREAMING (STOPS ROOM SILENCE FROM CAUSING REPEAT LOOPS!)
  socket.on("stream_mic_pcm_16k", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const pcmBase64 = payload?.pcmBase64;

    if (!roomId || !speakerKey || !pcmBase64 || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const speaker = room.participants.get(speakerKey);
    if (!speaker) return;

    const now = Date.now();

    // 1. ECHO SHIELD: Ignore speaker's mic while their loudspeaker is playing translated audio
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

    // 2. DOMINANT-SPEAKER CROSSTALK LOCK (Blocks Phone B's mic from echoing Phone A's voice!)
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

    if (rms >= 0.013) {
      // Active human speech -> stream immediately + set natural tail + digital silence end trigger
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 450;

      speaker.hangoverLeft = 2; // 400ms natural word ending
      speaker.silenceFlushLeft = 3; // Followed by 300ms pure digital 0x00 silence to finalize phrase cleanly

      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        if (speaker.preRollChunks.length > 0) {
          for (const preB64 of speaker.preRollChunks) {
            sendPcm16kToListenerSession(roomId, listenerKey, listener, preB64);
          }
        }
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
      }
      speaker.preRollChunks = [];
    } else if (speaker.hangoverLeft > 0) {
      speaker.hangoverLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
      }
    } else if (speaker.silenceFlushLeft > 0) {
      // Pure digital 0x00 silence -> tells Gemini the spoken phrase ended so it NEVER loops!
      speaker.silenceFlushLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          SILENT_PCM_100MS_B64
        );
      }
    } else {
      // Speaker is silent -> do NOT feed room static into Gemini! Keep 1 pre-roll chunk for next word.
      speaker.preRollChunks = [pcmBase64];
    }
  });

  // 4. DIRECT PARTNER PCM FORWARDING
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

    sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
  });

  // 5. IMMEDIATE CLEANUP ON HANGUP / LEAVE / DISCONNECT
  const cleanupSocketParticipant = () => {
    const meta = socketToMeta.get(socket.id);
    socketToMeta.delete(socket.id);
    if (!meta) return;

    const { roomId, emailKey } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    const participant = room.participants.get(emailKey);
    if (participant && participant.socketId === socket.id) {
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