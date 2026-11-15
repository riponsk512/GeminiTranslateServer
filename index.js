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

// Pure digital 100ms 16kHz 16-bit mono silence frame (3200 zero bytes)
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
// 🔊 NATIVE 24kHz 16-BIT MONO LITTLE-ENDIAN WAV BUILDER (NO RESAMPLING!)
// ============================================================================
function buildNative24kWavBase64(pcm24kBuffers) {
  const pcmData = Buffer.concat(pcm24kBuffers);
  const sampleRate = 24000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 1ms (24-sample) edge de-click
  const fadeSamples = Math.min(24, Math.floor(totalSamples / 20));
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
  header.writeUInt16LE(2, 32); // BlockAlign = 2
  header.writeUInt16LE(16, 34); // 16-bit
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
// ⚡ GOOGLE AI STUDIO LIVE TRANSLATE SESSION (1 SESSION PER DIRECTION)
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

  const sessionId = `gsess_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 6)}`;
  listener.sessionId = sessionId;

  const wsUrl = getGeminiWsUrl(apiVersion);
  logTranslate(
    "WS_CONNECTING",
    `Connecting ${GEMINI_LIVE_MODEL} (${apiVersion}) for listener ${listener.email}`,
    { roomId, sessionId, targetLanguageCode: targetLang }
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
  let t3FirstGeminiOutMs = 0;

  const flush24kAudioStreamToListener = () => {
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
      return;
    }

    const chunks = listener.outPcm24kBuffers.splice(
      0,
      listener.outPcm24kBuffers.length
    );
    const { wavBase64, durationMs } = buildNative24kWavBase64(chunks);
    if (durationMs < 60) return;

    isFirstBurstOfSpeech = false;
    const outSeq = ++listener.audioSeq;
    const t4BackendForwardMs = Date.now();

    // Echo Shield: Block listener's mic while their loudspeaker plays this audio (+ 300ms tail)
    listener.ignoreMicUntil =
      Math.max(t4BackendForwardMs, listener.ignoreMicUntil || 0) +
      durationMs +
      300;

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      sessionId,
      seq: outSeq,
      corrInSeq: listener.lastActiveInSeq || 0,
      wavBase64,
      sampleRate: 24000,
      durationMs,
      targetLang,
      telemetry: {
        t1_captureMs: listener.lastActiveT1Ms || t4BackendForwardMs,
        t2_sentToGeminiMs: listener.lastActiveT2Ms || t4BackendForwardMs,
        t3_firstGeminiOutMs: t3FirstGeminiOutMs || t4BackendForwardMs,
        t4_backendForwardMs,
      },
      timestamp: t4BackendForwardMs,
    });
  };

  ws.on("open", () => {
    if (listener.geminiWs !== ws) return;
    listener.isConnecting = false;

    // Exact documented Google AI Studio Live Translate setup:
    // - No custom conversational prompts
    // - responseModalities: ["AUDIO"]
    // - translationConfig: { targetLanguageCode, echoTargetLanguage: false }
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
      if (listener.geminiWs !== ws || !listener.translatorEnabled) return;

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
          sessionId,
          mode: apiVersion,
          targetLanguageCode: targetLang,
        });

        if (listener.pendingInputQueue.length > 0) {
          const queued = listener.pendingInputQueue.splice(
            0,
            listener.pendingInputQueue.length
          );
          for (const item of queued) {
            sendPcm16kToListenerSession(
              roomId,
              listenerKey,
              listener,
              item.pcmBase64,
              item.inSeq,
              item.t1_captureMs
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
        t3FirstGeminiOutMs = 0;
        if (listener.outFlushTimer) {
          clearTimeout(listener.outFlushTimer);
          listener.outFlushTimer = null;
        }
        io.to(listener.socketId).emit("translated_interrupted", {
          roomId,
          sessionId,
          timestamp: Date.now(),
        });
      }

      const nowMs = Date.now();
      let hasTextUpdate = false;

      if (sc.inputTranscription?.text) {
        hasTextUpdate = true;
        currentInputText = `${currentInputText} ${sc.inputTranscription.text}`
          .replace(/\s+/g, " ")
          .trim()
          .slice(-110);
      }

      if (sc.outputTranscription?.text) {
        hasTextUpdate = true;
        currentOutputText =
          `${currentOutputText} ${sc.outputTranscription.text}`
            .replace(/\s+/g, " ")
            .trim()
            .slice(-110);
      }

      // Immediate 24kHz PCM Audio Forwarding
      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (
          part?.inlineData?.data &&
          listener.translateMode !== "text_only"
        ) {
          const pcmBuf = Buffer.from(part.inlineData.data, "base64");
          const isBuildingPhrase = listener.outPcm24kBuffers.length > 0;
          const outRms = isBuildingPhrase
            ? 1.0
            : calculatePcmBufferRms(pcmBuf);

          if (isBuildingPhrase || outRms >= 0.002) {
            if (isFirstBurstOfSpeech && listener.outPcm24kBuffers.length === 0) {
              t3FirstGeminiOutMs = Date.now();
            }
            listener.outPcm24kBuffers.push(pcmBuf);

            // First chunk dispatches immediately (0ms wait) so playback starts while speaker is talking;
            // subsequent chunks while phone is playing group into 3-chunk (~700ms) smooth buffers!
            const targetChunks = isFirstBurstOfSpeech ? 1 : 3;
            if (listener.outPcm24kBuffers.length >= targetChunks) {
              flush24kAudioStreamToListener();
            } else {
              clearTimeout(listener.outFlushTimer);
              listener.outFlushTimer = setTimeout(() => {
                flush24kAudioStreamToListener();
              }, 180);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          hasTextUpdate = true;
          currentOutputText = `${currentOutputText} ${part.text.trim()}`
            .replace(/\s+/g, " ")
            .trim()
            .slice(-110);
        }
      }

      if (hasTextUpdate) {
        clearTimeout(listener.captionResetTimer);
        listener.captionResetTimer = setTimeout(() => {
          currentInputText = "";
          currentOutputText = "";
        }, 1400);

        if (
          listener.translateMode !== "voice_only" &&
          (currentInputText || currentOutputText) &&
          nowMs - (listener.lastSubtitleEmitAt || 0) >= 400
        ) {
          listener.lastSubtitleEmitAt = nowMs;
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            sessionId,
            originalText: currentInputText || null,
            translatedText: currentOutputText || null,
            targetLang,
            isFinal: Boolean(sc.turnComplete),
          });
        }
      }

      if (sc.turnComplete || sc.generationComplete) {
        flush24kAudioStreamToListener();
        isFirstBurstOfSpeech = true;
        t3FirstGeminiOutMs = 0;
        if (
          listener.translateMode !== "voice_only" &&
          (currentInputText || currentOutputText)
        ) {
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            sessionId,
            originalText: currentInputText || null,
            translatedText: currentOutputText || null,
            targetLang,
            isFinal: true,
          });
        }
        currentInputText = "";
        currentOutputText = "";
        io.to(listener.socketId).emit("translated_turn_complete", {
          roomId,
          sessionId,
          targetLang,
          timestamp: Date.now(),
        });
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
    if (listener.geminiWs !== ws) return;
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
    if (listener.geminiWs !== ws) return;
    listener.isConnecting = false;
    listener.geminiWs = null;
    listener.isSetupComplete = false;
    io.to(listener.socketId).emit("gemini_session_status", {
      connected: false,
      targetLanguageCode: targetLang,
    });
  });
}

// ============================================================================
// 📤 SEND 100ms 16kHz PCM CHUNK TO LISTENER'S GEMINI LIVE SESSION
// ============================================================================
function sendPcm16kToListenerSession(
  roomId,
  listenerKey,
  listener,
  pcmBase64,
  inSeq = 0,
  t1CaptureMs = Date.now()
) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  const preferredVersion = listener.useV1Alpha ? "v1alpha" : "v1beta";

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    if (listener.pendingInputQueue.length < 10) {
      listener.pendingInputQueue.push({
        pcmBase64,
        inSeq,
        t1_captureMs: t1CaptureMs,
      });
    }
    ensureListenerGeminiSession(roomId, listenerKey, preferredVersion);
    return;
  }

  if (
    listener.geminiWs.readyState === WebSocket.OPEN &&
    listener.isSetupComplete
  ) {
    listener.lastActiveInSeq = inSeq;
    listener.lastActiveT1Ms = t1CaptureMs;
    listener.lastActiveT2Ms = Date.now();
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
    listener.pendingInputQueue.push({
      pcmBase64,
      inSeq,
      t1_captureMs: t1CaptureMs,
    });
  }
}

function flushListenerGeminiInputOnMute(listener) {
  if (
    !listener?.geminiWs ||
    listener.geminiWs.readyState !== WebSocket.OPEN ||
    !listener.isSetupComplete
  ) {
    return;
  }
  try {
    for (let i = 0; i < 4; i++) {
      listener.geminiWs.send(
        JSON.stringify({
          realtimeInput: {
            audio: {
              data: SILENT_PCM_100MS_B64,
              mimeType: "audio/pcm;rate=16000",
            },
          },
        })
      );
    }
    listener.geminiWs.send(
      JSON.stringify({
        realtimeInput: {
          audioStreamEnd: true,
        },
      })
    );
  } catch (e) {}
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

  socket.on("client_latency_telemetry", (t) => {
    if (!t) return;
    logTranslate("LATENCY_REPORT", `⏱️ End-to-End Pipeline Telemetry:`, {
      listener: t.email,
      seq: t.seq,
      corrInSeq: t.corrInSeq,
      geminiInferenceMs: t.geminiInferenceMs,
      backendOverheadMs: t.backendOverheadMs,
      clientQueueDelayMs: t.clientQueueDelayMs,
      totalEndToEndMs: t.totalEndToEndMs,
    });
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
      sessionId: null,
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
      lastInSeqReceived: 0,
      lastActiveInSeq: 0,
      lastActiveT1Ms: 0,
      lastActiveT2Ms: 0,
      audioSeq: 0,
      outPcm24kBuffers: [],
      pendingInputQueue: [],
      preRollChunks: [],
      hangoverLeft: 0,
      silenceFlushLeft: 0,
    };

    participant.socketId = socket.id;
    participant.lastInSeqReceived = 0;
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

  // 3. IMMEDIATE MIC-MUTE FLUSH
  socket.on("flush_muted_mic", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;

    if (!roomId || !speakerKey || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const speaker = room.participants.get(speakerKey);
    if (speaker) {
      speaker.preRollChunks = [];
      speaker.hangoverLeft = 0;
      speaker.silenceFlushLeft = 0;
    }

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;
      flushListenerGeminiInputOnMute(listener);
    }
  });

  // 4. CONTINUOUS 100ms 16kHz PCM STREAMING
  socket.on("stream_mic_pcm_16k", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const pcmBase64 = payload?.pcmBase64;
    const inSeq = Number(payload?.inSeq) || 0;
    const t1CaptureMs = Number(payload?.t1_captureMs) || Date.now();

    if (!roomId || !speakerKey || !pcmBase64 || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const speaker = room.participants.get(speakerKey);
    if (!speaker) return;

    if (inSeq > 0 && inSeq <= (speaker.lastInSeqReceived || 0)) {
      return;
    }
    if (inSeq > 0) {
      speaker.lastInSeqReceived = inSeq;
    }

    const now = Date.now();

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

    if (rms >= 0.011) {
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 400;

      speaker.hangoverLeft = 3;
      speaker.silenceFlushLeft = 3;

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
              inSeq,
              t1CaptureMs
            );
          }
        }
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          pcmBase64,
          inSeq,
          t1CaptureMs
        );
      }
      speaker.preRollChunks = [];
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
          inSeq,
          t1CaptureMs
        );
      }
    } else if (speaker.silenceFlushLeft > 0) {
      speaker.silenceFlushLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          SILENT_PCM_100MS_B64,
          inSeq,
          t1CaptureMs
        );
      }
    } else {
      speaker.preRollChunks.push(pcmBase64);
      if (speaker.preRollChunks.length > 2) {
        speaker.preRollChunks.shift();
      }
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

    sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
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