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

// Official Gemini Live API BidiGenerateContent WebSocket URL (v1beta primary, v1alpha fallback)
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

// Fast check to filter out pure digital 0x0000 idle ticks from Gemini's continuous output stream
function hasAudiblePcmSignal(pcmBuf) {
  if (!pcmBuf || pcmBuf.length < 4) return false;
  const samples = Math.floor(pcmBuf.length / 2);
  const step = Math.max(1, Math.floor(samples / 120));
  let sumSq = 0;
  let count = 0;
  for (let i = 0; i < samples; i += step) {
    const val = pcmBuf.readInt16LE(i * 2) / 32768.0;
    sumSq += val * val;
    count++;
  }
  const rms = count > 0 ? Math.sqrt(sumSq / count) : 0;
  return rms >= 0.0025;
}

// ============================================================================
// 🗂️ IN-MEMORY ROOM & CONTINUOUS SESSION STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

function closeListenerGeminiSession(participant) {
  if (!participant) return;
  const ws = participant.geminiWs;
  participant.geminiWs = null;
  participant.isSetupComplete = false;
  participant.sessionTargetLang = null;
  participant.isConnecting = false;
  participant.pendingInputQueue = [];
  if (ws) {
    try {
      ws.removeAllListeners();
      ws.close();
    } catch (e) {}
  }
}

// ============================================================================
// ⚡ OFFICIAL GOOGLE AI STUDIO GEMINI 3.5 LIVE TRANSLATE SESSION
// Spec: https://aistudio.google.com/docs/live-api/live-translate
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
    `Connecting ${GEMINI_LIVE_MODEL} (${apiVersion}) for ${listener.email}`,
    { roomId, targetLanguageCode: targetLang }
  );

  const ws = new WebSocket(wsUrl);
  listener.geminiWs = ws;
  listener.sessionTargetLang = targetLang;
  listener.isSetupComplete = false;
  listener.audioSeq = 0;

  ws.on("open", () => {
    listener.isConnecting = false;

    // Exact Setup Message from Google AI Studio Live Translate Documentation:
    // - model: "models/gemini-3.5-live-translate-preview"
    // - inputAudioTranscription: {} & outputAudioTranscription: {} for captions
    // - generationConfig.responseModalities: ["AUDIO"]
    // - generationConfig.translationConfig: { targetLanguageCode, echoTargetLanguage: false }
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
      const jsonStr = Buffer.isBuffer(rawData)
        ? rawData.toString("utf8")
        : String(rawData);
      const response = JSON.parse(jsonStr);

      // 1. Setup Complete Handshake -> Flush any queued 16kHz input chunks immediately
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

      // 2. Native Interruption Handling -> Notify client to clear obsolete playback audio immediately
      if (sc.interrupted) {
        logTranslate("INTERRUPTED", `⚡ Stream interrupted for ${listener.email}`);
        io.to(listener.socketId).emit("translated_interrupted", {
          roomId,
          timestamp: Date.now(),
        });
      }

      // 3. Incremental Input & Output Audio Transcriptions (for live captions & debugging)
      const inText = sc.inputTranscription?.text || null;
      const outText = sc.outputTranscription?.text || null;

      if (inText || outText) {
        io.to(listener.socketId).emit("translated_subtitle", {
          roomId,
          inputDelta: inText,
          outputDelta: outText,
          targetLang,
          turnComplete: Boolean(sc.turnComplete),
          timestamp: Date.now(),
        });
      }

      // 4. Immediate 24kHz PCM16 Mono Little-Endian Audio Forwarding (DO NOT wait for turnComplete!)
      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (part?.inlineData?.data) {
          const pcm24kBase64 = part.inlineData.data;
          const pcmBuf = Buffer.from(pcm24kBase64, "base64");

          // Skip 0x0000 digital silence idle frames; forward all audible speech chunks immediately!
          if (hasAudiblePcmSignal(pcmBuf)) {
            const seq = ++listener.audioSeq;
            const durationMs = Math.round((pcmBuf.length / 48000) * 1000);

            // Acoustic Echo Guard window so listener's loudspeaker doesn't feed back into partner's session
            listener.ignoreMicUntil =
              Math.max(Date.now(), listener.ignoreMicUntil || 0) +
              durationMs +
              250;

            io.to(listener.socketId).emit("translated_audio_chunk", {
              roomId,
              seq,
              pcm24kBase64,
              sampleRate: 24000,
              durationMs,
              targetLang,
              timestamp: Date.now(),
            });
          }
        }

        if (part?.text && part.text.trim()) {
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            inputDelta: null,
            outputDelta: part.text,
            targetLang,
            turnComplete: Boolean(sc.turnComplete),
            timestamp: Date.now(),
          });
        }
      }

      if (sc.turnComplete || sc.generationComplete) {
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
    // Automatic fallback from v1beta to v1alpha if endpoint returns 404
    if (apiVersion === "v1beta" && msg.includes("404")) {
      listener.useV1Alpha = true;
      listener.lastConnectAttemptAt = 0;
      ensureListenerGeminiSession(roomId, listenerKey, "v1alpha");
    }
  });

  ws.on("close", (code, reasonBuf) => {
    listener.isConnecting = false;
    const reason = reasonBuf ? reasonBuf.toString("utf8") : "";
    logTranslate(
      "WS_CLOSE",
      `Session closed (${apiVersion}) for ${listener.email} (code: ${code}, reason: ${
        reason || "none"
      })`
    );
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
// 📤 CONTINUOUS 100ms 16kHz PCM STREAMING TO GEMINI LIVE API
// Spec: { "realtimeInput": { "audio": { "data": "<b64>", "mimeType": "audio/pcm;rate=16000" } } }
// ============================================================================
function sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  const preferredVersion = listener.useV1Alpha ? "v1alpha" : "v1beta";

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    if (listener.pendingInputQueue.length < 15) {
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
  } else if (listener.pendingInputQueue.length < 15) {
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
      translatorEnabled: Boolean(translatorEnabled),
      hasMinutes: Boolean(hasMinutes),
      geminiWs: null,
      isSetupComplete: false,
      sessionTargetLang: null,
      isConnecting: false,
      useV1Alpha: false,
      lastConnectAttemptAt: 0,
      ignoreMicUntil: 0,
      audioSeq: 0,
      pendingInputQueue: [],
    };

    participant.socketId = socket.id;
    participant.speakLang = String(speakLang || participant.speakLang || "en");
    participant.listenLang = String(
      listenLang || participant.listenLang || "bn"
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

  // 2. UPDATE TRANSLATE CONFIG (Only reconnects if target listenLang changed)
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
      participant.translatorEnabled === nextEnabled &&
      participant.hasMinutes === nextMins
    ) {
      return;
    }

    const listenChanged = participant.listenLang !== nextListen;

    participant.speakLang = nextSpeak;
    participant.listenLang = nextListen;
    participant.translatorEnabled = nextEnabled;
    participant.hasMinutes = nextMins;

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

  // 3. CONTINUOUS 100ms 16kHz PCM STREAMING (Direction A -> B and Direction B -> A)
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

    // Echo Guard: Do not forward speaker's mic while their loudspeaker is actively playing translated audio
    if (Date.now() < (speaker.ignoreMicUntil || 0)) {
      return;
    }

    // Stream continuously into the receiving participant's Gemini 3.5 Live Translate session
    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;
      sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
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

  // 5. CLEANUP ON LEAVE / DISCONNECT
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