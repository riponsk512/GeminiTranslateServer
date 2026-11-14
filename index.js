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
  pingInterval: 4000,
  pingTimeout: 8000,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 10000;
const GEMINI_API_KEY =
  process.env.GEMINI_API_KEY ||
  "AQ.Ab8RN6J18Lri4K34bssqzHMCnIAzIQBVQsxfGW31jGH5qerzJQ";
const GEMINI_LIVE_MODEL = "models/gemini-3.5-live-translate-preview";

// Pure digital 128ms zero-silence frame at 16kHz 16-bit mono (4096 zero bytes)
// Feeding pure 0x00 silence right after speech makes Gemini's VAD translate immediately!
const SILENT_PCM_128MS_B64 = Buffer.alloc(4096).toString("base64");

const getGeminiWsUrl = (apiVersion = "v1alpha") =>
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
// ⚡ 24kHz -> 8kHz FAST VOIP DOWNSAMPLER (3x SMALLER PAYLOAD = ZERO VIDEO LAG!)
// ============================================================================
function downsample24kTo8kPcm(pcm24kBuf) {
  const inSamples = Math.floor(pcm24kBuf.length / 2);
  const outSamples = Math.floor(inSamples / 3);
  if (outSamples <= 0) return Buffer.alloc(0);

  const outBuf = Buffer.allocUnsafe(outSamples * 2);
  for (let i = 0; i < outSamples; i++) {
    const idx = i * 6; // 3 samples * 2 bytes
    const s0 = pcm24kBuf.readInt16LE(idx);
    const s1 = idx + 2 < pcm24kBuf.length ? pcm24kBuf.readInt16LE(idx + 2) : s0;
    const s2 = idx + 4 < pcm24kBuf.length ? pcm24kBuf.readInt16LE(idx + 4) : s1;
    const avg = Math.max(
      -32768,
      Math.min(32767, Math.round((s0 + s1 + s2) / 3))
    );
    outBuf.writeInt16LE(avg, i * 2);
  }
  return outBuf;
}

function buildFast8kWavFrom24kBuffers(pcm24kBuffers) {
  const downsampledChunks = pcm24kBuffers.map((b) => downsample24kTo8kPcm(b));
  const pcmData = Buffer.concat(downsampledChunks);
  const sampleRate = 8000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 5ms anti-pop fade-in/out (40 samples at 8kHz)
  const fadeSamples = Math.min(40, Math.floor(totalSamples / 8));
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
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28); // 16,000 bytes/sec
  header.writeUInt16LE(2, 32);
  header.writeUInt16LE(16, 34);
  header.write("data", 36);
  header.writeUInt32LE(dataLen, 40);

  return {
    wavBuffer: Buffer.concat([header, pcmData]),
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

function trimToLatestSentence(text, maxChars = 75) {
  const clean = String(text || "").replace(/\s+/g, " ").trim();
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

function closeListenerGeminiSession(participant) {
  if (!participant) return;
  if (participant.outAudioFlushTimer) {
    clearTimeout(participant.outAudioFlushTimer);
    participant.outAudioFlushTimer = null;
  }
  if (participant.transcriptResetTimer) {
    clearTimeout(participant.transcriptResetTimer);
    participant.transcriptResetTimer = null;
  }
  participant.outPcm24kBuffers = [];
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
// ⚡ OFFICIAL GOOGLE AI STUDIO LIVE TRANSLATE WEBSOCKET (ZERO 400/429 ERRORS)
// ============================================================================
function ensureListenerGeminiSession(
  roomId,
  listenerKey,
  apiVersion = "v1alpha"
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
  // Prevent 429 TooManyRequests: enforce minimum 3s cooldown between connection attempts
  if (
    listener.isConnecting ||
    now - (listener.lastConnectAttemptAt || 0) < 3000
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
  listener.outPcm24kBuffers = [];

  let currentInputText = "";
  let currentOutputText = "";
  let lastTranscriptAt = 0;

  const flushFast8kAudioToListener = () => {
    if (listener.outAudioFlushTimer) {
      clearTimeout(listener.outAudioFlushTimer);
      listener.outAudioFlushTimer = null;
    }
    if (!listener.outPcm24kBuffers || listener.outPcm24kBuffers.length === 0) {
      return;
    }

    const chunks = listener.outPcm24kBuffers.splice(
      0,
      listener.outPcm24kBuffers.length
    );
    const { wavBuffer, durationMs } = buildFast8kWavFrom24kBuffers(chunks);

    if (durationMs < 80) return;

    // Echo Shield: Block listener's mic on server while their speaker plays this audio (+ 400ms)
    listener.ignoreMicUntil =
      Math.max(Date.now(), listener.ignoreMicUntil || 0) + durationMs + 400;

    logTranslate(
      "FAST_8K_OUT",
      `⚡ Sent 8kHz WAV (${durationMs}ms, ${wavBuffer.length}B, -> ${targetLang}) to ${listener.email}`
    );

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      wavBase64: wavBuffer.toString("base64"),
      audioFormat: "wav",
      sampleRate: 8000,
      durationMs,
      originalText: currentInputText || null,
      translatedText: currentOutputText || null,
      targetLang,
      timestamp: Date.now(),
    });
  };

  ws.on("open", () => {
    listener.isConnecting = false;

    // Exact, minimal Google AI Studio Setup JSON (Zero 400 BadRequest errors!)
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

      if (response.setupComplete !== undefined) {
        listener.isSetupComplete = true;
        logTranslate(
          "SETUP_COMPLETE",
          `✅ setupComplete for ${listener.email} (-> "${targetLang}")`
        );
        io.to(listener.socketId).emit("gemini_session_status", {
          connected: true,
          mode: apiVersion,
          targetLanguageCode: targetLang,
        });
        return;
      }

      const sc = response.serverContent;
      if (!sc) return;

      const nowMs = Date.now();
      if (lastTranscriptAt > 0 && nowMs - lastTranscriptAt > 950) {
        currentInputText = "";
        currentOutputText = "";
      }

      let hasNewText = false;

      if (sc.inputTranscription?.text) {
        lastTranscriptAt = nowMs;
        hasNewText = true;
        currentInputText = trimToLatestSentence(
          `${currentInputText} ${sc.inputTranscription.text}`
        );
      }

      if (sc.outputTranscription?.text) {
        lastTranscriptAt = nowMs;
        hasNewText = true;
        currentOutputText = trimToLatestSentence(
          `${currentOutputText} ${sc.outputTranscription.text}`
        );
      }

      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (part?.inlineData?.data) {
          const outBuf = Buffer.from(part.inlineData.data, "base64");
          const outRms = calculatePcmBufferRms(outBuf);

          // Ignore Gemini's 0.0000 idle silence frames
          if (outRms >= 0.004) {
            listener.outPcm24kBuffers.push(outBuf);

            // Flush every 2 chunks (~500ms) or after 85ms burst gap for instant playback!
            if (listener.outPcm24kBuffers.length >= 2) {
              flushFast8kAudioToListener();
            } else {
              clearTimeout(listener.outAudioFlushTimer);
              listener.outAudioFlushTimer = setTimeout(() => {
                flushFast8kAudioToListener();
              }, 85);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          lastTranscriptAt = nowMs;
          hasNewText = true;
          currentOutputText = trimToLatestSentence(
            `${currentOutputText} ${part.text.trim()}`
          );
        }
      }

      if (hasNewText && (currentInputText || currentOutputText)) {
        io.to(listener.socketId).emit("translated_subtitle", {
          roomId,
          originalText: currentInputText || null,
          translatedText: currentOutputText || null,
          targetLang,
          isFinal: Boolean(sc.turnComplete),
        });

        clearTimeout(listener.transcriptResetTimer);
        listener.transcriptResetTimer = setTimeout(() => {
          currentInputText = "";
          currentOutputText = "";
        }, 1000);
      }

      if (sc.turnComplete || sc.generationComplete) {
        flushFast8kAudioToListener();
        currentInputText = "";
        currentOutputText = "";
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
    listener.isConnecting = false;
    logTranslate(
      "WS_ERROR",
      `WebSocket error for ${listener.email}: ${err?.message || "error"}`
    );
  });

  ws.on("close", (code, reasonBuffer) => {
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
// 📤 STREAM 16kHz PCM CHUNK TO LISTENER'S GEMINI LIVE SESSION
// ============================================================================
function sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    ensureListenerGeminiSession(roomId, listenerKey, "v1alpha");
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
      translatorEnabled: Boolean(translatorEnabled),
      hasMinutes: Boolean(hasMinutes),
      geminiWs: null,
      isSetupComplete: false,
      sessionTargetLang: null,
      isConnecting: false,
      lastConnectAttemptAt: 0,
      ignoreMicUntil: 0,
      outPcm24kBuffers: [],
      preRollChunks: [],
      silenceTailLeft: 0,
      wasStreamingSpeech: false,
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
      ensureListenerGeminiSession(roomId, emailKey, "v1alpha");
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

    const prevListenLang = participant.listenLang;

    if (payload.speakLang !== undefined) {
      participant.speakLang = String(payload.speakLang);
    }
    if (payload.listenLang !== undefined) {
      participant.listenLang = String(payload.listenLang);
    }
    if (payload.translatorEnabled !== undefined) {
      participant.translatorEnabled = Boolean(payload.translatorEnabled);
    }
    if (payload.hasMinutes !== undefined) {
      participant.hasMinutes = Boolean(payload.hasMinutes);
    }

    broadcastRoomTranslateState(roomId);

    if (participant.translatorEnabled && participant.hasMinutes) {
      if (prevListenLang !== participant.listenLang) {
        closeListenerGeminiSession(participant);
      }
      participant.lastConnectAttemptAt = 0;
      ensureListenerGeminiSession(roomId, emailKey, "v1alpha");
    } else {
      closeListenerGeminiSession(participant);
    }
  });

  // 3. STREAM SPEAKER'S 16kHz PCM MICROPHONE AUDIO
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

    // 1. ECHO SHIELD: Ignore speaker's mic while their phone is playing translated audio
    if (now < (speaker.ignoreMicUntil || 0)) {
      speaker.preRollChunks = [];
      speaker.silenceTailLeft = 0;
      speaker.wasStreamingSpeech = false;
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

    const isVoiceActive = rms >= 0.018;

    // 2. DOMINANT-SPEAKER FLOOR LOCK (Blocks partner's mic from echoing your own voice back to you!)
    if (
      room.activeSpeakerKey &&
      room.activeSpeakerKey !== speakerKey &&
      now < (room.floorLockUntil || 0)
    ) {
      if (rms < (room.activeSpeakerRms || 0.04) * 1.7) {
        speaker.preRollChunks = [];
        speaker.silenceTailLeft = 0;
        speaker.wasStreamingSpeech = false;
        return;
      }
    }

    if (isVoiceActive) {
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 500;

      // Stream 4 pure digital zero-silence frames (~500ms) right after speech stops
      // (NO audioStreamEnd so Gemini's stream never resets or stalls!)
      speaker.silenceTailLeft = 4;

      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        // If speaker started a new phrase after a pause, clear any unplayed old output buffers on server!
        if (!speaker.wasStreamingSpeech) {
          listener.outPcm24kBuffers = [];
          if (speaker.preRollChunks.length > 0) {
            for (const preB64 of speaker.preRollChunks) {
              sendPcm16kToListenerSession(roomId, listenerKey, listener, preB64);
            }
          }
        }
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
      }
      speaker.preRollChunks = [];
      speaker.wasStreamingSpeech = true;
    } else if (speaker.silenceTailLeft > 0) {
      speaker.silenceTailLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        // Feed pure digital 0x00 silence frames so Gemini's VAD triggers translation immediately!
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          SILENT_PCM_128MS_B64
        );
      }
      if (speaker.silenceTailLeft === 0) {
        speaker.wasStreamingSpeech = false;
      }
    } else {
      speaker.preRollChunks.push(pcmBase64);
      if (speaker.preRollChunks.length > 1) {
        speaker.preRollChunks.shift();
      }
      speaker.wasStreamingSpeech = false;
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