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

// Official Gemini Live WebSocket Endpoints (v1beta & v1alpha)
const getGeminiWsUrl = (apiVersion = "v1beta") =>
  `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.${apiVersion}.GenerativeService.BidiGenerateContent?key=${encodeURIComponent(
    GEMINI_API_KEY
  )}`;

const LANGUAGE_NAMES = {
  en: "English",
  bn: "Bengali",
  hi: "Hindi",
  ur: "Urdu",
  ar: "Arabic",
  id: "Indonesian",
  pt: "Portuguese",
  es: "Spanish",
  ru: "Russian",
  ja: "Japanese",
  ko: "Korean",
  fr: "French",
  de: "German",
  tr: "Turkish",
  vi: "Vietnamese",
  th: "Thai",
  ms: "Malay",
  tl: "Filipino",
  zh: "Chinese",
  ta: "Tamil",
  te: "Telugu",
  mr: "Marathi",
  gu: "Gujarati",
  kn: "Kannada",
  ml: "Malayalam",
  pa: "Punjabi",
  ne: "Nepali",
};

function getLangName(code) {
  const clean = String(code || "en")
    .split("-")[0]
    .toLowerCase();
  return LANGUAGE_NAMES[clean] || clean;
}

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
// 🔊 WAV HEADER BUILDER (24kHz 16-BIT MONO LITTLE-ENDIAN PCM) & RMS METER
// ============================================================================
function buildWavBufferFromPcmBuffers(pcmBuffers, sampleRate = 24000) {
  const pcmData = Buffer.concat(pcmBuffers);
  const dataLen = pcmData.length;
  const header = Buffer.alloc(44);
  header.write("RIFF", 0);
  header.writeUInt32LE(36 + dataLen, 4);
  header.write("WAVE", 8);
  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM = 1
  header.writeUInt16LE(1, 22); // Mono = 1
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28); // ByteRate = sampleRate * 2
  header.writeUInt16LE(2, 32); // BlockAlign = 2
  header.writeUInt16LE(16, 34); // BitsPerSample = 16
  header.write("data", 36);
  header.writeUInt32LE(dataLen, 40);
  return Buffer.concat([header, pcmData]);
}

function calculatePcmBufferRms(pcmBuf) {
  if (!pcmBuf || pcmBuf.length < 4) return 0;
  const samples = Math.floor(pcmBuf.length / 2);
  const step = Math.max(1, Math.floor(samples / 200));
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
  if (participant.outAudioFlushTimer) {
    clearTimeout(participant.outAudioFlushTimer);
    participant.outAudioFlushTimer = null;
  }
  if (participant.keepAliveInterval) {
    clearInterval(participant.keepAliveInterval);
    participant.keepAliveInterval = null;
  }
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
// 🤖 OFFICIAL GEMINI 3.5 LIVE TRANSLATE WEBSOCKET SESSION
// Spec: https://aistudio.google.com/docs/live-api/live-translate
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
  if (
    listener.isConnecting ||
    now - (listener.lastConnectAttemptAt || 0) < 2000
  ) {
    return;
  }

  listener.isConnecting = true;
  listener.lastConnectAttemptAt = now;

  closeListenerGeminiSession(listener);

  // Find partner's speaking language to provide accurate translation context
  let speakerLangCode = "en";
  for (const [k, p] of room.participants.entries()) {
    if (k !== listenerKey && p.speakLang) {
      speakerLangCode = String(p.speakLang).split("-")[0].toLowerCase();
    }
  }

  const targetLangName = getLangName(targetLang);
  const sourceLangName = getLangName(speakerLangCode);

  const wsUrl = getGeminiWsUrl(apiVersion);
  logTranslate(
    "WS_CONNECTING",
    `Connecting ${GEMINI_LIVE_MODEL} (${apiVersion}) for listener ${listener.email}`,
    {
      roomId,
      sourceLang: speakerLangCode,
      targetLanguageCode: targetLang,
    }
  );

  const ws = new WebSocket(wsUrl);
  listener.geminiWs = ws;
  listener.sessionTargetLang = targetLang;
  listener.isSetupComplete = false;

  let currentInputText = "";
  let currentOutputText = "";
  let outPcm24kBuffers = [];

  const flushOutPcm24kToClient = () => {
    if (listener.outAudioFlushTimer) {
      clearTimeout(listener.outAudioFlushTimer);
      listener.outAudioFlushTimer = null;
    }
    if (outPcm24kBuffers.length === 0) return;

    const chunks = outPcm24kBuffers.splice(0, outPcm24kBuffers.length);
    const wavBuf = buildWavBufferFromPcmBuffers(chunks, 24000);
    const wavBase64 = wavBuf.toString("base64");

    logTranslate(
      "GEMINI_AUDIO_SENT",
      `🔊 Delivered 24kHz translated WAV (${chunks.length} chunks, ${wavBuf.length} bytes, target: ${targetLang}) to ${listener.email}`
    );

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      wavBase64,
      audioFormat: "wav",
      sampleRate: 24000,
      originalText: currentInputText || null,
      translatedText: currentOutputText || null,
      targetLang,
      timestamp: Date.now(),
    });
  };

  ws.on("open", () => {
    listener.isConnecting = false;

    // EXACT Setup JSON from https://aistudio.google.com/docs/live-api/live-translate
    // - translationConfig MUST be inside generationConfig
    // - inputAudioTranscription & outputAudioTranscription at setup root
    const setupMessage = {
      setup: {
        model: GEMINI_LIVE_MODEL,
        systemInstruction: {
          parts: [
            {
              text: `You are translating live spoken conversation from ${sourceLangName} into ${targetLangName}. Always translate into ${targetLangName}.`,
            },
          ],
        },
        inputAudioTranscription: {},
        outputAudioTranscription: {},
        generationConfig: {
          responseModalities: ["AUDIO"],
          translationConfig: {
            targetLanguageCode: targetLang,
            echoTargetLanguage: true,
          },
        },
      },
    };

    logTranslate(
      "SETUP_SENT",
      `📤 Sent official Live Translate setup for ${listener.email} (targetLanguageCode: "${targetLang}")`
    );

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

      // 1. Setup Complete Handshake
      if (response.setupComplete !== undefined) {
        listener.isSetupComplete = true;
        logTranslate(
          "SETUP_COMPLETE",
          `✅ setupComplete received for ${listener.email} (targetLanguageCode: "${targetLang}")`
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

      // 2. Input Audio Transcription (Speaker's original words)
      if (sc.inputTranscription?.text) {
        currentInputText =
          `${currentInputText} ${sc.inputTranscription.text}`.trim();
        logTranslate(
          "INPUT_TRANSCRIPT",
          `🗣️ Original (${listener.email} hears): "${currentInputText}"`
        );
      }

      // 3. Output Audio Transcription (Translated words in targetLanguageCode)
      if (sc.outputTranscription?.text) {
        currentOutputText =
          `${currentOutputText} ${sc.outputTranscription.text}`.trim();
        logTranslate(
          "OUTPUT_TRANSCRIPT",
          `🌐 Translated (${targetLang} for ${listener.email}): "${currentOutputText}"`
        );
      }

      // 4. Translated 24kHz 16-bit Mono PCM Audio Chunks
      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (part?.inlineData?.data) {
          const outBuf = Buffer.from(part.inlineData.data, "base64");
          const outRms = calculatePcmBufferRms(outBuf);

          // Discard digital silence frames (rms < 0.004)
          if (outRms >= 0.004) {
            outPcm24kBuffers.push(outBuf);

            // Flush every ~1.5 seconds of continuous speech (6 chunks of 250ms)
            if (outPcm24kBuffers.length >= 6) {
              flushOutPcm24kToClient();
            } else {
              clearTimeout(listener.outAudioFlushTimer);
              listener.outAudioFlushTimer = setTimeout(() => {
                flushOutPcm24kToClient();
              }, 320);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          currentOutputText =
            `${currentOutputText} ${part.text.trim()}`.trim();
        }
      }

      // Emit real-time subtitles to listener
      if (currentInputText || currentOutputText) {
        io.to(listener.socketId).emit("translated_subtitle", {
          roomId,
          originalText: currentInputText || null,
          translatedText: currentOutputText || null,
          targetLang,
          isFinal: Boolean(sc.turnComplete),
        });
      }

      // 5. Turn Complete
      if (sc.turnComplete || sc.generationComplete) {
        flushOutPcm24kToClient();
        const finalOrig = currentInputText;
        const finalTrans = currentOutputText;
        currentInputText = "";
        currentOutputText = "";

        if (finalOrig || finalTrans) {
          io.to(listener.socketId).emit("translated_turn_complete", {
            roomId,
            originalText: finalOrig || null,
            translatedText: finalTrans || null,
            targetLang,
            timestamp: Date.now(),
          });
        }
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
    listener.isConnecting = false;
    logTranslate(
      "WS_ERROR",
      `WebSocket error (${apiVersion}) for ${listener.email}: ${
        err?.message || "error"
      }`
    );
    // If v1beta returned 404, immediately fall back to v1alpha
    if (
      apiVersion === "v1beta" &&
      String(err?.message || "").includes("404")
    ) {
      listener.lastConnectAttemptAt = 0;
      ensureListenerGeminiSession(roomId, listenerKey, "v1alpha");
    }
  });

  ws.on("close", (code, reasonBuffer) => {
    listener.isConnecting = false;
    const reasonStr = reasonBuffer ? reasonBuffer.toString("utf8") : "";
    logTranslate(
      "WS_CLOSE",
      `WebSocket closed (${apiVersion}) for ${listener.email} (code: ${code}, reason: ${
        reasonStr || "none"
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
// 📤 SEND 16kHz PCM CHUNK TO LISTENER'S GEMINI LIVE SESSION
// Spec: { "realtimeInput": { "audio": { "data": b64, "mimeType": "audio/pcm;rate=16000" } } }
// ============================================================================
function sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  if (
    !listener.geminiWs ||
    listener.geminiWs.readyState > WebSocket.OPEN
  ) {
    ensureListenerGeminiSession(roomId, listenerKey, "v1alpha");
    return;
  }

  if (
    listener.geminiWs.readyState === WebSocket.OPEN &&
    listener.isSetupComplete
  ) {
    const audioMessage = {
      realtimeInput: {
        audio: {
          data: pcmBase64,
          mimeType: "audio/pcm;rate=16000",
        },
      },
    };
    try {
      listener.geminiWs.send(JSON.stringify(audioMessage));
    } catch (e) {}
  }
}

function sendAudioStreamEndToListenerSession(listener) {
  if (
    listener?.geminiWs &&
    listener.geminiWs.readyState === WebSocket.OPEN &&
    listener.isSetupComplete
  ) {
    try {
      listener.geminiWs.send(
        JSON.stringify({
          realtimeInput: {
            audioStreamEnd: true,
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

  socket.on("client_audio_diag", (diag) => {
    logTranslate("CLIENT_DIAG", `📱 Device audio status:`, diag);
  });

  socket.on("client_playback_log", (info) => {
    logTranslate("CLIENT_PLAYBACK", `🔊 Phone playback status:`, info);
  });

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
      rooms.set(roomId, { participants: new Map() });
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
      pcmChunkCounter: 0,
      preRollChunks: [],
      hangoverChunksLeft: 0,
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

    logTranslate("JOIN_ROOM", `User ${email} synced in room ${roomId}`, {
      speakLang: participant.speakLang,
      listenLang: participant.listenLang,
      translatorEnabled: participant.translatorEnabled,
      hasMinutes: participant.hasMinutes,
    });

    broadcastRoomTranslateState(roomId);

    if (participant.translatorEnabled && participant.hasMinutes) {
      ensureListenerGeminiSession(roomId, emailKey, "v1alpha");
    } else {
      closeListenerGeminiSession(participant);
    }
  });

  // 2. UPDATE TRANSLATE CONFIG (Reconnects session if target listenLang changed)
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

    logTranslate(
      "CONFIG_UPDATED",
      `Updated config for ${participant.email} in room ${roomId}`,
      {
        speakLang: participant.speakLang,
        listenLang: participant.listenLang,
        translatorEnabled: participant.translatorEnabled,
        hasMinutes: participant.hasMinutes,
      }
    );

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

  // 3. STREAM SPEAKER'S 16kHz PCM MICROPHONE AUDIO (WITH HYBRID VAD GATE)
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

    let pcmBuf = null;
    let rms = 0;
    try {
      pcmBuf = Buffer.from(pcmBase64, "base64");
      rms = calculatePcmBufferRms(pcmBuf);
    } catch (e) {
      return;
    }

    speaker.pcmChunkCounter = (speaker.pcmChunkCounter || 0) + 1;
    if (speaker.pcmChunkCounter % 40 === 1 || rms >= 0.02) {
      logTranslate(
        "MIC_PCM_IN",
        `🎙️ 16kHz PCM from ${speaker.email} (chunk #${
          speaker.pcmChunkCounter
        }, rms: ${rms.toFixed(4)})`
      );
    }

    // Hybrid VAD Gate:
    // - When rms >= 0.012 (human speech), stream pre-roll + active speech chunks + 8 hangover chunks (800ms tail).
    // - When speaker is silent, send audioStreamEnd: true once and stop feeding room static to Gemini!
    const isVoiceActive = rms >= 0.012;

    if (isVoiceActive) {
      speaker.hangoverChunksLeft = 8; // Keep streaming for 800ms after voice dips
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        if (!speaker.wasStreamingSpeech && speaker.preRollChunks.length > 0) {
          for (const preB64 of speaker.preRollChunks) {
            sendPcm16kToListenerSession(roomId, listenerKey, listener, preB64);
          }
        }
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
      }
      speaker.preRollChunks = [];
      speaker.wasStreamingSpeech = true;
    } else if (speaker.hangoverChunksLeft > 0) {
      speaker.hangoverChunksLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);

        if (speaker.hangoverChunksLeft === 0) {
          sendAudioStreamEndToListenerSession(listener);
        }
      }
      if (speaker.hangoverChunksLeft === 0) {
        speaker.wasStreamingSpeech = false;
      }
    } else {
      // Keep last 3 chunks (300ms) in pre-roll ring buffer so start of next word is never cut off
      speaker.preRollChunks.push(pcmBase64);
      if (speaker.preRollChunks.length > 3) {
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