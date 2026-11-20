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
  pingTimeout: 25000,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 10000;
const GEMINI_API_KEY =
  process.env.GEMINI_API_KEY ||
  "AQ.Ab8RN6J18Lri4K34bssqzHMCnIAzIQBVQsxfGW31jGH5qerzJQ";
const GEMINI_LIVE_MODEL = "models/gemini-3.5-live-translate-preview";

// Pure digital 128ms zero-silence frame at 16kHz 16-bit mono (4096 zero bytes)
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
// 🔊 NATIVE 24,000 Hz 16-BIT MONO LITTLE-ENDIAN WAV BUILDER
// ============================================================================
function buildNative24kWav(pcm24kBuffers) {
  const pcmData = Buffer.concat(pcm24kBuffers);
  const sampleRate = 24000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 2ms soft edge fade (48 samples at 24kHz) so consecutive bursts join click-free
  const fadeSamples = Math.min(48, Math.floor(totalSamples / 16));
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
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(1, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28);
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
  const step = Math.max(1, Math.floor(samples / 140));
  let sumSq = 0;
  let count = 0;
  for (let i = 0; i < samples; i += step) {
    const val = pcmBuf.readInt16LE(i * 2) / 32768.0;
    sumSq += val * val;
    count++;
  }
  return count > 0 ? Math.sqrt(sumSq / count) : 0;
}

function cleanCaption(text, maxChars = 120) {
  let clean = String(text || "").replace(/\s+/g, " ").trim();
  if (!clean) return "";
  for (let i = 0; i < 2; i++) {
    clean = clean.replace(/\b(\S+(?:\s+\S+){0,4})\s+\1\b/gi, "$1");
  }
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

// Detects when Gemini's autoregressive decoder starts repeating the same phrase in a loop
function hasRepetitionLoop(text) {
  const words = normalizeForDedup(text).split(/\s+/).filter(Boolean);
  if (words.length < 6) return false;
  for (let len = 2; len <= 6; len++) {
    if (words.length >= len * 2) {
      const a = words.slice(words.length - len).join(" ");
      const b = words
        .slice(words.length - len * 2, words.length - len)
        .join(" ");
      if (a === b) return true;
    }
  }
  return false;
}

// ============================================================================
// 🗂️ IN-MEMORY ROOM & PERSISTENT 1-SESSION-PER-DIRECTION STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

function stopSilenceTailPacer(participant) {
  if (participant?.silencePacerTimer) {
    clearInterval(participant.silencePacerTimer);
    participant.silencePacerTimer = null;
  }
}

function closeListenerGeminiSession(participant) {
  if (!participant) return;
  stopSilenceTailPacer(participant);
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
// ⚡ ANTI-REPEAT GEMINI 3.5 LIVE TRANSLATE SESSION
// ============================================================================
function ensureListenerGeminiSession(
  roomId,
  listenerKey,
  apiVersion = "v1alpha",
  forceReset = false
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
    !forceReset &&
    listener.geminiWs &&
    listener.geminiWs.readyState === WebSocket.OPEN &&
    listener.sessionTargetLang === targetLang &&
    !listener.isConnecting
  ) {
    return;
  }

  const now = Date.now();
  if (
    !forceReset &&
    (listener.isConnecting || now - (listener.lastConnectAttemptAt || 0) < 1800)
  ) {
    return;
  }

  listener.isConnecting = true;
  listener.lastConnectAttemptAt = now;

  closeListenerGeminiSession(listener);

  const sessionId = `gs_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 6)}`;
  listener.sessionId = sessionId;

  const wsUrl = getGeminiWsUrl(apiVersion);
  logTranslate(
    "WS_CONNECTING",
    `Connecting ${GEMINI_LIVE_MODEL} (${apiVersion}) for ${listener.email} (target: ${targetLang})`,
    { roomId, sessionId, forceReset }
  );

  const ws = new WebSocket(wsUrl);
  listener.geminiWs = ws;
  listener.sessionTargetLang = targetLang;
  listener.isSetupComplete = false;
  listener.outPcm24kBuffers = [];
  listener.audioSeq = 0;

  let currentInputText = "";
  let currentOutputText = "";
  let silentOutTicks = 0;
  let isFirstBurstOfTurn = true;
  let lastTurnNormText = "";
  let lastTurnAt = 0;
  let currentTurnId = `turn_${Date.now()}`;
  let turnsCompletedInSession = 0;

  const resetStuckLoopSession = (reason) => {
    logTranslate(
      "LOOP_RESET",
      `🛑 Resetting Gemini session for ${listener.email} (${reason})`
    );
    listener.outPcm24kBuffers = [];
    currentInputText = "";
    currentOutputText = "";
    io.to(listener.socketId).emit("translated_interrupted", {
      roomId,
      sessionId,
      timestamp: Date.now(),
    });
    ensureListenerGeminiSession(roomId, listenerKey, apiVersion, true);
  };

  const flushPhraseWav = (isTurnEnd = false) => {
    if (listener.outFlushTimer) {
      clearTimeout(listener.outFlushTimer);
      listener.outFlushTimer = null;
    }

    const cleanOrig = cleanCaption(currentInputText, 120);
    const cleanTrans = cleanCaption(currentOutputText, 120);
    const normTrans = normalizeForDedup(cleanTrans);
    const sendSubtitles = listener.translateMode !== "voice_only";

    // Block exact duplicate of the previous completed turn within 7 seconds
    if (
      isFirstBurstOfTurn &&
      normTrans &&
      normTrans.length > 3 &&
      normTrans === lastTurnNormText &&
      Date.now() - lastTurnAt < 7000
    ) {
      listener.outPcm24kBuffers = [];
      if (isTurnEnd) {
        currentInputText = "";
        currentOutputText = "";
      }
      return;
    }

    if (
      listener.outPcm24kBuffers &&
      listener.outPcm24kBuffers.length > 0 &&
      listener.translateMode !== "text_only"
    ) {
      const chunks = listener.outPcm24kBuffers.splice(
        0,
        listener.outPcm24kBuffers.length
      );
      const { wavBuffer, durationMs } = buildNative24kWav(chunks);

      if (durationMs >= 100) {
        isFirstBurstOfTurn = false;
        const seq = ++listener.audioSeq;

        // Server-side safety echo shield (accounts for network transit + playback + reverb)
        listener.ignoreMicUntil =
          Math.max(Date.now(), listener.ignoreMicUntil || 0) +
          durationMs +
          900;

        logTranslate(
          "VOICE_24K_SENT",
          `🔊 Sent 24kHz WAV #${seq} (${durationMs}ms, ${chunks.length} chunks, ->${targetLang}) to ${listener.email}`,
          { text: cleanTrans || cleanOrig }
        );

        io.to(listener.socketId).emit("translated_audio_chunk", {
          roomId,
          sessionId,
          utteranceId: currentTurnId,
          seq,
          wavBase64: wavBuffer.toString("base64"),
          audioFormat: "wav",
          sampleRate: 24000,
          durationMs,
          originalText: sendSubtitles && cleanOrig ? cleanOrig : null,
          translatedText: sendSubtitles && cleanTrans ? cleanTrans : null,
          targetLang,
          timestamp: Date.now(),
        });
      }
    } else if (sendSubtitles && (cleanOrig || cleanTrans) && isTurnEnd) {
      io.to(listener.socketId).emit("translated_subtitle", {
        roomId,
        sessionId,
        utteranceId: currentTurnId,
        originalText: cleanOrig || null,
        translatedText: cleanTrans || null,
        targetLang,
        isFinal: true,
      });
    }

    if (isTurnEnd) {
      if (normTrans && normTrans.length > 3) {
        lastTurnNormText = normTrans;
        lastTurnAt = Date.now();
      }
      currentInputText = "";
      currentOutputText = "";
      silentOutTicks = 0;
      isFirstBurstOfTurn = true;
      turnsCompletedInSession += 1;
      currentTurnId = `turn_${Date.now()}_${Math.random()
        .toString(36)
        .substring(2, 5)}`;

      // Proactively refresh the WebSocket every 18 turns during idle pause so Gemini's context never bloats/loops
      if (
        turnsCompletedInSession >= 18 &&
        !listener.partnerActivelySpeaking
      ) {
        turnsCompletedInSession = 0;
        setTimeout(() => {
          if (
            listener.geminiWs === ws &&
            !listener.partnerActivelySpeaking &&
            listener.translatorEnabled
          ) {
            ensureListenerGeminiSession(roomId, listenerKey, apiVersion, true);
          }
        }, 400);
      }
    }
  };

  ws.on("open", () => {
    if (listener.geminiWs !== ws) return;
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
      if (listener.geminiWs !== ws || !listener.translatorEnabled) return;

      const jsonStr = Buffer.isBuffer(rawData)
        ? rawData.toString("utf8")
        : String(rawData);
      const response = JSON.parse(jsonStr);

      if (response.setupComplete !== undefined) {
        listener.isSetupComplete = true;
        logTranslate(
          "SETUP_COMPLETE",
          `✅ setupComplete (${apiVersion}) for ${listener.email} (target: ${targetLang})`
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
          for (const b64 of queued) {
            sendPcm16kToListenerSession(roomId, listenerKey, listener, b64);
          }
        }
        return;
      }

      const sc = response.serverContent;
      if (!sc) return;

      // 🛡️ SILENCE OUTPUT GATE:
      // Drop any hallucinated/repeated Gemini output if the partner hasn't spoken recently!
      const nowMs = Date.now();
      if (
        !listener.partnerActivelySpeaking &&
        nowMs > (listener.acceptOutputUntil || 0)
      ) {
        listener.outPcm24kBuffers = [];
        currentInputText = "";
        currentOutputText = "";
        return;
      }

      if (sc.interrupted) {
        listener.outPcm24kBuffers = [];
        currentInputText = "";
        currentOutputText = "";
        silentOutTicks = 0;
        isFirstBurstOfTurn = true;
        if (listener.outFlushTimer) {
          clearTimeout(listener.outFlushTimer);
          listener.outFlushTimer = null;
        }
        io.to(listener.socketId).emit("translated_interrupted", {
          roomId,
          sessionId,
          timestamp: nowMs,
        });
      }

      let hasNewText = false;

      if (sc.inputTranscription?.text) {
        const piece = String(sc.inputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          currentInputText = `${currentInputText} ${piece}`
            .replace(/\s+/g, " ")
            .trim();
        }
      }

      if (sc.outputTranscription?.text) {
        const piece = String(sc.outputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          currentOutputText = `${currentOutputText} ${piece}`
            .replace(/\s+/g, " ")
            .trim();
        }
      }

      // 🚨 DECODER LOOP BREAKER: If Gemini starts repeating the same phrase back-to-back, reset immediately!
      if (
        hasRepetitionLoop(currentOutputText) ||
        hasRepetitionLoop(currentInputText)
      ) {
        resetStuckLoopSession("repetition_loop_detected");
        return;
      }

      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (
          part?.inlineData?.data &&
          listener.translateMode !== "text_only"
        ) {
          const outBuf = Buffer.from(part.inlineData.data, "base64");
          const outRms = calculatePcmBufferRms(outBuf);

          if (outRms >= 0.0022) {
            silentOutTicks = 0;
            listener.outPcm24kBuffers.push(outBuf);

            // First burst: 3 chunks (~750ms); follow-up bursts: 4 chunks (~1000ms)
            const targetChunks = isFirstBurstOfTurn ? 3 : 4;
            if (listener.outPcm24kBuffers.length >= targetChunks) {
              flushPhraseWav(false);
            } else {
              clearTimeout(listener.outFlushTimer);
              listener.outFlushTimer = setTimeout(() => {
                flushPhraseWav(true);
              }, 280);
            }
          } else if (listener.outPcm24kBuffers.length > 0) {
            silentOutTicks += 1;
            if (silentOutTicks === 1) {
              listener.outPcm24kBuffers.push(outBuf);
            } else {
              flushPhraseWav(true);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          hasNewText = true;
          currentOutputText = `${currentOutputText} ${part.text.trim()}`
            .replace(/\s+/g, " ")
            .trim();
        }
      }

      if (
        hasNewText &&
        listener.translateMode !== "voice_only" &&
        (currentInputText || currentOutputText)
      ) {
        const cleanO = cleanCaption(currentInputText, 120);
        const cleanT = cleanCaption(currentOutputText, 120);
        const normT = normalizeForDedup(cleanT);

        if (
          !normT ||
          normT !== lastTurnNormText ||
          nowMs - lastTurnAt >= 7000
        ) {
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            sessionId,
            utteranceId: currentTurnId,
            originalText: cleanO || null,
            translatedText: cleanT || null,
            targetLang,
            isFinal: Boolean(sc.turnComplete),
          });
        }

        clearTimeout(listener.captionResetTimer);
        listener.captionResetTimer = setTimeout(() => {
          currentInputText = "";
          currentOutputText = "";
        }, 1800);
      }

      if (sc.turnComplete || sc.generationComplete) {
        flushPhraseWav(true);
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
    if (listener.geminiWs !== ws) return;
    listener.isConnecting = false;
    logTranslate(
      "WS_ERROR",
      `WebSocket error (${apiVersion}) for ${listener.email}: ${
        err?.message || "error"
      }`
    );
  });

  ws.on("close", (code) => {
    if (listener.geminiWs !== ws) return;
    listener.isConnecting = false;
    listener.geminiWs = null;
    listener.isSetupComplete = false;
    logTranslate(
      "WS_CLOSE",
      `Session closed (${apiVersion}, code: ${code}) for ${listener.email}`
    );
    io.to(listener.socketId).emit("gemini_session_status", {
      connected: false,
      targetLanguageCode: targetLang,
    });
  });
}

// ============================================================================
// 📤 STREAM 16kHz PCM TO LISTENER'S PERSISTENT GEMINI LIVE SESSION
// ============================================================================
function sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    if (listener.pendingInputQueue.length < 10) {
      listener.pendingInputQueue.push(pcmBase64);
    }
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
  } else if (listener.pendingInputQueue.length < 10) {
    listener.pendingInputQueue.push(pcmBase64);
  }
}

function triggerCleanDigitalSilenceTail(roomId, listenerKey, listener) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;
  stopSilenceTailPacer(listener);

  listener.partnerActivelySpeaking = false;
  listener.acceptOutputUntil = Math.max(
    listener.acceptOutputUntil || 0,
    Date.now() + 3800
  );

  let remainingFrames = 8;
  listener.silencePacerTimer = setInterval(() => {
    if (
      remainingFrames <= 0 ||
      !listener.geminiWs ||
      listener.geminiWs.readyState !== WebSocket.OPEN ||
      !listener.isSetupComplete
    ) {
      stopSilenceTailPacer(listener);
      return;
    }
    remainingFrames -= 1;
    sendPcm16kToListenerSession(
      roomId,
      listenerKey,
      listener,
      SILENT_PCM_128MS_B64
    );
  }, 95);
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
      lastConnectAttemptAt: 0,
      isClientPlayingAudio: false,
      ignoreMicUntil: 0,
      partnerActivelySpeaking: false,
      acceptOutputUntil: 0,
      lastInSeqReceived: 0,
      audioSeq: 0,
      outPcm24kBuffers: [],
      pendingInputQueue: [],
      preRollChunks: [],
      hangoverLeft: 0,
      wasSpeaking: false,
      silencePacerTimer: null,
    };

    participant.socketId = socket.id;
    participant.lastInSeqReceived = 0;
    participant.isClientPlayingAudio = false;
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
      ensureListenerGeminiSession(roomId, emailKey, "v1alpha");
    } else {
      closeListenerGeminiSession(participant);
    }
  });

  // 🔇 REAL-TIME CLIENT PLAYBACK ECHO SHIELD:
  // Phone tells server the exact millisecond its speaker starts/stops playing translated voice!
  socket.on("listener_playback_state", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const emailKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    if (!roomId || !emailKey || !rooms.has(roomId)) return;

    const participant = rooms.get(roomId).participants.get(emailKey);
    if (!participant) return;

    const playing = Boolean(payload?.isPlaying);
    participant.isClientPlayingAudio = playing;
    if (playing) {
      participant.preRollChunks = [];
      participant.hangoverLeft = 0;
      participant.wasSpeaking = false;
    } else {
      // 700ms post-playback cooldown absorbs Android's OS capture buffer + room echo
      participant.ignoreMicUntil = Math.max(
        participant.ignoreMicUntil || 0,
        Date.now() + 700
      );
    }
  });

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
      ensureListenerGeminiSession(roomId, emailKey, "v1alpha");
    } else {
      closeListenerGeminiSession(participant);
    }
  });

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
      speaker.wasSpeaking = false;
    }

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;
      triggerCleanDigitalSilenceTail(roomId, listenerKey, listener);
    }
  });

  socket.on("stream_mic_pcm_16k", (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const pcmBase64 = payload?.pcmBase64;
    const inSeq = Number(payload?.inSeq) || 0;

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

    // 1. STRICT ECHO SHIELD: Drop mic audio while speaker's device is playing translated voice or cooling down
    if (speaker.isClientPlayingAudio || now < (speaker.ignoreMicUntil || 0)) {
      speaker.preRollChunks = [];
      speaker.hangoverLeft = 0;
      speaker.wasSpeaking = false;
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

    // 2. DOMINANT-SPEAKER CROSSTALK LOCK
    if (
      room.activeSpeakerKey &&
      room.activeSpeakerKey !== speakerKey &&
      now < (room.floorLockUntil || 0)
    ) {
      if (rms < (room.activeSpeakerRms || 0.03) * 1.65) {
        speaker.preRollChunks = [];
        speaker.hangoverLeft = 0;
        speaker.wasSpeaking = false;
        return;
      }
    }

    // Threshold 0.013 catches normal human speech cleanly while rejecting room static & faint speaker bleed
    if (rms >= 0.013) {
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 450;

      speaker.hangoverLeft = 3;
      speaker.wasSpeaking = true;

      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        stopSilenceTailPacer(listener);
        listener.partnerActivelySpeaking = true;
        listener.acceptOutputUntil = now + 4500;

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
        listener.acceptOutputUntil = now + 4000;
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
      }
    } else if (speaker.wasSpeaking) {
      speaker.wasSpeaking = false;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        triggerCleanDigitalSilenceTail(roomId, listenerKey, listener);
      }
    } else {
      speaker.preRollChunks = [pcmBase64];
    }
  });

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

    listener.acceptOutputUntil = Date.now() + 4000;
    sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
  });

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