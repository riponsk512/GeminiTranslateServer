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
  pingInterval: 3000,
  pingTimeout: 6000,
  transports: ["websocket", "polling"],
});

const PORT = process.env.PORT || 10000;
const GEMINI_API_KEY =
  process.env.GEMINI_API_KEY ||
  "AQ.Ab8RN6J18Lri4K34bssqzHMCnIAzIQBVQsxfGW31jGH5qerzJQ";
const GEMINI_LIVE_MODEL = "models/gemini-3.5-live-translate-preview";

// Pure digital 128ms zero-silence frame at 16kHz 16-bit mono (4096 zero bytes)
const SILENT_PCM_128MS_B64 = Buffer.alloc(4096).toString("base64");

// Official Gemini Live API BidiGenerateContent endpoint (v1beta primary, v1alpha fallback)
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
// 🔊 NATIVE 24,000 Hz 16-BIT MONO LITTLE-ENDIAN WAV BUILDER (NO RESAMPLING!)
// Preserves 100% of Gemini's natural voice pitch, speed, and clarity
// ============================================================================
function buildNative24kWav(pcm24kBuffers) {
  const pcmData = Buffer.concat(pcm24kBuffers);
  const sampleRate = 24000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 2ms soft edge fade (48 samples at 24kHz) so audio never clicks
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
  header.writeUInt16LE(1, 20); // PCM = 1
  header.writeUInt16LE(1, 22); // Mono = 1
  header.writeUInt32LE(sampleRate, 24); // 24,000 Hz
  header.writeUInt32LE(sampleRate * 2, 28); // 48,000 bytes/sec
  header.writeUInt16LE(2, 32); // BlockAlign = 2
  header.writeUInt16LE(16, 34); // BitsPerSample = 16
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

// ============================================================================
// 🗂️ IN-MEMORY ROOM & PERSISTENT 1-SESSION-PER-DIRECTION STORE
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
// ⚡ PERSISTENT GEMINI 3.5 LIVE TRANSLATE SESSION (UNBROKEN FULL PHRASES)
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
    now - (listener.lastConnectAttemptAt || 0) < 1800
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
    { roomId, sessionId }
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
  let currentTurnId = `turn_${Date.now()}`;

  // Sends ONE unbroken 24kHz WAV for the entire spoken phrase so audio never breaks!
  const flushPhraseWav = (isTurnEnd = false) => {
    if (listener.outFlushTimer) {
      clearTimeout(listener.outFlushTimer);
      listener.outFlushTimer = null;
    }

    const cleanOrig = cleanCaption(currentInputText, 120);
    const cleanTrans = cleanCaption(currentOutputText, 120);
    const sendSubtitles = listener.translateMode !== "voice_only";

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

      if (durationMs >= 120) {
        const seq = ++listener.audioSeq;

        // Echo Shield: Block listener's mic while their loudspeaker plays this phrase (+ 300ms tail)
        listener.ignoreMicUntil =
          Math.max(Date.now(), listener.ignoreMicUntil || 0) +
          durationMs +
          300;

        logTranslate(
          "PHRASE_WAV_SENT",
          `🔊 Sent smooth 24kHz WAV #${seq} (${durationMs}ms, ${chunks.length} chunks, ->${targetLang}) to ${listener.email}`,
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
      currentInputText = "";
      currentOutputText = "";
      silentOutTicks = 0;
      currentTurnId = `turn_${Date.now()}_${Math.random()
        .toString(36)
        .substring(2, 5)}`;
    }
  };

  ws.on("open", () => {
    if (listener.geminiWs !== ws) return;
    listener.isConnecting = false;

    // Clean official Google AI Studio Live Translate setup (no conflicting prompts)
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

      if (sc.interrupted) {
        listener.outPcm24kBuffers = [];
        currentInputText = "";
        currentOutputText = "";
        silentOutTicks = 0;
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

      let hasNewText = false;

      if (sc.inputTranscription?.text) {
        const piece = String(sc.inputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          currentInputText = cleanCaption(`${currentInputText} ${piece}`, 120);
        }
      }

      if (sc.outputTranscription?.text) {
        const piece = String(sc.outputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          currentOutputText = cleanCaption(
            `${currentOutputText} ${piece}`,
            120
          );
        }
      }

      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (
          part?.inlineData?.data &&
          listener.translateMode !== "text_only"
        ) {
          const outBuf = Buffer.from(part.inlineData.data, "base64");
          const outRms = calculatePcmBufferRms(outBuf);

          if (outRms >= 0.002) {
            silentOutTicks = 0;
            listener.outPcm24kBuffers.push(outBuf);

            // Allow up to 12 chunks (~3.0s) in a single WAV so full sentences play smoothly without breaking!
            // As soon as Gemini pauses for 360ms or sends 2 silent ticks, flush immediately!
            if (listener.outPcm24kBuffers.length >= 12) {
              flushPhraseWav(false);
            } else {
              clearTimeout(listener.outFlushTimer);
              listener.outFlushTimer = setTimeout(() => {
                flushPhraseWav(true);
              }, 360);
            }
          } else if (listener.outPcm24kBuffers.length > 0) {
            silentOutTicks += 1;
            if (silentOutTicks === 1) {
              // Keep 1 brief silence chunk for natural pacing inside a sentence
              listener.outPcm24kBuffers.push(outBuf);
            } else {
              // 2 consecutive silent chunks -> phrase finished! Flush complete phrase now!
              flushPhraseWav(true);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          hasNewText = true;
          currentOutputText = cleanCaption(
            `${currentOutputText} ${part.text.trim()}`,
            120
          );
        }
      }

      if (
        hasNewText &&
        listener.translateMode !== "voice_only" &&
        (currentInputText || currentOutputText)
      ) {
        io.to(listener.socketId).emit("translated_subtitle", {
          roomId,
          sessionId,
          utteranceId: currentTurnId,
          originalText: currentInputText || null,
          translatedText: currentOutputText || null,
          targetLang,
          isFinal: Boolean(sc.turnComplete),
        });

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

  const preferredVersion = listener.useV1Alpha ? "v1alpha" : "v1beta";

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    if (listener.pendingInputQueue.length < 12) {
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
  } else if (listener.pendingInputQueue.length < 12) {
    listener.pendingInputQueue.push(pcmBase64);
  }
}

function sendSilenceEndTrigger(listener) {
  if (
    !listener?.geminiWs ||
    listener.geminiWs.readyState !== WebSocket.OPEN ||
    !listener.isSetupComplete
  ) {
    return;
  }
  try {
    // 3 frames (~384ms) of pure digital 0x00 silence cleanly tells Gemini's VAD the sentence ended
    // WITHOUT closing the continuous Live session!
    for (let i = 0; i < 3; i++) {
      listener.geminiWs.send(
        JSON.stringify({
          realtimeInput: {
            audio: {
              data: SILENT_PCM_128MS_B64,
              mimeType: "audio/pcm;rate=16000",
            },
          },
        })
      );
    }
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
      ignoreMicUntil: 0,
      lastInSeqReceived: 0,
      audioSeq: 0,
      outPcm24kBuffers: [],
      pendingInputQueue: [],
      preRollChunks: [],
      hangoverLeft: 0,
      wasSpeaking: false,
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
      sendSilenceEndTrigger(listener);
    }
  });

  // Gentle 0.008 Speech Gate: catches normal & soft voices reliably while blocking idle static!
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

    if (now < (speaker.ignoreMicUntil || 0)) {
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

    if (
      room.activeSpeakerKey &&
      room.activeSpeakerKey !== speakerKey &&
      now < (room.floorLockUntil || 0)
    ) {
      if (rms < (room.activeSpeakerRms || 0.025) * 1.55) {
        speaker.preRollChunks = [];
        speaker.hangoverLeft = 0;
        speaker.wasSpeaking = false;
        return;
      }
    }

    if (rms >= 0.008) {
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 450;

      // 5 chunks (~640ms) of hangover keeps natural mid-sentence pauses inside the same stream!
      speaker.hangoverLeft = 5;
      speaker.wasSpeaking = true;

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
    } else if (speaker.wasSpeaking) {
      speaker.wasSpeaking = false;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendSilenceEndTrigger(listener);
      }
    } else {
      // Keep 2 chunks (~256ms) of pre-roll so the first syllable is never cut off
      speaker.preRollChunks.push(pcmBase64);
      if (speaker.preRollChunks.length > 2) {
        speaker.preRollChunks.shift();
      }
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