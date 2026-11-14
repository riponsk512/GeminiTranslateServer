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

const getGeminiWsUrl = (apiVersion = "v1alpha") =>
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
  return LANGUAGE_NAMES[clean] || clean.toUpperCase();
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
// 🔊 24kHz -> 16kHz CLEAN WAV BUILDER (CRISP VOICE + 33% SMALLER PAYLOAD)
// ============================================================================
function resample24kTo16kPcm(pcm24kBuf) {
  const inSamples = Math.floor(pcm24kBuf.length / 2);
  const outSamples = Math.floor((inSamples * 2) / 3);
  if (outSamples <= 0) return Buffer.alloc(0);

  const outBuf = Buffer.allocUnsafe(outSamples * 2);
  for (let i = 0; i < outSamples; i++) {
    const srcPos = i * 1.5;
    const idx0 = Math.floor(srcPos);
    const frac = srcPos - idx0;
    const idx1 = Math.min(inSamples - 1, idx0 + 1);
    const s0 = pcm24kBuf.readInt16LE(idx0 * 2);
    const s1 = pcm24kBuf.readInt16LE(idx1 * 2);
    const sample = Math.round(s0 + (s1 - s0) * frac);
    outBuf.writeInt16LE(sample, i * 2);
  }
  return outBuf;
}

function buildCompletePhraseWav16k(pcm24kBuffers) {
  const resampled = pcm24kBuffers.map((b) => resample24kTo16kPcm(b));
  const pcmData = Buffer.concat(resampled);
  const sampleRate = 16000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 6ms fade-in/out (96 samples at 16kHz) so audio never clicks
  const fadeSamples = Math.min(96, Math.floor(totalSamples / 10));
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
  header.writeUInt32LE(sampleRate * 2, 28); // 32,000 bytes/sec
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
  const step = Math.max(1, Math.floor(samples / 150));
  let sumSq = 0;
  let count = 0;
  for (let i = 0; i < samples; i += step) {
    const val = pcmBuf.readInt16LE(i * 2) / 32768.0;
    sumSq += val * val;
    count++;
  }
  return count > 0 ? Math.sqrt(sumSq / count) : 0;
}

function trimToLatestSentence(text, maxChars = 85) {
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
  participant.sessionSourceLang = null;
  participant.isConnecting = false;
  if (ws) {
    try {
      ws.removeAllListeners();
      ws.close();
    } catch (e) {}
  }
}

// ============================================================================
// ⚡ AI-STUDIO CONTINUOUS STREAM GEMINI 3.5 LIVE TRANSLATE SESSION
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

  // Identify partner's speaking language so Gemini never guesses Spanish/Russian!
  let partnerSpeakLang = "en";
  for (const [k, p] of room.participants.entries()) {
    if (k !== listenerKey && p.speakLang) {
      partnerSpeakLang = String(p.speakLang).split("-")[0].toLowerCase();
    }
  }

  if (
    listener.geminiWs &&
    listener.geminiWs.readyState === WebSocket.OPEN &&
    listener.sessionTargetLang === targetLang &&
    listener.sessionSourceLang === partnerSpeakLang &&
    !listener.isConnecting
  ) {
    return;
  }

  const now = Date.now();
  if (
    listener.isConnecting ||
    now - (listener.lastConnectAttemptAt || 0) < 2500
  ) {
    return;
  }

  listener.isConnecting = true;
  listener.lastConnectAttemptAt = now;

  closeListenerGeminiSession(listener);

  const sourceLangName = getLangName(partnerSpeakLang);
  const targetLangName = getLangName(targetLang);

  const wsUrl = getGeminiWsUrl(apiVersion);
  logTranslate(
    "WS_CONNECTING",
    `Connecting ${GEMINI_LIVE_MODEL} for ${listener.email} (${sourceLangName} -> ${targetLangName})`,
    { roomId, sourceLang: partnerSpeakLang, targetLanguageCode: targetLang }
  );

  const ws = new WebSocket(wsUrl);
  listener.geminiWs = ws;
  listener.sessionTargetLang = targetLang;
  listener.sessionSourceLang = partnerSpeakLang;
  listener.isSetupComplete = false;
  listener.outPcm24kBuffers = [];

  let currentInputText = "";
  let currentOutputText = "";
  let lastTranscriptAt = 0;

  // CRITICAL: Gemini sends one 250ms chunk every 250ms.
  // We flush only when a full phrase finishes (turnComplete OR 420ms after the last chunk)
  // or after 6 chunks (1.5 seconds of continuous speech), so sentences are NEVER chopped into 250ms pieces!
  const flushCompletePhraseWavToListener = () => {
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
    const { wavBuffer, durationMs } = buildCompletePhraseWav16k(chunks);

    if (durationMs < 150) return;

    // Echo Shield: Mute listener's mic while their loudspeaker plays this phrase (+ 350ms tail)
    listener.ignoreMicUntil =
      Math.max(Date.now(), listener.ignoreMicUntil || 0) + durationMs + 350;

    logTranslate(
      "PHRASE_WAV_SENT",
      `🔊 Sent complete phrase WAV (${durationMs}ms, ${chunks.length} chunks, ${sourceLangName}->${targetLangName}) to ${listener.email}`
    );

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      wavBase64: wavBuffer.toString("base64"),
      audioFormat: "wav",
      sampleRate: 16000,
      durationMs,
      originalText: currentInputText || null,
      translatedText: currentOutputText || null,
      targetLang,
      timestamp: Date.now(),
    });
  };

  ws.on("open", () => {
    listener.isConnecting = false;

    // Official Setup JSON with strict first-person source->target language lock
    // (Prevents Spanish "Bueno" hallucinations AND prevents "He's saying..." narration!)
    const setupMessage = {
      setup: {
        model: GEMINI_LIVE_MODEL,
        systemInstruction: {
          parts: [
            {
              text: `The speaker is speaking ${sourceLangName} (${partnerSpeakLang}). Translate directly in first-person into ${targetLangName} (${targetLang}). Never say "he is saying" or "she is saying", and never output Spanish or Russian.`,
            },
          ],
        },
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
          `✅ setupComplete for ${listener.email} (${partnerSpeakLang} -> ${targetLang})`
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
      if (lastTranscriptAt > 0 && nowMs - lastTranscriptAt > 1200) {
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

          // Filter out Gemini's 0.0000 idle silence frames
          if (outRms >= 0.004) {
            listener.outPcm24kBuffers.push(outBuf);

            // Because Gemini sends chunks 250ms apart:
            // - Wait up to 6 chunks (1.5 seconds of speech) OR 420ms after the last chunk arrives!
            // - This guarantees full sentences stay together instead of splitting into 250ms pieces!
            if (listener.outPcm24kBuffers.length >= 6) {
              flushCompletePhraseWavToListener();
            } else {
              clearTimeout(listener.outAudioFlushTimer);
              listener.outAudioFlushTimer = setTimeout(() => {
                flushCompletePhraseWavToListener();
              }, 420);
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
        }, 1400);
      }

      if (sc.turnComplete || sc.generationComplete) {
        flushCompletePhraseWavToListener();
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
// 📤 STREAM CONTINUOUS 16kHz PCM TO LISTENER'S GEMINI LIVE SESSION
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
      sessionSourceLang: null,
      isConnecting: false,
      lastConnectAttemptAt: 0,
      ignoreMicUntil: 0,
      outPcm24kBuffers: [],
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

    // Ensure sessions for all enabled listeners in this room (so sourceLang is always synced!)
    for (const [k, p] of room.participants.entries()) {
      if (p.translatorEnabled && p.hasMinutes) {
        ensureListenerGeminiSession(roomId, k, "v1alpha");
      } else {
        closeListenerGeminiSession(p);
      }
    }
  });

  // 2. UPDATE TRANSLATE CONFIG (Deduplicated so it never spams!)
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

    // Ignore duplicate config updates with identical values
    if (
      participant.speakLang === nextSpeak &&
      participant.listenLang === nextListen &&
      participant.translatorEnabled === nextEnabled &&
      participant.hasMinutes === nextMins
    ) {
      return;
    }

    const speakChanged = participant.speakLang !== nextSpeak;
    const listenChanged = participant.listenLang !== nextListen;

    participant.speakLang = nextSpeak;
    participant.listenLang = nextListen;
    participant.translatorEnabled = nextEnabled;
    participant.hasMinutes = nextMins;

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
      if (listenChanged) {
        closeListenerGeminiSession(participant);
        participant.lastConnectAttemptAt = 0;
      }
      ensureListenerGeminiSession(roomId, emailKey, "v1alpha");
    } else {
      closeListenerGeminiSession(participant);
    }

    // If this user changed their speaking language, refresh partner's session so partner's Gemini knows the new source language!
    if (speakChanged) {
      for (const [otherKey, otherP] of room.participants.entries()) {
        if (
          otherKey !== emailKey &&
          otherP.translatorEnabled &&
          otherP.hasMinutes
        ) {
          closeListenerGeminiSession(otherP);
          otherP.lastConnectAttemptAt = 0;
          ensureListenerGeminiSession(roomId, otherKey, "v1alpha");
        }
      }
    }
  });

  // 3. CONTINUOUS AI-STUDIO AUDIO STREAMING (NO CHOPPING WORDS!)
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

    // 2. DOMINANT-SPEAKER FLOOR LOCK (Blocks partner's mic from echoing your own voice back to you!)
    if (
      room.activeSpeakerKey &&
      room.activeSpeakerKey !== speakerKey &&
      now < (room.floorLockUntil || 0)
    ) {
      if (rms < (room.activeSpeakerRms || 0.03) * 1.6) {
        return;
      }
    }

    if (rms >= 0.015) {
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 450;
    }

    // Stream continuously into listener's Gemini Live session (Just like Google AI Studio!)
    // Gemini's built-in neural VAD handles speech detection naturally without clipping words!
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
