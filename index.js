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
// Injected after 250ms of speaker pause so Gemini translates immediately!
const SILENT_PCM_128MS_B64 = Buffer.alloc(4096).toString("base64");

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
// ⚡ 24kHz -> 16kHz LINEAR RESAMPLER & SMOOTH WAV BUILDER
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

function buildFastPhraseWav16k(pcm24kBuffers) {
  const resampled = pcm24kBuffers.map((b) => resample24kTo16kPcm(b));
  const pcmData = Buffer.concat(resampled);
  const sampleRate = 16000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 5ms fade-in/out (80 samples at 16kHz) so consecutive bursts join seamlessly without clicks
  const fadeSamples = Math.min(80, Math.floor(totalSamples / 10));
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
// ⚡ PROGRESSIVE FIRST-BURST GEMINI 3.5 LIVE TRANSLATE SESSION
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
  let isFirstBurstOfTurn = true;

  const flushPhraseBurstToListener = () => {
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
    const { wavBuffer, durationMs } = buildFastPhraseWav16k(chunks);

    if (durationMs < 120) return;

    isFirstBurstOfTurn = false;

    // Echo Shield: Mute listener's mic while their loudspeaker plays this burst (+ 300ms tail)
    listener.ignoreMicUntil =
      Math.max(Date.now(), listener.ignoreMicUntil || 0) + durationMs + 300;

    logTranslate(
      "INSTANT_WAV_SENT",
      `⚡ Sent 16kHz WAV (${durationMs}ms, ${chunks.length} chunks, ${sourceLangName}->${targetLangName}) to ${listener.email}`
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

    const setupMessage = {
      setup: {
        model: GEMINI_LIVE_MODEL,
        systemInstruction: {
          parts: [
            {
              text: `The speaker is speaking ${sourceLangName} (${partnerSpeakLang}). Translate directly and immediately in first-person into ${targetLangName} (${targetLang}). Never say "he is saying" or "she is saying", and never output Spanish or Russian.`,
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
      if (lastTranscriptAt > 0 && nowMs - lastTranscriptAt > 1100) {
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

          if (outRms >= 0.004) {
            listener.outPcm24kBuffers.push(outBuf);

            // PROGRESSIVE INSTANT FLUSH:
            // - First burst of a spoken turn flushes at 2 chunks (~500ms) so the voice starts speaking IMMEDIATELY!
            // - Follow-up bursts in the same sentence group 4 chunks (~1.0s) so the rest of the sentence plays smoothly!
            const targetChunks = isFirstBurstOfTurn ? 2 : 4;
            if (listener.outPcm24kBuffers.length >= targetChunks) {
              flushPhraseBurstToListener();
            } else {
              clearTimeout(listener.outAudioFlushTimer);
              listener.outAudioFlushTimer = setTimeout(() => {
                flushPhraseBurstToListener();
              }, 310);
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
        }, 1200);
      }

      if (sc.turnComplete || sc.generationComplete) {
        flushPhraseBurstToListener();
        isFirstBurstOfTurn = true;
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
// 📤 STREAM 16kHz PCM TO LISTENER'S GEMINI LIVE SESSION
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
      preRollChunks: [],
      hangoverLeft: 0,
      silencePushLeft: 0,
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

    for (const [k, p] of room.participants.entries()) {
      if (p.translatorEnabled && p.hasMinutes) {
        ensureListenerGeminiSession(roomId, k, "v1alpha");
      } else {
        closeListenerGeminiSession(p);
      }
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

    const speakChanged = participant.speakLang !== nextSpeak;
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
      ensureListenerGeminiSession(roomId, emailKey, "v1alpha");
    } else {
      closeListenerGeminiSession(participant);
    }

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

  // 3. FAST CONTINUOUS STREAM + 250ms DIGITAL SILENCE END-OF-PHRASE TRIGGER
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

    if (rms >= 0.012) {
      // Active human speech (including soft syllables)
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 420;

      speaker.hangoverLeft = 2; // Keep real audio for 250ms after speech dips
      speaker.silencePushLeft = 3; // Then push 380ms of pure digital 0x00 silence to trigger instant translation!

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
      // Send natural word ending (250ms) so last consonant is never clipped
      speaker.hangoverLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
      }
    } else if (speaker.silencePushLeft > 0) {
      // Inject pure digital 0x00 silence so Gemini's VAD translates immediately without waiting on room noise!
      speaker.silencePushLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        sendPcm16kToListenerSession(
          roomId,
          listenerKey,
          listener,
          SILENT_PCM_128MS_B64
        );
      }
    } else {
      // Keep 1 chunk (128ms) in pre-roll so the start of the next word is never missed
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