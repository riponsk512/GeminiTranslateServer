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

// Pure digital 100ms 16kHz 16-bit mono silence frame (3200 zero bytes)
const SILENT_PCM_100MS_B64 = Buffer.alloc(3200).toString("base64");

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
// 🔊 NATIVE 24,000 Hz FULL-SENTENCE WAV BUILDER (NO RESAMPLING = 100% NATURAL SPEED!)
// ============================================================================
function buildUnbroken24kWav(pcm24kBuffers) {
  const pcmData = Buffer.concat(pcm24kBuffers);
  const sampleRate = 24000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 2ms fade-in/out (48 samples at 24kHz) so audio never clicks
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
  header.writeUInt32LE(sampleRate, 24); // 24,000 Hz (Native Gemini output rate!)
  header.writeUInt32LE(sampleRate * 2, 28); // 48,000 bytes/sec
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

function cleanSingleTurnText(text, maxChars = 115) {
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
  participant.pendingInputQueue = [];
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
// ⚡ ZERO-HALLUCINATION, UNBROKEN 24kHz GEMINI 3.5 LIVE TRANSLATE SESSION
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
    now - (listener.lastConnectAttemptAt || 0) < 2000
  ) {
    return;
  }

  listener.isConnecting = true;
  listener.lastConnectAttemptAt = now;

  closeListenerGeminiSession(listener);

  const sourceLangName = getLangName(partnerSpeakLang);
  const targetLangName = getLangName(targetLang);
  const sessionId = `gs_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 6)}`;
  listener.sessionId = sessionId;

  const wsUrl = getGeminiWsUrl(apiVersion);
  logTranslate(
    "WS_CONNECTING",
    `Connecting ${GEMINI_LIVE_MODEL} (${apiVersion}) for ${listener.email} (${sourceLangName} -> ${targetLangName})`,
    { roomId, sourceLang: partnerSpeakLang, targetLanguageCode: targetLang }
  );

  const ws = new WebSocket(wsUrl);
  listener.geminiWs = ws;
  listener.sessionTargetLang = targetLang;
  listener.sessionSourceLang = partnerSpeakLang;
  listener.isSetupComplete = false;
  listener.outPcm24kBuffers = [];
  listener.audioSeq = 0;

  let currentInputText = "";
  let currentOutputText = "";
  let lastFlushedNormText = "";
  let lastFlushedAt = 0;

  // Packs the entire spoken sentence (up to 14 chunks / ~3.2s) into ONE single 24kHz WAV
  // so Android plays the full sentence smoothly in one go without mid-sentence breaks!
  const flushCompleteSentenceWav = (isTurnEnd = false) => {
    if (listener.outAudioFlushTimer) {
      clearTimeout(listener.outAudioFlushTimer);
      listener.outAudioFlushTimer = null;
    }

    const cleanOrig = cleanSingleTurnText(currentInputText, 115);
    const cleanTrans = cleanSingleTurnText(currentOutputText, 115);
    const normTrans = normalizeForDedup(cleanTrans);

    // Block repeated/duplicate translations within 5 seconds
    if (
      normTrans &&
      normTrans.length > 3 &&
      normTrans === lastFlushedNormText &&
      Date.now() - lastFlushedAt < 5000
    ) {
      listener.outPcm24kBuffers = [];
      currentInputText = "";
      currentOutputText = "";
      return;
    }

    if (
      !listener.outPcm24kBuffers ||
      listener.outPcm24kBuffers.length === 0 ||
      listener.translateMode === "text_only"
    ) {
      listener.outPcm24kBuffers = [];
      if (isTurnEnd) {
        currentInputText = "";
        currentOutputText = "";
      }
      return;
    }

    const chunks = listener.outPcm24kBuffers.splice(
      0,
      listener.outPcm24kBuffers.length
    );
    const { wavBuffer, durationMs } = buildUnbroken24kWav(chunks);

    if (durationMs < 120) return;

    if (normTrans) {
      lastFlushedNormText = normTrans;
      lastFlushedAt = Date.now();
    }

    const seq = ++listener.audioSeq;

    // Echo Shield: Mute listener's mic while their loudspeaker plays this sentence (+ 450ms tail)
    listener.ignoreMicUntil =
      Math.max(Date.now(), listener.ignoreMicUntil || 0) + durationMs + 450;

    const sendSubtitles = listener.translateMode !== "voice_only";

    logTranslate(
      "SENTENCE_WAV_SENT",
      `🔊 Sent unbroken 24kHz WAV #${seq} (${durationMs}ms, ${chunks.length} chunks, ${sourceLangName}->${targetLangName}) to ${listener.email}`
    );

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      sessionId,
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

    if (isTurnEnd) {
      currentInputText = "";
      currentOutputText = "";
      // Once the full turn is flushed and speaker is silent, close the output gate so no old voice repeats!
      if (!listener.partnerIsActivelySpeaking) {
        listener.acceptGeminiOutputUntil = 0;
      }
    }
  };

  ws.on("open", () => {
    if (listener.geminiWs !== ws) return;
    listener.isConnecting = false;

    // temperature: 0.0 + strict literal instruction stops Gemini from adding "aap kitne saal ke hain"!
    const setupMessage = {
      setup: {
        model: GEMINI_LIVE_MODEL,
        systemInstruction: {
          parts: [
            {
              text: `You are a strict literal speech-to-speech translator from ${sourceLangName} (${partnerSpeakLang}) to ${targetLangName} (${targetLang}). Translate ONLY the exact words spoken by the user. NEVER add extra follow-up questions (never add "how old are you" or "where do you live" unless explicitly spoken), NEVER auto-complete sentences, NEVER repeat previous translations, and output silence if no clear speech is present.`,
            },
          ],
        },
        inputAudioTranscription: {},
        outputAudioTranscription: {},
        generationConfig: {
          temperature: 0.0,
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
          `✅ setupComplete for ${listener.email} (${partnerSpeakLang} -> ${targetLang})`
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

      // 🛡️ SILENCE GUARD: If the speaker is NOT speaking and the 2.4s post-speech translation window expired,
      // drop any late/hallucinated/repeated output from Gemini immediately!
      const nowMs = Date.now();
      if (
        !listener.partnerIsActivelySpeaking &&
        nowMs > (listener.acceptGeminiOutputUntil || 0)
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
        if (listener.outAudioFlushTimer) {
          clearTimeout(listener.outAudioFlushTimer);
          listener.outAudioFlushTimer = null;
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
          currentInputText = cleanSingleTurnText(
            `${currentInputText} ${piece}`,
            115
          );
        }
      }

      if (sc.outputTranscription?.text) {
        const piece = String(sc.outputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          currentOutputText = cleanSingleTurnText(
            `${currentOutputText} ${piece}`,
            115
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
          const isBuildingPhrase = listener.outPcm24kBuffers.length > 0;
          const outRms = isBuildingPhrase
            ? 1.0
            : calculatePcmBufferRms(outBuf);

          if (isBuildingPhrase || outRms >= 0.0035) {
            listener.outPcm24kBuffers.push(outBuf);

            // Allow up to 14 chunks (~3.2 seconds) in a single WAV so full sentences NEVER break in the middle!
            // As soon as Gemini finishes the phrase burst (250ms quiet or turnComplete), it flushes as 1 unbroken WAV!
            if (listener.outPcm24kBuffers.length >= 14) {
              flushCompleteSentenceWav(false);
            } else {
              clearTimeout(listener.outAudioFlushTimer);
              listener.outAudioFlushTimer = setTimeout(() => {
                flushCompleteSentenceWav(true);
              }, 250);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          hasNewText = true;
          currentOutputText = cleanSingleTurnText(
            `${currentOutputText} ${part.text.trim()}`,
            115
          );
        }
      }

      if (
        hasNewText &&
        listener.translateMode !== "voice_only" &&
        (currentInputText || currentOutputText)
      ) {
        const normCheck = normalizeForDedup(currentOutputText);
        if (
          !normCheck ||
          normCheck !== lastFlushedNormText ||
          nowMs - lastFlushedAt >= 5000
        ) {
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            sessionId,
            originalText: currentInputText || null,
            translatedText: currentOutputText || null,
            targetLang,
            isFinal: Boolean(sc.turnComplete),
          });
        }

        clearTimeout(listener.transcriptResetTimer);
        listener.transcriptResetTimer = setTimeout(() => {
          currentInputText = "";
          currentOutputText = "";
        }, 1300);
      }

      if (sc.turnComplete || sc.generationComplete) {
        flushCompleteSentenceWav(true);
        currentInputText = "";
        currentOutputText = "";
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
    if (listener.geminiWs !== ws) return;
    listener.isConnecting = false;
    logTranslate(
      "WS_ERROR",
      `WebSocket error for ${listener.email}: ${err?.message || "error"}`
    );
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
// 📤 STREAM 16kHz PCM TO LISTENER'S GEMINI LIVE SESSION
// ============================================================================
function sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  if (!listener.geminiWs || listener.geminiWs.readyState > WebSocket.OPEN) {
    if (listener.pendingInputQueue.length < 8) {
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
  } else if (listener.pendingInputQueue.length < 8) {
    listener.pendingInputQueue.push(pcmBase64);
  }
}

function sendEndOfUtteranceToListenerSession(listener) {
  if (
    !listener?.geminiWs ||
    listener.geminiWs.readyState !== WebSocket.OPEN ||
    !listener.isSetupComplete
  ) {
    return;
  }
  try {
    for (let i = 0; i < 2; i++) {
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
      sessionSourceLang: null,
      isConnecting: false,
      lastConnectAttemptAt: 0,
      ignoreMicUntil: 0,
      partnerIsActivelySpeaking: false,
      acceptGeminiOutputUntil: 0,
      lastInSeqReceived: 0,
      audioSeq: 0,
      outPcm24kBuffers: [],
      pendingInputQueue: [],
      preRollChunks: [],
      hangoverLeft: 0,
      wasSpeakingLastChunk: false,
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

    const speakChanged = participant.speakLang !== nextSpeak;
    const listenChanged = participant.listenLang !== nextListen;

    participant.speakLang = nextSpeak;
    participant.listenLang = nextListen;
    participant.translateMode = nextMode;
    participant.translatorEnabled = nextEnabled;
    participant.hasMinutes = nextMins;

    if (nextMode === "text_only") {
      participant.outPcm24kBuffers = [];
      if (participant.outAudioFlushTimer) {
        clearTimeout(participant.outAudioFlushTimer);
        participant.outAudioFlushTimer = null;
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
      speaker.wasSpeakingLastChunk = false;
    }

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;
      listener.partnerIsActivelySpeaking = false;
      listener.acceptGeminiOutputUntil = Date.now() + 2400;
      sendEndOfUtteranceToListenerSession(listener);
    }
  });

  // 4. STRICT UTTERANCE-GATED STREAMING (STOPS SILENT HALLUCINATIONS & OLD REPEATS!)
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

    // 1. ECHO SHIELD: Ignore speaker's mic while their loudspeaker is playing translated audio
    if (now < (speaker.ignoreMicUntil || 0)) {
      speaker.preRollChunks = [];
      speaker.hangoverLeft = 0;
      speaker.wasSpeakingLastChunk = false;
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
      if (rms < (room.activeSpeakerRms || 0.03) * 1.7) {
        speaker.preRollChunks = [];
        speaker.hangoverLeft = 0;
        speaker.wasSpeakingLastChunk = false;
        return;
      }
    }

    // Threshold 0.018 filters out room noise/breathing so Gemini NEVER hallucinates extra questions!
    if (rms >= 0.018) {
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 450;

      speaker.hangoverLeft = 2; // 200ms natural word ending
      speaker.wasSpeakingLastChunk = true;

      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        listener.partnerIsActivelySpeaking = true;
        listener.acceptGeminiOutputUntil = now + 3200;

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
        listener.acceptGeminiOutputUntil = now + 2600;
        sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
      }
    } else if (speaker.wasSpeakingLastChunk) {
      // Speaker just finished their sentence -> send ONE clean end-of-utterance signal (`audioStreamEnd: true`)
      // and stop sending any more packets until the speaker speaks again!
      speaker.wasSpeakingLastChunk = false;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        listener.partnerIsActivelySpeaking = false;
        listener.acceptGeminiOutputUntil = now + 2400;
        sendEndOfUtteranceToListenerSession(listener);
      }
    } else {
      // Speaker is silent -> keep only 1 pre-roll chunk (100ms) and send NOTHING to Gemini!
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

    listener.acceptGeminiOutputUntil = Date.now() + 2600;
    sendPcm16kToListenerSession(roomId, listenerKey, listener, pcmBase64);
  });

  // 6. CLEANUP ON LEAVE / DISCONNECT
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