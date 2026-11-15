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
// 🔊 24kHz -> 16kHz SMOOTH INTERPOLATED WAV BUILDER (100% NATURAL SPEED & PITCH)
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

function buildCompleteSentenceWav16k(pcm24kBuffers) {
  const resampled = pcm24kBuffers.map((b) => resample24kTo16kPcm(b));
  const pcmData = Buffer.concat(resampled);
  const sampleRate = 16000;
  const dataLen = pcmData.length;
  const totalSamples = Math.floor(dataLen / 2);

  // 4ms fade-in/out (64 samples at 16kHz) so audio never clicks
  const fadeSamples = Math.min(64, Math.floor(totalSamples / 14));
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
  header.writeUInt32LE(sampleRate, 24); // 16,000 Hz
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

// ============================================================================
// 🗂️ IN-MEMORY ROOM & HOT-STANDBY SESSION POOL STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

function destroySingleGeminiSession(sess) {
  if (!sess) return;
  sess.closed = true;
  if (sess.flushTimer) {
    clearTimeout(sess.flushTimer);
    sess.flushTimer = null;
  }
  if (sess.drainKillTimer) {
    clearTimeout(sess.drainKillTimer);
    sess.drainKillTimer = null;
  }
  sess.outPcm24kBuffers = [];
  sess.pendingInputQueue = [];
  if (sess.ws) {
    try {
      sess.ws.removeAllListeners();
      sess.ws.close();
    } catch (e) {}
    sess.ws = null;
  }
}

function closeAllParticipantSessions(participant) {
  if (!participant) return;
  destroySingleGeminiSession(participant.activeSession);
  destroySingleGeminiSession(participant.standbySession);
  participant.activeSession = null;
  participant.standbySession = null;
  if (participant.drainingSessions) {
    for (const ds of participant.drainingSessions) {
      destroySingleGeminiSession(ds);
    }
    participant.drainingSessions.clear();
  }
}

// ============================================================================
// ⚡ SINGLE-UTTERANCE GEMINI SESSION FACTORY (ZERO OLD MEMORY!)
// Each session translates ONE spoken utterance, emits ONE unbroken WAV, and closes!
// ============================================================================
function createFreshGeminiSession(
  roomId,
  listenerKey,
  listener,
  roleLabel = "active"
) {
  const room = rooms.get(roomId);
  if (!room || !listener) return null;

  const targetLang = String(listener.listenLang || "en")
    .split("-")[0]
    .toLowerCase();

  let partnerSpeakLang = "en";
  for (const [k, p] of room.participants.entries()) {
    if (k !== listenerKey && p.speakLang) {
      partnerSpeakLang = String(p.speakLang).split("-")[0].toLowerCase();
    }
  }

  const sourceLangName = getLangName(partnerSpeakLang);
  const targetLangName = getLangName(targetLang);
  const utteranceId = `utt_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 6)}`;

  const sess = {
    utteranceId,
    ws: null,
    targetLang,
    sourceLang: partnerSpeakLang,
    isSetupComplete: false,
    hasReceivedAudioInput: false,
    isDraining: false,
    closed: false,
    outPcm24kBuffers: [],
    pendingInputQueue: [],
    inputText: "",
    outputText: "",
    flushTimer: null,
    drainKillTimer: null,
  };

  const wsUrl = getGeminiWsUrl("v1alpha");
  const ws = new WebSocket(wsUrl);
  sess.ws = ws;

  const flushUtteranceWavAndCloseIfDraining = (isFinalTurn = false) => {
    if (sess.closed) return;
    if (sess.flushTimer) {
      clearTimeout(sess.flushTimer);
      sess.flushTimer = null;
    }

    const cleanOrig = sess.inputText.replace(/\s+/g, " ").trim();
    const cleanTrans = sess.outputText.replace(/\s+/g, " ").trim();
    const sendSubtitles = listener.translateMode !== "voice_only";

    if (
      sess.outPcm24kBuffers.length > 0 &&
      listener.translateMode !== "text_only"
    ) {
      const chunks = sess.outPcm24kBuffers.splice(
        0,
        sess.outPcm24kBuffers.length
      );
      const { wavBuffer, durationMs } = buildCompleteSentenceWav16k(chunks);

      if (durationMs >= 140) {
        const seq = ++listener.audioSeq;

        // Echo Shield: Block listener's mic while their loudspeaker plays this sentence (+ 400ms tail)
        listener.ignoreMicUntil =
          Math.max(Date.now(), listener.ignoreMicUntil || 0) +
          durationMs +
          400;

        logTranslate(
          "COMPLETE_WAV_SENT",
          `🔊 Sent complete unbroken WAV #${seq} (${durationMs}ms, ${chunks.length} chunks, ${sourceLangName}->${targetLangName}) to ${listener.email}`,
          { utteranceId, text: cleanTrans || cleanOrig }
        );

        io.to(listener.socketId).emit("translated_audio_chunk", {
          roomId,
          sessionId: utteranceId,
          utteranceId,
          seq,
          wavBase64: wavBuffer.toString("base64"),
          audioFormat: "wav",
          sampleRate: 16000,
          durationMs,
          originalText: sendSubtitles && cleanOrig ? cleanOrig : null,
          translatedText: sendSubtitles && cleanTrans ? cleanTrans : null,
          targetLang,
          timestamp: Date.now(),
        });
      }
    } else if (sendSubtitles && (cleanOrig || cleanTrans) && isFinalTurn) {
      io.to(listener.socketId).emit("translated_subtitle", {
        roomId,
        sessionId: utteranceId,
        utteranceId,
        originalText: cleanOrig || null,
        translatedText: cleanTrans || null,
        targetLang,
        isFinal: true,
      });
    }

    // CRITICAL: Once this utterance's turn completes, destroy this session so its memory
    // can NEVER pollute or repeat into the next sentence!
    if (isFinalTurn || sess.isDraining) {
      listener.drainingSessions?.delete(sess);
      if (listener.activeSession === sess) {
        listener.activeSession = null;
        promoteStandbySession(roomId, listenerKey, listener);
      }
      destroySingleGeminiSession(sess);
    }
  };

  ws.on("open", () => {
    if (sess.closed) return;

    const setupMessage = {
      setup: {
        model: GEMINI_LIVE_MODEL,
        systemInstruction: {
          parts: [
            {
              text: `The speaker is speaking ${sourceLangName} (${partnerSpeakLang}). Translate ONLY the exact spoken words directly in first-person into ${targetLangName} (${targetLang}). Do NOT add any extra questions or words that were not spoken. Never say "he is saying" or "she is saying", and never output Spanish or Russian.`,
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
    if (sess.closed || !listener.translatorEnabled) return;
    try {
      const jsonStr = Buffer.isBuffer(rawData)
        ? rawData.toString("utf8")
        : String(rawData);
      const response = JSON.parse(jsonStr);

      if (response.setupComplete !== undefined) {
        sess.isSetupComplete = true;
        if (listener.activeSession === sess) {
          io.to(listener.socketId).emit("gemini_session_status", {
            connected: true,
            sessionId: utteranceId,
            mode: "v1alpha",
            targetLanguageCode: targetLang,
          });
        }

        if (sess.pendingInputQueue.length > 0) {
          const queued = sess.pendingInputQueue.splice(
            0,
            sess.pendingInputQueue.length
          );
          for (const b64 of queued) {
            sendPcmToSession(sess, b64);
          }
        }
        return;
      }

      const sc = response.serverContent;
      if (!sc) return;

      let hasNewText = false;

      if (sc.inputTranscription?.text) {
        const piece = String(sc.inputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          sess.inputText = `${sess.inputText} ${piece}`
            .replace(/\s+/g, " ")
            .trim();
        }
      }

      if (sc.outputTranscription?.text) {
        const piece = String(sc.outputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          sess.outputText = `${sess.outputText} ${piece}`
            .replace(/\s+/g, " ")
            .trim();
        }
      }

      const parts = sc.modelTurn?.parts || [];
      for (const part of parts) {
        if (
          part?.inlineData?.data &&
          listener.translateMode !== "text_only"
        ) {
          const outBuf = Buffer.from(part.inlineData.data, "base64");
          const isBuilding = sess.outPcm24kBuffers.length > 0;
          const outRms = isBuilding ? 1.0 : calculatePcmBufferRms(outBuf);

          // Keep 100% of audio chunks inside the sentence so voice NEVER breaks!
          if (isBuilding || outRms >= 0.0035) {
            sess.outPcm24kBuffers.push(outBuf);

            // Wait for full sentence completion (`sc.turnComplete`), or flush at 16 chunks (~4.0s) for long monologues,
            // or 650ms after the last audio chunk arrives so Gemini's internal 350ms pauses NEVER split a sentence in half!
            if (sess.outPcm24kBuffers.length >= 16) {
              flushUtteranceWavAndCloseIfDraining(false);
            } else {
              clearTimeout(sess.flushTimer);
              sess.flushTimer = setTimeout(() => {
                flushUtteranceWavAndCloseIfDraining(sess.isDraining);
              }, 650);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          hasNewText = true;
          sess.outputText = `${sess.outputText} ${part.text.trim()}`
            .replace(/\s+/g, " ")
            .trim();
        }
      }

      if (
        hasNewText &&
        listener.translateMode !== "voice_only" &&
        (sess.inputText || sess.outputText)
      ) {
        io.to(listener.socketId).emit("translated_subtitle", {
          roomId,
          sessionId: utteranceId,
          utteranceId,
          originalText: sess.inputText || null,
          translatedText: sess.outputText || null,
          targetLang,
          isFinal: Boolean(sc.turnComplete),
        });
      }

      if (sc.turnComplete || sc.generationComplete) {
        flushUtteranceWavAndCloseIfDraining(true);
      }
    } catch (e) {}
  });

  ws.on("error", () => {
    if (sess.closed) return;
    destroySingleGeminiSession(sess);
  });

  ws.on("close", () => {
    if (sess.closed) return;
    sess.closed = true;
    listener.drainingSessions?.delete(sess);
    if (listener.activeSession === sess) {
      listener.activeSession = null;
    }
    if (listener.standbySession === sess) {
      listener.standbySession = null;
    }
  });

  return sess;
}

// Ensures `activeSession` and `standbySession` are both ready and pre-warmed
function ensureParticipantSessionPool(roomId, listenerKey) {
  const room = rooms.get(roomId);
  if (!room) return;
  const listener = room.participants.get(listenerKey);
  if (!listener) return;

  if (!listener.translatorEnabled || !listener.hasMinutes) {
    closeAllParticipantSessions(listener);
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

  // If language pair changed, reset pool
  if (
    listener.activeSession &&
    (listener.activeSession.targetLang !== targetLang ||
      listener.activeSession.sourceLang !== partnerSpeakLang)
  ) {
    closeAllParticipantSessions(listener);
  }

  if (!listener.activeSession || listener.activeSession.closed) {
    if (listener.standbySession && !listener.standbySession.closed) {
      listener.activeSession = listener.standbySession;
      listener.standbySession = null;
    } else {
      listener.activeSession = createFreshGeminiSession(
        roomId,
        listenerKey,
        listener,
        "active"
      );
    }
  }

  if (!listener.standbySession || listener.standbySession.closed) {
    listener.standbySession = createFreshGeminiSession(
      roomId,
      listenerKey,
      listener,
      "standby"
    );
  }
}

// Rotates `activeSession` -> `drainingSessions` at the end of a spoken sentence,
// and promotes the pre-warmed `standbySession` -> `activeSession` in 0ms!
function promoteStandbySession(roomId, listenerKey, listener) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;

  if (listener.standbySession && !listener.standbySession.closed) {
    listener.activeSession = listener.standbySession;
    listener.standbySession = null;
  } else {
    listener.activeSession = createFreshGeminiSession(
      roomId,
      listenerKey,
      listener,
      "active"
    );
  }

  // Immediately pre-warm the next standby session in the background!
  listener.standbySession = createFreshGeminiSession(
    roomId,
    listenerKey,
    listener,
    "standby"
  );
}

function finalizeCurrentUtteranceAndRotateSession(
  roomId,
  listenerKey,
  listener
) {
  if (!listener || !listener.activeSession) return;
  const currentSess = listener.activeSession;
  if (!currentSess.hasReceivedAudioInput) return;

  currentSess.isDraining = true;
  listener.drainingSessions.add(currentSess);

  // Send clean silence + audioStreamEnd so Gemini finishes translating this utterance immediately
  if (
    currentSess.ws &&
    currentSess.ws.readyState === WebSocket.OPEN &&
    currentSess.isSetupComplete
  ) {
    try {
      for (let i = 0; i < 3; i++) {
        currentSess.ws.send(
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
      currentSess.ws.send(
        JSON.stringify({
          realtimeInput: {
            audioStreamEnd: true,
          },
        })
      );
    } catch (e) {}
  }

  // Safety cleanup after 3.5s if Gemini doesn't send turnComplete
  currentSess.drainKillTimer = setTimeout(() => {
    if (!currentSess.closed) {
      if (currentSess.flushTimer) {
        clearTimeout(currentSess.flushTimer);
      }
      listener.drainingSessions.delete(currentSess);
      destroySingleGeminiSession(currentSess);
    }
  }, 3500);

  // Swap in the pre-warmed standby session with 100% empty memory for the next sentence!
  listener.activeSession = null;
  promoteStandbySession(roomId, listenerKey, listener);
}

function sendPcmToSession(sess, pcmBase64) {
  if (!sess || sess.closed) return;
  sess.hasReceivedAudioInput = true;
  if (sess.ws && sess.ws.readyState === WebSocket.OPEN && sess.isSetupComplete) {
    try {
      sess.ws.send(
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
  } else if (sess.pendingInputQueue.length < 15) {
    sess.pendingInputQueue.push(pcmBase64);
  }
}

function streamPcm16kToListenerPool(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;
  ensureParticipantSessionPool(roomId, listenerKey);
  if (listener.activeSession) {
    sendPcmToSession(listener.activeSession, pcmBase64);
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
      activeSession: null,
      standbySession: null,
      drainingSessions: new Set(),
      ignoreMicUntil: 0,
      lastInSeqReceived: 0,
      audioSeq: 0,
      preRollChunks: [],
      hangoverLeft: 0,
      isInActiveUtterance: false,
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
        ensureParticipantSessionPool(roomId, k);
      } else {
        closeAllParticipantSessions(p);
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

    broadcastRoomTranslateState(roomId);

    if (participant.translatorEnabled && participant.hasMinutes) {
      if (listenChanged) {
        closeAllParticipantSessions(participant);
      }
      ensureParticipantSessionPool(roomId, emailKey);
    } else {
      closeAllParticipantSessions(participant);
    }

    if (speakChanged) {
      for (const [otherKey, otherP] of room.participants.entries()) {
        if (
          otherKey !== emailKey &&
          otherP.translatorEnabled &&
          otherP.hasMinutes
        ) {
          closeAllParticipantSessions(otherP);
          ensureParticipantSessionPool(roomId, otherKey);
        }
      }
    }
  });

  // 3. IMMEDIATE MIC-MUTE FLUSH & SESSION ROTATION
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
      speaker.isInActiveUtterance = false;
    }

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;
      finalizeCurrentUtteranceAndRotateSession(roomId, listenerKey, listener);
    }
  });

  // 4. FULL-SENTENCE UTTERANCE STREAMING + HOT-STANDBY ROTATION
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
      speaker.isInActiveUtterance = false;
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
        speaker.isInActiveUtterance = false;
        return;
      }
    }

    if (rms >= 0.015) {
      room.activeSpeakerKey = speakerKey;
      room.activeSpeakerRms = rms;
      room.floorLockUntil = now + 450;

      // 6 chunks (~650ms) of continuous hangover so natural pauses between words
      // ("What is ... your name?") NEVER split a sentence in half!
      speaker.hangoverLeft = 6;
      speaker.isInActiveUtterance = true;

      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;

        if (speaker.preRollChunks.length > 0) {
          for (const preB64 of speaker.preRollChunks) {
            streamPcm16kToListenerPool(roomId, listenerKey, listener, preB64);
          }
        }
        streamPcm16kToListenerPool(roomId, listenerKey, listener, pcmBase64);
      }
      speaker.preRollChunks = [];
    } else if (speaker.hangoverLeft > 0) {
      // Keep streaming natural mid-sentence word pauses & soft ending syllables intact!
      speaker.hangoverLeft -= 1;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        streamPcm16kToListenerPool(roomId, listenerKey, listener, pcmBase64);
      }
    } else if (speaker.isInActiveUtterance) {
      // Speaker has been silent for ~650ms -> sentence is complete!
      // Finalize this utterance's session and rotate to the fresh standby session (0% old context)!
      speaker.isInActiveUtterance = false;
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;
        if (!listener.translatorEnabled || !listener.hasMinutes) continue;
        finalizeCurrentUtteranceAndRotateSession(
          roomId,
          listenerKey,
          listener
        );
      }
    } else {
      // Speaker is silent -> keep last 2 chunks (200ms) in pre-roll so the first syllable of the next sentence is never missed
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

    streamPcm16kToListenerPool(roomId, listenerKey, listener, pcmBase64);
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
      closeAllParticipantSessions(participant);
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