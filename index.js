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

// Pure digital 100ms zero-silence frame at 16kHz 16-bit mono (3200 zero bytes)
const SILENT_PCM_100MS_B64 = Buffer.alloc(3200).toString("base64");

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

  // 3ms soft edge fade (72 samples at 24kHz) for click-free audio
  const fadeSamples = Math.min(72, Math.floor(totalSamples / 16));
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

// Fuzzy phrase similarity check (catches near-identical repeats even if 1 word differs)
function isFuzzyDuplicate(textA, textB) {
  const a = normalizeForDedup(textA);
  const b = normalizeForDedup(textB);
  if (!a || !b || a.length < 4 || b.length < 4) return false;
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;

  const wordsA = new Set(a.split(/\s+/).filter(Boolean));
  const wordsB = new Set(b.split(/\s+/).filter(Boolean));
  if (wordsA.size === 0 || wordsB.size === 0) return false;

  let overlap = 0;
  for (const w of wordsA) {
    if (wordsB.has(w)) overlap++;
  }
  const ratio = overlap / Math.min(wordsA.size, wordsB.size);
  return ratio >= 0.72;
}

// ============================================================================
// 🗂️ IN-MEMORY ROOM & HOT-STANDBY SINGLE-UTTERANCE SESSION POOL
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

function destroySingleSession(sess) {
  if (!sess) return;
  sess.closed = true;
  if (sess.outFlushTimer) {
    clearTimeout(sess.outFlushTimer);
    sess.outFlushTimer = null;
  }
  if (sess.drainTimeoutTimer) {
    clearTimeout(sess.drainTimeoutTimer);
    sess.drainTimeoutTimer = null;
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
  destroySingleSession(participant.activeSession);
  destroySingleSession(participant.standbySession);
  participant.activeSession = null;
  participant.standbySession = null;
  if (participant.drainingSessions) {
    for (const ds of participant.drainingSessions) {
      destroySingleSession(ds);
    }
    participant.drainingSessions.clear();
  }
}

// ============================================================================
// ⚡ SINGLE-UTTERANCE GEMINI SESSION FACTORY (1 SPEAK -> 1 TRANSLATE -> WIPE!)
// ============================================================================
function createBlankMemorySession(roomId, listenerKey, listener) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) {
    return null;
  }

  const targetLang = String(listener.listenLang || "en")
    .trim()
    .split("-")[0]
    .toLowerCase();

  const utteranceId = `utt_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 6)}`;

  const sess = {
    utteranceId,
    ws: null,
    targetLang,
    isSetupComplete: false,
    hasReceivedRealSpeech: false,
    isDraining: false,
    turnDelivered: false,
    closed: false,
    silentOutTicks: 0,
    outPcm24kBuffers: [],
    pendingInputQueue: [],
    inputText: "",
    outputText: "",
    outFlushTimer: null,
    drainTimeoutTimer: null,
  };

  const wsUrl = getGeminiWsUrl("v1alpha");
  const ws = new WebSocket(wsUrl);
  sess.ws = ws;

  const deliverSingleTranslationAndDestroy = (isFinal = false) => {
    if (sess.closed || sess.turnDelivered) return;

    if (sess.outFlushTimer) {
      clearTimeout(sess.outFlushTimer);
      sess.outFlushTimer = null;
    }

    // Never emit anything if this session never received real human speech!
    if (!sess.hasReceivedRealSpeech) {
      sess.outPcm24kBuffers = [];
      return;
    }

    const cleanOrig = cleanCaption(sess.inputText, 120);
    const cleanTrans = cleanCaption(sess.outputText, 120);
    const sendSubtitles = listener.translateMode !== "voice_only";
    const now = Date.now();

    // Fuzzy duplicate blocker (stops identical/near-identical repeats within 10s)
    const isRepeat =
      cleanTrans &&
      listener.lastDeliveredTransText &&
      now - (listener.lastDeliveredAt || 0) < 10000 &&
      isFuzzyDuplicate(cleanTrans, listener.lastDeliveredTransText);

    if (isRepeat) {
      logTranslate(
        "REPEAT_BLOCKED",
        `🛡️ Blocked duplicate repeat for ${listener.email}`,
        { text: cleanTrans }
      );
      sess.outPcm24kBuffers = [];
      if (isFinal || sess.isDraining) {
        sess.turnDelivered = true;
        listener.drainingSessions.delete(sess);
        if (listener.activeSession === sess) {
          listener.activeSession = null;
          ensureSessionPool(roomId, listenerKey);
        }
        destroySingleSession(sess);
      }
      return;
    }

    let emittedSomething = false;

    if (
      sess.outPcm24kBuffers.length > 0 &&
      listener.translateMode !== "text_only"
    ) {
      const chunks = sess.outPcm24kBuffers.splice(
        0,
        sess.outPcm24kBuffers.length
      );
      const { wavBuffer, durationMs } = buildNative24kWav(chunks);

      if (durationMs >= 110) {
        emittedSomething = true;
        const seq = ++listener.audioSeq;

        // Server-side echo shield covers network transit + full playback + room echo tail
        listener.ignoreMicUntil =
          Math.max(now, listener.ignoreMicUntil || 0) + durationMs + 1100;

        logTranslate(
          "VOICE_24K_SENT",
          `🔊 Sent unbroken 24kHz WAV #${seq} (${durationMs}ms, ${chunks.length} chunks, ->${targetLang}) to ${listener.email}`,
          { utteranceId, text: cleanTrans || cleanOrig }
        );

        io.to(listener.socketId).emit("translated_audio_chunk", {
          roomId,
          sessionId: listener.roomSessionId,
          utteranceId,
          seq,
          wavBase64: wavBuffer.toString("base64"),
          audioFormat: "wav",
          sampleRate: 24000,
          durationMs,
          originalText: sendSubtitles && cleanOrig ? cleanOrig : null,
          translatedText: sendSubtitles && cleanTrans ? cleanTrans : null,
          targetLang,
          isFinal: Boolean(isFinal || sess.isDraining),
          timestamp: now,
        });
      }
    } else if (sendSubtitles && (cleanOrig || cleanTrans) && isFinal) {
      emittedSomething = true;
      io.to(listener.socketId).emit("translated_subtitle", {
        roomId,
        sessionId: listener.roomSessionId,
        utteranceId,
        originalText: cleanOrig || null,
        translatedText: cleanTrans || null,
        targetLang,
        isFinal: true,
      });
    }

    if (emittedSomething && cleanTrans) {
      listener.lastDeliveredTransText = cleanTrans;
      listener.lastDeliveredAt = now;
    }

    // HARD LOCK & WIPE: Once the final turn finishes, DESTROY this WebSocket forever!
    // Old memory can NEVER repeat again!
    if (isFinal || sess.isDraining) {
      sess.turnDelivered = true;
      listener.drainingSessions.delete(sess);
      if (listener.activeSession === sess) {
        listener.activeSession = null;
        ensureSessionPool(roomId, listenerKey);
      }
      destroySingleSession(sess);
    }
  };

  ws.on("open", () => {
    if (sess.closed) return;

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
    if (sess.closed || sess.turnDelivered || !listener.translatorEnabled) return;
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
            sessionId: listener.roomSessionId,
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
            sendAudioToSession(sess, b64);
          }
        }
        return;
      }

      // STRICT GATE: Ignore any message from Gemini if this session hasn't received real speech!
      if (!sess.hasReceivedRealSpeech) return;

      const sc = response.serverContent;
      if (!sc) return;

      let hasNewText = false;

      if (sc.inputTranscription?.text) {
        const piece = String(sc.inputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          sess.inputText = cleanCaption(`${sess.inputText} ${piece}`, 120);
        }
      }

      if (sc.outputTranscription?.text) {
        const piece = String(sc.outputTranscription.text).trim();
        if (piece) {
          hasNewText = true;
          sess.outputText = cleanCaption(`${sess.outputText} ${piece}`, 120);
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
            sess.silentOutTicks = 0;
            sess.outPcm24kBuffers.push(outBuf);

            // Keep an entire normal sentence (up to 10 chunks / ~2.5s) in ONE unbroken WAV!
            // Flushes immediately on `turnComplete` OR 380ms after the last audible chunk!
            if (sess.outPcm24kBuffers.length >= 10) {
              deliverSingleTranslationAndDestroy(false);
            } else {
              clearTimeout(sess.outFlushTimer);
              sess.outFlushTimer = setTimeout(() => {
                deliverSingleTranslationAndDestroy(sess.isDraining);
              }, 380);
            }
          } else if (sess.outPcm24kBuffers.length > 0) {
            sess.silentOutTicks += 1;
            if (sess.silentOutTicks === 1) {
              sess.outPcm24kBuffers.push(outBuf);
            } else {
              // 2 consecutive silent output ticks -> phrase finished!
              deliverSingleTranslationAndDestroy(sess.isDraining);
            }
          }
        }
        if (part?.text && part.text.trim()) {
          hasNewText = true;
          sess.outputText = cleanCaption(
            `${sess.outputText} ${part.text.trim()}`,
            120
          );
        }
      }

      if (
        hasNewText &&
        listener.translateMode !== "voice_only" &&
        (sess.inputText || sess.outputText)
      ) {
        const isRepeat =
          sess.outputText &&
          listener.lastDeliveredTransText &&
          Date.now() - (listener.lastDeliveredAt || 0) < 10000 &&
          isFuzzyDuplicate(sess.outputText, listener.lastDeliveredTransText);

        if (!isRepeat) {
          io.to(listener.socketId).emit("translated_subtitle", {
            roomId,
            sessionId: listener.roomSessionId,
            utteranceId,
            originalText: sess.inputText || null,
            translatedText: sess.outputText || null,
            targetLang,
            isFinal: Boolean(sc.turnComplete),
          });
        }
      }

      if (sc.turnComplete || sc.generationComplete) {
        deliverSingleTranslationAndDestroy(true);
      }
    } catch (e) {}
  });

  ws.on("error", (err) => {
    if (sess.closed) return;
    logTranslate(
      "WS_ERROR",
      `Session error for ${listener.email}: ${err?.message || "error"}`
    );
    destroySingleSession(sess);
  });

  ws.on("close", () => {
    if (sess.closed) return;
    sess.closed = true;
    listener.drainingSessions.delete(sess);
    if (listener.activeSession === sess) listener.activeSession = null;
    if (listener.standbySession === sess) listener.standbySession = null;
  });

  sess.forceFlushAndDestroy = () => deliverSingleTranslationAndDestroy(true);

  return sess;
}

function ensureSessionPool(roomId, listenerKey) {
  const room = rooms.get(roomId);
  if (!room) return;
  const listener = room.participants.get(listenerKey);
  if (!listener) return;

  if (!listener.translatorEnabled || !listener.hasMinutes) {
    closeAllParticipantSessions(listener);
    return;
  }

  const targetLang = String(listener.listenLang || "en")
    .trim()
    .split("-")[0]
    .toLowerCase();

  if (
    listener.activeSession &&
    listener.activeSession.targetLang !== targetLang
  ) {
    closeAllParticipantSessions(listener);
  }

  if (!listener.activeSession || listener.activeSession.closed) {
    if (listener.standbySession && !listener.standbySession.closed) {
      listener.activeSession = listener.standbySession;
      listener.standbySession = null;
    } else {
      listener.activeSession = createBlankMemorySession(
        roomId,
        listenerKey,
        listener
      );
    }
  }

  // Pre-warm 1 blank standby session (rate-limited to avoid rapid reconnect storms)
  const now = Date.now();
  if (
    (!listener.standbySession || listener.standbySession.closed) &&
    now - (listener.lastStandbyCreateAt || 0) >= 1200
  ) {
    listener.lastStandbyCreateAt = now;
    listener.standbySession = createBlankMemorySession(
      roomId,
      listenerKey,
      listener
    );
  }
}

function sendAudioToSession(sess, pcmBase64, isRealSpeech = true) {
  if (!sess || sess.closed || sess.turnDelivered) return;
  if (isRealSpeech) sess.hasReceivedRealSpeech = true;

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
  } else if (sess.pendingInputQueue.length < 20) {
    sess.pendingInputQueue.push(pcmBase64);
  }
}

// Called the moment the speaker stops speaking (or mutes mic):
// 1. Sends 500ms of pure digital 0x00 silence + `audioStreamEnd: true` to finish the current sentence immediately.
// 2. Moves current session into draining (destroys itself right after emitting the 1 translation).
// 3. Promotes the 100% blank-memory standby session into `activeSession` in 0ms!
function finalizeUtteranceAndSwapBlankSession(roomId, listenerKey, listener) {
  if (!listener || !listener.activeSession) return;
  const draining = listener.activeSession;
  if (!draining.hasReceivedRealSpeech) return;

  draining.isDraining = true;
  listener.drainingSessions.add(draining);

  if (
    draining.ws &&
    draining.ws.readyState === WebSocket.OPEN &&
    draining.isSetupComplete
  ) {
    try {
      // Send 5 frames (500ms) of digital silence + audioStreamEnd to finalize immediately
      for (let i = 0; i < 5; i++) {
        draining.ws.send(
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
      draining.ws.send(
        JSON.stringify({
          realtimeInput: {
            audioStreamEnd: true,
          },
        })
      );
    } catch (e) {}
  }

  // Hard 3.2s ceiling: always flush whatever audio arrived, then destroy this session permanently!
  draining.drainTimeoutTimer = setTimeout(() => {
    if (!draining.closed) {
      draining.forceFlushAndDestroy?.();
    }
  }, 3200);

  // Swap in the blank standby session immediately so the next spoken sentence has ZERO old memory!
  listener.activeSession = null;
  ensureSessionPool(roomId, listenerKey);
}

function streamSpeechChunkToListener(roomId, listenerKey, listener, pcmBase64) {
  if (!listener || !listener.translatorEnabled || !listener.hasMinutes) return;
  ensureSessionPool(roomId, listenerKey);
  if (listener.activeSession) {
    sendAudioToSession(listener.activeSession, pcmBase64, true);
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
      roomSessionId: `rs_${Date.now()}_${Math.random()
        .toString(36)
        .substring(2, 5)}`,
      speakLang: String(speakLang || "en"),
      listenLang: String(listenLang || "bn"),
      translateMode: String(translateMode || "both"),
      translatorEnabled: Boolean(translatorEnabled),
      hasMinutes: Boolean(hasMinutes),
      activeSession: null,
      standbySession: null,
      drainingSessions: new Set(),
      lastStandbyCreateAt: 0,
      isClientPlayingAudio: false,
      ignoreMicUntil: 0,
      lastInSeqReceived: 0,
      audioSeq: 0,
      lastDeliveredTransText: "",
      lastDeliveredAt: 0,
      // Adaptive noise floor & speech state machine
      noiseFloorRms: 0.008,
      speechStreakCount: 0,
      speechStreakChunks: [],
      preRollChunks: [],
      hangoverLeft: 0,
      isSpeaking: false,
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
      ensureSessionPool(roomId, emailKey);
    } else {
      closeAllParticipantSessions(participant);
    }
  });

  // 🔇 REAL-TIME CLIENT PLAYBACK ECHO SHIELD
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
      participant.speechStreakCount = 0;
      participant.speechStreakChunks = [];
      participant.hangoverLeft = 0;
      participant.isSpeaking = false;
    } else {
      // 900ms post-playback cooldown blocks Android's audio capture buffer + room reverb
      participant.ignoreMicUntil = Math.max(
        participant.ignoreMicUntil || 0,
        Date.now() + 900
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

    broadcastRoomTranslateState(roomId);

    if (participant.translatorEnabled && participant.hasMinutes) {
      if (listenChanged) {
        closeAllParticipantSessions(participant);
        participant.lastStandbyCreateAt = 0;
      }
      ensureSessionPool(roomId, emailKey);
    } else {
      closeAllParticipantSessions(participant);
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
      speaker.speechStreakCount = 0;
      speaker.speechStreakChunks = [];
      speaker.hangoverLeft = 0;
      if (speaker.isSpeaking) {
        speaker.isSpeaking = false;
        for (const [listenerKey, listener] of room.participants.entries()) {
          if (listenerKey === speakerKey) continue;
          if (!listener.translatorEnabled || !listener.hasMinutes) continue;
          finalizeUtteranceAndSwapBlankSession(roomId, listenerKey, listener);
        }
      }
    }
  });

  // ADAPTIVE NOISE-FLOOR SPEECH STATE MACHINE (ZERO SILENCE HALLUCINATIONS!)
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

    // 1. CLIENT SILENCE / ECHO SHIELD: Never forward loudspeaker bleed!
    if (
      pcmBase64 === "SILENCE" ||
      speaker.isClientPlayingAudio ||
      now < (speaker.ignoreMicUntil || 0)
    ) {
      speaker.preRollChunks = [];
      speaker.speechStreakCount = 0;
      speaker.speechStreakChunks = [];
      speaker.hangoverLeft = 0;
      if (speaker.isSpeaking) {
        speaker.isSpeaking = false;
        for (const [listenerKey, listener] of room.participants.entries()) {
          if (listenerKey === speakerKey) continue;
          if (!listener.translatorEnabled || !listener.hasMinutes) continue;
          finalizeUtteranceAndSwapBlankSession(roomId, listenerKey, listener);
        }
      }
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
        speaker.speechStreakCount = 0;
        speaker.speechStreakChunks = [];
        return;
      }
    }

    // Adaptive speech threshold: automatically stays above room fan/static noise!
    const adaptiveThreshold = Math.min(
      0.045,
      Math.max(0.016, (speaker.noiseFloorRms || 0.008) * 2.6)
    );
    const continueThreshold = adaptiveThreshold * 0.72;

    if (speaker.isSpeaking) {
      if (rms >= continueThreshold) {
        room.activeSpeakerKey = speakerKey;
        room.activeSpeakerRms = rms;
        room.floorLockUntil = now + 450;
        // 500ms (5 x 100ms) natural pause hangover inside a sentence
        speaker.hangoverLeft = 5;

        for (const [listenerKey, listener] of room.participants.entries()) {
          if (listenerKey === speakerKey) continue;
          if (!listener.translatorEnabled || !listener.hasMinutes) continue;
          streamSpeechChunkToListener(roomId, listenerKey, listener, pcmBase64);
        }
      } else if (speaker.hangoverLeft > 0) {
        speaker.hangoverLeft -= 1;
        for (const [listenerKey, listener] of room.participants.entries()) {
          if (listenerKey === speakerKey) continue;
          if (!listener.translatorEnabled || !listener.hasMinutes) continue;
          streamSpeechChunkToListener(roomId, listenerKey, listener, pcmBase64);
        }
      } else {
        // Speaker stopped talking for 500ms -> Finalize this ONE sentence and swap to blank session!
        speaker.isSpeaking = false;
        speaker.speechStreakCount = 0;
        speaker.speechStreakChunks = [];
        for (const [listenerKey, listener] of room.participants.entries()) {
          if (listenerKey === speakerKey) continue;
          if (!listener.translatorEnabled || !listener.hasMinutes) continue;
          finalizeUtteranceAndSwapBlankSession(roomId, listenerKey, listener);
        }
      }
    } else {
      // Currently IDLE (silent): require 2 consecutive chunks (200ms) above adaptive threshold
      // so clicks, taps, breaths, and room noise NEVER wake up Gemini!
      if (rms >= adaptiveThreshold) {
        speaker.speechStreakCount += 1;
        speaker.speechStreakChunks.push(pcmBase64);

        if (speaker.speechStreakCount >= 2) {
          speaker.isSpeaking = true;
          speaker.hangoverLeft = 5;
          room.activeSpeakerKey = speakerKey;
          room.activeSpeakerRms = rms;
          room.floorLockUntil = now + 450;

          const startChunks = [
            ...speaker.preRollChunks,
            ...speaker.speechStreakChunks,
          ];
          speaker.preRollChunks = [];
          speaker.speechStreakCount = 0;
          speaker.speechStreakChunks = [];

          for (const [listenerKey, listener] of room.participants.entries()) {
            if (listenerKey === speakerKey) continue;
            if (!listener.translatorEnabled || !listener.hasMinutes) continue;
            for (const c of startChunks) {
              streamSpeechChunkToListener(roomId, listenerKey, listener, c);
            }
          }
        }
      } else {
        // Slowly adapt ambient room noise floor while silent
        speaker.noiseFloorRms =
          (speaker.noiseFloorRms || 0.008) * 0.92 + rms * 0.08;
        speaker.speechStreakCount = 0;
        speaker.speechStreakChunks = [];
        speaker.preRollChunks.push(pcmBase64);
        if (speaker.preRollChunks.length > 2) {
          speaker.preRollChunks.shift();
        }
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

    streamSpeechChunkToListener(roomId, listenerKey, listener, pcmBase64);
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