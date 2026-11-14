// GeminiTranslateServer/index.js
require("dotenv").config();
const express = require("express");
const http = require("http");
const https = require("https");
const cors = require("cors");
const { Server } = require("socket.io");

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
const GEMINI_FLASH_MODELS = [
  "gemini-2.0-flash",
  "gemini-1.5-flash",
  "gemini-3.8-flash",
];

const LANGUAGE_NAMES = {
  en: "English",
  bn: "Bengali (বাংলা)",
  hi: "Hindi (हिन्दी)",
  ur: "Urdu (اردو)",
  ar: "Arabic (العربية)",
  id: "Indonesian",
  pt: "Portuguese",
  es: "Spanish",
  ru: "Russian",
  ja: "Japanese (日本語)",
  ko: "Korean (한국어)",
  fr: "French",
  de: "German",
  tr: "Turkish",
  vi: "Vietnamese",
  th: "Thai",
  ms: "Malay",
  tl: "Filipino",
  zh: "Chinese (中文)",
  ta: "Tamil (தமிழ்)",
  te: "Telugu (తెలుగు)",
  mr: "Marathi (मराठी)",
  gu: "Gujarati (ગુજરાતી)",
  kn: "Kannada (ಕನ್ನಡ)",
  ml: "Malayalam (മലയാളം)",
  pa: "Punjabi (ਪੰਜਾਬੀ)",
  ne: "Nepali (नेपाली)",
};

function getLangFullName(code) {
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
// 🔊 WAV HEADER BUILDER & RMS AUDIO ENERGY METER
// ============================================================================
function buildWavBufferFromPcmBuffers(pcmBuffers, sampleRate = 16000) {
  const pcmData = Buffer.concat(pcmBuffers);
  const dataLen = pcmData.length;
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
// 🌐 GEMINI AI TEXT TRANSLATOR (NEVER BLOCKED ON RENDER!)
// ============================================================================
let genaiModule = null;
async function getGenAiSdk() {
  if (genaiModule) return genaiModule;
  try {
    genaiModule = require("@google/genai");
  } catch (e) {
    genaiModule = await import("@google/genai");
  }
  return genaiModule;
}

async function translateTextWithGemini(
  text,
  targetLangCode = "hi",
  sourceLangCode = "en"
) {
  const cleanText = String(text || "").trim();
  if (!cleanText) return "";

  const targetName = getLangFullName(targetLangCode);
  const sourceName = getLangFullName(sourceLangCode);

  try {
    const { GoogleGenAI } = await getGenAiSdk();
    const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

    for (const modelName of GEMINI_FLASH_MODELS) {
      try {
        const resp = await ai.models.generateContent({
          model: modelName,
          contents: `Translate the following spoken phrase from ${sourceName} into natural conversational ${targetName}. Return ONLY the translated text in ${targetName} script (no quotes, no explanations):\n\n${cleanText}`,
          config: {
            temperature: 0.1,
          },
        });
        const out = String(resp?.text || "").trim();
        if (out) return out;
      } catch (e) {}
    }
  } catch (e) {}

  return cleanText;
}

function synthesizeTranslatedSpeechMp3Base64(text, targetLang = "en") {
  return new Promise((resolve) => {
    const cleanText = String(text || "").trim();
    if (!cleanText) return resolve(null);
    const tl = String(targetLang || "en").split("-")[0].toLowerCase();
    const url = `https://translate.google.com/translate_tts?ie=UTF-8&q=${encodeURIComponent(
      cleanText.slice(0, 200)
    )}&tl=${encodeURIComponent(tl)}&client=tw-ob`;

    https
      .get(
        url,
        {
          headers: {
            "User-Agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          },
        },
        (res) => {
          if (res.statusCode !== 200) {
            return resolve(null);
          }
          const chunks = [];
          res.on("data", (d) => chunks.push(d));
          res.on("end", () => {
            try {
              const buf = Buffer.concat(chunks);
              if (buf.length > 100) {
                return resolve(buf.toString("base64"));
              }
            } catch (e) {}
            resolve(null);
          });
        }
      )
      .on("error", () => resolve(null));
  });
}

// ============================================================================
// 🗂️ IN-MEMORY ROOM & BIDIRECTIONAL SESSION STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

async function closeListenerGeminiSession(participant) {
  if (!participant) return;
  if (participant.keepAliveInterval) {
    clearInterval(participant.keepAliveInterval);
    participant.keepAliveInterval = null;
  }
  if (participant.outAudioFlushTimer) {
    clearTimeout(participant.outAudioFlushTimer);
    participant.outAudioFlushTimer = null;
  }
  const sess = participant.geminiSession;
  participant.geminiSession = null;
  participant.sessionTargetLang = null;
  participant.isConnecting = false;
  if (sess) {
    try {
      sess.close();
    } catch (e) {}
  }
}

// ============================================================================
// 🤖 CONNECT GEMINI 3.5 LIVE TRANSLATE (WITH SERVER-SIDE 24kHz WAV ASSEMBLY)
// ============================================================================
async function ensureListenerGeminiSession(roomId, listenerKey) {
  const room = rooms.get(roomId);
  if (!room) return null;
  const listener = room.participants.get(listenerKey);
  if (!listener) return null;

  if (!listener.translatorEnabled || !listener.hasMinutes) {
    await closeListenerGeminiSession(listener);
    return null;
  }

  const targetLang = String(listener.listenLang || "en")
    .split("-")[0]
    .toLowerCase();

  if (
    listener.geminiSession &&
    listener.sessionTargetLang === targetLang &&
    !listener.isConnecting
  ) {
    return listener.geminiSession;
  }

  const now = Date.now();
  if (
    listener.isConnecting ||
    now - (listener.lastConnectAttemptAt || 0) < 2500
  ) {
    return listener.geminiSession || null;
  }

  listener.isConnecting = true;
  listener.lastConnectAttemptAt = now;

  await closeListenerGeminiSession(listener);

  let activeSessionRef = null;
  try {
    const { GoogleGenAI, Modality } = await getGenAiSdk();
    const ai = new GoogleGenAI({
      apiKey: GEMINI_API_KEY,
      httpOptions: { apiVersion: "v1alpha" },
    });

    // Enable inputAudioTranscription & outputAudioTranscription so Gemini Live emits subtitles!
    const config = {
      responseModalities: [Modality?.AUDIO || "AUDIO"],
      inputAudioTranscription: {},
      outputAudioTranscription: {},
      translationConfig: {
        targetLanguageCode: targetLang,
      },
    };

    logTranslate(
      "SESSION_CONNECTING",
      `Connecting ${GEMINI_LIVE_MODEL} (v1alpha) for ${listener.email}`,
      { roomId, targetLanguageCode: targetLang }
    );

    let currentInputText = "";
    let currentOutputText = "";
    let outPcm24kBuffers = [];

    const flushOutPcm24kAsCompleteWav = () => {
      if (listener.outAudioFlushTimer) {
        clearTimeout(listener.outAudioFlushTimer);
        listener.outAudioFlushTimer = null;
      }
      if (outPcm24kBuffers.length === 0) return;

      const chunks = outPcm24kBuffers.splice(0, outPcm24kBuffers.length);
      const wavBuf = buildWavBufferFromPcmBuffers(chunks, 24000);
      const wavBase64 = wavBuf.toString("base64");

      logTranslate(
        "GEMINI_WAV_SENT",
        `🔊 Sent complete 24kHz translated WAV (${chunks.length} chunks, ${wavBuf.length} bytes) to ${listener.email}`
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

    activeSessionRef = await ai.live.connect({
      model: GEMINI_LIVE_MODEL,
      config,
      callbacks: {
        onopen: () => {
          logTranslate(
            "SESSION_OPEN",
            `✅ Gemini Live (v1alpha) connected & stable for ${listener.email} (-> ${targetLang})`
          );
          io.to(listener.socketId).emit("gemini_session_status", {
            connected: true,
            mode: "v1alpha",
            targetLanguageCode: targetLang,
          });
        },
        onmessage: async (message) => {
          try {
            const sc = message?.serverContent;
            if (!sc) return;

            if (sc.inputTranscription?.text) {
              currentInputText =
                `${currentInputText} ${sc.inputTranscription.text}`.trim();
            }

            if (sc.outputTranscription?.text) {
              currentOutputText =
                `${currentOutputText} ${sc.outputTranscription.text}`.trim();
            }

            const parts = sc.modelTurn?.parts || [];
            for (const part of parts) {
              if (part?.inlineData?.data) {
                const outBuf = Buffer.from(part.inlineData.data, "base64");
                const outRms = calculatePcmBufferRms(outBuf);

                // Filter out Gemini's 0.0000 idle silence frames
                if (outRms >= 0.004) {
                  listener.lastLiveAudioAt = Date.now();
                  outPcm24kBuffers.push(outBuf);

                  // Flush if we accumulated ~1.75 seconds of continuous speech (7 chunks)
                  if (outPcm24kBuffers.length >= 7) {
                    flushOutPcm24kAsCompleteWav();
                  } else {
                    clearTimeout(listener.outAudioFlushTimer);
                    listener.outAudioFlushTimer = setTimeout(() => {
                      flushOutPcm24kAsCompleteWav();
                    }, 380);
                  }
                }
              }
              if (part?.text && part.text.trim()) {
                currentOutputText =
                  `${currentOutputText} ${part.text.trim()}`.trim();
              }
            }

            if (currentInputText || currentOutputText) {
              // If Gemini gave inputTranscription without outputTranscription, translate it with Gemini Flash
              if (currentInputText && !currentOutputText && sc.turnComplete) {
                currentOutputText = await translateTextWithGemini(
                  currentInputText,
                  targetLang,
                  "auto"
                );
              }

              io.to(listener.socketId).emit("translated_subtitle", {
                roomId,
                originalText: currentInputText || null,
                translatedText: currentOutputText || null,
                targetLang,
                isFinal: Boolean(sc.turnComplete),
              });
            }

            if (sc.turnComplete || sc.generationComplete) {
              flushOutPcm24kAsCompleteWav();
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
          } catch (err) {}
        },
        onerror: (err) => {
          logTranslate(
            "SESSION_WARN",
            `Error on session for ${listener.email}: ${err?.message || "error"}`
          );
        },
        onclose: (ev) => {
          logTranslate(
            "SESSION_CLOSE",
            `Session closed for ${listener.email} (${ev?.reason || "closed"})`
          );
          if (
            activeSessionRef &&
            listener.geminiSession === activeSessionRef
          ) {
            listener.geminiSession = null;
          }
        },
      },
    });

    listener.geminiSession = activeSessionRef;
    listener.sessionTargetLang = targetLang;
    listener.isConnecting = false;

    // 15s Keepalive so Gemini never closes the session due to client inactivity
    const silentPcmBase64 = Buffer.alloc(640).toString("base64");
    listener.keepAliveInterval = setInterval(() => {
      if (
        listener.geminiSession &&
        Date.now() - (listener.lastInputSentAt || 0) > 15000
      ) {
        try {
          listener.geminiSession.sendRealtimeInput({
            media: {
              data: silentPcmBase64,
              mimeType: "audio/pcm;rate=16000",
            },
          });
        } catch (e) {}
      }
    }, 15000);

    return activeSessionRef;
  } catch (err) {
    listener.isConnecting = false;
    return null;
  }
}

// ============================================================================
// 🎙️ DELIVER TRANSLATED SUBTITLES + VOICE AUDIO TO LISTENER
// ============================================================================
async function deliverTranslatedSpeechToListener(
  roomId,
  speaker,
  listener,
  originalText,
  preTranslatedText = null
) {
  const cleanOrig = String(originalText || "").trim();
  if (!cleanOrig || !listener.translatorEnabled || !listener.hasMinutes) return;

  const sourceLang = String(speaker?.speakLang || "en")
    .split("-")[0]
    .toLowerCase();
  const targetLang = String(listener.listenLang || "en")
    .split("-")[0]
    .toLowerCase();

  try {
    const translatedText =
      preTranslatedText && String(preTranslatedText).trim()
        ? String(preTranslatedText).trim()
        : await translateTextWithGemini(cleanOrig, targetLang, sourceLang);

    logTranslate(
      "TRANSLATED_SPEECH",
      `🗣️ ${speaker?.email || "Speaker"} (${sourceLang}) -> 🎧 ${
        listener.email
      } (${targetLang}): "${cleanOrig}" => "${translatedText}"`
    );

    // 1. Emit original + translated subtitles to Listener
    io.to(listener.socketId).emit("translated_subtitle", {
      roomId,
      originalText: cleanOrig,
      translatedText: translatedText || cleanOrig,
      sourceLang,
      targetLang,
      isFinal: true,
    });

    // 2. Echo speaker's own subtitle back to Speaker
    if (speaker && speaker.translatorEnabled && speaker.hasMinutes) {
      io.to(speaker.socketId).emit("speaker_own_subtitle", {
        roomId,
        originalText: cleanOrig,
        translatedText: translatedText || cleanOrig,
        targetLang,
      });
    }

    // 3. Synthesize MP3 + send `spokenTextForTts` so the phone speaks it aloud over STREAM_VOICE_CALL!
    const mp3Base64 = await synthesizeTranslatedSpeechMp3Base64(
      translatedText || cleanOrig,
      targetLang
    );

    logTranslate(
      "TTS_VOICE_OUT",
      `🔊 Delivering translated voice (${targetLang}: "${translatedText}") to ${listener.email}`
    );

    io.to(listener.socketId).emit("translated_audio_chunk", {
      roomId,
      mp3Base64: mp3Base64 || null,
      audioFormat: "mp3",
      originalText: cleanOrig,
      translatedText: translatedText || cleanOrig,
      targetLang,
      timestamp: Date.now(),
    });
  } catch (e) {}
}

// ============================================================================
// 🧠 SINGLE-STEP GEMINI AUDIO TRANSCRIBE + TRANSLATE (ZERO HALLUCINATIONS!)
// ============================================================================
async function transcribeAndTranslateWavBuffer(
  roomId,
  speaker,
  listener,
  pcmBuffers
) {
  if (!pcmBuffers || pcmBuffers.length === 0) return;
  const wavBuf = buildWavBufferFromPcmBuffers(pcmBuffers, 16000);
  const wavBase64 = wavBuf.toString("base64");

  const sourceLangCode = String(speaker?.speakLang || "en")
    .split("-")[0]
    .toLowerCase();
  const targetLangCode = String(listener?.listenLang || "hi")
    .split("-")[0]
    .toLowerCase();
  const sourceLangName = getLangFullName(sourceLangCode);
  const targetLangName = getLangFullName(targetLangCode);

  const { GoogleGenAI } = await getGenAiSdk();
  const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

  const prompt = `You are a real-time voice call interpreter.
The speaker's selected language is ${sourceLangName} (or English/Hindi/Bengali).
The listener wants to hear ${targetLangName}.
Listen carefully to the audio:
1. If the audio only contains silence, breathing, static, or background noise with no clear spoken words, return exactly: {"original":"","translated":""}
2. If there is clear human speech, transcribe the spoken words into "original", and translate them naturally into ${targetLangName} in "translated".
Return ONLY valid JSON: {"original":"...","translated":"..."}`;

  for (const modelName of GEMINI_FLASH_MODELS) {
    try {
      const response = await ai.models.generateContent({
        model: modelName,
        contents: [
          {
            role: "user",
            parts: [
              {
                inlineData: {
                  mimeType: "audio/wav",
                  data: wavBase64,
                },
              },
              {
                text: prompt,
              },
            ],
          },
        ],
        config: {
          temperature: 0.0,
          responseMimeType: "application/json",
        },
      });

      const rawJson = String(response?.text || "").trim();
      if (!rawJson) return;

      let parsed = null;
      try {
        parsed = JSON.parse(rawJson);
      } catch (e) {
        const match = rawJson.match(/\{[\s\S]*\}/);
        if (match) parsed = JSON.parse(match[0]);
      }

      const orig = String(parsed?.original || "").trim();
      const trans = String(parsed?.translated || "").trim();

      if (orig.length > 1 && trans.length > 0) {
        await deliverTranslatedSpeechToListener(
          roomId,
          speaker,
          listener,
          orig,
          trans
        );
      }
      return;
    } catch (err) {
      // Try next Flash model if unavailable
    }
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

  // 1. JOIN / SYNC TRANSLATE ROOM
  socket.on("join_translate_room", async (payload) => {
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
      geminiSession: null,
      sessionTargetLang: null,
      isConnecting: false,
      lastConnectAttemptAt: 0,
      lastInputSentAt: 0,
      lastLiveAudioAt: 0,
      pcmChunkCounter: 0,
      vadPcmBuffers: [],
      vadSilenceCount: 0,
      vadSpeaking: false,
      lastTextTurnAt: 0,
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
      await ensureListenerGeminiSession(roomId, emailKey);
    } else {
      await closeListenerGeminiSession(participant);
    }
  });

  // 2. UPDATE TRANSLATE CONFIG
  socket.on("update_translate_config", async (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const emailKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;

    if (!roomId || !emailKey || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const participant = room.participants.get(emailKey);
    if (!participant) return;

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
      participant.lastConnectAttemptAt = 0;
      await ensureListenerGeminiSession(roomId, emailKey);
    } else {
      await closeListenerGeminiSession(participant);
    }
  });

  // 3. STREAM SPOKEN TEXT TURN
  socket.on("stream_spoken_text", async (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const text = String(payload?.text || "").trim();

    if (!roomId || !speakerKey || !text || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const speaker = room.participants.get(speakerKey);
    if (speaker) {
      speaker.lastTextTurnAt = Date.now();
      speaker.vadPcmBuffers = [];
      speaker.vadSpeaking = false;
    }

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;

      if (payload.isFinal === false) {
        const partialTrans = await translateTextWithGemini(
          text,
          listener.listenLang,
          speaker?.speakLang || "en"
        );
        io.to(listener.socketId).emit("translated_subtitle", {
          roomId,
          originalText: text,
          translatedText: partialTrans,
          targetLang: listener.listenLang,
          isFinal: false,
        });
      } else {
        await deliverTranslatedSpeechToListener(
          roomId,
          speaker,
          listener,
          text
        );
      }
    }
  });

  // 4. STREAM SPEAKER'S 16kHz PCM MICROPHONE AUDIO
  socket.on("stream_mic_pcm_16k", async (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const pcmBase64 = payload?.pcmBase64;

    if (!roomId || !speakerKey || !pcmBase64 || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);
    const speaker = room.participants.get(speakerKey);

    let pcmBuf = null;
    let rms = 0;
    try {
      pcmBuf = Buffer.from(pcmBase64, "base64");
      rms = calculatePcmBufferRms(pcmBuf);
    } catch (e) {
      return;
    }

    if (speaker) {
      speaker.pcmChunkCounter = (speaker.pcmChunkCounter || 0) + 1;
      if (speaker.pcmChunkCounter % 40 === 1 || rms > 0.025) {
        logTranslate(
          "MIC_PCM_IN",
          `🎙️ 16kHz PCM from ${speaker.email} (chunk #${
            speaker.pcmChunkCounter
          }, rms: ${rms.toFixed(4)})`
        );
      }
    }

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;

      let session = listener.geminiSession;
      if (!session && !listener.isConnecting) {
        session = await ensureListenerGeminiSession(roomId, listenerKey);
      }

      // 1. Stream into Gemini 3.5 Live session
      if (session) {
        listener.lastInputSentAt = Date.now();
        try {
          session.sendRealtimeInput({
            media: {
              data: pcmBase64,
              mimeType: "audio/pcm;rate=16000",
            },
          });
        } catch (err) {
          try {
            session.sendRealtimeInput({
              audio: {
                data: pcmBase64,
                mimeType: "audio/pcm;rate=16000",
              },
            });
          } catch (e2) {}
        }
      }

      // 2. Parallel VAD Utterance Buffer (Requires real speech RMS >= 0.022 so room noise is ignored!)
      if (speaker && Date.now() - (speaker.lastTextTurnAt || 0) > 3000) {
        if (rms >= 0.022) {
          speaker.vadSpeaking = true;
          speaker.vadSilenceCount = 0;
          speaker.vadPcmBuffers.push(pcmBuf);
          if (speaker.vadPcmBuffers.length >= 26) {
            const phrase = speaker.vadPcmBuffers.splice(
              0,
              speaker.vadPcmBuffers.length
            );
            speaker.vadSpeaking = false;
            transcribeAndTranslateWavBuffer(roomId, speaker, listener, phrase);
          }
        } else if (speaker.vadSpeaking) {
          speaker.vadSilenceCount += 1;
          speaker.vadPcmBuffers.push(pcmBuf);
          if (speaker.vadSilenceCount >= 4) {
            const phrase = speaker.vadPcmBuffers.splice(
              0,
              speaker.vadPcmBuffers.length
            );
            speaker.vadSpeaking = false;
            speaker.vadSilenceCount = 0;
            // Require at least ~600ms of speech (6 chunks)
            if (phrase.length >= 6) {
              transcribeAndTranslateWavBuffer(
                roomId,
                speaker,
                listener,
                phrase
              );
            }
          }
        }
      }
    }
  });

  // 5. DIRECT PARTNER PCM FORWARDING
  socket.on("translate_partner_pcm_16k", async (payload) => {
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

    let session = listener.geminiSession;
    if (!session && !listener.isConnecting) {
      session = await ensureListenerGeminiSession(roomId, listenerKey);
    }

    if (session) {
      listener.lastInputSentAt = Date.now();
      try {
        session.sendRealtimeInput({
          media: {
            data: pcmBase64,
            mimeType: "audio/pcm;rate=16000",
          },
        });
      } catch (e) {}
    }
  });

  // 6. CLEANUP ON LEAVE / DISCONNECT
  const cleanupSocketParticipant = async () => {
    const meta = socketToMeta.get(socket.id);
    socketToMeta.delete(socket.id);
    if (!meta) return;

    const { roomId, emailKey } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    const participant = room.participants.get(emailKey);
    if (participant && participant.socketId === socket.id) {
      await closeListenerGeminiSession(participant);
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