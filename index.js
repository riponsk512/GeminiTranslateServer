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

// Primary active models for your API key
const GEMINI_PRIMARY_FLASH_MODEL = "models/gemini-3.8-flash";
const GEMINI_LIVE_MODEL = "models/gemini-3.5-live-translate-preview";

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
// 🌐 GEMINI 3.8 FLASH TRANSLATOR & GOOGLE TTS MP3 SYNTHESIZER
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
  sourceLangCode = "bn"
) {
  const cleanText = String(text || "").trim();
  if (!cleanText) return "";

  const targetName = getLangFullName(targetLangCode);
  const sourceName = getLangFullName(sourceLangCode);

  try {
    const { GoogleGenAI } = await getGenAiSdk();
    const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

    const resp = await ai.models.generateContent({
      model: GEMINI_PRIMARY_FLASH_MODEL,
      contents: `Translate the following spoken phrase from ${sourceName} into natural conversational ${targetName}. Return ONLY the translated text in ${targetName} script (no quotes, no explanations):\n\n${cleanText}`,
      config: {
        temperature: 0.1,
      },
    });
    const out = String(resp?.text || "").trim();
    if (out) return out;
  } catch (e) {
    logTranslate("TEXT_TRANS_ERR", `Text translate error: ${e?.message}`);
  }

  return cleanText;
}

function synthesizeTranslatedSpeechMp3Base64(text, targetLang = "hi") {
  return new Promise((resolve) => {
    const cleanText = String(text || "").trim();
    if (!cleanText) return resolve(null);
    const tl = String(targetLang || "hi").split("-")[0].toLowerCase();

    const endpoints = [
      `https://translate.google.com/translate_tts?ie=UTF-8&q=${encodeURIComponent(
        cleanText.slice(0, 200)
      )}&tl=${encodeURIComponent(tl)}&client=tw-ob`,
      `https://translate.googleapis.com/translate_tts?ie=UTF-8&q=${encodeURIComponent(
        cleanText.slice(0, 200)
      )}&tl=${encodeURIComponent(tl)}&client=gtx`,
    ];

    const tryDownload = (idx) => {
      if (idx >= endpoints.length) return resolve(null);
      https
        .get(
          endpoints[idx],
          {
            headers: {
              "User-Agent":
                "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            },
          },
          (res) => {
            if (res.statusCode !== 200) {
              return tryDownload(idx + 1);
            }
            const chunks = [];
            res.on("data", (d) => chunks.push(d));
            res.on("end", () => {
              try {
                const buf = Buffer.concat(chunks);
                if (buf.length > 150) {
                  return resolve(buf.toString("base64"));
                }
              } catch (e) {}
              tryDownload(idx + 1);
            });
          }
        )
        .on("error", () => tryDownload(idx + 1));
    };

    tryDownload(0);
  });
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
  const targetLang = String(listener.listenLang || "hi")
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

    // 3. Synthesize MP3 + send translated text so the phone plays MP3 AND/OR speaks via native TTS!
    const mp3Base64 = await synthesizeTranslatedSpeechMp3Base64(
      translatedText || cleanOrig,
      targetLang
    );

    logTranslate(
      "TTS_VOICE_OUT",
      `🔊 Sent translated voice (${targetLang}, mp3: ${
        mp3Base64 ? mp3Base64.length + " chars" : "tts_only"
      }) to ${listener.email}`
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

    io.to(listener.socketId).emit("translated_turn_complete", {
      roomId,
      originalText: cleanOrig,
      translatedText: translatedText || cleanOrig,
      targetLang,
      timestamp: Date.now(),
    });
  } catch (e) {}
}

// ============================================================================
// 🧠 GEMINI 3.8 FLASH AUDIO TRANSCRIBE + TRANSLATE (EXACT LANGUAGE LOCK!)
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

  try {
    const { GoogleGenAI } = await getGenAiSdk();
    const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

    const prompt = `You are a strict real-time voice call translator.
The speaker is speaking in ${sourceLangName}.
The listener wants the translation in ${targetLangName}.
Rules:
1. If the audio is silence, breathing, static, or background noise without clear human words, return exactly: {"original":"","translated":""}
2. NEVER output Russian, Vietnamese, or random languages. Transcribe the spoken words in ${sourceLangName} (or English if they spoke English words) into "original".
3. Translate "original" accurately into ${targetLangName} native script in "translated".
Return ONLY valid JSON: {"original":"...","translated":"..."}`;

    const response = await ai.models.generateContent({
      model: GEMINI_PRIMARY_FLASH_MODEL,
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
  } catch (err) {
    logTranslate(
      "WAV_TRANSLATE_ERR",
      `Audio translate error (${GEMINI_PRIMARY_FLASH_MODEL}): ${
        err?.message || err
      }`
    );
  }
}

// ============================================================================
// 🗂️ IN-MEMORY ROOM STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

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
      pcmChunkCounter: 0,
      vadPcmBuffers: [],
      vadSilenceCount: 0,
      vadSpeaking: false,
      isTranscribing: false,
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

    io.to(participant.socketId).emit("gemini_session_status", {
      connected: Boolean(participant.translatorEnabled && participant.hasMinutes),
      mode: GEMINI_PRIMARY_FLASH_MODEL,
      targetLanguageCode: participant.listenLang,
    });
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

    io.to(participant.socketId).emit("gemini_session_status", {
      connected: Boolean(participant.translatorEnabled && participant.hasMinutes),
      mode: GEMINI_PRIMARY_FLASH_MODEL,
      targetLanguageCode: participant.listenLang,
    });
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
    if (speaker.pcmChunkCounter % 40 === 1 || rms >= 0.025) {
      logTranslate(
        "MIC_PCM_IN",
        `🎙️ 16kHz PCM from ${speaker.email} (chunk #${
          speaker.pcmChunkCounter
        }, rms: ${rms.toFixed(4)})`
      );
    }

    // Find active listener(s) who have enabled Live Translate
    const activeListeners = [];
    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (listener.translatorEnabled && listener.hasMinutes) {
        activeListeners.push(listener);
      }
    }
    if (activeListeners.length === 0) return;

    // Smart VAD: Only record when speaker is actually speaking (rms >= 0.020)
    if (rms >= 0.020) {
      speaker.vadSpeaking = true;
      speaker.vadSilenceCount = 0;
      speaker.vadPcmBuffers.push(pcmBuf);

      // Flush every ~2.4 seconds of continuous speech (24 chunks of 100ms)
      if (speaker.vadPcmBuffers.length >= 24 && !speaker.isTranscribing) {
        const phrase = speaker.vadPcmBuffers.splice(
          0,
          speaker.vadPcmBuffers.length
        );
        speaker.vadSpeaking = false;
        speaker.isTranscribing = true;
        Promise.all(
          activeListeners.map((listener) =>
            transcribeAndTranslateWavBuffer(roomId, speaker, listener, phrase)
          )
        ).finally(() => {
          speaker.isTranscribing = false;
        });
      }
    } else if (speaker.vadSpeaking) {
      speaker.vadSilenceCount += 1;
      speaker.vadPcmBuffers.push(pcmBuf);

      // 350ms of silence after speaking -> transcribe & translate the spoken phrase immediately!
      if (speaker.vadSilenceCount >= 3) {
        const phrase = speaker.vadPcmBuffers.splice(
          0,
          speaker.vadPcmBuffers.length
        );
        speaker.vadSpeaking = false;
        speaker.vadSilenceCount = 0;

        if (phrase.length >= 4 && !speaker.isTranscribing) {
          speaker.isTranscribing = true;
          Promise.all(
            activeListeners.map((listener) =>
              transcribeAndTranslateWavBuffer(roomId, speaker, listener, phrase)
            )
          ).finally(() => {
            speaker.isTranscribing = false;
          });
        }
      }
    }
  });

  // 5. CLEANUP ON LEAVE / DISCONNECT
  const cleanupSocketParticipant = async () => {
    const meta = socketToMeta.get(socket.id);
    socketToMeta.delete(socket.id);
    if (!meta) return;

    const { roomId, emailKey } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    const participant = room.participants.get(emailKey);
    if (participant && participant.socketId === socket.id) {
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
    model: GEMINI_PRIMARY_FLASH_MODEL,
    liveModel: GEMINI_LIVE_MODEL,
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
    `🚀 GeminiTranslateServer (${GEMINI_PRIMARY_FLASH_MODEL}) running on 0.0.0.0:${PORT}`
  );
});