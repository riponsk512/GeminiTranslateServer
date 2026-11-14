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
const GEMINI_FLASH_MODEL = "gemini-3.8-flash";

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
// 🌐 SERVER-SIDE TRANSLATION & VOICE AUDIO SYNTHESIS
// ============================================================================
function translateTextOnServer(text, targetLang = "en", sourceLang = "auto") {
  return new Promise((resolve) => {
    const cleanText = String(text || "").trim();
    if (!cleanText) return resolve("");
    const tl = String(targetLang || "en").split("-")[0].toLowerCase();
    const sl = String(sourceLang || "auto").split("-")[0].toLowerCase();
    const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=${encodeURIComponent(
      sl
    )}&tl=${encodeURIComponent(tl)}&dt=t&q=${encodeURIComponent(cleanText)}`;

    https
      .get(url, (res) => {
        let raw = "";
        res.on("data", (c) => (raw += c));
        res.on("end", () => {
          try {
            const json = JSON.parse(raw);
            if (Array.isArray(json) && Array.isArray(json[0])) {
              const out = json[0]
                .map((seg) => (Array.isArray(seg) ? seg[0] : ""))
                .join("");
              return resolve(out || cleanText);
            }
          } catch (e) {}
          resolve(cleanText);
        });
      })
      .on("error", () => resolve(cleanText));
  });
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

// ============================================================================
// 🗂️ IN-MEMORY ROOM & BIDIRECTIONAL SESSION STORE
// ============================================================================
const rooms = new Map();
const socketToMeta = new Map();

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

async function closeListenerGeminiSession(participant) {
  if (!participant) return;
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
// 🤖 CONNECT GEMINI 3.5 LIVE TRANSLATE (NO `mediaResolution` -> ZERO 400 ERRORS!)
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

  // Cooldown guard: never spam reconnects faster than once every 3 seconds
  const now = Date.now();
  if (
    listener.isConnecting ||
    now - (listener.lastConnectAttemptAt || 0) < 3000
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

    // Clean Audio-Only Config: NO `mediaResolution` (which caused "MediaResolution is not supported")!
    const config = {
      responseModalities: [Modality?.AUDIO || "AUDIO"],
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
                listener.lastLiveAudioAt = Date.now();
                logTranslate(
                  "GEMINI_AUDIO_OUT",
                  `🔊 Streaming 24kHz translated PCM chunk to ${listener.email} (${part.inlineData.data.length} chars)`
                );
                io.to(listener.socketId).emit("translated_audio_chunk", {
                  roomId,
                  pcm24kBase64: part.inlineData.data,
                  sampleRate: 24000,
                  targetLang,
                  timestamp: Date.now(),
                });
              }
              if (part?.text && part.text.trim()) {
                currentOutputText =
                  `${currentOutputText} ${part.text.trim()}`.trim();
              }
            }

            if (currentInputText || currentOutputText) {
              io.to(listener.socketId).emit("translated_subtitle", {
                roomId,
                originalText: currentInputText || null,
                translatedText: currentOutputText || null,
                targetLang,
                isFinal: Boolean(sc.turnComplete),
              });
            }

            if (sc.turnComplete || sc.generationComplete) {
              const finalOrig = currentInputText;
              const finalTrans = currentOutputText;
              currentInputText = "";
              currentOutputText = "";

              logTranslate(
                "GEMINI_TURN_DONE",
                `✅ Turn complete for ${listener.email}: "${
                  finalTrans || finalOrig || "audio_stream"
                }"`
              );

              io.to(listener.socketId).emit("translated_turn_complete", {
                roomId,
                originalText: finalOrig || null,
                translatedText: finalTrans || null,
                targetLang,
                timestamp: Date.now(),
              });
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
    return activeSessionRef;
  } catch (err) {
    listener.isConnecting = false;
    logTranslate(
      "SESSION_FALLBACK",
      `Using Server-Side Speech-to-Speech fallback for ${listener.email}: ${err?.message}`
    );
    return null;
  }
}

// ============================================================================
// 🎙️ DELIVER TRANSLATED SPEECH + AUDIO TO LISTENER
// ============================================================================
async function deliverTranslatedSpeechToListener(
  roomId,
  speaker,
  listener,
  originalText
) {
  const cleanOrig = String(originalText || "").trim();
  if (!cleanOrig || !listener.translatorEnabled || !listener.hasMinutes) return;

  const sourceLang = String(speaker?.speakLang || "auto")
    .split("-")[0]
    .toLowerCase();
  const targetLang = String(listener.listenLang || "en")
    .split("-")[0]
    .toLowerCase();

  try {
    const translatedText = await translateTextOnServer(
      cleanOrig,
      targetLang,
      sourceLang
    );

    logTranslate(
      "TRANSLATED_SPEECH",
      `🗣️ ${speaker?.email || "Speaker"} (${sourceLang}) -> 🎧 ${
        listener.email
      } (${targetLang}): "${cleanOrig}" => "${translatedText}"`
    );

    io.to(listener.socketId).emit("translated_subtitle", {
      roomId,
      originalText: cleanOrig,
      translatedText: translatedText || cleanOrig,
      sourceLang,
      targetLang,
      isFinal: true,
    });

    if (speaker && speaker.translatorEnabled && speaker.hasMinutes) {
      io.to(speaker.socketId).emit("speaker_own_subtitle", {
        roomId,
        originalText: cleanOrig,
        translatedText: translatedText || cleanOrig,
        targetLang,
      });
    }

    // If Gemini Live WebSocket didn't stream audio within 500ms, stream synthesized MP3 voice audio!
    setTimeout(async () => {
      const timeSinceLiveAudio = Date.now() - (listener.lastLiveAudioAt || 0);
      if (timeSinceLiveAudio < 1200) {
        return;
      }
      const mp3Base64 = await synthesizeTranslatedSpeechMp3Base64(
        translatedText || cleanOrig,
        targetLang
      );
      if (mp3Base64) {
        logTranslate(
          "TTS_MP3_OUT",
          `🔊 Sent translated MP3 voice audio (${targetLang}) to ${listener.email}`
        );
        io.to(listener.socketId).emit("translated_audio_chunk", {
          roomId,
          mp3Base64,
          audioFormat: "mp3",
          originalText: cleanOrig,
          translatedText: translatedText || cleanOrig,
          targetLang,
          timestamp: Date.now(),
        });
      }
    }, 500);
  } catch (e) {}
}

// Backup WAV transcriber using `gemini-3.8-flash` (as requested by Google's API response!)
async function transcribeWavBufferAndDeliver(
  roomId,
  speaker,
  listener,
  pcmBuffers
) {
  if (!pcmBuffers || pcmBuffers.length === 0) return;
  const wavBuf = buildWavBufferFromPcmBuffers(pcmBuffers, 16000);
  const wavBase64 = wavBuf.toString("base64");
  const speakLangName = speaker?.speakLang || "en";

  try {
    const { GoogleGenAI } = await getGenAiSdk();
    const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

    const response = await ai.models.generateContent({
      model: GEMINI_FLASH_MODEL,
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
              text: `Transcribe the exact human speech in this audio (language hint: ${speakLangName}). Return ONLY the plain spoken words, or empty string if silence/noise.`,
            },
          ],
        },
      ],
    });

    const transcript = String(response?.text || "").trim();
    if (transcript && transcript.length > 1) {
      await deliverTranslatedSpeechToListener(
        roomId,
        speaker,
        listener,
        transcript
      );
    }
  } catch (err) {
    logTranslate(
      "WAV_STT_WARN",
      `WAV backup transcription skipped: ${err?.message || "error"}`
    );
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

  // 3. STREAM SPOKEN TEXT TURN -> TRANSLATE & SYNTHESIZE AUDIO FOR LISTENER
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

    logTranslate("STT_IN", `📝 Speech from ${speakerKey}: "${text}"`, {
      isFinal: payload?.isFinal !== false,
    });

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;

      if (payload.isFinal === false) {
        const partialTrans = await translateTextOnServer(
          text,
          listener.listenLang,
          speaker?.speakLang || "auto"
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
      if (speaker.pcmChunkCounter % 25 === 1 || rms > 0.015) {
        logTranslate(
          "MIC_PCM_IN",
          `🎙️ Receiving 16kHz PCM from ${speaker.email} (chunk #${
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

      // 2. Parallel Server-Side VAD Utterance Buffer using `gemini-3.8-flash`
      if (speaker && Date.now() - (speaker.lastTextTurnAt || 0) > 3500) {
        if (rms > 0.012) {
          speaker.vadSpeaking = true;
          speaker.vadSilenceCount = 0;
          speaker.vadPcmBuffers.push(pcmBuf);
          if (speaker.vadPcmBuffers.length >= 26) {
            const phrase = speaker.vadPcmBuffers.splice(
              0,
              speaker.vadPcmBuffers.length
            );
            speaker.vadSpeaking = false;
            if (Date.now() - (listener.lastLiveAudioAt || 0) > 1400) {
              transcribeWavBufferAndDeliver(roomId, speaker, listener, phrase);
            }
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
            if (
              phrase.length >= 4 &&
              Date.now() - (listener.lastLiveAudioAt || 0) > 1400
            ) {
              transcribeWavBufferAndDeliver(roomId, speaker, listener, phrase);
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
    flashModel: GEMINI_FLASH_MODEL,
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