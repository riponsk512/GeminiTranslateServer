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
const GEMINI_MODEL = "models/gemini-3.5-live-translate-preview";

// Prevent any unhandled WebSocket error from crashing the Render instance
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
// 🌐 SERVER-SIDE TRANSLATION & NEURAL VOICE AUDIO SYNTHESIS (FALLBACK ENGINE)
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

// Wrap 16kHz 16-bit mono PCM buffer into a 44-byte WAV header for REST transcription
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
// 🤖 CONNECT GEMINI 3.5 LIVE SESSION (WITH MULTI-ENDPOINT & FALLBACK GUARD)
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

  if (listener.liveWsUnsupported) {
    // Already switched to guaranteed server-side speech-to-speech pipeline
    io.to(listener.socketId).emit("gemini_session_status", {
      connected: true,
      mode: "server_hybrid",
      targetLanguageCode: targetLang,
    });
    return null;
  }

  if (listener.isConnecting) return null;
  listener.isConnecting = true;

  await closeListenerGeminiSession(listener);

  const attemptConfigs = [
    {
      label: "v1alpha",
      clientOpts: {
        apiKey: GEMINI_API_KEY,
        httpOptions: { apiVersion: "v1alpha" },
      },
      model: GEMINI_MODEL,
    },
    {
      label: "v1beta",
      clientOpts: {
        apiKey: GEMINI_API_KEY,
        httpOptions: { apiVersion: "v1beta" },
      },
      model: GEMINI_MODEL,
    },
    {
      label: "vertexai_express",
      clientOpts: {
        apiKey: GEMINI_API_KEY,
        vertexai: true,
      },
      model: GEMINI_MODEL,
    },
  ];

  const { GoogleGenAI, Modality, MediaResolution } = await getGenAiSdk();

  for (const attempt of attemptConfigs) {
    let activeSessionRef = null;
    let failedImmediately = false;

    try {
      const ai = new GoogleGenAI(attempt.clientOpts);
      const config = {
        responseModalities: [Modality?.AUDIO || "AUDIO"],
        mediaResolution:
          MediaResolution?.MEDIA_RESOLUTION_MEDIUM || "MEDIA_RESOLUTION_MEDIUM",
        inputAudioTranscription: {},
        outputAudioTranscription: {},
        contextWindowCompression: {
          triggerTokens: "0",
          slidingWindow: { targetTokens: "0" },
        },
        translationConfig: {
          targetLanguageCode: targetLang,
        },
      };

      logTranslate(
        "SESSION_CONNECTING",
        `Trying ${attempt.model} (${attempt.label}) for ${listener.email}`,
        { roomId, targetLanguageCode: targetLang }
      );

      let currentInputText = "";
      let currentOutputText = "";

      activeSessionRef = await ai.live.connect({
        model: attempt.model,
        config,
        callbacks: {
          onopen: () => {
            logTranslate(
              "SESSION_OPEN",
              `✅ Gemini Live (${attempt.label}) connected for ${listener.email} (-> ${targetLang})`
            );
            io.to(listener.socketId).emit("gemini_session_status", {
              connected: true,
              mode: attempt.label,
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
                io.to(listener.socketId).emit("translated_subtitle", {
                  roomId,
                  originalText: currentInputText,
                  translatedText: currentOutputText || null,
                  targetLang,
                  isFinal: false,
                });
              }

              if (sc.outputTranscription?.text) {
                currentOutputText =
                  `${currentOutputText} ${sc.outputTranscription.text}`.trim();
                io.to(listener.socketId).emit("translated_subtitle", {
                  roomId,
                  originalText: currentInputText || null,
                  translatedText: currentOutputText,
                  targetLang,
                  isFinal: false,
                });
              }

              const parts = sc.modelTurn?.parts || [];
              for (const part of parts) {
                if (part?.inlineData?.data) {
                  io.to(listener.socketId).emit("translated_audio_chunk", {
                    roomId,
                    pcm24kBase64: part.inlineData.data,
                    sampleRate: 24000,
                    targetLang,
                    timestamp: Date.now(),
                  });
                }
                if (part?.text && part.text.trim()) {
                  currentOutputText = part.text.trim();
                  io.to(listener.socketId).emit("translated_subtitle", {
                    roomId,
                    originalText: currentInputText || null,
                    translatedText: currentOutputText,
                    targetLang,
                    isFinal: false,
                  });
                }
              }

              if (sc.turnComplete || sc.generationComplete) {
                const finalOrig = currentInputText;
                let finalTrans = currentOutputText;
                currentInputText = "";
                currentOutputText = "";

                if (finalOrig && !finalTrans) {
                  finalTrans = await translateTextOnServer(
                    finalOrig,
                    targetLang,
                    "auto"
                  );
                }

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
            failedImmediately = true;
            logTranslate(
              "SESSION_WARN",
              `Attempt ${attempt.label} for ${listener.email}: ${
                err?.message || "404/unsupported"
              }`
            );
          },
          onclose: (ev) => {
            // Safe check: never throws ReferenceError even if closed during connect!
            if (
              activeSessionRef &&
              listener.geminiSession === activeSessionRef
            ) {
              listener.geminiSession = null;
            }
          },
        },
      });

      // Wait 350ms to verify the WebSocket didn't immediately close with 404
      await new Promise((r) => setTimeout(r, 350));

      if (activeSessionRef && !failedImmediately) {
        listener.geminiSession = activeSessionRef;
        listener.sessionTargetLang = targetLang;
        listener.isConnecting = false;
        return activeSessionRef;
      }
    } catch (err) {
      logTranslate(
        "ATTEMPT_SKIP",
        `${attempt.label} skipped for ${listener.email}: ${err?.message}`
      );
    }
  }

  // If all Bidi WebSocket endpoints returned 404 for this key, activate Server-Side Real-Time Speech-to-Speech Mode!
  listener.liveWsUnsupported = true;
  listener.isConnecting = false;
  logTranslate(
    "HYBRID_MODE_ACTIVE",
    `⚡ Activated Server-Side Real-Time Speech-to-Speech Pipeline for ${listener.email} (-> ${targetLang})`
  );
  io.to(listener.socketId).emit("gemini_session_status", {
    connected: true,
    mode: "server_hybrid",
    targetLanguageCode: targetLang,
  });
  return null;
}

// ============================================================================
// 🎙️ SERVER-SIDE SPEECH-TO-SPEECH TRANSLATION DELIVERER
// Translates spoken text or audio into Listener's `listenLang` & streams MP3 audio!
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
    // 1. Translate text into Listener's chosen listening language
    const translatedText = await translateTextOnServer(
      cleanOrig,
      targetLang,
      sourceLang
    );

    logTranslate(
      "TRANSLATED_SPEECH",
      `${speaker?.email || "Speaker"} (${sourceLang}) -> ${
        listener.email
      } (${targetLang}): "${cleanOrig}" => "${translatedText}"`
    );

    // 2. Emit live original + translated subtitles to Listener immediately
    io.to(listener.socketId).emit("translated_subtitle", {
      roomId,
      originalText: cleanOrig,
      translatedText: translatedText || cleanOrig,
      sourceLang,
      targetLang,
      isFinal: true,
    });

    // Also echo speaker's own transcript back to Speaker if Speaker has translator ON
    if (speaker && speaker.translatorEnabled && speaker.hasMinutes) {
      io.to(speaker.socketId).emit("speaker_own_subtitle", {
        roomId,
        originalText: cleanOrig,
        translatedText: translatedText || cleanOrig,
        targetLang,
      });
    }

    // 3. Synthesize high-clarity translated voice audio (MP3 Base64) and stream to Listener!
    const mp3Base64 = await synthesizeTranslatedSpeechMp3Base64(
      translatedText || cleanOrig,
      targetLang
    );

    if (mp3Base64) {
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

    io.to(listener.socketId).emit("translated_turn_complete", {
      roomId,
      originalText: cleanOrig,
      translatedText: translatedText || cleanOrig,
      targetLang,
      timestamp: Date.now(),
    });
  } catch (e) {}
}

// Server-side WAV utterance recognizer using Gemini REST / Vertex REST when Bidi WS is 404
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

  const endpoints = [
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${encodeURIComponent(
      GEMINI_API_KEY
    )}`,
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${encodeURIComponent(
      GEMINI_API_KEY
    )}`,
    `https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.0-flash-001:generateContent?key=${encodeURIComponent(
      GEMINI_API_KEY
    )}`,
  ];

  for (const url of endpoints) {
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
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
                  text: `Transcribe the exact human speech in this audio (spoken language hint: ${speakLangName}). Return ONLY the plain spoken words, or empty string if silence/noise.`,
                },
              ],
            },
          ],
          generationConfig: {
            temperature: 0.0,
          },
        }),
      });

      if (resp.ok) {
        const json = await resp.json();
        const transcript = String(
          json?.candidates?.[0]?.content?.parts?.[0]?.text || ""
        ).trim();
        if (transcript && transcript.length > 1) {
          await deliverTranslatedSpeechToListener(
            roomId,
            speaker,
            listener,
            transcript
          );
        }
        return;
      }
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
      liveWsUnsupported: false,
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

    broadcastRoomTranslateState(roomId);

    if (participant.translatorEnabled && participant.hasMinutes) {
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

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;

      if (payload.isFinal === false) {
        // Live partial subtitle preview
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
        // Final spoken sentence -> Translate & Stream Spoken Audio to Listener!
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

    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;
      if (!listener.translatorEnabled || !listener.hasMinutes) continue;

      // Path A: Active Gemini Live Bidi WebSocket Session
      if (listener.geminiSession) {
        try {
          listener.geminiSession.sendRealtimeInput({
            audio: {
              data: pcmBase64,
              mimeType: "audio/pcm;rate=16000",
            },
          });
          continue;
        } catch (err) {}
      }

      // Path B: Server-Side VAD Utterance Buffer (when Bidi WS is 404 and client STT didn't fire recently)
      if (speaker && Date.now() - (speaker.lastTextTurnAt || 0) > 4000) {
        try {
          const pcmBuf = Buffer.from(pcmBase64, "base64");
          const rms = calculatePcmBufferRms(pcmBuf);
          if (rms > 0.015) {
            speaker.vadSpeaking = true;
            speaker.vadSilenceCount = 0;
            speaker.vadPcmBuffers.push(pcmBuf);
            if (speaker.vadPcmBuffers.length >= 28) {
              const phrase = speaker.vadPcmBuffers.splice(
                0,
                speaker.vadPcmBuffers.length
              );
              speaker.vadSpeaking = false;
              transcribeWavBufferAndDeliver(roomId, speaker, listener, phrase);
            }
          } else if (speaker.vadSpeaking) {
            speaker.vadSilenceCount += 1;
            speaker.vadPcmBuffers.push(pcmBuf);
            if (speaker.vadSilenceCount >= 5) {
              const phrase = speaker.vadPcmBuffers.splice(
                0,
                speaker.vadPcmBuffers.length
              );
              speaker.vadSpeaking = false;
              speaker.vadSilenceCount = 0;
              if (phrase.length >= 4) {
                transcribeWavBufferAndDeliver(
                  roomId,
                  speaker,
                  listener,
                  phrase
                );
              }
            }
          }
        } catch (e) {}
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

    if (listener.geminiSession) {
      try {
        listener.geminiSession.sendRealtimeInput({
          audio: {
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
    model: GEMINI_MODEL,
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
    `🚀 GeminiTranslateServer (${GEMINI_MODEL}) running on 0.0.0.0:${PORT}`
  );
});