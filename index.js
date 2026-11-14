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

const PORT = process.env.PORT || 5001;
const GEMINI_API_KEY =
  process.env.GEMINI_API_KEY ||
  "AQ.Ab8RN6J18Lri4K34bssqzHMCnIAzIQBVQsxfGW31jGH5qerzJQ";
const GEMINI_MODEL = "models/gemini-3.5-live-translate-preview";

// ============================================================================
// 📝 STRUCTURED LOGGER
// ============================================================================
function logTranslate(tag, msg, meta = null) {
  const ts = new Date().toISOString();
  if (meta) {
    console.log(`[${ts}] [GEMINI:${tag}] ${msg}`, JSON.stringify(meta));
  } else {
    console.log(`[${ts}] [GEMINI:${tag}] ${msg}`);
  }
}

// Fast fallback text translator for subtitle enrichment
function translateTextFast(text, targetLang = "en", sourceLang = "auto") {
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

// ============================================================================
// 🗂️ IN-MEMORY ROOM & BIDIRECTIONAL GEMINI SESSION STORE
// ============================================================================
// roomId -> {
//   participants: Map<emailKey, {
//     email, socketId, speakLang, listenLang, translatorEnabled, hasMinutes,
//     geminiSession, sessionTargetLang, isConnecting
//   }>
// }
const rooms = new Map();
const socketToMeta = new Map(); // socket.id -> { roomId, emailKey }

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

// ============================================================================
// 🤖 CREATE OR UPDATE DEDICATED GEMINI 3.5 LIVE SESSION FOR A LISTENER
// ============================================================================
async function ensureListenerGeminiSession(roomId, listenerKey) {
  const room = rooms.get(roomId);
  if (!room) return null;
  const listener = room.participants.get(listenerKey);
  if (!listener) return null;

  // STRICT BILLING GATE: Only open a Gemini translation session if this listener has minutes & enabled Live Translate
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

  if (listener.isConnecting) return null;
  listener.isConnecting = true;

  await closeListenerGeminiSession(listener);

  try {
    const { GoogleGenAI, Modality, MediaResolution } = await getGenAiSdk();
    const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

    const config = {
      responseModalities: [Modality.AUDIO],
      mediaResolution: MediaResolution.MEDIA_RESOLUTION_MEDIUM,
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
      `Connecting ${GEMINI_MODEL} for listener ${listener.email}`,
      { roomId, targetLanguageCode: targetLang }
    );

    let currentInputText = "";
    let currentOutputText = "";

    const session = await ai.live.connect({
      model: GEMINI_MODEL,
      config,
      callbacks: {
        onopen: () => {
          logTranslate(
            "SESSION_OPEN",
            `✅ Gemini 3.5 Live session active for ${listener.email} (-> ${targetLang})`
          );
          io.to(listener.socketId).emit("gemini_session_status", {
            connected: true,
            targetLanguageCode: targetLang,
          });
        },
        onmessage: async (message) => {
          try {
            const sc = message?.serverContent;
            if (!sc) return;

            // 1. Original Speech Transcription
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

            // 2. Translated Speech Transcription
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

            // 3. Translated 24kHz PCM Audio Chunks & Model Text Parts
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

            // 4. Turn / Generation Complete -> Flush audio & finalize subtitles
            if (sc.turnComplete || sc.generationComplete) {
              const finalOrig = currentInputText;
              let finalTrans = currentOutputText;
              currentInputText = "";
              currentOutputText = "";

              if (finalOrig && !finalTrans) {
                finalTrans = await translateTextFast(
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
          logTranslate(
            "SESSION_ERROR",
            `Error for ${listener.email}: ${err?.message || "unknown"}`
          );
        },
        onclose: (ev) => {
          logTranslate(
            "SESSION_CLOSE",
            `Closed for ${listener.email} (reason: ${ev?.reason || "closed"})`
          );
          if (listener.geminiSession === session) {
            listener.geminiSession = null;
          }
          io.to(listener.socketId).emit("gemini_session_status", {
            connected: false,
            targetLanguageCode: targetLang,
          });
        },
      },
    });

    listener.geminiSession = session;
    listener.sessionTargetLang = targetLang;
    listener.isConnecting = false;
    return session;
  } catch (err) {
    listener.isConnecting = false;
    logTranslate(
      "CONNECT_FAILED",
      `Failed to connect Gemini Live for ${listener.email}: ${err?.message}`
    );
    return null;
  }
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

  // 1. JOIN / SYNC TRANSLATE ROOM STATE
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
    if (existing && existing.socketId !== socket.id) {
      existing.socketId = socket.id;
    }

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

  // 2. UPDATE LANGUAGE OR TRANSLATOR TOGGLE DURING ACTIVE CALL
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

  // 3. STREAM SPEAKER'S 16kHz PCM MICROPHONE AUDIO -> PARTNER'S GEMINI SESSION
  // Direction A -> B (Speaker = A, Listener = B) and Direction B -> A (Speaker = B, Listener = A)
  socket.on("stream_mic_pcm_16k", async (payload) => {
    const meta = socketToMeta.get(socket.id);
    const roomId = payload?.roomId || meta?.roomId;
    const speakerKey = payload?.email
      ? String(payload.email).trim().toLowerCase()
      : meta?.emailKey;
    const pcmBase64 = payload?.pcmBase64;

    if (!roomId || !speakerKey || !pcmBase64 || !rooms.has(roomId)) return;
    const room = rooms.get(roomId);

    // Find the OTHER participant (the listener) in this room
    for (const [listenerKey, listener] of room.participants.entries()) {
      if (listenerKey === speakerKey) continue;

      // STRICT ONE-SIDED BILLING RULE:
      // Only translate Speaker's voice for Listener if Listener has bought minutes & enabled Live Translate!
      if (!listener.translatorEnabled || !listener.hasMinutes) {
        continue;
      }

      let session = listener.geminiSession;
      if (!session && !listener.isConnecting) {
        session = await ensureListenerGeminiSession(roomId, listenerKey);
      }

      if (session) {
        try {
          session.sendRealtimeInput({
            audio: {
              data: pcmBase64,
              mimeType: "audio/pcm;rate=16000",
            },
          });
        } catch (err) {
          // Fallback for older @google/genai versions that expect `media`
          try {
            session.sendRealtimeInput({
              media: {
                data: pcmBase64,
                mimeType: "audio/pcm;rate=16000",
              },
            });
          } catch (e2) {}
        }
      }
    }
  });

  // 4. DIRECT PARTNER PCM FORWARDING (If Listener received Speaker's PCM over WebRTC DataChannel)
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
          audio: {
            data: pcmBase64,
            mimeType: "audio/pcm;rate=16000",
          },
        });
      } catch (err) {
        try {
          session.sendRealtimeInput({
            media: {
              data: pcmBase64,
              mimeType: "audio/pcm;rate=16000",
            },
          });
        } catch (e2) {}
      }
    }
  });

  // 5. LEAVE TRANSLATE ROOM / HANGUP CLEANUP
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