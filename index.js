
// GeminiTranslateServer/index.js

require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const { Server } = require("socket.io");
const { GoogleGenAI } = require("@google/genai");

// -----------------------------------------------------------------------------
// SERVER CONFIGURATION
// -----------------------------------------------------------------------------

const PORT = Number(process.env.PORT) || 10000;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_LIVE_MODEL =
  process.env.GEMINI_LIVE_MODEL || "gemini-3.5-live-translate-preview";

if (!GEMINI_API_KEY) {
  console.error("[FATAL] GEMINI_API_KEY is missing.");
  process.exit(1);
}

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
  maxHttpBufferSize: 10 * 1024 * 1024,
  pingInterval: 25000,
  pingTimeout: 20000,
  transports: ["websocket", "polling"],
});

const ai = new GoogleGenAI({
  apiKey: GEMINI_API_KEY,
});

// -----------------------------------------------------------------------------
// IN-MEMORY ROOMS
// -----------------------------------------------------------------------------

const rooms = new Map();
const socketToMeta = new Map();

function log(tag, message, extra) {
  const time = new Date().toISOString();

  if (extra !== undefined) {
    console.log(`[${time}] [${tag}] ${message}`, extra);
  } else {
    console.log(`[${time}] [${tag}] ${message}`);
  }
}

function getRoom(roomId) {
  return rooms.get(String(roomId));
}

function getParticipant(roomId, emailKey) {
  const room = getRoom(roomId);
  if (!room) return null;

  return room.participants.get(emailKey) || null;
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function normalizeLanguage(language) {
  const value = String(language || "en").trim();

  // Keep BCP-47 script and region subtags.
  // For example: zh-Hans, zh-Hant, pt-BR.
  if (!/^[a-zA-Z]{2,3}(-[a-zA-Z0-9]{2,8})*$/.test(value)) {
    return "en";
  }

  return value;
}

// -----------------------------------------------------------------------------
// PCM / WAV HELPERS
// -----------------------------------------------------------------------------

function buildWavBufferFromPcm(pcmBuffer, sampleRate = 24000) {
  const pcm = Buffer.from(pcmBuffer);
  const header = Buffer.alloc(44);

  header.write("RIFF", 0);
  header.writeUInt32LE(36 + pcm.length, 4);
  header.write("WAVE", 8);

  header.write("fmt ", 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20); // PCM
  header.writeUInt16LE(1, 22); // Mono
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(sampleRate * 2, 28);
  header.writeUInt16LE(2, 32); // 16-bit
  header.writeUInt16LE(16, 34);

  header.write("data", 36);
  header.writeUInt32LE(pcm.length, 40);

  return Buffer.concat([header, pcm]);
}

function isValidBase64Audio(value) {
  if (typeof value !== "string" || value.length < 8) {
    return false;
  }

  // Reject unusually large chunks. A normal 100ms PCM16 16kHz
  // mono chunk is approximately 4.3KB in base64.
  if (value.length > 500000) {
    return false;
  }

  return /^[A-Za-z0-9+/]+={0,2}$/.test(value);
}

// -----------------------------------------------------------------------------
// GEMINI SESSION CLEANUP
// -----------------------------------------------------------------------------

function closeGeminiSession(participant) {
  if (!participant) return;

  if (participant.audioFlushTimer) {
    clearTimeout(participant.audioFlushTimer);
    participant.audioFlushTimer = null;
  }

  const session = participant.geminiSession;

  participant.geminiSession = null;
  participant.sessionTargetLang = null;
  participant.audioBuffers = [];
  participant.audioBufferBytes = 0;
  participant.currentInputText = "";
  participant.currentOutputText = "";

  if (session) {
    try {
      session.close();
    } catch (error) {
      log("SESSION_CLOSE_ERROR", error.message);
    }
  }
}

// -----------------------------------------------------------------------------
// SEND GEMINI OUTPUT AUDIO TO LISTENER
// -----------------------------------------------------------------------------

function flushTranslatedAudio(roomId, participant) {
  if (!participant || !participant.audioBuffers.length) {
    return;
  }

  if (participant.audioFlushTimer) {
    clearTimeout(participant.audioFlushTimer);
    participant.audioFlushTimer = null;
  }

  const pcm = Buffer.concat(participant.audioBuffers);

  participant.audioBuffers = [];
  participant.audioBufferBytes = 0;

  if (!pcm.length) return;

  const wav = buildWavBufferFromPcm(pcm, 24000);

  const wavBase64 = wav.toString("base64");

  const socketId = participant.socketId;

  if (!socketId) return;

  io.to(socketId).emit("translated_audio_chunk", {
    roomId,
    wavBase64,
    audioFormat: "wav",
    sampleRate: 24000,
    originalText: participant.currentInputText || null,
    translatedText: participant.currentOutputText || null,
    targetLang: participant.listenLang,
    timestamp: Date.now(),
  });

  log("AUDIO_SENT", "Translated audio sent", {
    roomId,
    listener: participant.email,
    bytes: pcm.length,
  });
}

function queueTranslatedAudio(roomId, participant, pcmBuffer) {
  if (!pcmBuffer || !pcmBuffer.length) return;

  participant.audioBuffers.push(Buffer.from(pcmBuffer));
  participant.audioBufferBytes += pcmBuffer.length;

  // 24kHz * 16-bit mono = 48,000 bytes per second.
  // 14,400 bytes is approximately 300ms of audio.
  const packetSize = 14400;

  if (participant.audioBufferBytes >= packetSize) {
    flushTranslatedAudio(roomId, participant);
    return;
  }

  if (participant.audioFlushTimer) {
    clearTimeout(participant.audioFlushTimer);
  }

  participant.audioFlushTimer = setTimeout(() => {
    flushTranslatedAudio(roomId, participant);
  }, 180);
}

// -----------------------------------------------------------------------------
// GEMINI LIVE SESSION — ONE SESSION PER LISTENER
// -----------------------------------------------------------------------------

async function ensureListenerGeminiSession(roomId, listenerKey) {
  const room = getRoom(roomId);
  if (!room) return null;

  const listener = room.participants.get(listenerKey);
  if (!listener) return null;

  if (!listener.translatorEnabled || !listener.hasMinutes) {
    closeGeminiSession(listener);
    return null;
  }

  const targetLang = normalizeLanguage(listener.listenLang);

  if (
    listener.geminiSession &&
    listener.sessionTargetLang === targetLang
  ) {
    return listener.geminiSession;
  }

  if (listener.isConnecting) {
    return null;
  }

  listener.isConnecting = true;

  // Close the previous session before creating a new one.
  closeGeminiSession(listener);

  // closeGeminiSession clears audio state, so set this again.
  listener.isConnecting = true;

  try {
    const config = {
      responseModalities: ["AUDIO"],

      inputAudioTranscription: {},
      outputAudioTranscription: {},

      translationConfig: {
        targetLanguageCode: targetLang,
        echoTargetLanguage: true,
      },
    };

    log("SESSION_CONNECTING", "Connecting Gemini Live", {
      roomId,
      listener: listener.email,
      targetLang,
      model: GEMINI_LIVE_MODEL,
    });

    const session = await ai.live.connect({
      model: GEMINI_LIVE_MODEL,
      config,

      callbacks: {
        onopen: () => {
          log("SESSION_OPEN", "Gemini Live connected", {
            roomId,
            listener: listener.email,
            targetLang,
          });

          io.to(listener.socketId).emit("gemini_session_status", {
            connected: true,
            mode: "gemini-live-translate",
            targetLanguageCode: targetLang,
          });
        },

        onmessage: (message) => {
          try {
            const serverContent = message?.serverContent;
            if (!serverContent) return;

            // Handle input transcription.
            if (serverContent.inputTranscription?.text) {
              const text = String(
                serverContent.inputTranscription.text
              ).trim();

              if (text) {
                listener.currentInputText =
                  `${listener.currentInputText} ${text}`.trim();
              }
            }

            // Handle translated output transcription.
            if (serverContent.outputTranscription?.text) {
              const text = String(
                serverContent.outputTranscription.text
              ).trim();

              if (text) {
                listener.currentOutputText =
                  `${listener.currentOutputText} ${text}`.trim();
              }
            }

            // A single message may contain multiple audio parts.
            const parts = serverContent.modelTurn?.parts || [];

            for (const part of parts) {
              const inlineData = part?.inlineData;

              if (
                inlineData?.data &&
                String(inlineData.mimeType || "").startsWith("audio/")
              ) {
                const pcm = Buffer.from(inlineData.data, "base64");

                queueTranslatedAudio(roomId, listener, pcm);
              }
            }

            // Send subtitle updates.
            if (
              listener.currentInputText ||
              listener.currentOutputText
            ) {
              io.to(listener.socketId).emit("translated_subtitle", {
                roomId,
                originalText: listener.currentInputText || null,
                translatedText: listener.currentOutputText || null,
                targetLang,
                isFinal: Boolean(serverContent.turnComplete),
              });
            }

            // Flush the final audio packet at the end of a turn.
            if (
              serverContent.turnComplete ||
              serverContent.generationComplete
            ) {
              flushTranslatedAudio(roomId, listener);

              const originalText = listener.currentInputText;
              const translatedText = listener.currentOutputText;

              listener.currentInputText = "";
              listener.currentOutputText = "";

              io.to(listener.socketId).emit("translated_turn_complete", {
                roomId,
                originalText: originalText || null,
                translatedText: translatedText || null,
                targetLang,
                timestamp: Date.now(),
              });
            }

            // Clear the queued audio if Gemini interrupts generation.
            if (serverContent.interrupted) {
              if (listener.audioFlushTimer) {
                clearTimeout(listener.audioFlushTimer);
                listener.audioFlushTimer = null;
              }

              listener.audioBuffers = [];
              listener.audioBufferBytes = 0;

              io.to(listener.socketId).emit(
                "translated_audio_interrupted",
                { roomId }
              );
            }
          } catch (error) {
            log("MESSAGE_ERROR", error.message, {
              roomId,
              listener: listener.email,
            });
          }
        },

        onerror: (error) => {
          log("SESSION_ERROR", error?.message || "Gemini session error", {
            roomId,
            listener: listener.email,
          });

          io.to(listener.socketId).emit("gemini_session_status", {
            connected: false,
            error: error?.message || "Gemini session error",
          });
        },

        onclose: (event) => {
          log("SESSION_CLOSED", "Gemini session closed", {
            roomId,
            listener: listener.email,
            reason: event?.reason || "No reason provided",
          });

          if (listener.geminiSession === session) {
            listener.geminiSession = null;
            listener.sessionTargetLang = null;
          }

          io.to(listener.socketId).emit("gemini_session_status", {
            connected: false,
            reason: event?.reason || "Session closed",
          });
        },
      },
    });

    // Do not attach a session if the participant left or changed
    // configuration while the connection was being established.
    const currentRoom = getRoom(roomId);
    const currentListener = currentRoom?.participants.get(listenerKey);

    if (
      !currentListener ||
      currentListener !== listener ||
      !listener.translatorEnabled ||
      !listener.hasMinutes ||
      normalizeLanguage(listener.listenLang) !== targetLang
    ) {
      try {
        session.close();
      } catch (_) {}

      listener.isConnecting = false;
      return null;
    }

    listener.geminiSession = session;
    listener.sessionTargetLang = targetLang;
    listener.isConnecting = false;

    io.to(listener.socketId).emit("gemini_session_status", {
      connected: true,
      mode: "gemini-live-translate",
      targetLanguageCode: targetLang,
    });

    return session;
  } catch (error) {
    listener.isConnecting = false;

    log("SESSION_CONNECT_ERROR", error?.message || String(error), {
      roomId,
      listener: listener.email,
      targetLang,
    });

    io.to(listener.socketId).emit("gemini_session_status", {
      connected: false,
      error: error?.message || "Could not connect to Gemini",
    });

    return null;
  }
}

// -----------------------------------------------------------------------------
// SEND PCM AUDIO TO A LISTENER'S GEMINI SESSION
// -----------------------------------------------------------------------------

function sendPcmToGemini(session, pcmBase64) {
  if (!session || !pcmBase64) return false;

  try {
    // IMPORTANT:
    // Gemini Live translation expects "audio", not "media".
    session.sendRealtimeInput({
      audio: {
        data: pcmBase64,
        mimeType: "audio/pcm;rate=16000",
      },
    });

    return true;
  } catch (error) {
    log("PCM_SEND_ERROR", error.message);
    return false;
  }
}

// -----------------------------------------------------------------------------
// ROOM STATE
// -----------------------------------------------------------------------------

function broadcastRoomTranslateState(roomId) {
  const room = getRoom(roomId);
  if (!room) return;

  const participants = [];

  for (const participant of room.participants.values()) {
    participants.push({
      email: participant.email,
      speakLang: participant.speakLang,
      listenLang: participant.listenLang,
      translatorEnabled: Boolean(
        participant.translatorEnabled && participant.hasMinutes
      ),
    });
  }

  io.to(roomId).emit("room_translate_state", {
    roomId,
    participants,
    timestamp: Date.now(),
  });
}

// -----------------------------------------------------------------------------
// JOIN TRANSLATION ROOM
// -----------------------------------------------------------------------------

io.on("connection", (socket) => {
  log("SOCKET_CONNECT", "Client connected", {
    socketId: socket.id,
  });

  socket.on("join_translate_room", async (payload = {}) => {
    try {
      const roomId = String(payload.roomId || "").trim();
      const email = String(payload.email || "").trim();

      const emailKey = normalizeEmail(email);

      if (!roomId || !emailKey) {
        socket.emit("translate_error", {
          message: "roomId and email are required",
        });
        return;
      }

      let room = getRoom(roomId);

      if (!room) {
        room = {
          participants: new Map(),
        };

        rooms.set(roomId, room);
      }

      const existing = room.participants.get(emailKey);

      // If this same user reconnects with a new socket, close
      // their previous Gemini session.
      if (existing && existing.socketId !== socket.id) {
        closeGeminiSession(existing);
      }

      const participant = existing || {
        email,
        emailKey,
        socketId: socket.id,

        speakLang: "en",
        listenLang: "bn",

        translatorEnabled: false,
        hasMinutes: false,

        geminiSession: null,
        sessionTargetLang: null,
        isConnecting: false,

        audioBuffers: [],
        audioBufferBytes: 0,
        audioFlushTimer: null,

        currentInputText: "",
        currentOutputText: "",

        pcmChunkCounter: 0,
        lastInputSentAt: 0,
      };

      participant.email = email;
      participant.socketId = socket.id;

      participant.speakLang = normalizeLanguage(
        payload.speakLang || participant.speakLang
      );

      participant.listenLang = normalizeLanguage(
        payload.listenLang || participant.listenLang
      );

      participant.translatorEnabled = Boolean(
        payload.translatorEnabled
      );

      participant.hasMinutes = Boolean(payload.hasMinutes);

      room.participants.set(emailKey, participant);

      socketToMeta.set(socket.id, {
        roomId,
        emailKey,
      });

      socket.join(roomId);

      log("JOIN_ROOM", "User joined translation room", {
        roomId,
        email,
        speakLang: participant.speakLang,
        listenLang: participant.listenLang,
        translatorEnabled: participant.translatorEnabled,
        hasMinutes: participant.hasMinutes,
      });

      broadcastRoomTranslateState(roomId);

      if (participant.translatorEnabled && participant.hasMinutes) {
        await ensureListenerGeminiSession(roomId, emailKey);
      } else {
        closeGeminiSession(participant);
      }

      socket.emit("translate_room_joined", {
        roomId,
        email,
        success: true,
      });
    } catch (error) {
      log("JOIN_ERROR", error.message);

      socket.emit("translate_error", {
        message: error.message,
      });
    }
  });

  // ---------------------------------------------------------------------------
  // UPDATE TRANSLATION CONFIGURATION
  // ---------------------------------------------------------------------------

  socket.on("update_translate_config", async (payload = {}) => {
    try {
      const meta = socketToMeta.get(socket.id);
      if (!meta) return;

      const roomId = meta.roomId;
      const emailKey = meta.emailKey;

      const participant = getParticipant(roomId, emailKey);
      if (!participant || participant.socketId !== socket.id) return;

      const oldTargetLang = participant.listenLang;

      if (payload.speakLang !== undefined) {
        participant.speakLang = normalizeLanguage(payload.speakLang);
      }

      if (payload.listenLang !== undefined) {
        participant.listenLang = normalizeLanguage(payload.listenLang);
      }

      if (payload.translatorEnabled !== undefined) {
        participant.translatorEnabled = Boolean(
          payload.translatorEnabled
        );
      }

      if (payload.hasMinutes !== undefined) {
        participant.hasMinutes = Boolean(payload.hasMinutes);
      }

      log("CONFIG_UPDATED", "Translation configuration updated", {
        roomId,
        email: participant.email,
        speakLang: participant.speakLang,
        listenLang: participant.listenLang,
        translatorEnabled: participant.translatorEnabled,
        hasMinutes: participant.hasMinutes,
      });

      broadcastRoomTranslateState(roomId);

      const languageChanged =
        oldTargetLang !== participant.listenLang;

      if (
        !participant.translatorEnabled ||
        !participant.hasMinutes
      ) {
        closeGeminiSession(participant);
      } else if (languageChanged || !participant.geminiSession) {
        await ensureListenerGeminiSession(roomId, emailKey);
      }
    } catch (error) {
      log("CONFIG_UPDATE_ERROR", error.message);
    }
  });

  // ---------------------------------------------------------------------------
  // STREAM SPEAKER'S 16 kHz PCM AUDIO
  //
  // The speaker sends microphone audio here.
  // The server forwards it to every OTHER participant's Gemini session.
  // ---------------------------------------------------------------------------

  socket.on("stream_mic_pcm_16k", async (payload = {}) => {
    try {
      const meta = socketToMeta.get(socket.id);
      if (!meta) return;

      const roomId = meta.roomId;
      const speakerKey = meta.emailKey;

      const room = getRoom(roomId);
      if (!room) return;

      const speaker = room.participants.get(speakerKey);

      if (!speaker || speaker.socketId !== socket.id) return;

      const pcmBase64 = payload.pcmBase64;

      if (!isValidBase64Audio(pcmBase64)) return;

      const pcmBuffer = Buffer.from(pcmBase64, "base64");

      // PCM16 must contain complete 16-bit samples.
      if (!pcmBuffer.length || pcmBuffer.length % 2 !== 0) return;

      speaker.pcmChunkCounter += 1;

      if (speaker.pcmChunkCounter % 50 === 1) {
        log("MIC_PCM_IN", "Microphone PCM received", {
          roomId,
          speaker: speaker.email,
          chunk: speaker.pcmChunkCounter,
          bytes: pcmBuffer.length,
        });
      }

      // Forward speaker audio to every listener in this room.
      for (const [listenerKey, listener] of room.participants.entries()) {
        if (listenerKey === speakerKey) continue;

        if (
          !listener.translatorEnabled ||
          !listener.hasMinutes
        ) {
          continue;
        }

        let session = listener.geminiSession;

        if (!session && !listener.isConnecting) {
          session = await ensureListenerGeminiSession(
            roomId,
            listenerKey
          );
        }

        if (!session) continue;

        const sent = sendPcmToGemini(session, pcmBase64);

        if (sent) {
          listener.lastInputSentAt = Date.now();
        }
      }
    } catch (error) {
      log("MIC_STREAM_ERROR", error.message);
    }
  });

  // ---------------------------------------------------------------------------
  // OPTIONAL DIRECT PCM EVENT
  //
  // This event feeds the connected user's own listener session.
  // Do not send the same microphone chunk through BOTH this event and
  // stream_mic_pcm_16k, or Gemini will receive duplicate audio.
  // ---------------------------------------------------------------------------

  socket.on("translate_partner_pcm_16k", async (payload = {}) => {
    try {
      const meta = socketToMeta.get(socket.id);
      if (!meta) return;

      const roomId = meta.roomId;
      const emailKey = meta.emailKey;

      const participant = getParticipant(roomId, emailKey);

      if (
        !participant ||
        participant.socketId !== socket.id ||
        !participant.translatorEnabled ||
        !participant.hasMinutes
      ) {
        return;
      }

      const pcmBase64 = payload.pcmBase64;

      if (!isValidBase64Audio(pcmBase64)) return;

      let session = participant.geminiSession;

      if (!session && !participant.isConnecting) {
        session = await ensureListenerGeminiSession(
          roomId,
          emailKey
        );
      }

      if (session) {
        sendPcmToGemini(session, pcmBase64);
      }
    } catch (error) {
      log("PARTNER_PCM_ERROR", error.message);
    }
  });

  // ---------------------------------------------------------------------------
  // LEGACY TEXT EVENT
  //
  // Gemini Live Translate is an audio-to-audio translation model.
  // This event is retained so old clients do not crash, but it is not
  // used as a second translation pipeline.
  // ---------------------------------------------------------------------------

  socket.on("stream_spoken_text", () => {
    log(
      "LEGACY_TEXT_EVENT",
      "stream_spoken_text ignored; send microphone PCM instead"
    );
  });

  // ---------------------------------------------------------------------------
  // LEAVE / DISCONNECT
  // ---------------------------------------------------------------------------

  const cleanupParticipant = async () => {
    const meta = socketToMeta.get(socket.id);

    socketToMeta.delete(socket.id);

    if (!meta) return;

    const { roomId, emailKey } = meta;

    const room = getRoom(roomId);
    if (!room) return;

    const participant = room.participants.get(emailKey);

    // Do not remove a newer connection belonging to the same user.
    if (participant && participant.socketId === socket.id) {
      closeGeminiSession(participant);

      room.participants.delete(emailKey);

      log("LEAVE_ROOM", "User left translation room", {
        roomId,
        email: participant.email,
      });
    }

    if (room.participants.size === 0) {
      rooms.delete(roomId);
    } else {
      broadcastRoomTranslateState(roomId);
    }
  };

  socket.on("leave_translate_room", cleanupParticipant);
  socket.on("disconnect", cleanupParticipant);
});

// -----------------------------------------------------------------------------
// HEALTH CHECKS
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// START SERVER
// -----------------------------------------------------------------------------

server.listen(PORT, "0.0.0.0", () => {
  log("SERVER_START", "Gemini translation server started", {
    port: PORT,
    model: GEMINI_LIVE_MODEL,
  });
});

// -----------------------------------------------------------------------------
// PROCESS ERROR HANDLING
// -----------------------------------------------------------------------------

process.on("unhandledRejection", (reason) => {
  log(
    "UNHANDLED_REJECTION",
    reason?.message || String(reason)
  );
});

process.on("uncaughtException", (error) => {
  log("UNCAUGHT_EXCEPTION", error?.stack || error.message);
});
