async function connectGeminiLive(listener, roomId, targetLang) {
  const { GoogleGenAI } = await import("@google/genai");

  const ai = new GoogleGenAI({
    apiKey: "AQ.Ab8RN6L5udfI-yMzJVNOyCfIvVtxVjR3mndu9P9EsSGd3HjpHQ",
    httpOptions: {
      apiVersion: "v1beta",
    },
  });

  let session = null;

  const config = {
    responseModalities: ["AUDIO"],

    inputAudioTranscription: {},
    outputAudioTranscription: {},

    translationConfig: {
      targetLanguageCode: targetLang,
      echoTargetLanguage: true,
    },
  };

  try {
    session = await ai.live.connect({
      model: GEMINI_LIVE_MODEL,
      config,

      callbacks: {
        onopen: () => {
          console.log("[GEMINI_CONNECTED]", {
            roomId,
            listener: listener.email,
            targetLang,
            model: GEMINI_LIVE_MODEL,
            apiVersion: "v1beta",
          });

          io.to(listener.socketId).emit(
            "gemini_session_status",
            {
              connected: true,
              targetLanguageCode: targetLang,
            }
          );
        },

        onmessage: async (message) => {
          try {
            const content = message?.serverContent;

            if (!content) return;

            if (content.inputTranscription?.text) {
              io.to(listener.socketId).emit(
                "translated_subtitle",
                {
                  roomId,
                  originalText:
                    content.inputTranscription.text,
                  translatedText: null,
                  targetLang,
                  isFinal: false,
                }
              );
            }

            if (content.outputTranscription?.text) {
              io.to(listener.socketId).emit(
                "translated_subtitle",
                {
                  roomId,
                  originalText: null,
                  translatedText:
                    content.outputTranscription.text,
                  targetLang,
                  isFinal: Boolean(content.turnComplete),
                }
              );
            }

            for (const part of content.modelTurn?.parts || []) {
              if (part?.inlineData?.data) {
                const pcmBuffer = Buffer.from(
                  part.inlineData.data,
                  "base64"
                );

                // Send the PCM to your existing WAV
                // packet-building/playback pipeline.
                listener.onGeminiAudio?.(pcmBuffer);
              }
            }
          } catch (error) {
            console.error("[GEMINI_MESSAGE_ERROR]", error);
          }
        },

        onerror: (error) => {
          console.error(
            "[GEMINI_SESSION_ERROR]",
            error?.message || error
          );
        },

        onclose: (event) => {
          console.log("[GEMINI_SESSION_CLOSED]", {
            roomId,
            reason: event?.reason || "No reason provided",
          });

          // Do not reference session here.
          // The connection is already closing.
        },
      },
    });

    listener.geminiSession = session;

    return session;
  } catch (error) {
    console.error("[GEMINI_CONNECT_FAILED]", {
      roomId,
      listener: listener.email,
      targetLang,
      message: error?.message || String(error),
    });

    listener.geminiSession = null;

    throw error;
  }
}