# import os
# from dotenv import load_dotenv
# from google import genai

# # Load environment variables from the local .env file
# load_dotenv()

# def list_available_models():
#     # Ensure the key exists before calling the client
#     if not os.getenv("GEMINI_API_KEY"):
#         print("Error: GEMINI_API_KEY is not set in your environment or .env file.")
#         return

#     # Initialize the client (automatically reads GEMINI_API_KEY from os.environ)
#     client = genai.Client()

#     print("Fetching available models for your API key...\n")
#     try:
#         for model in client.models.list():
#             print(f"Model ID: {model.name}")
#             print(f"Display Name: {getattr(model, 'display_name', 'N/A')}")
#             print(f"Supported Methods: {getattr(model, 'supported_generation_methods', [])}")
#             print("-" * 60)
            
#     except Exception as e:
#         print(f"An error occurred while listing models: {e}")

# if __name__ == "__main__":
#     list_main = list_available_models()

# uv run tests/test_llm/test_gemini.py

# Fetching available models for your API key...

# Model ID: models/gemini-2.5-flash
# Display Name: Gemini 2.5 Flash
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-pro
# Display Name: Gemini 2.5 Pro
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-flash-preview-tts
# Display Name: Gemini 2.5 Flash Preview TTS
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-pro-preview-tts
# Display Name: Gemini 2.5 Pro Preview TTS
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemma-4-26b-a4b-it
# Display Name: Gemma 4 26B A4B IT
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemma-4-31b-it
# Display Name: Gemma 4 31B IT
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-flash-latest
# Display Name: Gemini Flash Latest
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-flash-lite-latest
# Display Name: Gemini Flash-Lite Latest
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-pro-latest
# Display Name: Gemini Pro Latest
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-flash-lite
# Display Name: Gemini 2.5 Flash-Lite
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-flash-image
# Display Name: Nano Banana
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3-flash-preview
# Display Name: Gemini 3 Flash Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-pro-preview
# Display Name: Gemini 3.1 Pro Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-pro-preview-customtools
# Display Name: Gemini 3.1 Pro Preview Custom Tools
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-flash-lite-preview
# Display Name: Gemini 3.1 Flash Lite Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-flash-lite
# Display Name: Gemini 3.1 Flash Lite
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3-pro-image-preview
# Display Name: Nano Banana Pro
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3-pro-image
# Display Name: Nano Banana Pro
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/nano-banana-pro-preview
# Display Name: Nano Banana Pro
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-flash-image-preview
# Display Name: Nano Banana 2
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-flash-image
# Display Name: Nano Banana 2
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-flash-lite-image
# Display Name: Nano Banana 2 Lite
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.5-flash
# Display Name: Gemini 3.5 Flash
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.5-flash-lite
# Display Name: Gemini 3.5 Flash Lite
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-omni-flash-preview
# Display Name: Gemini Omni Flash Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-omni-1.1-flash
# Display Name: Gemini Omni 1.1 Flash
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.5-transcribe
# Display Name: Gemini 3.5 Transcribe
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.6-flash
# Display Name: Gemini 3.6 Flash
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.7-flash
# Display Name: Gemini 3.7 Flash
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.8-flash
# Display Name: Gemini 3.8 Flash
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/lyria-3-clip-preview
# Display Name: Lyria 3 Clip Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/lyria-3-pro-preview
# Display Name: Lyria 3 Pro Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/lyria-3.5
# Display Name: Lyria 3.5
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-flash-tts-preview
# Display Name: Gemini 3.1 Flash TTS Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.8-flash-tts
# Display Name: Gemini 3.8 Flash TTS
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.8-flash-lite-tts
# Display Name: Gemini 3.8 Flash Lite TTS
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-robotics-er-2-preview
# Display Name: Gemini Robotics-ER 2 Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-computer-use-preview-10-2025
# Display Name: Gemini 2.5 Computer Use Preview 10-2025
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/antigravity-preview-05-2026
# Display Name: Antigravity Agent Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/antigravity-preview-09-2026
# Display Name: Antigravity Agent Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/antigravity-preview-latest
# Display Name: Antigravity Agent Preview Latest
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/deep-research-max-preview-04-2026
# Display Name: Deep Research Max Preview (Apr-21-2026)
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/deep-research-preview-04-2026
# Display Name: Deep Research Preview (Apr-21-2026)
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/deep-research-pro-preview-12-2025
# Display Name: Deep Research Pro Preview (Dec-12-2025)
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-embedding-001
# Display Name: Gemini Embedding 001
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-embedding-2-preview
# Display Name: Gemini Embedding 2 Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-embedding-2
# Display Name: Gemini Embedding 2
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/aqa
# Display Name: Model that performs Attributed Question Answering.
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/veo-3.1-generate-preview
# Display Name: Veo 3.1
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/veo-3.1-fast-generate-preview
# Display Name: Veo 3.1 fast
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/veo-3.1-lite-generate-preview
# Display Name: Veo 3.1 lite
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.5-transcribe-live
# Display Name: Gemini 3.5 Transcribe Live
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-flash-native-audio-latest
# Display Name: Gemini 2.5 Flash Native Audio Latest
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-flash-native-audio-preview-09-2025
# Display Name: Gemini 2.5 Flash Native Audio Preview 09-2025
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-2.5-flash-native-audio-preview-12-2025
# Display Name: Gemini 2.5 Flash Native Audio Preview 12-2025
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.1-flash-live-preview
# Display Name: Gemini 3.1 Flash Live Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.8-live
# Display Name: Gemini 3.8 Live
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.8-live-extended-thinking
# Display Name: Gemini 3.8 Live Extended Thinking
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-robotics-er-2-streaming-preview
# Display Name: Gemini Robotics-ER 2 Streaming Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/gemini-3.5-live-translate-preview
# Display Name: Gemini 3.5 Live Translate Preview
# Supported Methods: []
# ------------------------------------------------------------
# Model ID: models/lyria-realtime-exp
# Display Name: Lyria Realtime Experimental
# Supported Methods: []
# ------------------------------------------------------------