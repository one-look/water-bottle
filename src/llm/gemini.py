import time
from google import genai
from google.genai import errors, types
from typing import Optional

from src.core.logging import setup_logger

logger = setup_logger(__name__)

class GeminiProvider:
    '''
    Gemini LLM provider wrapper.
    '''
    def __init__(self, config: dict):
        self.model = config.get("model", "gemini-2.5-flash")
        self.temperature = config.get("temperature", 0.2)
        # Bumped to 4096 so large file writes/generations don't get trucated
        self.max_tokens = config.get("max_tokens", 4096)
        self.client = genai.Client()

        self.system_instruction = config.get(
            "system_instruction",
            "YYou are a helpful college assistant. Answer queries factually based on provided information."
        )

    def generate(self, prompt: str, system_instruction_override: Optional[str] = None) -> str:
        """Runs prompt through a chat session with automatic retries and trace logging.

        Args:
            prompt: User query or constructed RAG context prompt.
            system_instruction_override: Optional dynamic system prompt per tenant.

        Returns:
            Generated text string response from the LLM.

        Raises:
            RuntimeError: If all retries are exhausted or an unrecoverable API error occurs.
        """
        effective_instruction = system_instruction_override or self.system_instruction

        try:
            chat = self.client.chats.create(
                model=self.model,
                config=types.GenerateContentConfig(
                    system_instruction=effective_instruction,
                    temperature=self.temperature,
                    max_output_tokens=self.max_tokens,
                ),
            )
        except Exception as e:
            logger.error(f"Failed to initialize Gemini chat session: {str(e)}")
            raise RuntimeError(f"Could not initialize Gemini provider: {str(e)}") from e

        max_retries = 3
        backoff_seconds = 2.0

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Sending generation prompt to model '{self.model}' (Attempt {attempt}/{max_retries})")
                
                response = chat.send_message(prompt)

                # Safely log message history and function calls for debugging
                try:
                    for msg in chat.get_history():
                        if not hasattr(msg, "parts") or not msg.parts:
                            continue
                        for part in msg.parts:
                            if hasattr(part, "function_call") and part.function_call:
                                logger.debug(f"Function call executed: {part.function_call.name}")
                except Exception as log_err:
                    logger.warning(f"Failed to extract chat history debug trace: {str(log_err)}")

                if not response.text:
                    raise ValueError("Gemini returned an empty text response.")

                return response.text

            except (errors.APIError, errors.ClientError) as api_err:
                logger.warning(f"Gemini API Error on attempt {attempt}/{max_retries}: {str(api_err)}")
                if attempt == max_retries:
                    logger.error("Exhausted all retries for Gemini API request.")
                    raise RuntimeError(f"Gemini API error after {max_retries} attempts: {str(api_err)}") from api_err
                time.sleep(backoff_seconds * attempt)

            except Exception as e:
                logger.error(f"Unexpected error during generation on attempt {attempt}: {str(e)}", exc_info=True)
                if attempt == max_retries:
                    raise RuntimeError(f"LLM generation failed: {str(e)}") from e
                time.sleep(backoff_seconds * attempt)

        raise RuntimeError("LLM generation failed after maximum retries.")