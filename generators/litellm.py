import logging
import time
from typing import Dict, Any
import litellm
import asyncio
from .base import GeneratorBase

logger = logging.getLogger("water-bottle.generator.litellm")


class LiteLLMGenerator(GeneratorBase):
    """LiteLLM implementation for text generation."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize LiteLLM generator.
        
        Args:
            config: Configuration with model_name and other settings
        """
        self.model_name = config.get("model_name", "gpt-3.5-turbo")
        self.temperature = config.get("temperature", 0.7)
        self.max_tokens = config.get("max_tokens", 1000)
        self.api_base = config.get("api_base")
        self.api_key = config.get("api_key")
        
        # Configure litellm if needed
        if self.api_base:
            litellm.api_base = self.api_base
        if self.api_key:
            litellm.api_key = self.api_key
            
        logger.info("action=initialize generator=litellm model=%s temperature=%.1f max_tokens=%d", self.model_name, self.temperature, self.max_tokens)
    
    async def generate(self, prompt: str, **kwargs) -> str:
        """Generate text response based on prompt.
        
        Args:
            prompt: Input prompt for generation
            **kwargs: Additional generation parameters
            
        Returns:
            Generated text response
        """
        start_time = time.time()
        logger.info("action=generate generator=litellm model=%s prompt_length=%d", self.model_name, len(prompt))
        
        # Merge default parameters with any provided kwargs
        params = {
        "model": self.model_name,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": kwargs.get("temperature", self.temperature),
        "max_tokens": kwargs.get("max_tokens", self.max_tokens),
        "api_key": self.api_key,   # Pass locally to avoid global state issues
        "api_base": self.api_base,
    }
        
        # Add any additional parameters
        for key, value in kwargs.items():
            if key not in ["temperature", "max_tokens"]:
                params[key] = value
        
        try:
            response = await asyncio.to_thread(litellm.completion, **params)
            generated_text = response.choices[0].message.content
            duration = time.time() - start_time
            logger.info("action=generate_complete generator=litellm model=%s response_length=%d duration=%.3fs tokens_used=%d", self.model_name, len(generated_text), duration, response.usage.total_tokens if hasattr(response, 'usage') and response.usage else 'unknown')
            return generated_text
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error("action=generate_failed generator=litellm model=%s duration=%.3fs error=%s", self.model_name, duration, str(e))
            raise RuntimeError(f"LiteLLM generation failed: {str(e)}")
