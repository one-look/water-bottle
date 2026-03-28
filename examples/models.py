""" 
Model listing for API key
"""

import os
from google import genai
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get API key from environment
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY not found in environment variables")

# Create client
client = genai.Client(api_key=api_key)

print("Checking available models...")
try:
    models = client.models.list_models()
    for m in models:
        if hasattr(m, 'supported_generation_methods') and 'generateContent' in m.supported_generation_methods:
            print(f"Model Name: {m.name}")
except Exception as e:
    print(f"Error: {e}")