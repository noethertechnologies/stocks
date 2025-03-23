import os
import sys
from dotenv import load_dotenv
import google.generativeai as genai
# Load environment variables from .env file
load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-2.0-flash')
response = model.generate_content("write a short story on a rainy day!!! ")
print(response.text)
