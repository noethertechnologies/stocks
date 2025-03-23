from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from crawl4ai import AsyncWebCrawler
from bs4 import BeautifulSoup
from fastapi.middleware.cors import CORSMiddleware
from groq import Groq
import os

# Initialize FastAPI app
app = FastAPI()

# Enable CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace "*" with the frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Groq client
client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# Pydantic model for input
class URLRequest(BaseModel):
    url: str

# Function to fetch and clean content
async def fetch_url_content(url: str) -> str:
    try:
        async with AsyncWebCrawler(verbose=True) as crawler:
            result = await crawler.arun(url=url)
            soup = BeautifulSoup(result.html, "html.parser")
            cleaned_text = soup.get_text(separator="\n").strip()
            return cleaned_text
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching content: {e}")

# Route to summarize URL content
@app.post("/summarize")
async def summarize_url(request: URLRequest):
    try:
        # Fetch cleaned content
        content = await fetch_url_content(request.url)
        
        # Truncate content if too large
        if len(content) > 10000:
            content = content[:10000] + " [Content truncated...]"
        
        # Summarize content using Groq
        chat_completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a summarizer."},
                {"role": "user", "content": f"Summarize this content: {content}"},
            ],
            model="llama3-8b-8192",  # Replace with the desired model
        )
        
        summary = chat_completion.choices[0].message.content
        return {"summary": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error summarizing content: {e}")
