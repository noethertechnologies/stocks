from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import JSONResponse
import ollama

app = FastAPI()

# Enable CORS Middleware
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Replace "*" with the frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/process-image/")
async def process_image(
    instruction: str = Form(...),  # User's instruction
    image: UploadFile = File(...)  # Uploaded image
):
    """
    API endpoint to process an image and an instruction.
    """
    try:
        # Save the uploaded image to a temporary file
        temp_image_path = f"/tmp/{image.filename}"
        with open(temp_image_path, "wb") as f:
            f.write(await image.read())
        
        # Send the image and instruction to the model
        response = ollama.chat(
            model='llama3.2-vision',
            messages=[{
                'role': 'user',
                'content': instruction,
                'images': [temp_image_path]
            }]
        )
        
        # Extract the assistant's response
        assistant_message = response.get('message', {}).get('content', "No response received.")
        
        # Return the response as JSON
        return JSONResponse(content={
            "user_instruction": instruction,
            "response": assistant_message
        })
    
    except Exception as e:
        # Handle errors and return the error message
        return JSONResponse(content={"error": str(e)}, status_code=500)
