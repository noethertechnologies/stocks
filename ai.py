import os
from dotenv import load_dotenv
import streamlit as st
import google.generativeai as genai
# Load environment variables from the .env file
load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-2.0-flash')

# Initialize session state for conversation history
if "messages" not in st.session_state:
    st.session_state.messages = []

st.title("AI Chatbot")

# Create a form for the user input with automatic clearing after submission
with st.form("chat_form", clear_on_submit=True):
    user_question = st.text_input("Enter your question:")
    submitted = st.form_submit_button("Send")
    if submitted and user_question:
        # Append user's message to session state
        st.session_state.messages.append({"role": "User", "content": user_question})
        
        # Get the AI response
        response = model.generate_content(user_question)
        ai_reply = response.text
        
        # Append AI's message to session state
        st.session_state.messages.append({"role": "AI", "content": ai_reply})

# Group messages into pairs (user and AI)
pairs = list(zip(st.session_state.messages[::2], st.session_state.messages[1::2]))

# Display the conversation with the latest pair first
for user_msg, ai_msg in reversed(pairs):
    # User message box: light blue background
    st.markdown(
        f"""
        <div style="background-color: #D0E6FF; padding: 10px; border-radius: 10px; margin-bottom: 5px;">
            <strong>User:</strong> {user_msg['content']}
        </div>
        """, unsafe_allow_html=True
    )
    # AI message box: light green background
    st.markdown(
        f"""
        <div style="background-color: #E0FFE0; padding: 10px; border-radius: 10px; margin-bottom: 15px;">
            <strong>AI:</strong> {ai_msg['content']}
        """, unsafe_allow_html=True
    )
