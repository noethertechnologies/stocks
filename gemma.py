import os
from dotenv import load_dotenv
import streamlit as st
from ollama import chat

# Load environment variables from the .env file
load_dotenv()

# Initialize session state for conversation history
if "messages" not in st.session_state:
    st.session_state.messages = []

st.title("AI Chatbot")

# Create a form for user input with automatic clearing after submission
with st.form("chat_form", clear_on_submit=True):
    user_question = st.text_input("Enter your question:")
    submitted = st.form_submit_button("Send")
    if submitted and user_question:
        # Append the user's message to session state
        st.session_state.messages.append({"role": "User", "content": user_question})
        
        # Use Ollama to stream the AI answer
        messages = [{'role': 'user', 'content': user_question}]
        stream = chat(model='gemma3:12b', messages=messages, stream=True)
        ai_reply = ""
        
        # Create a placeholder to display streaming response
        placeholder = st.empty()
        for chunk in stream:
            ai_reply += chunk['message']['content']
            placeholder.markdown(
                f"""
                <div style="background-color: #E0FFE0; padding: 10px; border-radius: 10px; margin-bottom: 15px;">
                    <strong>AI:</strong> {ai_reply}
                </div>
                """, unsafe_allow_html=True
            )
        # Clear the streaming placeholder after completion
        placeholder.empty()
        # Append the final AI answer to the conversation history
        st.session_state.messages.append({"role": "AI", "content": ai_reply})

# Group messages into pairs (each user message followed by an AI answer)
pairs = list(zip(st.session_state.messages[::2], st.session_state.messages[1::2]))

# Display the conversation history with the latest exchange first
for user_msg, ai_msg in reversed(pairs):
    # User message box: light blue background
    st.markdown(
        f"""
        <div style="background-color: #D0E6FF; padding: 10px; border-radius: 10px; margin-bottom: 5px;">
            <strong>User:</strong> {user_msg['content']}
        </div>
        """, unsafe_allow_html=True
    )
    # AI message box: light green background (with proper closing </div>)
    st.markdown(
        f"""
        <div style="background-color: #E0FFE0; padding: 10px; border-radius: 10px; margin-bottom: 15px;">
            <strong>AI:</strong> {ai_msg['content']}
        """, unsafe_allow_html=True
    )
