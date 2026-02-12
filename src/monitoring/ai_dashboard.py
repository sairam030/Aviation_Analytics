import streamlit as st
import google.generativeai as genai
import time
import os
import glob
from datetime import datetime

# --- CONFIGURATION ---
# Get API Key from environment variable
API_KEY = os.getenv("GEMINI_API_KEY")
LOG_DIR = "/opt/airflow/logs"
CODE_DIR = "/opt/airflow/src"

# Configure Gemini (using lite model for free tier)
if API_KEY:
    genai.configure(api_key=API_KEY)
    model = genai.GenerativeModel('models/gemini-2.5-flash-lite')

# --- HELPER FUNCTIONS ---
def get_full_code_context():
    """Reads project source code to give the LLM context for debugging."""
    # Limit context to avoid quota issues - only include key files
    context = "--- SOURCE CODE CONTEXT (Key Files Only) ---\n"
    key_dirs = ['/opt/airflow/dags', '/opt/airflow/src/speed', '/opt/airflow/src/serving']
    for key_dir in key_dirs:
        if os.path.exists(key_dir):
            for file in os.listdir(key_dir):
                if file.endswith('.py'):
                    path = os.path.join(key_dir, file)
                    try:
                        with open(path, 'r') as f:
                            # Only read first 200 lines to save tokens
                            lines = f.readlines()[:200]
                            context += f"\nFILE: {path} (first 200 lines)\n```\n{''.join(lines)}\n```\n"
                    except:
                        pass
    return context

def get_recent_logs(n_lines=100, hours_window=1):
    """Reads logs from the last N hours only (default: 1 hour window) to focus on CURRENT issues."""
    import time
    
    log_files = glob.glob(f"{LOG_DIR}/**/*.log", recursive=True)
    current_time = time.time()
    time_threshold = current_time - (hours_window * 3600)  # Convert hours to seconds
    
    # Filter files modified within the time window
    recent_files = [f for f in log_files if os.path.getmtime(f) >= time_threshold]
    recent_files.sort(key=os.path.getmtime, reverse=True)
    
    combined_logs = f"--- ANALYZING LOGS FROM LAST {hours_window} HOUR(S) ---\n"
    combined_logs += f"Found {len(recent_files)} log files modified in the last {hours_window} hour(s)\n\n"
    
    dag_summary = {}
    
    # Process only recent log files
    for log_file in recent_files:
        try:
            # Extract DAG name from path
            if 'dag_id=' in log_file:
                dag_name = log_file.split('dag_id=')[1].split('/')[0]
            elif 'scheduler' in log_file or 'wrapper' in log_file or 'spark' in log_file:
                dag_name = 'system-logs'
            else:
                dag_name = 'other'
            
            # Get file modification time for display
            mod_time = datetime.fromtimestamp(os.path.getmtime(log_file))
            
            with open(log_file, 'r') as f:
                lines = f.readlines()
                
                # Filter for important lines only
                important_lines = []
                for line in lines:
                    line_upper = line.upper()
                    if any(keyword in line_upper for keyword in ['ERROR', 'WARNING', 'CRITICAL', 'EXCEPTION', 
                                                                   'FAILED', 'TRACEBACK', 'DURATION', 'SUCCESS',
                                                                   'TASK INSTANCE', 'MARKING TASK', 'FATAL']):
                        important_lines.append(line)
                
                # Only include logs that have important info
                if important_lines:
                    if dag_name not in dag_summary:
                        dag_summary[dag_name] = []
                    
                    # Take last n_lines of important info from this file
                    relevant_content = "".join(important_lines[-n_lines:])
                    dag_summary[dag_name].append({
                        'file': log_file,
                        'content': relevant_content,
                        'line_count': len(important_lines),
                        'mod_time': mod_time
                    })
        except Exception as e:
            pass
    
    # Format summary by DAG
    for dag_name, log_entries in dag_summary.items():
        combined_logs += f"\n{'='*80}\n"
        combined_logs += f"DAG: {dag_name} ({len(log_entries)} recent log files)\n"
        combined_logs += f"{'='*80}\n"
        
        # Show most recent run details for each DAG
        for entry in log_entries[:3]:  # Top 3 runs per DAG
            combined_logs += f"\n--- {entry['file']} ---\n"
            combined_logs += f"Last Modified: {entry['mod_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            combined_logs += f"Relevant Lines: {entry['line_count']}\n"
            combined_logs += entry['content']
    
    if not dag_summary:
        combined_logs += "\n✅ No errors or warnings found in the last hour - pipeline appears healthy!"
    
    return combined_logs

# --- STREAMLIT UI ---
st.set_page_config(page_title="Aviation Pipeline AI Ops", layout="wide")
st.title("✈️ Aviation Pipeline AI Operations")

if not API_KEY:
    st.error("Please set GEMINI_API_KEY environment variable in docker-compose.yml")
    st.stop()

tab1, tab2 = st.tabs(["🔍 Live Log Analysis", "💬 Pipeline Chatbot"])

# --- TAB 1: LOG ANALYSIS (WATCHDOG) ---
with tab1:
    st.header("Micro-Batch Log Analysis")
    
    # Time window selector
    col1, col2 = st.columns([3, 1])
    with col1:
        analyze_button = st.button("🔍 Analyze Recent Logs Now", use_container_width=True)
    with col2:
        hours_window = st.selectbox("Time Window", [1, 2, 6, 12, 24], index=0, help="Analyze logs from the last N hours")
    
    if analyze_button:
        with st.spinner(f"Reading logs from last {hours_window} hour(s)..."):
            code_ctx = get_full_code_context()
            logs = get_recent_logs(hours_window=hours_window)
            
            prompt = f"""
            CONTEXT:
            You are a DevOps expert monitoring an Airflow/Spark pipeline.
            Here is the source code of the project:
            {code_ctx}

            TASK:
            Analyze the following recent log entries from the last {hours_window} hour(s). 
            1. Identify any ERRORS or WARNINGS that occurred in this time window.
            2. If an error exists, use the source code to explain exactly WHY it happened and suggest a fix.
            3. Ignore standard INFO messages unless they indicate a stall.
            4. Focus ONLY on issues from this time period - these are CURRENT issues.
            
            LOGS:
            {logs}
            """
            
            try:
                response = model.generate_content(prompt)
                st.markdown(response.text)
            except Exception as e:
                st.error(f"AI Error: {e}")

# --- TAB 2: CHATBOT ---
with tab2:
    st.header("Ask about Pipeline Status")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ex: 'How long did the last speed layer ingestion take?'"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Checking logs..."):
                # We fetch logs fresh for every query to get the latest stats (1 hour window for recent data)
                logs = get_recent_logs(n_lines=150, hours_window=1) 
                
                ai_prompt = f"""
                You are an assistant for the Aviation Data Pipeline.
                User Query: "{prompt}"
                
                Available Logs from the last hour (contains execution times and metrics):
                {logs}
                
                Answer the user's question based strictly on the logs provided.
                If the user asks about duration, look for 'Duration' or start/end timestamps.
                If no relevant information is found, say so clearly.
                """
                
                response = model.generate_content(ai_prompt)
                st.markdown(response.text)
                st.session_state.messages.append({"role": "assistant", "content": response.text})