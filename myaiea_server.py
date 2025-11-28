"""
MyAiEa - Voice-enabled AI Assistant for Company Data
Uses Gemini LLM with MCP tools for querying company database
"""

import os
import json
import requests
from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

app = Flask(__name__)
CORS(app)

# Configure Gemini
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

MCP_SERVER_URL = os.getenv('MCP_SERVER_URL', 'http://localhost:5002')

# System prompt for the AI
SYSTEM_PROMPT = """You are MyAiEa, a helpful voice assistant specialized in UK company data.

You have access to a database of 5.68 million UK companies with information including:
- Company names, numbers, addresses
- Directors/officers
- Email addresses
- Phone numbers
- Websites
- SIC codes (industry classification)

You can ONLY answer questions about companies in this database. You cannot:
- Answer general knowledge questions
- Browse the internet
- Provide information not in your tools

When asked about a company:
1. First search for it in the database
2. If found, provide the requested information
3. If directors/emails are missing, you can fetch them using the appropriate tools
4. Always be concise in voice responses

For voice responses, keep answers brief and natural. Speak in a friendly, professional tone.

If you cannot find information, say so clearly. Do not make up data.

Available tools:
- search_companies: Find companies by name, SIC code, or postcode
- get_company_details: Get full details for a company
- get_directors: Get directors for a company (fetches from API if needed)
- get_company_website: Find website for a company
- get_company_emails: Get known emails for a company
- find_email_for_person: Find email for a specific person (uses Hunter.io credits)
- get_company_phone: Get phone number for a company
"""


def call_mcp_tool(tool_name, parameters):
    """Call an MCP tool and return results"""
    try:
        response = requests.post(
            f"{MCP_SERVER_URL}/mcp/execute",
            json={"tool": tool_name, "parameters": parameters},
            timeout=30
        )
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def process_with_gemini(user_message, conversation_history=None):
    """Process user message with Gemini, using function calling"""
    if not GEMINI_API_KEY:
        return {"error": "Gemini API key not configured", "response": "I'm sorry, I'm not configured properly. Please add a Gemini API key."}
    
    try:
        # Get available tools from MCP
        tools_response = requests.get(f"{MCP_SERVER_URL}/mcp/tools", timeout=5)
        mcp_tools = tools_response.json().get('tools', [])
        
        # Convert MCP tools to Gemini function declarations
        gemini_tools = []
        for tool in mcp_tools:
            gemini_tools.append({
                "name": tool["name"],
                "description": tool["description"],
                "parameters": tool["parameters"]
            })
        
        # Create the model with tools
        model = genai.GenerativeModel(
            model_name='gemini-2.0-flash',
            tools=[{"function_declarations": gemini_tools}],
            system_instruction=SYSTEM_PROMPT
        )
        
        # Start chat
        chat = model.start_chat(history=conversation_history or [])
        
        # Send user message
        response = chat.send_message(user_message)
        
        # Check if model wants to call functions
        function_calls = []
        final_response = None
        
        while response.candidates[0].content.parts:
            part = response.candidates[0].content.parts[0]
            
            # Check for function call
            if hasattr(part, 'function_call') and part.function_call:
                fc = part.function_call
                tool_name = fc.name
                parameters = dict(fc.args) if fc.args else {}
                
                # Execute the tool via MCP
                tool_result = call_mcp_tool(tool_name, parameters)
                function_calls.append({
                    "tool": tool_name,
                    "parameters": parameters,
                    "result": tool_result
                })
                
                # Send function result back to model
                response = chat.send_message(
                    genai.protos.Content(
                        parts=[genai.protos.Part(
                            function_response=genai.protos.FunctionResponse(
                                name=tool_name,
                                response={"result": tool_result}
                            )
                        )]
                    )
                )
            else:
                # Text response
                final_response = part.text
                break
        
        return {
            "response": final_response or "I couldn't process that request.",
            "function_calls": function_calls,
            "conversation_history": [{"role": m.role, "parts": [p.text for p in m.parts if hasattr(p, 'text')]} for m in chat.history]
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "response": f"Sorry, I encountered an error: {str(e)}"
        }


@app.route('/')
def index():
    """Serve the MyAiEa voice interface"""
    return render_template_string(MYAIEA_HTML)


@app.route('/api/chat', methods=['POST'])
def chat():
    """Process a chat message"""
    data = request.json
    user_message = data.get('message', '')
    history = data.get('history', [])
    
    result = process_with_gemini(user_message, history)
    return jsonify(result)


@app.route('/api/health', methods=['GET'])
def health():
    """Health check"""
    mcp_status = "unknown"
    try:
        r = requests.get(f"{MCP_SERVER_URL}/mcp/health", timeout=2)
        if r.status_code == 200:
            mcp_status = "connected"
    except:
        mcp_status = "disconnected"
    
    return jsonify({
        "status": "ok",
        "gemini_configured": bool(GEMINI_API_KEY),
        "mcp_server": mcp_status
    })


# =============================================================================
# HTML Template for MyAiEa Voice Interface
# =============================================================================

MYAIEA_HTML = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MyAiEa - AI Company Assistant</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: linear-gradient(135deg, #0a0a0f 0%, #1a1a2e 50%, #16213e 100%);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            color: #fff;
            overflow: hidden;
        }
        
        .container {
            text-align: center;
            padding: 2rem;
            max-width: 600px;
        }
        
        h1 {
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
            background: linear-gradient(90deg, #00d9ff, #00ff88);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .subtitle {
            color: #888;
            margin-bottom: 2rem;
            font-size: 1.1rem;
        }
        
        /* Lips Container */
        .lips-container {
            position: relative;
            width: 300px;
            height: 200px;
            margin: 0 auto 2rem;
        }
        
        /* SVG Lips */
        .lips {
            width: 100%;
            height: 100%;
        }
        
        .lip-upper, .lip-lower {
            fill: #ff4757;
            transition: d 0.05s ease;
        }
        
        .lip-highlight {
            fill: rgba(255, 255, 255, 0.3);
        }
        
        /* Equalizer bars inside lips */
        .equalizer {
            position: absolute;
            bottom: 60px;
            left: 50%;
            transform: translateX(-50%);
            display: flex;
            gap: 4px;
            align-items: flex-end;
            height: 40px;
        }
        
        .eq-bar {
            width: 6px;
            background: linear-gradient(to top, #00d9ff, #00ff88);
            border-radius: 3px;
            height: 5px;
            transition: height 0.05s ease;
        }
        
        /* Status indicator */
        .status {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            margin-bottom: 1.5rem;
        }
        
        .status-dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #666;
        }
        
        .status-dot.listening {
            background: #00ff88;
            box-shadow: 0 0 10px #00ff88;
            animation: pulse 1s infinite;
        }
        
        .status-dot.speaking {
            background: #00d9ff;
            box-shadow: 0 0 10px #00d9ff;
            animation: pulse 0.5s infinite;
        }
        
        .status-dot.thinking {
            background: #ffaa00;
            box-shadow: 0 0 10px #ffaa00;
            animation: pulse 0.3s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.7; transform: scale(1.1); }
        }
        
        .status-text {
            font-size: 0.9rem;
            color: #aaa;
        }
        
        /* Controls */
        .controls {
            display: flex;
            gap: 1rem;
            justify-content: center;
            margin-bottom: 1.5rem;
        }
        
        .btn {
            padding: 1rem 2rem;
            border: none;
            border-radius: 50px;
            cursor: pointer;
            font-size: 1rem;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        
        .btn-primary {
            background: linear-gradient(90deg, #00d9ff, #00ff88);
            color: #000;
        }
        
        .btn-primary:hover {
            transform: scale(1.05);
            box-shadow: 0 5px 20px rgba(0, 217, 255, 0.4);
        }
        
        .btn-primary:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
        }
        
        .btn-secondary {
            background: rgba(255, 255, 255, 0.1);
            color: #fff;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        /* Transcript */
        .transcript {
            background: rgba(0, 0, 0, 0.3);
            border-radius: 12px;
            padding: 1rem;
            margin-top: 1rem;
            max-height: 200px;
            overflow-y: auto;
            text-align: left;
        }
        
        .transcript-line {
            margin-bottom: 0.5rem;
            padding: 0.5rem;
            border-radius: 8px;
        }
        
        .transcript-line.user {
            background: rgba(0, 217, 255, 0.1);
            border-left: 3px solid #00d9ff;
        }
        
        .transcript-line.assistant {
            background: rgba(0, 255, 136, 0.1);
            border-left: 3px solid #00ff88;
        }
        
        .transcript-line .role {
            font-size: 0.75rem;
            color: #888;
            margin-bottom: 4px;
        }
        
        /* Text input fallback */
        .text-input-container {
            display: flex;
            gap: 0.5rem;
            margin-top: 1rem;
        }
        
        .text-input {
            flex: 1;
            padding: 0.75rem 1rem;
            border-radius: 25px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            background: rgba(0, 0, 0, 0.3);
            color: #fff;
            font-size: 1rem;
        }
        
        .text-input:focus {
            outline: none;
            border-color: #00d9ff;
        }
        
        /* Connection status */
        .connection-status {
            position: fixed;
            bottom: 20px;
            right: 20px;
            display: flex;
            gap: 10px;
            font-size: 0.8rem;
            color: #666;
        }
        
        .connection-badge {
            display: flex;
            align-items: center;
            gap: 5px;
            padding: 5px 10px;
            background: rgba(0, 0, 0, 0.5);
            border-radius: 20px;
        }
        
        .connection-badge.connected::before {
            content: '';
            width: 8px;
            height: 8px;
            background: #00ff88;
            border-radius: 50%;
        }
        
        .connection-badge.disconnected::before {
            content: '';
            width: 8px;
            height: 8px;
            background: #ff4757;
            border-radius: 50%;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>MyAiEa</h1>
        <p class="subtitle">Your AI Company Data Assistant</p>
        
        <!-- Animated Lips with Equalizer -->
        <div class="lips-container">
            <svg class="lips" viewBox="0 0 200 120">
                <!-- Upper Lip -->
                <path class="lip-upper" id="upper-lip" 
                    d="M 30,60 
                       Q 50,45 75,50 
                       Q 100,40 125,50 
                       Q 150,45 170,60 
                       Q 150,65 125,60 
                       Q 100,55 75,60 
                       Q 50,65 30,60 Z"/>
                
                <!-- Lower Lip -->
                <path class="lip-lower" id="lower-lip"
                    d="M 30,60 
                       Q 50,65 75,60 
                       Q 100,55 125,60 
                       Q 150,65 170,60 
                       Q 150,85 100,90 
                       Q 50,85 30,60 Z"/>
                
                <!-- Highlight -->
                <ellipse class="lip-highlight" cx="100" cy="55" rx="30" ry="8" opacity="0.3"/>
            </svg>
            
            <!-- Equalizer inside lips -->
            <div class="equalizer" id="equalizer">
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
                <div class="eq-bar"></div>
            </div>
        </div>
        
        <!-- Status -->
        <div class="status">
            <div class="status-dot" id="status-dot"></div>
            <span class="status-text" id="status-text">Click to speak</span>
        </div>
        
        <!-- Controls -->
        <div class="controls">
            <button class="btn btn-primary" id="speak-btn" onclick="toggleListening()">
                🎤 Hold to Speak
            </button>
            <button class="btn btn-secondary" onclick="stopSpeaking()">
                ⏹️ Stop
            </button>
        </div>
        
        <!-- Text input fallback -->
        <div class="text-input-container">
            <input type="text" class="text-input" id="text-input" 
                   placeholder="Or type your question here..." 
                   onkeypress="if(event.key==='Enter') sendTextMessage()">
            <button class="btn btn-primary" onclick="sendTextMessage()">Send</button>
        </div>
        
        <!-- Transcript -->
        <div class="transcript" id="transcript">
            <div class="transcript-line assistant">
                <div class="role">MyAiEa</div>
                <div>Hello! I'm MyAiEa, your AI assistant for UK company data. Ask me about any company - their directors, emails, websites, or contact details.</div>
            </div>
        </div>
    </div>
    
    <!-- Connection Status -->
    <div class="connection-status">
        <div class="connection-badge" id="mcp-status">MCP Server</div>
        <div class="connection-badge" id="gemini-status">Gemini AI</div>
    </div>
    
    <script>
        // State
        let isListening = false;
        let isSpeaking = false;
        let recognition = null;
        let synthesis = window.speechSynthesis;
        let conversationHistory = [];
        let audioContext = null;
        let analyser = null;
        
        // Initialize speech recognition
        if ('webkitSpeechRecognition' in window || 'SpeechRecognition' in window) {
            const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
            recognition = new SpeechRecognition();
            recognition.continuous = false;
            recognition.interimResults = true;
            recognition.lang = 'en-GB';
            
            recognition.onstart = () => {
                isListening = true;
                updateStatus('listening', 'Listening...');
            };
            
            recognition.onend = () => {
                isListening = false;
                if (!isSpeaking) {
                    updateStatus('idle', 'Click to speak');
                }
            };
            
            recognition.onresult = (event) => {
                const transcript = Array.from(event.results)
                    .map(result => result[0].transcript)
                    .join('');
                
                if (event.results[0].isFinal) {
                    processUserInput(transcript);
                }
            };
            
            recognition.onerror = (event) => {
                console.error('Speech recognition error:', event.error);
                updateStatus('idle', 'Click to speak');
            };
        }
        
        // Toggle listening
        function toggleListening() {
            if (!recognition) {
                alert('Speech recognition not supported in this browser. Please use Chrome.');
                return;
            }
            
            if (isListening) {
                recognition.stop();
            } else {
                recognition.start();
            }
        }
        
        // Process user input
        async function processUserInput(text) {
            if (!text.trim()) return;
            
            // Add to transcript
            addToTranscript('user', text);
            
            // Update status
            updateStatus('thinking', 'Thinking...');
            
            try {
                const response = await fetch('/api/chat', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        message: text,
                        history: conversationHistory
                    })
                });
                
                const data = await response.json();
                
                if (data.response) {
                    addToTranscript('assistant', data.response);
                    speakResponse(data.response);
                    
                    // Update history
                    if (data.conversation_history) {
                        conversationHistory = data.conversation_history;
                    }
                }
            } catch (error) {
                console.error('Error:', error);
                addToTranscript('assistant', 'Sorry, I encountered an error. Please try again.');
                updateStatus('idle', 'Click to speak');
            }
        }
        
        // Send text message
        function sendTextMessage() {
            const input = document.getElementById('text-input');
            const text = input.value.trim();
            if (text) {
                input.value = '';
                processUserInput(text);
            }
        }
        
        // Speak response using Web Speech API
        function speakResponse(text) {
            if (!synthesis) return;
            
            // Cancel any ongoing speech
            synthesis.cancel();
            
            const utterance = new SpeechSynthesisUtterance(text);
            utterance.rate = 1.0;
            utterance.pitch = 1.0;
            utterance.volume = 1.0;
            
            // Try to get a nice British voice
            const voices = synthesis.getVoices();
            const britishVoice = voices.find(v => v.lang === 'en-GB') || voices[0];
            if (britishVoice) {
                utterance.voice = britishVoice;
            }
            
            utterance.onstart = () => {
                isSpeaking = true;
                updateStatus('speaking', 'Speaking...');
                startLipAnimation();
            };
            
            utterance.onend = () => {
                isSpeaking = false;
                updateStatus('idle', 'Click to speak');
                stopLipAnimation();
            };
            
            synthesis.speak(utterance);
        }
        
        // Stop speaking
        function stopSpeaking() {
            if (synthesis) {
                synthesis.cancel();
            }
            isSpeaking = false;
            stopLipAnimation();
            updateStatus('idle', 'Click to speak');
        }
        
        // Update status display
        function updateStatus(state, text) {
            const dot = document.getElementById('status-dot');
            const statusText = document.getElementById('status-text');
            
            dot.className = 'status-dot ' + state;
            statusText.textContent = text;
        }
        
        // Add message to transcript
        function addToTranscript(role, text) {
            const transcript = document.getElementById('transcript');
            const line = document.createElement('div');
            line.className = 'transcript-line ' + role;
            line.innerHTML = `
                <div class="role">${role === 'user' ? 'You' : 'MyAiEa'}</div>
                <div>${text}</div>
            `;
            transcript.appendChild(line);
            transcript.scrollTop = transcript.scrollHeight;
        }
        
        // Lip animation
        let lipAnimationId = null;
        
        function startLipAnimation() {
            const upperLip = document.getElementById('upper-lip');
            const lowerLip = document.getElementById('lower-lip');
            const eqBars = document.querySelectorAll('.eq-bar');
            
            function animate() {
                // Random mouth opening
                const openAmount = Math.random() * 15 + 5;
                
                upperLip.setAttribute('d', `
                    M 30,${60 - openAmount/2} 
                    Q 50,${45 - openAmount/2} 75,${50 - openAmount/2} 
                    Q 100,${40 - openAmount/2} 125,${50 - openAmount/2} 
                    Q 150,${45 - openAmount/2} 170,${60 - openAmount/2} 
                    Q 150,${65 - openAmount/2} 125,${60 - openAmount/2} 
                    Q 100,${55 - openAmount/2} 75,${60 - openAmount/2} 
                    Q 50,${65 - openAmount/2} 30,${60 - openAmount/2} Z
                `);
                
                lowerLip.setAttribute('d', `
                    M 30,${60 + openAmount/2} 
                    Q 50,${65 + openAmount/2} 75,${60 + openAmount/2} 
                    Q 100,${55 + openAmount/2} 125,${60 + openAmount/2} 
                    Q 150,${65 + openAmount/2} 170,${60 + openAmount/2} 
                    Q 150,${85 + openAmount} 100,${90 + openAmount} 
                    Q 50,${85 + openAmount} 30,${60 + openAmount/2} Z
                `);
                
                // Animate equalizer bars
                eqBars.forEach(bar => {
                    bar.style.height = (Math.random() * 35 + 5) + 'px';
                });
                
                lipAnimationId = requestAnimationFrame(animate);
            }
            
            animate();
        }
        
        function stopLipAnimation() {
            if (lipAnimationId) {
                cancelAnimationFrame(lipAnimationId);
                lipAnimationId = null;
            }
            
            // Reset lips to closed position
            document.getElementById('upper-lip').setAttribute('d', `
                M 30,60 Q 50,45 75,50 Q 100,40 125,50 Q 150,45 170,60 
                Q 150,65 125,60 Q 100,55 75,60 Q 50,65 30,60 Z
            `);
            document.getElementById('lower-lip').setAttribute('d', `
                M 30,60 Q 50,65 75,60 Q 100,55 125,60 Q 150,65 170,60 
                Q 150,85 100,90 Q 50,85 30,60 Z
            `);
            
            // Reset equalizer
            document.querySelectorAll('.eq-bar').forEach(bar => {
                bar.style.height = '5px';
            });
        }
        
        // Check connection status
        async function checkConnections() {
            try {
                const response = await fetch('/api/health');
                const data = await response.json();
                
                const mcpStatus = document.getElementById('mcp-status');
                const geminiStatus = document.getElementById('gemini-status');
                
                mcpStatus.className = 'connection-badge ' + (data.mcp_server === 'connected' ? 'connected' : 'disconnected');
                geminiStatus.className = 'connection-badge ' + (data.gemini_configured ? 'connected' : 'disconnected');
            } catch (e) {
                console.error('Health check failed:', e);
            }
        }
        
        // Load voices when available
        if (synthesis) {
            synthesis.onvoiceschanged = () => {
                console.log('Voices loaded:', synthesis.getVoices().length);
            };
        }
        
        // Initialize
        checkConnections();
        setInterval(checkConnections, 10000);
    </script>
</body>
</html>
'''

if __name__ == '__main__':
    print("🎤 MyAiEa Voice Assistant starting on http://localhost:5001")
    print("📡 Connecting to MCP Server at", MCP_SERVER_URL)
    app.run(host='0.0.0.0', port=5001, debug=True)

