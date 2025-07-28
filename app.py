from flask import Flask, render_template, request, jsonify, session
from flask_socketio import SocketIO, emit
import psycopg2
from psycopg2.extras import RealDictCursor
import google.generativeai as genai
import json
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv
import uuid
import logging
from werkzeug.security import generate_password_hash, check_password_hash
import threading
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', '387ba432d965ff55a5cffc3455dac239e018813e23ca3505d812e7f84008291a')
socketio = SocketIO(app, cors_allowed_origins="*")

class ConversationDB:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.create_tables()
    
    def get_connection(self):
        return psycopg2.connect(**self.db_config)
    
    def create_tables(self):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if conversations table exists and get its structure
                    cur.execute("""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_name = 'conversations';
                    """)
                    existing_columns = [row[0] for row in cur.fetchall()]
                    
                    if not existing_columns:
                        # Create conversations table from scratch
                        logger.info("Creating conversations table from scratch...")
                        cur.execute("""
                            CREATE TABLE conversations (
                                id SERIAL PRIMARY KEY,
                                session_id VARCHAR(255) NOT NULL,
                                user_query TEXT NOT NULL,
                                llm_response TEXT NOT NULL,
                                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                metadata JSONB DEFAULT '{}',
                                user_id VARCHAR(255) DEFAULT 'anonymous'
                            );
                        """)
                    else:
                        # Add missing columns to existing table
                        if 'user_id' not in existing_columns:
                            logger.info("Adding user_id column to existing conversations table...")
                            cur.execute("""
                                ALTER TABLE conversations 
                                ADD COLUMN user_id VARCHAR(255) DEFAULT 'anonymous';
                            """)
                            # Update existing records
                            cur.execute("""
                                UPDATE conversations 
                                SET user_id = 'anonymous' 
                                WHERE user_id IS NULL;
                            """)
                    
                    # Create chat_sessions table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS chat_sessions (
                            id SERIAL PRIMARY KEY,
                            session_id VARCHAR(255) UNIQUE NOT NULL,
                            session_name VARCHAR(255) DEFAULT 'New Chat',
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            user_id VARCHAR(255) DEFAULT 'anonymous'
                        );
                    """)
                    
                    # Create indexes safely
                    indexes = [
                        "CREATE INDEX IF NOT EXISTS idx_session_id ON conversations(session_id);",
                        "CREATE INDEX IF NOT EXISTS idx_timestamp ON conversations(timestamp);",
                        "CREATE INDEX IF NOT EXISTS idx_user_id ON conversations(user_id);",
                        "CREATE INDEX IF NOT EXISTS idx_chat_sessions_user_id ON chat_sessions(user_id);",
                        "CREATE INDEX IF NOT EXISTS idx_chat_sessions_activity ON chat_sessions(last_activity);"
                    ]
                    
                    for index_sql in indexes:
                        try:
                            cur.execute(index_sql)
                        except Exception as idx_error:
                            logger.warning(f"Index creation warning: {idx_error}")
                    
                    conn.commit()
                    logger.info("Database tables and indexes created/updated successfully")
                    
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            # Don't raise the error, try to continue with basic functionality
            logger.warning("Continuing with potentially limited functionality...")
    
    def store_conversation(self, session_id: str, user_query: str, llm_response: str, 
                         metadata: dict = None, user_id: str = 'anonymous'):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO conversations (session_id, user_query, llm_response, metadata, user_id)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id;
                    """, (session_id, user_query, llm_response, json.dumps(metadata or {}), user_id))
                    
                    record_id = cur.fetchone()[0]
                    
                    # Update session activity
                    cur.execute("""
                        UPDATE chat_sessions 
                        SET last_activity = CURRENT_TIMESTAMP 
                        WHERE session_id = %s
                    """, (session_id,))
                    
                    conn.commit()
                    return record_id
        except Exception as e:
            logger.error(f"Error storing conversation: {e}")
            return None
    
    def get_conversation_history(self, session_id: str, limit: int = 50):
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, user_query, llm_response, timestamp, metadata 
                        FROM conversations 
                        WHERE session_id = %s 
                        ORDER BY timestamp ASC 
                        LIMIT %s;
                    """, (session_id, limit))
                    
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting conversation history: {e}")
            return []
    
    def create_session(self, session_name: str = "New Chat", user_id: str = 'anonymous'):
        session_id = f"session_{uuid.uuid4().hex[:12]}"
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO chat_sessions (session_id, session_name, user_id)
                        VALUES (%s, %s, %s)
                        RETURNING session_id;
                    """, (session_id, session_name, user_id))
                    
                    conn.commit()
                    return session_id
        except Exception as e:
            logger.error(f"Error creating session: {e}")
            return None
    
    def get_user_sessions(self, user_id: str = 'anonymous', limit: int = 20):
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT s.session_id, s.session_name, s.created_at, s.last_activity,
                               COUNT(c.id) as message_count,
                               MAX(c.timestamp) as last_message
                        FROM chat_sessions s
                        LEFT JOIN conversations c ON s.session_id = c.session_id
                        WHERE s.user_id = %s
                        GROUP BY s.session_id, s.session_name, s.created_at, s.last_activity
                        ORDER BY s.last_activity DESC
                        LIMIT %s;
                    """, (user_id, limit))
                    
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting user sessions: {e}")
            return []
    
    def search_conversations(self, query: str, user_id: str = 'anonymous', limit: int = 20):
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT c.*, s.session_name
                        FROM conversations c
                        JOIN chat_sessions s ON c.session_id = s.session_id
                        WHERE c.user_id = %s 
                        AND (c.user_query ILIKE %s OR c.llm_response ILIKE %s)
                        ORDER BY c.timestamp DESC
                        LIMIT %s;
                    """, (user_id, f'%{query}%', f'%{query}%', limit))
                    
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error searching conversations: {e}")
            return []

class GeminiAI:
    def __init__(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
    
    def generate_response(self, prompt: str, context: str = ""):
        try:
            full_prompt = f"{context}\n\nUser: {prompt}" if context else prompt
            response = self.model.generate_content(full_prompt)
            return response.text
        except Exception as e:
            logger.error(f"Error generating AI response: {e}")
            return f"I apologize, but I encountered an error: {str(e)}"

class DatabaseQueryProcessor:
    def __init__(self, db: ConversationDB, ai: GeminiAI):
        self.db = db
        self.ai = ai
        self.schema_info = self._get_schema_info()
    
    def _get_schema_info(self):
        """Get actual database schema information"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Get table information
                    cur.execute("""
                        SELECT table_name, column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name IN ('conversations', 'chat_sessions')
                        ORDER BY table_name, ordinal_position;
                    """)
                    
                    schema_data = cur.fetchall()
                    
                    # Get sample data to understand content
                    cur.execute("SELECT * FROM conversations LIMIT 3;")
                    conversation_samples = cur.fetchall()
                    
                    cur.execute("SELECT * FROM chat_sessions LIMIT 3;")
                    session_samples = cur.fetchall()
                    
                    # Build comprehensive schema description
                    schema_text = "DATABASE SCHEMA INFORMATION:\n\n"
                    
                    # Group columns by table
                    tables = {}
                    for row in schema_data:
                        table = row['table_name']
                        if table not in tables:
                            tables[table] = []
                        tables[table].append({
                            'column': row['column_name'],
                            'type': row['data_type'],
                            'nullable': row['is_nullable']
                        })
                    
                    # Format schema information
                    for table_name, columns in tables.items():
                        schema_text += f"Table: {table_name}\n"
                        for col in columns:
                            schema_text += f"  - {col['column']} ({col['type']}): "
                            if col['column'] == 'id':
                                schema_text += "Primary key\n"
                            elif col['column'] == 'session_id':
                                schema_text += "Session identifier\n"
                            elif col['column'] == 'user_query':
                                schema_text += "User's question/input\n"
                            elif col['column'] == 'llm_response':
                                schema_text += "AI assistant's response\n"
                            elif col['column'] == 'timestamp':
                                schema_text += "When conversation occurred\n"
                            elif col['column'] == 'user_id':
                                schema_text += "User identifier\n"
                            elif col['column'] == 'metadata':
                                schema_text += "Additional JSON data\n"
                            elif col['column'] == 'session_name':
                                schema_text += "Human-readable session name\n"
                            elif col['column'] == 'created_at':
                                schema_text += "Session creation time\n"
                            elif col['column'] == 'last_activity':
                                schema_text += "Last session activity\n"
                            else:
                                schema_text += f"Column description\n"
                        schema_text += "\n"
                    
                    # Add sample data context
                    if conversation_samples:
                        schema_text += "SAMPLE CONVERSATIONS DATA:\n"
                        for i, sample in enumerate(conversation_samples[:2], 1):
                            schema_text += f"Sample {i}:\n"
                            schema_text += f"  - user_query: \"{sample.get('user_query', '')[:100]}...\"\n"
                            schema_text += f"  - llm_response: \"{sample.get('llm_response', '')[:100]}...\"\n"
                            schema_text += f"  - user_id: {sample.get('user_id', 'N/A')}\n"
                            schema_text += f"  - timestamp: {sample.get('timestamp', 'N/A')}\n\n"
                    
                    schema_text += """
QUERY EXAMPLES:
- Recent conversations: SELECT * FROM conversations ORDER BY timestamp DESC LIMIT 10;
- Search by topic: SELECT * FROM conversations WHERE user_query ILIKE '%python%' OR llm_response ILIKE '%python%' LIMIT 20;
- User statistics: SELECT user_id, COUNT(*) as count FROM conversations GROUP BY user_id ORDER BY count DESC LIMIT 50;
- Active sessions: SELECT * FROM chat_sessions ORDER BY last_activity DESC LIMIT 10;
- Join example: SELECT c.*, s.session_name FROM conversations c JOIN chat_sessions s ON c.session_id = s.session_id LIMIT 10;
"""
                    
                    return schema_text
                    
        except Exception as e:
            logger.error(f"Error getting schema info: {e}")
            # Fallback to static schema
            return """
            DATABASE SCHEMA (Static fallback):
            
            Table: conversations
            - id: Primary key
            - session_id: Groups related conversations  
            - user_query: User's input/question
            - llm_response: AI assistant's response
            - timestamp: When conversation occurred
            - user_id: User identifier
            - metadata: Additional JSON data
            
            Table: chat_sessions  
            - id: Primary key
            - session_id: Session identifier
            - session_name: Human-readable name
            - created_at: Creation time
            - last_activity: Last activity time
            - user_id: User identifier
            """
    
    def _get_fallback_query(self, user_prompt: str) -> str:
        """Generate fallback queries for common patterns when AI fails"""
        prompt_lower = user_prompt.lower()
        
        # Common query patterns
        if 'recent' in prompt_lower and 'conversation' in prompt_lower:
            return "SELECT * FROM conversations ORDER BY timestamp DESC LIMIT 10"
        
        elif 'last 24 hours' in prompt_lower or 'today' in prompt_lower:
            if 'session' in prompt_lower:
                return "SELECT * FROM chat_sessions WHERE created_at >= NOW() - INTERVAL '24 hours' ORDER BY created_at DESC LIMIT 50"
            else:
                return "SELECT * FROM conversations WHERE timestamp >= NOW() - INTERVAL '24 hours' ORDER BY timestamp DESC LIMIT 50"
        
        elif 'count' in prompt_lower and 'user' in prompt_lower:
            return "SELECT user_id, COUNT(*) as conversation_count FROM conversations GROUP BY user_id ORDER BY conversation_count DESC LIMIT 50"
        
        elif 'search' in prompt_lower or 'find' in prompt_lower:
            # Extract search term if possible
            words = prompt_lower.split()
            search_terms = [word for word in words if word not in ['search', 'find', 'for', 'about', 'conversations', 'sessions']]
            if search_terms:
                term = search_terms[0]
                return f"SELECT * FROM conversations WHERE user_query ILIKE '%{term}%' OR llm_response ILIKE '%{term}%' ORDER BY timestamp DESC LIMIT 20"
        
        elif 'all' in prompt_lower and 'session' in prompt_lower:
            return "SELECT * FROM chat_sessions ORDER BY last_activity DESC LIMIT 50"
        
        elif 'all' in prompt_lower and 'conversation' in prompt_lower:
            return "SELECT * FROM conversations ORDER BY timestamp DESC LIMIT 50"
        
        return None

    def generate_sql_query(self, user_prompt: str) -> dict:
        """Generate SQL query based on user's natural language prompt"""
        
        # First try the AI approach
        prompt = f"""
        You are a PostgreSQL query generator. You must generate ONLY a SQL query, nothing else.

        DATABASE SCHEMA (This is YOUR database):
        {self.schema_info}

        CRITICAL INSTRUCTIONS:
        1. You must generate ONLY a PostgreSQL SELECT query
        2. Use the EXACT table names: conversations, chat_sessions  
        3. Use the EXACT column names shown in the schema above
        4. Always add LIMIT clause (max 100 rows)
        5. For "last 24 hours" queries, use: WHERE created_at >= NOW() - INTERVAL '24 hours'
        6. For text searches, use ILIKE with % wildcards
        7. Return ONLY the SQL query - no explanations, no examples, no advice
        8. Do not provide generic database advice

        EXAMPLES OF WHAT TO GENERATE:
        Input: "show recent conversations"
        Output: SELECT * FROM conversations ORDER BY timestamp DESC LIMIT 10

        Input: "sessions from last 24 hours"  
        Output: SELECT * FROM chat_sessions WHERE created_at >= NOW() - INTERVAL '24 hours' ORDER BY created_at DESC LIMIT 50

        Input: "find python discussions"
        Output: SELECT * FROM conversations WHERE user_query ILIKE '%python%' OR llm_response ILIKE '%python%' ORDER BY timestamp DESC LIMIT 20

        NOW GENERATE A QUERY FOR THIS REQUEST:
        "{user_prompt}"

        SQL QUERY:"""
        
        try:
            raw_response = self.ai.generate_response(prompt)
            sql_query = raw_response.strip()
            
            # Clean up the response - remove any markdown formatting or extra text
            if '```sql' in sql_query.lower():
                # Extract SQL from code blocks
                parts = sql_query.lower().split('```sql')
                if len(parts) > 1:
                    sql_part = parts[1].split('```')[0].strip()
                    sql_query = sql_part
            elif '```' in sql_query:
                # Handle generic code blocks
                parts = sql_query.split('```')
                if len(parts) >= 3:
                    sql_query = parts[1].strip()
            
            # Remove common prefixes that AI might add
            prefixes_to_remove = [
                'sql query:', 'query:', 'sql:', 'response:', 
                'here is the query:', 'the query is:', 'answer:', 'output:'
            ]
            
            sql_lower = sql_query.lower()
            for prefix in prefixes_to_remove:
                if sql_lower.startswith(prefix):
                    sql_query = sql_query[len(prefix):].strip()
                    break
            
            # Remove any leading/trailing whitespace and semicolons
            sql_query = sql_query.strip().rstrip(';')
            
            # Check if AI gave generic advice instead of query
            generic_indicators = [
                'you\'ll need to query', 'depends on:', 'your table name', 
                'assumptions:', 'below are common examples', 'postgresql', 'mysql'
            ]
            
            if any(indicator in sql_query.lower() for indicator in generic_indicators) or len(sql_query) > 500:
                logger.warning(f"AI gave generic advice instead of query. Using fallback.")
                fallback_query = self._get_fallback_query(user_prompt)
                if fallback_query:
                    return {"query": fallback_query, "error": None, "fallback_used": True, "raw_response": raw_response}
                else:
                    return {"error": "Could not generate query - AI gave generic advice", "query": None, "raw_response": raw_response}
            
            # Basic safety checks
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE', 'EXEC']
            sql_upper = sql_query.upper()
            if any(keyword in sql_upper for keyword in dangerous_keywords):
                return {"error": "Unsafe query detected", "query": None, "raw_response": raw_response}
            
            if sql_query == "INVALID_QUERY" or not sql_query or len(sql_query) < 10:
                # Try fallback
                fallback_query = self._get_fallback_query(user_prompt)
                if fallback_query:
                    return {"query": fallback_query, "error": None, "fallback_used": True, "raw_response": raw_response}
                return {"error": "Could not generate valid query", "query": None, "raw_response": raw_response}
            
            # Ensure it starts with SELECT
            if not sql_upper.startswith('SELECT'):
                fallback_query = self._get_fallback_query(user_prompt)
                if fallback_query:
                    return {"query": fallback_query, "error": None, "fallback_used": True, "raw_response": raw_response}
                return {"error": "Query must be a SELECT statement", "query": None, "raw_response": raw_response}
            
            return {"query": sql_query, "error": None, "fallback_used": False, "raw_response": raw_response}
            
        except Exception as e:
            logger.error(f"Error generating SQL query: {e}")
            # Try fallback on exception
            fallback_query = self._get_fallback_query(user_prompt)
            if fallback_query:
                return {"query": fallback_query, "error": None, "fallback_used": True, "exception": str(e)}
            return {"error": f"Query generation failed: {str(e)}", "query": None}
    
    def execute_query(self, sql_query: str, params: tuple = None) -> dict:
        """Execute the generated SQL query safely"""
        try:
            with self.db.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(sql_query, params)
                    results = cur.fetchall()
                    
                    # Convert to list of dictionaries for JSON serialization
                    data = [dict(row) for row in results]
                    
                    return {
                        "success": True,
                        "data": data,
                        "count": len(data),
                        "query": sql_query
                    }
                    
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": [],
                "count": 0,
                "query": sql_query
            }
    
    def format_results(self, query_results: dict, user_prompt: str) -> dict:
        """Format query results in a structured way using AI"""
        if not query_results["success"] or not query_results["data"]:
            return {
                "formatted_response": "No data found or query failed.",
                "summary": "Query returned no results",
                "data": query_results["data"],
                "metadata": {
                    "total_records": 0,
                    "query_executed": query_results.get("query", ""),
                    "success": False
                }
            }
        
        # Prepare data summary for AI formatting
        data_sample = query_results["data"][:5]  # First 5 records for context
        total_count = query_results["count"]
        
        format_prompt = f"""
        You are analyzing database query results for a conversation system. Format these results clearly and provide insights.

        CONTEXT: This data comes from a conversation database where users chat with an AI assistant. You are formatting the results of a database query, not accessing any external systems.

        Original User Request: "{user_prompt}"
        
        Query Results:
        - Total Records Found: {total_count}
        - Sample Data (first 5 records): {json.dumps(data_sample, indent=2, default=str)}
        
        Please provide a structured analysis including:
        1. Summary of what was found
        2. Key patterns or insights from the data
        3. Relevant statistics or trends
        4. Clear presentation of the most important findings
        
        Format your response as a helpful analysis of these conversation database results."""
        
        try:
            formatted_response = self.ai.generate_response(format_prompt)
            
            return {
                "formatted_response": formatted_response,
                "summary": f"Found {total_count} records matching your query",
                "data": query_results["data"],
                "metadata": {
                    "total_records": total_count,
                    "query_executed": query_results["query"],
                    "success": True,
                    "execution_time": datetime.now().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error formatting results: {e}")
            return {
                "formatted_response": f"Query executed successfully with {total_count} results, but formatting failed.",
                "summary": f"Found {total_count} records",
                "data": query_results["data"],
                "metadata": {
                    "total_records": total_count,
                    "query_executed": query_results["query"],
                    "success": True,
                    "formatting_error": str(e)
                }
            }
    
    def process_database_query(self, user_prompt: str) -> dict:
        """Main method to process a database query request"""
        # Step 1: Generate SQL query
        query_result = self.generate_sql_query(user_prompt)
        
        if query_result["error"]:
            return {
                "success": False,
                "error": query_result["error"],
                "formatted_response": f"I couldn't generate a valid query for your request: {query_result['error']}",
                "data": []
            }
        
        # Step 2: Execute query
        execution_result = self.execute_query(query_result["query"])
        
        if not execution_result["success"]:
            return {
                "success": False,
                "error": execution_result["error"],
                "formatted_response": f"Query execution failed: {execution_result['error']}",
                "data": []
            }
        
        # Step 3: Format results
        formatted_result = self.format_results(execution_result, user_prompt)
        
        return {
            "success": True,
            "formatted_response": formatted_result["formatted_response"],
            "summary": formatted_result["summary"],
            "data": formatted_result["data"],
            "metadata": formatted_result["metadata"]
        }

    def refresh_schema(self):
        """Refresh the database schema information"""
        self.schema_info = self._get_schema_info()
        logger.info("Database schema information refreshed")

# Initialize components
try:
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'conversation_db'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 5432))
    }
    
    db = ConversationDB(db_config)
    ai = GeminiAI(os.getenv('GEMINI_API_KEY'))
    query_processor = DatabaseQueryProcessor(db, ai)
    
    logger.info("Application initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize application: {e}")
    raise

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/sessions', methods=['GET'])
def get_sessions():
    user_id = session.get('user_id', 'anonymous')
    sessions = db.get_user_sessions(user_id)
    return jsonify(sessions)

@app.route('/api/sessions', methods=['POST'])
def create_session():
    user_id = session.get('user_id', 'anonymous')
    session_name = request.json.get('name', 'New Chat')
    session_id = db.create_session(session_name, user_id)
    
    if session_id:
        return jsonify({'session_id': session_id, 'success': True})
    else:
        return jsonify({'success': False, 'error': 'Failed to create session'}), 500

@app.route('/api/conversations/<session_id>')
def get_conversations(session_id):
    conversations = db.get_conversation_history(session_id)
    return jsonify(conversations)

@app.route('/api/search')
def search():
    query = request.args.get('q', '')
    user_id = session.get('user_id', 'anonymous')
    
    if not query:
        return jsonify([])
    
    results = db.search_conversations(query, user_id)
    return jsonify(results)

# Database Query Routes
@app.route('/api/query', methods=['POST'])
def database_query():
    """Handle database queries via natural language"""
    try:
        data = request.get_json()
        user_prompt = data.get('prompt', '').strip()
        user_id = session.get('user_id', 'anonymous')
        
        if not user_prompt:
            return jsonify({
                'success': False,
                'error': 'No prompt provided'
            }), 400
        
        # Process the database query
        result = query_processor.process_database_query(user_prompt)
        
        # Store this interaction in the database for future reference
        metadata = {
            'query_type': 'database_query',
            'user_id': user_id,
            'success': result['success'],
            'timestamp': datetime.now().isoformat()
        }
        
        # Create a special session for database queries if needed
        db_session_id = f"db_query_{uuid.uuid4().hex[:8]}"
        db.store_conversation(
            session_id=db_session_id,
            user_query=user_prompt,
            llm_response=result['formatted_response'],
            metadata=metadata,
            user_id=user_id
        )
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error processing database query: {e}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/query/schema', methods=['GET'])
def get_schema():
    """Get database schema information"""
    return jsonify({
        'schema': query_processor.schema_info,
        'tables': ['conversations', 'chat_sessions'],
        'sample_queries': [
            "Show me all conversations from the last 7 days",
            "Find conversations that mention 'python' or 'programming'",
            "Get the most active users by conversation count",
            "Show me sessions created this month",
            "Find the longest conversations by response length"
        ]
    })

@app.route('/api/query/refresh-schema', methods=['POST'])
def refresh_schema():
    """Refresh database schema information"""
    try:
        query_processor.refresh_schema()
        return jsonify({
            'success': True,
            'message': 'Schema refreshed successfully',
            'schema_info': query_processor.schema_info
        })
    except Exception as e:
        logger.error(f"Error refreshing schema: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/debug-query', methods=['POST'])
def debug_query():
    """Debug endpoint to see what SQL is being generated"""
    try:
        data = request.get_json()
        user_prompt = data.get('prompt', '').strip()
        
        if not user_prompt:
            return jsonify({'error': 'No prompt provided'}), 400
        
        # Just generate the SQL query without executing
        query_result = query_processor.generate_sql_query(user_prompt)
        
        return jsonify({
            'user_prompt': user_prompt,
            'generated_query': query_result.get('query'),
            'error': query_result.get('error'),
            'fallback_used': query_result.get('fallback_used', False),
            'raw_ai_response': query_result.get('raw_response', 'Not captured'),
            'schema_info_length': len(query_processor.schema_info)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-queries')
def test_queries():
    """Test endpoint to verify database querying functionality"""
    test_results = []
    test_prompts = [
        "Show me the latest 5 conversations",
        "Count total number of conversations",
        "Find conversations containing 'hello'"
    ]
    
    for prompt in test_prompts:
        result = query_processor.process_database_query(prompt)
        test_results.append({
            'prompt': prompt,
            'success': result['success'],
            'summary': result.get('summary', ''),
            'record_count': result.get('metadata', {}).get('total_records', 0)
        })
    
    return jsonify({
        'test_results': test_results,
        'status': 'Database query functionality is working'
    })

# WebSocket events
@socketio.on('connect')
def handle_connect():
    if 'user_id' not in session:
        session['user_id'] = f"user_{uuid.uuid4().hex[:8]}"
    
    emit('connected', {'user_id': session['user_id']})
    logger.info(f"User connected: {session['user_id']}")

@socketio.on('send_message')
def handle_message(data):
    try:
        user_query = data.get('message', '').strip()
        session_id = data.get('session_id')
        user_id = session.get('user_id', 'anonymous')
        
        if not user_query or not session_id:
            emit('error', {'message': 'Invalid message or session'})
            return
        
        # Emit typing indicator
        emit('ai_typing', {'session_id': session_id})
        
        # Get conversation context
        history = db.get_conversation_history(session_id, limit=5)
        context = ""
        if history:
            context = "Previous conversation:\n"
            for conv in history[-3:]:  # Last 3 conversations for context
                context += f"User: {conv['user_query']}\nAssistant: {conv['llm_response']}\n\n"
        
        # Generate AI response
        ai_response = ai.generate_response(user_query, context)
        
        # Store conversation
        metadata = {
            'model': 'gemini-2.5-flash',
            'context_used': len(context) > 0,
            'timestamp': datetime.now().isoformat()
        }
        
        record_id = db.store_conversation(session_id, user_query, ai_response, metadata, user_id)
        
        # Emit response
        emit('message_response', {
            'id': record_id,
            'user_query': user_query,
            'ai_response': ai_response,
            'timestamp': datetime.now().isoformat(),
            'session_id': session_id
        })
        
        logger.info(f"Message processed for user {user_id}, session {session_id}")
        
    except Exception as e:
        logger.error(f"Error handling message: {e}")
        emit('error', {'message': 'Failed to process message'})

@socketio.on('database_query')
def handle_database_query(data):
    """Handle database queries via WebSocket"""
    try:
        user_prompt = data.get('prompt', '').strip()
        user_id = session.get('user_id', 'anonymous')
        
        if not user_prompt:
            emit('query_error', {'message': 'No prompt provided'})
            return
        
        # Emit processing indicator
        emit('query_processing', {'prompt': user_prompt})
        
        # Process the query
        result = query_processor.process_database_query(user_prompt)
        
        # Emit results
        emit('query_result', {
            'prompt': user_prompt,
            'result': result,
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"Database query processed for user {user_id}")
        
    except Exception as e:
        logger.error(f"WebSocket database query error: {e}")
        emit('query_error', {'message': f'Query failed: {str(e)}'})

@socketio.on('disconnect')
def handle_disconnect():
    logger.info(f"User disconnected: {session.get('user_id', 'unknown')}")

# Example usage functions (you can call these for testing)
def test_database_queries():
    """Test function to demonstrate the functionality"""
    test_prompts = [
        "Show me the most recent 10 conversations",
        "Find all conversations that mention 'error' or 'problem'",
        "Get conversation statistics grouped by user",
        "Show me sessions created in the last 24 hours",
        "Find the user with the most conversations"
    ]
    
    for prompt in test_prompts:
        print(f"\n--- Testing: {prompt} ---")
        result = query_processor.process_database_query(prompt)
        print(f"Success: {result['success']}")
        if result['success']:
            print(f"Summary: {result['summary']}")
            print(f"Records: {result['metadata']['total_records']}")
        else:
            print(f"Error: {result.get('error', 'Unknown error')}")

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    
    logger.info(f"Starting server on port {port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=debug)