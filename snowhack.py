import streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
import yaml
from hashlib import sha256
from io import StringIO
import textwrap

# --- Helper Functions for Authentication ---
CONFIG_FILE = "config.yaml"

def load_config():
    """Load configuration from a YAML file."""
    try:
        with open(CONFIG_FILE, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        return {"users": {}}

def save_config(config):
    """Save configuration to a YAML file."""
    with open(CONFIG_FILE, "w") as file:
        yaml.dump(config, file)

def hash_password(password):
    """Hash a password for secure storage."""
    return sha256(password.encode()).hexdigest()

def authenticate(username, password):
    """Authenticate a user by username and password."""
    config = load_config()
    users = config.get("users", {})
    if username in users and users[username] == hash_password(password):
        return True
    return False

def register_user(username, password):
    """Register a new user."""
    config = load_config()
    users = config.get("users", {})
    if username in users:
        return False  # User already exists
    users[username] = hash_password(password)
    config["users"] = users
    save_config(config)
    return True

# --- Snowflake Functions ---
def init_snowflake_connection():
    """Initialize Snowflake connection with credentials"""
    try:
        conn = snowflake.connector.connect(
            user='SNOWHACK10',
            password='Snowhack10',
            account='nrdbnwt-qob04556',
            warehouse='COMPUTE_WH',
            database='SAMPLEDATA',
            schema='PUBLIC'
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

def list_stages(conn):
    """List all stages in the current schema"""
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW STAGES")
        stages = cursor.fetchall()
        return stages
    except ProgrammingError as e:
        st.error(f"Error listing stages: {str(e)}")
        return []
    finally:
        cursor.close()

def upload_files_to_stage(conn, stage_name, files):
    """Upload multiple files to a specific Snowflake stage"""
    uploaded_files = []
    for file in files:
        try:
            cursor = conn.cursor()
            
            # Save uploaded file locally
            file_path = f"./{file.name}"
            with open(file_path, "wb") as f:
                f.write(file.getbuffer())
            
            # Use PUT command to upload the file to the stage without compression
            put_command = f"PUT 'file://{file_path}' @{stage_name} AUTO_COMPRESS=FALSE"
            cursor.execute(put_command)
            st.success(f"File {file.name} successfully uploaded to stage {stage_name}.")
            uploaded_files.append(file)
            
            # Clean up local file after upload
            os.remove(file_path)
        
        except ProgrammingError as e:
            st.error(f"Error uploading file {file.name} to stage {stage_name}: {str(e)}")
        finally:
            cursor.close()
    return uploaded_files

def list_files_in_stage(conn, stage_name):
    """List all files in a specific stage"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"LIST @{stage_name}")
        files = cursor.fetchall()
        return files
    except ProgrammingError as e:
        st.error(f"Error listing files in stage {stage_name}: {str(e)}")
        return []
    finally:
        cursor.close()

# --- Chunking Functions ---
def chunk_and_store_file(conn, file, username, session_id, stage_name):
    """
    Chunk a file and store its metadata and chunks in the database.
    Handles both text and binary files.
    """
    cursor = None
    try:
        # Determine file type and handle accordingly
        file_extension = os.path.splitext(file.name)[1].lower()
        
        # Generate file URL and clean relative path
        file_url = f"@{stage_name}/{file.name}"
        relative_path = file.name
        if relative_path.startswith('@'):
            relative_path = relative_path.split('/', 1)[1] if '/' in relative_path else relative_path[1:]
        
        # Get file content
        file_content = file.getvalue()
        
        cursor = conn.cursor()
        
        # Store file metadata
        cursor.execute("""
            INSERT INTO UPLOADED_FILES_METADATA (USERNAME, SESSION_ID, STAGE_NAME, FILE_NAME)
            VALUES (%s, %s, %s, %s)
        """, (username, session_id, stage_name, relative_path))
        
        # Calculate chunk size (8KB for binary files, 1000 chars for text)
        CHUNK_SIZE = 8192  # 8KB chunks for binary files
        
        if file_extension in ['.txt', '.csv', '.md', '.py', '.js', '.html', '.css', '.json', '.xml', '.yaml', '.yml']:
            try:
                # Try to decode as text
                content = file_content.decode('utf-8')
                chunks = textwrap.wrap(content, width=1000, break_long_words=True, replace_whitespace=False)
                
                for chunk in chunks:
                    cursor.execute("""
                        INSERT INTO DOCS_CHUNKS_TABLE 
                        (RELATIVE_PATH, SIZE, FILE_URL, CHUNK, USERNAME, SESSION_ID)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (relative_path, len(chunk), file_url, chunk, username, session_id))
            except UnicodeDecodeError:
                # If decode fails, treat as binary
                for i in range(0, len(file_content), CHUNK_SIZE):
                    chunk = file_content[i:i + CHUNK_SIZE]
                    cursor.execute("""
                        INSERT INTO DOCS_CHUNKS_TABLE 
                        (RELATIVE_PATH, SIZE, FILE_URL, CHUNK, USERNAME, SESSION_ID)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (relative_path, len(chunk), file_url, chunk.hex(), username, session_id))
        else:
            # Handle as binary file
            for i in range(0, len(file_content), CHUNK_SIZE):
                chunk = file_content[i:i + CHUNK_SIZE]
                cursor.execute("""
                    INSERT INTO DOCS_CHUNKS_TABLE 
                    (RELATIVE_PATH, SIZE, FILE_URL, CHUNK, USERNAME, SESSION_ID)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (relative_path, len(chunk), file_url, chunk.hex(), username, session_id))
        
        st.success(f"Successfully chunked and stored {relative_path}")
        
    except Exception as e:
        st.error(f"Error processing file {file.name}: {str(e)}")
        if cursor:
            cursor.close()
        raise
    else:
        if cursor:
            cursor.close()

# --- Cortex Search Functions ---
def setup_cortex_search(conn):
    """Setup Cortex Search index and necessary configurations"""
    try:
        cursor = conn.cursor()
        
        # Create a Cortex Search index
        cursor.execute("""
        CREATE SEARCH INDEX IF NOT EXISTS docs_search_idx 
        ON DOCS_CHUNKS_TABLE(CHUNK)
        USING CORTEX;
        """)
        
        # Add content type detection for better search
        cursor.execute("""
        ALTER SEARCH INDEX docs_search_idx
        SET AUTO_DETECT_CONTENT_TYPE = true;
        """)
        
        st.success("Cortex Search index setup complete")
    except Exception as e:
        st.error(f"Error setting up Cortex Search: {str(e)}")
    finally:
        cursor.close()

def perform_search(conn, query, username=None, top_k=5):
    """
    Perform semantic search using Cortex Search
    """
    try:
        cursor = conn.cursor()
        
        # Base search query
        search_query = """
        SELECT 
            RELATIVE_PATH,
            CHUNK,
            SEARCH_SCORE,
            FILE_URL
        FROM DOCS_CHUNKS_TABLE
        WHERE SEARCH_BY_CORTEX(
            CHUNK,
            %s,
            TOP_K => %s
        )
        """
        
        # Add username filter if provided
        params = [query, top_k]
        if username:
            search_query += " AND USERNAME = %s"
            params.append(username)
            
        search_query += " ORDER BY SEARCH_SCORE DESC"
        
        cursor.execute(search_query, tuple(params))
        results = cursor.fetchall()
        return results
        
    except Exception as e:
        st.error(f"Error performing search: {str(e)}")
        return []
    finally:
        cursor.close()

def ask_question(conn, question, context_results):
    """
    Use Mistral to generate an answer based on search results
    """
    try:
        cursor = conn.cursor()
        
        # Prepare context from search results
        context = "\n".join([result[1] for result in context_results])
        
        # Use Mistral for question answering
        cursor.execute("""
        SELECT MISTRAL_GENERATE(
            prompt => %s,
            context => %s,
            max_tokens => 500,
            temperature => 0.7
        )
        """, (question, context))
        
        result = cursor.fetchone()
        return result[0] if result else None
        
    except Exception as e:
        st.error(f"Error generating answer: {str(e)}")
        return None
    finally:
        cursor.close()

# --- Streamlit UI Components ---
def add_search_interface(conn):
    """Add search interface to Streamlit app"""
    st.subheader("Document Search")
    
    # Search input
    search_query = st.text_input("Enter your search query:")
    top_k = st.slider("Number of results", min_value=1, max_value=20, value=5)
    
    # Search scope
    search_scope = st.radio(
        "Search scope:",
        ["My documents", "All documents"]
    )
    
    if st.button("Search"):
        if search_query:
            username = st.session_state.username if search_scope == "My documents" else None
            results = perform_search(conn, search_query, username, top_k)
            
            if results:
                st.write(f"Found {len(results)} results:")
                for result in results:
                    with st.expander(f"Result from {result[0]} (Score: {result[2]:.2f})"):
                        st.write("Content:", result[1])
                        st.write("Source:", result[3])
            else:
                st.info("No results found")
        else:
            st.warning("Please enter a search query")

def add_qa_interface(conn):
    """Add question-answering interface to Streamlit app"""
    st.subheader("Ask Questions About Your Documents")
    
    question = st.text_input("Ask a question about your documents:")
    
    if st.button("Get Answer"):
        if question:
            # First perform search to get relevant context
            results = perform_search(conn, question, st.session_state.username, top_k=3)
            
            if results:
                with st.spinner("Generating answer..."):
                    answer = ask_question(conn, question, results)
                    if answer:
                        st.write("Answer:", answer)
                        
                        # Show sources
                        with st.expander("View sources"):
                            for result in results:
                                st.write(f"- From {result[0]}")
            else:
                st.info("No relevant context found to answer the question")

# --- Main App ---
def main():
    st.title("Document Search & QA System")
    
    # Initialize session state
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    # Authentication
    if not st.session_state["authenticated"]:
        auth_choice = st.sidebar.radio("Authentication", ["Login", "Sign Up"])
        
        if auth_choice == "Login":
            st.subheader("Login")
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")
            
            if st.button("Login"):
                if authenticate(username, password):
                    st.session_state["authenticated"] = True
                    st.session_state["username"] = username
                    st.session_state["session_id"] = os.urandom(16).hex()
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid username or password.")
        else:
            st.subheader("Sign Up")
            username = st.text_input("New Username", key="signup_username")
            password = st.text_input("New Password", type="password", key="signup_password")
            confirm_password = st.text_input("Confirm Password", type="password", key="signup_confirm")
            
            if st.button("Sign Up"):
                if password != confirm_password:
                    st.error("Passwords do not match.")
                elif register_user(username, password):
                    st.success("User registered successfully! Please log in.")
                else:
                    st.error("Username already exists.")
    
    else:
        # Initialize Snowflake connection
        if "snowflake_connection" not in st.session_state:
            st.session_state.snowflake_connection = init_snowflake_connection()
        
        conn = st.session_state.snowflake_connection
        
        # Setup Cortex Search
        if conn and "cortex_search_initialized" not in st.session_state:
            setup_cortex_search(conn)
            st.session_state.cortex_search_initialized = True

        if conn:
            # File Upload Section
            st.header("File Upload")
            stages = list_stages(conn)
            
            if stages:
                stage_names = [stage[1] for stage in stages]
                selected_stage = st.selectbox("Select a stage to upload files:", stage_names)
                
                if selected_stage:
                    uploaded_files = st.file_uploader(
                        "Choose files to upload", 
                        accept_multiple_files=True,
                        key="file_uploader"
                    )
                    
                    if uploaded_files:
                        st.write(f"Uploading {len(uploaded_files)} files to stage {selected_stage}...")

                        # Upload files to stage
                        successful_uploads = upload_files_to_stage(conn, selected_stage, uploaded_files)
                        
                        # Process and chunk successful uploads
                        for file in successful_uploads:
                            chunk_and_store_file(
                                conn=conn,
                                file=file,
                                username=st.session_state.username,
                                session_id=st.session_state.session_id,
                                stage_name=selected_stage
                            )
                
                # Display files in stage
                st.subheader(f"Files in {selected_stage}")
                files = list_files_in_stage(conn, selected_stage)
                
                if files:
                    file_data = []
                    for file in files:
                        file_data.append({
                            "Name": file[0],
                            "Size (bytes)": file[1],
                            "Last Modified": file[2]
                        })
                    st.dataframe(file_data)
                else:
                    st.info("No files found in this stage.")

            # Search and Q&A Functionality
            if st.checkbox("Search and Ask Questions"):
                st.subheader("Search and Q&A")
                
                # Query input
                query = st.text_input("Enter your search query or question:")
                
                if st.button("Search and Answer"):
                    if query:
                        try:
                            cursor = conn.cursor()
                            
                            # Search in stage files using Cortex Search
                            cursor.execute("""
                            SELECT SEARCH_BY_CORTEX(
                                QUERY => %s,
                                TARGET => '@""" + selected_stage + """',
                                OPTIONS => OBJECT_CONSTRUCT(
                                    'returnCount', 3,
                                    'searchOptions', OBJECT_CONSTRUCT(
                                        'includeFileMetadata', TRUE
                                    )
                                )
                            )
                            """, (query,))
                            
                            search_results = cursor.fetchone()[0]
                            
                            if search_results:
                                # Display search results
                                st.write("Found relevant documents:")
                                for idx, result in enumerate(search_results['documents'], 1):
                                    with st.expander(f"Result {idx} - {result['fileName']}"):
                                        st.write("Relevance Score:", result['score'])
                                        st.write("Content:", result['content'][:500] + "...")
                                
                                # Generate answer using Mistral
                                context = "\n".join([doc['content'] for doc in search_results['documents']])
                                
                                prompt = f"""Based on the following context, please answer the question: {query}

Context:
{context}

Question: {query}
Answer:"""
                                
                                cursor.execute("""
                                SELECT MISTRAL_GENERATE(
                                    prompt => %s,
                                    temperature => 0.7,
                                    max_tokens => 500
                                )
                                """, (prompt,))
                                
                                answer = cursor.fetchone()[0]
                                
                                st.subheader("Answer:")
                                st.write(answer)
                                
                                # Show sources
                                st.subheader("Sources:")
                                for doc in search_results['documents']:
                                    st.write(f"- {doc['fileName']}")
                            else:
                                st.info("No relevant documents found")
                            
                        except Exception as e:
                            st.error(f"Error during search and answer: {str(e)}")
                        finally:
                            cursor.close()
                    else:
                        st.warning("Please enter a query or question")
            
            # Logout button
            if st.sidebar.button("Logout"):
                st.session_state.clear()
                st.rerun()

if __name__ == "__main__":
    main()