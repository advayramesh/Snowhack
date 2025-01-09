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
    """
    try:
        # Read file content
        content = StringIO(file.getvalue().decode('utf-8')).read()
        
        # Create chunks (approximately 1000 characters each)
        chunks = textwrap.wrap(content, width=1000, break_long_words=True, replace_whitespace=False)
        
        # Generate file URL
        file_url = f"@{stage_name}/{file.name}"
        
        cursor = conn.cursor()
        
        # Store file metadata
        cursor.execute("""
            INSERT INTO UPLOADED_FILES_METADATA (USERNAME, SESSION_ID, STAGE_NAME, FILE_NAME)
            VALUES (%s, %s, %s, %s)
        """, (username, session_id, stage_name, file.name))
        
        # Store chunks
        for chunk in chunks:
            cursor.execute("""
                INSERT INTO DOCS_CHUNKS_TABLE 
                (RELATIVE_PATH, SIZE, FILE_URL, CHUNK, USERNAME, SESSION_ID)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (file.name, len(chunk), file_url, chunk, username, session_id))
        
        st.success(f"Successfully chunked and stored {file.name}")
        
    except Exception as e:
        st.error(f"Error processing file {file.name}: {str(e)}")
    finally:
        cursor.close()

def get_stored_chunks(conn, username, session_id=None):
    """
    Retrieve stored chunks for a user and optionally filtered by session_id.
    """
    try:
        cursor = conn.cursor()
        
        if session_id:
            cursor.execute("""
                SELECT RELATIVE_PATH, SIZE, FILE_URL, CHUNK, SESSION_ID
                FROM DOCS_CHUNKS_TABLE
                WHERE USERNAME = %s AND SESSION_ID = %s
                ORDER BY RELATIVE_PATH
            """, (username, session_id))
        else:
            cursor.execute("""
                SELECT RELATIVE_PATH, SIZE, FILE_URL, CHUNK, SESSION_ID
                FROM DOCS_CHUNKS_TABLE
                WHERE USERNAME = %s
                ORDER BY RELATIVE_PATH
            """, (username,))
            
        return cursor.fetchall()
        
    except Exception as e:
        st.error(f"Error retrieving chunks: {str(e)}")
        return []
    finally:
        cursor.close()

# --- Streamlit App ---
st.title("Snowflake Stage Explorer & Multi-File Uploader")

# Initialize session state
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

def show_login():
    """Display the login interface."""
    st.subheader("Login")
    username = st.text_input("Username", key="login_username")
    password = st.text_input("Password", type="password", key="login_password")
    
    if st.button("Login"):
        if authenticate(username, password):
            st.session_state["authenticated"] = True
            st.session_state["username"] = username
            st.session_state["session_id"] = os.urandom(16).hex()
            st.success("Login successful!")
            st.rerun()  # Updated from experimental_rerun()
        else:
            st.error("Invalid username or password.")

def show_signup():
    """Display the signup interface."""
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

# Main application logic
if not st.session_state["authenticated"]:
    auth_choice = st.sidebar.radio("Authentication", ["Login", "Sign Up"])
    if auth_choice == "Login":
        show_login()
    else:
        show_signup()
else:
    # Initialize Snowflake connection
    if "snowflake_connection" not in st.session_state:
        st.session_state.snowflake_connection = init_snowflake_connection()
    
    conn = st.session_state.snowflake_connection

    if conn:
        # List all stages
        stages = list_stages(conn)
        
        if stages:
            # Create a selection box for stages
            stage_names = [stage[1] for stage in stages]
            selected_stage = st.selectbox("Select a stage to upload files:", stage_names)
            
            if selected_stage:
                # Multiple file uploader
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
                
                # View stored chunks
                if st.checkbox("View stored chunks"):
                    chunks = get_stored_chunks(
                        conn=conn,
                        username=st.session_state.username
                    )
                    
                    if chunks:
                        for chunk in chunks:
                            st.write(f"File: {chunk[0]}")
                            st.write(f"Size: {chunk[1]} bytes")
                            st.write(f"Content preview: {chunk[3][:200]}...")
                            st.write("---")
                    else:
                        st.info("No chunks stored yet.")
        
        # Logout button
        if st.sidebar.button("Logout"):
            st.session_state.clear()
            st.rerun()  # Updated from experimental_rerun()