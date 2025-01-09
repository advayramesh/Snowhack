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

# --- Document Processing Setup ---
def setup_document_processing(conn):
    """Setup document processing capabilities"""
    try:
        cursor = conn.cursor()
        
        # Initialize document processing
        cursor.execute("""
        CALL SYSTEM$INITIALIZE_DOCUMENT_PROCESSING();
        """)
        
        # Create document processing function
        cursor.execute("""
        CREATE OR REPLACE FUNCTION PROCESS_DOCUMENTS(stage_location STRING)
        RETURNS TABLE(processed_documents VARIANT)
        AS 'SELECT SYSTEM$PROCESS_DOCUMENTS(
            stage_location,
            OBJECT_CONSTRUCT(
                ''mode'', ''single_document'',
                ''granularity'', ''document'',
                ''include_metadata'', true
            )
        )';
        """)
        
        st.success("Document processing setup complete")
    except Exception as e:
        st.error(f"Error setting up document processing: {str(e)}")
    finally:
        cursor.close()

# --- Search and QA Functions ---
def perform_vector_search(conn, stage_name, query, top_k=3):
    """
    Perform vector search on documents in stage
    """
    try:
        cursor = conn.cursor()
        
        # Get vector embeddings for the query
        cursor.execute("""
        SELECT SYSTEM$GET_VECTORS_FROM_TEXT(
            INPUT => %s,
            MODEL_NAME => 'snowflake.snowflake_ml_embeddings'
        )
        """, (query,))
        
        # Search documents
        cursor.execute(f"""
        SELECT *
        FROM TABLE(CORTEX_DOCUMENTS_SEARCH(
            '@{stage_name}',
            SYSTEM$GET_VECTORS_FROM_TEXT(%s, 'snowflake.snowflake_ml_embeddings'),
            TOP_N => %s
        ));
        """, (query, top_k))
        
        results = cursor.fetchall()
        return results
    except Exception as e:
        st.error(f"Error performing search: {str(e)}")
        return []
    finally:
        cursor.close()

def generate_answer(conn, question, context):
    """
    Generate answer using Mistral
    """
    try:
        cursor = conn.cursor()
        
        prompt = f"""Based on the following context, please answer the question. If the context doesn't contain relevant information, say so.

Context:
{context}

Question: {question}
Answer:"""

        cursor.execute("""
        SELECT MISTRAL_GENERATE(
            prompt => %s,
            temperature => 0.7,
            max_tokens => 500
        )
        """, (prompt,))
        
        result = cursor.fetchone()
        return result[0] if result else None
        
    except Exception as e:
        st.error(f"Error generating answer: {str(e)}")
        return None
    finally:
        cursor.close()

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
        
        # Setup document processing
        if conn and "doc_processing_initialized" not in st.session_state:
            setup_document_processing(conn)
            st.session_state.doc_processing_initialized = True

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
                        successful_uploads = upload_files_to_stage(conn, selected_stage, uploaded_files)
                
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

                    # Search and Q&A Interface
                    if st.checkbox("Search and Ask Questions"):
                        st.subheader("Search and Q&A")
                        
                        query = st.text_input("Enter your search query or question:")
                        
                        if st.button("Search and Answer"):
                            if query:
                                # Perform vector search
                                search_results = perform_vector_search(conn, selected_stage, query)
                                
                                if search_results:
                                    st.write("Found relevant documents:")
                                    
                                    # Display results and collect context
                                    context = []
                                    for idx, result in enumerate(search_results, 1):
                                        with st.expander(f"Result {idx}"):
                                            st.write("Score:", result[1])
                                            content = result[2]
                                            st.write("Content:", content[:500] + "...")
                                            context.append(content)
                                    
                                    # Generate answer
                                    with st.spinner("Generating answer..."):
                                        answer = generate_answer(conn, query, "\n".join(context))
                                        if answer:
                                            st.subheader("Answer:")
                                            st.write(answer)
                                else:
                                    st.info("No relevant documents found")
                            else:
                                st.warning("Please enter a query or question")
            
            # Logout button
            if st.sidebar.button("Logout"):
                st.session_state.clear()
                st.rerun()

if __name__ == "__main__":
    main()