import streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
import yaml
from hashlib import sha256
from io import StringIO

def init_snowflake_connection():
    """Initialize Snowflake connection"""
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

def authenticate(conn, username, password):
    """Authenticate user against Snowflake database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT USER_ID FROM USERS 
        WHERE USERNAME = %s AND PASSWORD = %s
        """, (username, sha256(password.encode()).hexdigest()))
        result = cursor.fetchone()
        return bool(result)
    except Exception as e:
        st.error(f"Authentication error: {str(e)}")
        return False
    finally:
        cursor.close()

def register_user(conn, username, password):
    """Register new user in Snowflake database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO USERS (USERNAME, PASSWORD)
        VALUES (%s, %s)
        """, (username, sha256(password.encode()).hexdigest()))
        return True
    except ProgrammingError as e:
        if "duplicate key value violates unique constraint" in str(e):
            st.error("Username already exists")
        else:
            st.error(f"Registration error: {str(e)}")
        return False
    finally:
        cursor.close()

def process_and_upload_file(conn, file, stage_name="DOCS"):
    """Process a file, upload to stage, and store chunks"""
    cursor = None
    try:
        cursor = conn.cursor()
        
        # First, save file locally for stage upload
        file_path = f"./{file.name}"
        with open(file_path, "wb") as f:
            f.write(file.getvalue())
        
        # Upload to stage
        put_command = f"PUT 'file://{file_path}' @{stage_name} AUTO_COMPRESS=FALSE"
        cursor.execute(put_command)
        
        # Store metadata
        cursor.execute("""
        INSERT INTO UPLOADED_FILES_METADATA (
            USERNAME, 
            SESSION_ID, 
            STAGE_NAME, 
            FILE_NAME
        ) VALUES (%s, %s, %s, %s)
        """, (
            st.session_state.username,
            st.session_state.session_id,
            stage_name,
            file.name
        ))
        
        # Process file content into chunks
        file_content = file.getvalue()
        if file.type and file.type.startswith('text/'):
            try:
                # Handle text files
                text_content = file_content.decode('utf-8')
                chunks = [text_content[i:i+1000] for i in range(0, len(text_content), 1000)]
                
                for chunk in chunks:
                    cursor.execute("""
                    INSERT INTO DOCS_CHUNKS_TABLE (
                        RELATIVE_PATH,
                        SIZE,
                        FILE_URL,
                        CHUNK,
                        USERNAME,
                        SESSION_ID
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        file.name,
                        len(chunk.encode('utf-8')),
                        f"@{stage_name}/{file.name}",
                        chunk,
                        st.session_state.username,
                        st.session_state.session_id
                    ))
            except UnicodeDecodeError:
                # If text decode fails, treat as binary
                st.warning(f"Treating {file.name} as binary file")
                process_binary_content(cursor, file_content, file.name, stage_name)
        else:
            # Handle binary files
            process_binary_content(cursor, file_content, file.name, stage_name)
        
        st.success(f"Successfully processed {file.name}")
        return True
        
    except Exception as e:
        st.error(f"Error processing {file.name}: {str(e)}")
        return False
    finally:
        if cursor:
            cursor.close()
        if os.path.exists(file_path):
            os.remove(file_path)

def process_binary_content(cursor, content, filename, stage_name):
    """Process binary content into chunks"""
    chunk_size = 8192  # 8KB chunks
    for i in range(0, len(content), chunk_size):
        chunk = content[i:i+chunk_size]
        cursor.execute("""
        INSERT INTO DOCS_CHUNKS_TABLE (
            RELATIVE_PATH,
            SIZE,
            FILE_URL,
            CHUNK,
            USERNAME,
            SESSION_ID
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            filename,
            len(chunk),
            f"@{stage_name}/{filename}",
            chunk.hex(),
            st.session_state.username,
            st.session_state.session_id
        ))

def search_documents(conn, query):
    """Search documents using Cortex"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT DISTINCT FILE_NAME 
        FROM UPLOADED_FILES_METADATA
        WHERE USERNAME = %s
        """, (st.session_state.username,))
        
        files = cursor.fetchall()
        results = []
        
        for file in files:
            filename = file[0]
            cursor.execute("""
            SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                '@DOCS',
                %s
            )
            """, (filename,))
            
            content = cursor.fetchone()
            if content and content[0]:
                cursor.execute("""
                SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                    %s,
                    %s
                )
                """, (content[0], query))
                
                answer = cursor.fetchone()
                if answer and answer[0]:
                    results.append({
                        'file': filename,
                        'answer': answer[0]
                    })
        
        return results
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return []
    finally:
        cursor.close()

def main():
    st.set_page_config(page_title="Document Q&A System", layout="wide")
    
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    # Initialize Snowflake connection
    if 'snowflake_connection' not in st.session_state:
        st.session_state.snowflake_connection = init_snowflake_connection()
    
    conn = st.session_state.snowflake_connection
    if not conn:
        st.error("Failed to connect to Snowflake")
        return
    
    # Authentication UI
    if not st.session_state.authenticated:
        tab1, tab2 = st.tabs(["Login", "Sign Up"])
        
        with tab1:
            st.subheader("🔐 Login")
            username = st.text_input("Username", key="login_username")
            password = st.text_input("Password", type="password", key="login_password")
            
            if st.button("Login"):
                if authenticate(conn, username, password):
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.session_state.session_id = os.urandom(16).hex()
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid credentials")
        
        with tab2:
            st.subheader("📝 Sign Up")
            new_username = st.text_input("New Username", key="signup_username")
            new_password = st.text_input("New Password", type="password", key="signup_password")
            confirm_password = st.text_input("Confirm Password", type="password", key="confirm_password")
            
            if st.button("Sign Up"):
                if new_password != confirm_password:
                    st.error("Passwords do not match")
                elif register_user(conn, new_username, new_password):
                    st.success("Registration successful! Please login.")
    
    else:
        # Main application UI
        st.title("Document Q&A System")
        
        # File upload section
        st.header("📤 Upload Documents")
        uploaded_files = st.file_uploader(
            "Choose files to upload",
            accept_multiple_files=True,
            key="file_uploader"
        )
        
        if uploaded_files:
            with st.spinner("Processing files..."):
                for file in uploaded_files:
                    process_and_upload_file(conn, file)
        
        # Q&A section
        st.header("❓ Ask Questions")
        query = st.text_area("Enter your question:")
        
        if st.button("Search"):
            if query:
                with st.spinner("Searching..."):
                    results = search_documents(conn, query)
                    if results:
                        for result in results:
                            with st.expander(f"Answer from {result['file']}", expanded=True):
                                st.write(result['answer'])
                    else:
                        st.info("No relevant answers found")
            else:
                st.warning("Please enter a question")
        
        # Logout button
        if st.sidebar.button("Logout"):
            st.session_state.clear()
            st.rerun()

if __name__ == "__main__":
    main()