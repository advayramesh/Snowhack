def process_and_upload_file(conn, file, stage_name="DOCS"):
    """Process a file, upload to stage, and store chunks"""
    cursor = None
    temp_dir = "temp_uploads"
    local_file_path = None
    
    try:
        # Create temp directory if it doesn't exist
        os.makedirs(temp_dir, exist_ok=True)
        
        cursor = conn.cursor()
        
        # Save file locally for stage upload
        safe_filename = file.name.replace(" ", "_")
        local_file_path = os.path.join(temp_dir, safe_filename)
        
        with open(local_file_path, "wb") as f:
            f.write(file.getvalue())
        
        # Upload to stage
        try:
            put_command = f"PUT 'file://{local_file_path}' @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cursor.execute(put_command)
            st.success(f"✅ {file.name} uploaded to stage")
            
            # Parse document using Cortex
            cursor.execute("""
            SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                %s,
                %s,
                { 'mode': 'OCR' }
            )
            """, (f"@{stage_name}", safe_filename))
            
            parsed_content = cursor.fetchone()
            if parsed_content and parsed_content[0]:
                content = parsed_content[0].get('content', '')
                
                # Store metadata
                cursor.execute("""
                INSERT INTO UPLOADED_FILES_METADATA 
                (USERNAME, SESSION_ID, STAGE_NAME, FILE_NAME)
                VALUES (%s, %s, %s, %s)
                """, (
                    st.session_state.username,
                    st.session_state.session_id,
                    stage_name,
                    file.name
                ))
                
                # Process content into chunks
                chunk_size = 8192  # 8KB chunks
                chunks_created = 0
                
                for i in range(0, len(content), chunk_size):
                    chunk = content[i:i + chunk_size]
                    cursor.execute("""
                    INSERT INTO DOCS_CHUNKS_TABLE 
                    (RELATIVE_PATH, SIZE, FILE_URL, CHUNK, USERNAME, SESSION_ID)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        file.name,
                        len(chunk),
                        f"@{stage_name}/{safe_filename}",
                        chunk,
                        st.session_state.username,
                        st.session_state.session_id
                    ))
                    chunks_created += 1
                
                st.success(f"✅ Created {chunks_created} chunks for {file.name}")
                
                # Display extracted content
                st.markdown("### Extracted Content:")
                st.write(content[:1000] + "..." if len(content) > 1000 else content)
                
                # Display chunks in a table instead of nested expanders
                st.markdown("### Chunks:")
                chunks_data = []
                chunks = get_chunks_for_file(conn, file.name, st.session_state.username)
                for idx, (chunk, size) in enumerate(chunks, 1):
                    chunks_data.append({
                        "Chunk #": idx,
                        "Size (bytes)": size,
                        "Preview": chunk[:100] + "..." if len(chunk) > 100 else chunk
                    })
                st.table(chunks_data)
            
            return True
            
        except Exception as e:
            st.error(f"Stage upload error for {file.name}: {str(e)}")
            return False
            
    except Exception as e:
        st.error(f"Error processing {file.name}: {str(e)}")
        return False
    finally:
        if cursor:
            cursor.close()
        try:
            if local_file_path and os.path.exists(local_file_path):
                os.remove(local_file_path)
        except Exception:
            passimport streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
from hashlib import sha256

def init_snowflake_connection():
    """Initialize Snowflake connection"""
    return snowflake.connector.connect(
        user='SNOWHACK10',
        password='Snowhack10',
        account='nrdbnwt-qob04556',
        warehouse='COMPUTE_WH',
        database='SAMPLEDATA',
        schema='PUBLIC'
    )

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

def get_chunks_for_file(conn, filename, username):
    """Retrieve all chunks for a specific file"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT CHUNK, SIZE
        FROM DOCS_CHUNKS_TABLE 
        WHERE RELATIVE_PATH = %s 
        AND USERNAME = %s
        ORDER BY SIZE
        """, (filename, username))
        return cursor.fetchall()
    except Exception as e:
        st.error(f"Error fetching chunks: {str(e)}")
        return []
    finally:
        cursor.close()

def process_and_upload_file(conn, file, stage_name="DOCS"):
    """Process a file, upload to stage, and store chunks"""
    cursor = None
    temp_dir = "temp_uploads"
    local_file_path = None
    
    try:
        # Create temp directory if it doesn't exist
        os.makedirs(temp_dir, exist_ok=True)
        
        cursor = conn.cursor()
        file_content = file.getvalue()
        
        # Save file locally for stage upload
        safe_filename = file.name.replace(" ", "_")
        local_file_path = os.path.join(temp_dir, safe_filename)
        
        with open(local_file_path, "wb") as f:
            f.write(file_content)
        
        # Upload to stage
        try:
            put_command = f"PUT 'file://{local_file_path}' @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            cursor.execute(put_command)
            st.success(f"✅ {file.name} uploaded to stage")
            
            # Store metadata
            cursor.execute("""
            INSERT INTO UPLOADED_FILES_METADATA 
            (USERNAME, SESSION_ID, STAGE_NAME, FILE_NAME)
            VALUES (%s, %s, %s, %s)
            """, (
                st.session_state.username,
                st.session_state.session_id,
                stage_name,
                file.name
            ))
            
            # Process file content into chunks
            chunk_size = 8192  # 8KB chunks
            chunks_created = 0
            
            for i in range(0, len(file_content), chunk_size):
                chunk = file_content[i:i + chunk_size]
                
                cursor.execute("""
                INSERT INTO DOCS_CHUNKS_TABLE 
                (RELATIVE_PATH, SIZE, FILE_URL, CHUNK, USERNAME, SESSION_ID)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    file.name,
                    len(chunk),
                    f"@{stage_name}/{safe_filename}",
                    chunk.hex(),
                    st.session_state.username,
                    st.session_state.session_id
                ))
                chunks_created += 1
            
            st.success(f"✅ Created {chunks_created} chunks for {file.name}")
            
            # Display chunks
            st.write(f"### Chunks for {file.name}:")
            chunks = get_chunks_for_file(conn, file.name, st.session_state.username)
            
            for idx, (chunk, size) in enumerate(chunks, 1):
                with st.expander(f"Chunk {idx} (Size: {size} bytes)"):
                    try:
                        # Try to decode hex
                        chunk_content = bytes.fromhex(chunk).decode('utf-8', errors='ignore')
                        st.code(chunk_content[:200] + "..." if len(chunk_content) > 200 else chunk_content)
                    except (ValueError, TypeError):
                        st.code(f"Binary content: {chunk[:100]}...")
            
            return True
            
        except Exception as e:
            st.error(f"Stage upload error for {file.name}: {str(e)}")
            return False
            
    except Exception as e:
        st.error(f"Error processing {file.name}: {str(e)}")
        return False
    finally:
        if cursor:
            cursor.close()
        try:
            if local_file_path and os.path.exists(local_file_path):
                os.remove(local_file_path)
        except Exception:
            pass

def search_documents(conn, query):
    """Search documents using Cortex"""
    try:
        cursor = conn.cursor()
        
        # Get all documents for the user
        cursor.execute("""
        SELECT DISTINCT FILE_NAME 
        FROM UPLOADED_FILES_METADATA
        WHERE USERNAME = %s
        """, (st.session_state.username,))
        
        files = cursor.fetchall()
        results = []
        
        for file in files:
            filename = file[0]
            
            # Get document content
            cursor.execute("""
            SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                '@DOCS',
                %s
            )
            """, (filename,))
            
            doc_content = cursor.fetchone()
            if doc_content and doc_content[0]:
                # Get answer
                cursor.execute("""
                SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                    %s,
                    %s
                )
                """, (doc_content[0], query))
                
                answer = cursor.fetchone()
                if answer and answer[0]:
                    # Get associated chunks
                    chunks = get_chunks_for_file(conn, filename, st.session_state.username)
                    decoded_chunks = []
                    
                    for chunk, size in chunks:
                        try:
                            chunk_content = bytes.fromhex(chunk).decode('utf-8', errors='ignore')
                            decoded_chunks.append({
                                'content': chunk_content,
                                'size': size
                            })
                        except (ValueError, TypeError):
                            decoded_chunks.append({
                                'content': f"Binary content: {chunk[:100]}...",
                                'size': size
                            })
                    
                    results.append({
                        'file': filename,
                        'answer': answer[0],
                        'chunks': decoded_chunks
                    })
        
        return results
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return []
    finally:
        if cursor:
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
                    # Display search results
                    if results:
                        for i, result in enumerate(results, 1):
                            st.markdown(f"### Result {i} from {result['file']}")
                            st.markdown("**Question:**")
                            st.write(query)
                            st.markdown("**Answer:**")
                            st.write(result['answer'])
                            
                            # Display chunks in a table
                            st.markdown("**Source Chunks:**")
                            chunks_data = []
                            for j, chunk in enumerate(result['chunks'], 1):
                                chunks_data.append({
                                    "Chunk #": j,
                                    "Size (bytes)": chunk['size'],
                                    "Content": chunk['content'][:200] + "..." if len(chunk['content']) > 200 else chunk['content']
                                })
                            st.table(chunks_data)
                            st.markdown("---")
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