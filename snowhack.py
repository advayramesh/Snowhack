import streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
from hashlib import sha256
import PyPDF2
import io

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

def extract_text_from_pdf(file_content):
    """Extract text content from PDF file"""
    try:
        pdf_file = io.BytesIO(file_content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        text_content = ""
        
        for page in pdf_reader.pages:
            text_content += page.extract_text() + "\n\n"
        
        return text_content
    except Exception as e:
        st.error(f"Error extracting PDF content: {str(e)}")
        return None

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
        
        # Extract text if PDF, otherwise use raw content
        if file.name.lower().endswith('.pdf'):
            text_content = extract_text_from_pdf(file_content)
            if text_content is None:
                return False
        else:
            text_content = file_content.decode('utf-8', errors='ignore')
        
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
            
            # Process text content into chunks
            chunk_size = 4000  # Reduced chunk size for better readability
            chunks = [text_content[i:i + chunk_size] for i in range(0, len(text_content), chunk_size)]
            chunks_created = 0
            
            for chunk in chunks:
                if chunk.strip():  # Only store non-empty chunks
                    cursor.execute("""
                    INSERT INTO DOCS_CHUNKS_TABLE 
                    (RELATIVE_PATH, SIZE, FILE_URL, CHUNK, USERNAME, SESSION_ID)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        file.name,
                        len(chunk),
                        f"@{stage_name}/{safe_filename}",
                        chunk,  # Store plain text instead of hex
                        st.session_state.username,
                        st.session_state.session_id
                    ))
                    chunks_created += 1
            
            st.success(f"✅ Created {chunks_created} chunks for {file.name}")
            
            # Display chunks
            st.write(f"### Chunks for {file.name}:")
            chunks = get_chunks_for_file(conn, file.name, st.session_state.username, st.session_state.session_id)
            
            for idx, (chunk, size) in enumerate(chunks, 1):
                with st.expander(f"Chunk {idx} (Size: {size} bytes)"):
                    st.markdown(chunk)  # Display as markdown for better formatting
            
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

def get_chunks_for_file(conn, filename, username, session_id):
    """Retrieve all chunks for a specific file"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT CHUNK, SIZE
        FROM DOCS_CHUNKS_TABLE 
        WHERE RELATIVE_PATH = %s 
        AND USERNAME = %s
        AND SESSION_ID = %s
        ORDER BY SIZE
        """, (filename, username, session_id))
        return cursor.fetchall()
    except Exception as e:
        st.error(f"Error fetching chunks: {str(e)}")
        return []
    finally:
        cursor.close()

def search_documents(conn, query):
    """Search documents using Cortex with layout mode and session-based filtering"""
    try:
        cursor = conn.cursor()
        
        # Get documents only from current session
        cursor.execute("""
        SELECT DISTINCT FILE_NAME 
        FROM UPLOADED_FILES_METADATA
        WHERE USERNAME = %s
        AND SESSION_ID = %s
        """, (st.session_state.username, st.session_state.session_id))
        
        files = cursor.fetchall()
        results = []
        
        for file in files:
            filename = file[0]
            
            # Get document content using layout mode
            cursor.execute("""
            SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                '@DOCS',
                %s,
                {'mode': 'LAYOUT'}
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
                    # Get associated chunks from current session
                    chunks = get_chunks_for_file(
                        conn, 
                        filename, 
                        st.session_state.username,
                        st.session_state.session_id
                    )
                    results.append({
                        'file': filename,
                        'answer': answer[0],
                        'chunks': chunks
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
                            for j, (chunk_content, size) in enumerate(result['chunks'], 1):
                                chunks_data.append({
                                    "Chunk #": j,
                                    "Size (bytes)": size,
                                    "Content": chunk_content[:200] + "..." if len(chunk_content) > 200 else chunk_content
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