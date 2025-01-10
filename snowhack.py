import streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
import re
from hashlib import sha256
from pypdf import PdfReader
import io
import ftfy
import nltk



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
        pdf_reader = PdfReader(pdf_file)
        text_content = ""
        
        for page in pdf_reader.pages:
            text_content += page.extract_text() + "\n\n"
        
        return text_content
    except Exception as e:
        st.error(f"Error extracting PDF content: {str(e)}")
        return None

def clean_text(text):
    """Clean text using ftfy library and additional cleaning"""
    import ftfy
    import re
    
    # Fix text encoding issues
    text = ftfy.fix_text(text)
    
    # Additional cleaning steps
    text = re.sub(r'\s+', ' ', text)  # normalize whitespace
    text = text.strip()
    
    return text

def process_and_upload_file(conn, file, stage_name="DOCS"):
    """Process a file, upload to stage, and store chunks"""
    cursor = None
    temp_dir = "/tmp/uploads"
    local_file_path = None
    
    try:
        # Check if file already exists in session
        if check_file_exists(conn, file.name, st.session_state.username, st.session_state.session_id):
            st.warning(f"File {file.name} already processed in this session. Skipping...")
            return True

        os.makedirs(temp_dir, mode=0o777, exist_ok=True)
        
        cursor = conn.cursor()
        file_content = file.getvalue()
        
        # Extract text and clean it
        if file.name.lower().endswith('.pdf'):
            text_content = extract_text_from_pdf(file_content)
            if text_content is None:
                return False
        else:
            text_content = file_content.decode('utf-8', errors='ignore')
        
        # Clean the extracted text
        text_content = clean_text(text_content)
        
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
            
            # Process text content into chunks with overlap
            # Use NLTK for better sentence splitting
            import nltk
            try:
                nltk.data.find('tokenizers/punkt')
            except LookupError:
                nltk.download('punkt')
            
            sentences = nltk.sent_tokenize(text_content)
            
            # Combine sentences into chunks
            chunk_size = 4000
            current_chunk = []
            current_size = 0
            chunks = []
            
            for sentence in sentences:
                sentence = sentence.strip()
                sentence_size = len(sentence)
                
                if current_size + sentence_size > chunk_size and current_chunk:
                    # Join current chunk and add to chunks
                    chunk_text = ' '.join(current_chunk)
                    chunk_text = clean_text(chunk_text)
                    if chunk_text.strip():
                        chunks.append(chunk_text)
                    current_chunk = []
                    current_size = 0
                
                current_chunk.append(sentence)
                current_size += sentence_size
            
            # Add the last chunk if it exists
            if current_chunk:
                chunk_text = ' '.join(current_chunk)
                chunk_text = clean_text(chunk_text)
                if chunk_text.strip():
                    chunks.append(chunk_text)
            
            chunks_created = 0
            for chunk in chunks:
                if chunk.strip():
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
            
            # Display chunks
            st.write(f"### Chunks for {file.name}:")
            chunks = get_chunks_for_file(conn, file.name, st.session_state.username, st.session_state.session_id)
            
            for idx, (chunk, size) in enumerate(chunks, 1):
                with st.expander(f"Chunk {idx} (Size: {size} bytes)"):
                    st.markdown(chunk)
            
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

def check_file_exists(conn, filename, username, session_id):
    """Check if file already exists in the current session"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT COUNT(*) 
        FROM UPLOADED_FILES_METADATA 
        WHERE USERNAME = %s 
        AND SESSION_ID = %s 
        AND FILE_NAME = %s
        """, (username, session_id, filename))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        st.error(f"Error checking file existence: {str(e)}")
        return False
    finally:
        cursor.close()

def search_documents(conn, query):
    """Search documents using Snowflake Vector Search"""
    try:
        cursor = conn.cursor()
        
        # Execute vector search using Cortex
        cursor.execute("""
        CREATE FUNCTION IF NOT EXISTS EMBED_TEXT(input TEXT)
        RETURNS VECTOR
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.8'
        PACKAGES = ('sentence_transformers',)
        HANDLER = 'embed'
        AS $$
        from sentence_transformers import SentenceTransformer
        def embed(input_text):
            model = SentenceTransformer('all-MiniLM-L6-v2')
            return model.encode(input_text)
        $$;
        """)
        
        # Execute the vector search
        cursor.execute("""
        WITH vector_search AS (
            SELECT 
                chunk,
                relative_path,
                size,
                username,
                session_id,
                VECTOR_COSINE_SIMILARITY(
                    EMBED_TEXT(:query),
                    EMBED_TEXT(chunk)
                ) as similarity_score
            FROM docs_chunks_table
            WHERE username = :username 
            AND session_id = :session_id
            QUALIFY ROW_NUMBER() OVER (ORDER BY similarity_score DESC) <= 3
        )
        SELECT *
        FROM vector_search
        ORDER BY similarity_score DESC
        """, {
            'query': query,
            'username': st.session_state.username,
            'session_id': st.session_state.session_id
        })
        
        results = cursor.fetchall()
        
        if not results:
            return []
            
        # Format results
        formatted_results = []
        current_file = None
        current_chunks = []
        
        for chunk, file_name, size, username, session_id, score in results:
            if current_file != file_name:
                if current_file:
                    formatted_results.append({
                        'file': current_file,
                        'chunks': current_chunks
                    })
                current_file = file_name
                current_chunks = []
            
            current_chunks.append({
                'content': chunk,
                'size': size,
                'relevance': float(score)
            })
        
        # Add the last file's results
        if current_file:
            formatted_results.append({
                'file': current_file,
                'chunks': current_chunks
            })
        
        return formatted_results
        
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return []
    finally:
        if cursor:
            cursor.close()

def main():
    st.set_page_config(page_title="Document Search System", layout="wide")
    
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
        st.title("Document Search System")
        
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
        
        # Search section
        st.header("🔍 Search Documents")
        query = st.text_area("Enter your search query:")
        
        if st.button("Search"):
            if query:
                with st.spinner("Searching..."):
                    results = search_documents(conn, query)
                    # Display search results
                    if results:
                        st.markdown("### Most Relevant Chunks")
                        for i, result in enumerate(results, 1):
                            st.markdown(f"**Document: {result['file']}**")
                            
                            # Display chunks in a table
                            chunks_data = []
                            for j, chunk_info in enumerate(result['chunks'], 1):
                                chunks_data.append({
                                    "Chunk #": j,
                                    "Size (bytes)": chunk_info['size'],
                                    "Relevance Score": f"{chunk_info['relevance']:.3f}",
                                    "Content": chunk_info['content']
                                })
                            st.table(chunks_data)
                            st.markdown("---")
                    else:
                        st.info("No relevant chunks found")
            else:
                st.warning("Please enter a search query")
        
        # Logout button
        if st.sidebar.button("Logout"):
            st.session_state.clear()
            st.rerun()

if __name__ == "__main__":
    main()