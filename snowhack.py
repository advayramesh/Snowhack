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
import time

from snowflake.core import Root

# Add these constants at the top of the file
CORTEX_SEARCH_DATABASE = "SAMPLEDATA"
CORTEX_SEARCH_SCHEMA = "PUBLIC"
CORTEX_SEARCH_SERVICE = "docs_search_svc"

# Create NLTK data directory if it doesn't exist
nltk_data_dir = os.path.expanduser('~/nltk_data')
os.makedirs(nltk_data_dir, exist_ok=True)

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', download_dir=nltk_data_dir)

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab', download_dir=nltk_data_dir, quiet=True)
    # If punkt_tab fails, we'll fall back to punkt

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
    
    # Fix common OCR issues with word spacing
    text = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)  # Add space between camelCase
    text = re.sub(r'(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])', ' ', text)  # Add space between letters and numbers
    
    # Additional cleaning steps
    text = re.sub(r'\s+', ' ', text)  # normalize whitespace to single spaces
    text = text.replace(' .', '.').replace(' ,', ',')  # fix common spacing issues
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)  # Add space between words
    text = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', text)  # Add space between letters and numbers
    text = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', text)  # Add space between numbers and letters
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
            
            # Process text content into chunks with proper spacing
            try:
                sentences = nltk.sent_tokenize(text_content)
                # Add space after each sentence
                sentences = [s.strip() + ' ' for s in sentences]
            except LookupError:
                # Fall back to simple sentence splitting if NLTK fails
                sentences = [s.strip() + ' ' for s in re.split(r'[.!?]+', text_content) if s.strip()]
                st.warning("Using basic sentence splitting due to NLTK resource unavailability")
            
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
                        # Ensure proper spacing between sentences
                        chunk_text = chunk_text.replace('.', '. ').replace('!', '! ').replace('?', '? ')
                        chunk_text = re.sub(r'\s+', ' ', chunk_text).strip()
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
                    # Ensure proper spacing between sentences
                    chunk_text = chunk_text.replace('.', '. ').replace('!', '! ').replace('?', '? ')
                    chunk_text = re.sub(r'\s+', ' ', chunk_text).strip()
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
    """Search documents using Snowflake Cortex Search Service"""
    try:
        # Get Snowpark session and root
        session = st.session_state.snowflake_connection
        root = Root(session)
        
        # Get the search service
        svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
        
        # Create filter for current user and session using @and
        filter_obj = {
            "@and": [
                {"@eq": {"username": st.session_state.username}},
                {"@eq": {"session_id": st.session_state.session_id}}
            ]
        }
        
        # Execute search with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = svc.search(
                    query=query,
                    columns=["chunk", "relative_path", "size"],
                    filter=filter_obj,
                    limit=3
                )
                
                # Convert response to list of tuples for compatibility
                results = []
                if isinstance(response, dict):
                    response_data = response
                elif hasattr(response, 'to_json'):
                    response_data = response.to_json()
                else:
                    response_data = {'hits': []}
                
                st.write("Debug - Response:", response_data)  # Debug output
                
                if isinstance(response_data, dict) and 'hits' in response_data:
                    for hit in response_data['hits']:
                        if isinstance(hit, dict):
                            results.append((
                                hit.get('chunk', ''),
                                hit.get('relative_path', ''),
                                hit.get('size', 0),
                                hit.get('_score', 1.0)
                            ))
                
                if results:  # If we got results, break the retry loop
                    break
                elif attempt < max_retries - 1:  # If no results and not last attempt
                    st.warning(f"Retrying search... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(2)  # Wait before retrying
            
            except Exception as e:
                st.error(f"Search attempt {attempt + 1} error: {str(e)}")  # Debug output
                if attempt < max_retries - 1:
                    st.warning(f"Search attempt {attempt + 1} failed, retrying...")
                    time.sleep(2)
                else:
                    raise e
        
        return results
    
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return []

def generate_response(conn, query, context_chunks):
    """Generate response using basic text concatenation"""
    try:
        # Combine context chunks
        context = "\n\n".join([chunk[0] for chunk in context_chunks])
        
        # For now, return a simple response
        return f"Found {len(context_chunks)} relevant documents. Here are the key excerpts:\n\n{context}"
    except Exception as e:
        st.error(f"Error generating response: {str(e)}")
        return "Sorry, I couldn't generate a response."

def check_search_service_status(conn):
    """Check if the Cortex Search Service exists and has data"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SHOW SEARCH SERVICES IN SAMPLEDATA.PUBLIC;
        """)
        services = cursor.fetchall()
        
        for service in services:
            if service[1] == 'docs_search_svc':  # service name is in second column
                return "READY"  # If service exists, assume it's ready
        
        return "NOT FOUND"
    except Exception as e:
        st.error(f"Error checking service status: {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()

def main():
    st.set_page_config(page_title="Document Search & QA System", layout="wide")
    
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
        st.title("Document Search & QA System")
        
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
        
        # Search and QA section
        st.header("🔍 Search & Ask Questions")
        
        # Check service status
        service_status = check_search_service_status(conn)
        if service_status:
            st.sidebar.info(f"Search Service Status: {service_status}")
        
        query = st.text_area("Enter your question:")
        
        if st.button("Ask"):
            if query:
                with st.spinner("Thinking..."):
                    # Retrieve relevant chunks
                    relevant_chunks = search_documents(conn, query)
                    
                    st.write(relevant_chunks)
                    if relevant_chunks:
                        # Generate response using context
                        response = generate_response(conn, query, relevant_chunks)
                        
                        # Display response
                        st.markdown("### Answer")
                        st.write(response)
                        
                        # Display source documents
                        with st.expander("View Source Documents"):
                            for i, (chunk, file_name, size, score) in enumerate(relevant_chunks, 1):
                                st.markdown(f"**Source {i}: {file_name}**")
                                st.markdown(f"Relevance Score: {float(score):.3f}")
                                st.markdown(chunk)
                                st.markdown("---")
                    else:
                        st.info("No relevant information found in the documents.")
            else:
                st.warning("Please enter a question")
        
        # Logout button
        if st.sidebar.button("Logout"):
            st.session_state.clear()
            st.rerun()

if __name__ == "__main__":
    main()