import streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
import yaml
from hashlib import sha256

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

def create_user_in_db(conn, username, password_hash):
    """Create user in Snowflake database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO USERS (USERNAME, PASSWORD)
        VALUES (%s, %s)
        """, (username, password_hash))
        return True
    except Exception as e:
        st.error(f"Error creating user in database: {str(e)}")
        return False
    finally:
        cursor.close()

def authenticate_user_in_db(conn, username, password_hash):
    """Authenticate user from Snowflake database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT USER_ID FROM USERS 
        WHERE USERNAME = %s AND PASSWORD = %s
        """, (username, password_hash))
        result = cursor.fetchone()
        return bool(result)
    except Exception as e:
        st.error(f"Error authenticating user: {str(e)}")
        return False
    finally:
        cursor.close()

def register_user(conn, username, password):
    """Register a new user"""
    password_hash = sha256(password.encode()).hexdigest()
    return create_user_in_db(conn, username, password_hash)

def authenticate(conn, username, password):
    """Authenticate a user"""
    password_hash = sha256(password.encode()).hexdigest()
    return authenticate_user_in_db(conn, username, password_hash)

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
            
            # Use PUT command to upload the file
            put_command = f"PUT 'file://{file_path}' @{stage_name} AUTO_COMPRESS=FALSE"
            cursor.execute(put_command)
            
            # Store metadata in database
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
            
            uploaded_files.append({
                'name': file.name,
                'stage': stage_name,
                'path': f"@{stage_name}/{file.name}"
            })
            
            st.success(f"✅ {file.name} uploaded successfully")
            
            # Clean up local file
            os.remove(file_path)
            
        except Exception as e:
            st.error(f"❌ Error uploading {file.name}: {str(e)}")
        finally:
            cursor.close()
    
    if uploaded_files:
        if 'uploaded_files' not in st.session_state:
            st.session_state.uploaded_files = []
        st.session_state.uploaded_files.extend(uploaded_files)
    
    return uploaded_files

def search_documents(conn, query, username):
    """Search through documents using Cortex"""
    try:
        cursor = conn.cursor()
        
        # Get file URLs for the user's uploaded documents
        cursor.execute("""
        SELECT DISTINCT FILE_URL
        FROM DOCS_CHUNKS_TABLE
        WHERE USERNAME = %s
        """, (username,))
        
        files = cursor.fetchall()
        results = []
        
        for file in files:
            file_url = file[0]
            # Extract stage name and file path
            stage_name, file_path = file_url[1:].split('/', 1)
            
            # Search in this document
            cursor.execute("""
            SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                SNOWFLAKE.CORTEX.PARSE_DOCUMENT(%s, %s),
                %s
            )
            """, (stage_name, file_path, query))
            
            answer = cursor.fetchone()
            if answer and answer[0]:
                results.append({
                    'file': file_path,
                    'answer': answer[0]
                })
        
        return results
        
    except Exception as e:
        st.error(f"Error searching documents: {str(e)}")
        return []
    finally:
        cursor.close()

def main():
    st.set_page_config(page_title="Document Q&A System", layout="wide")
    
    # Initialize connection
    if 'snowflake_connection' not in st.session_state:
        st.session_state.snowflake_connection = init_snowflake_connection()
    
    conn = st.session_state.snowflake_connection
    if not conn:
        st.error("Failed to connect to Snowflake")
        return
    
    # Initialize session state
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    # Custom CSS
    st.markdown("""
        <style>
        .block-container {
            padding: 2rem;
        }
        .stButton > button {
            width: 100%;
            background-color: #4CAF50;
            color: white;
            padding: 0.5rem;
            border-radius: 0.3rem;
            border: none;
        }
        .auth-form {
            background-color: white;
            padding: 2rem;
            border-radius: 1rem;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
            margin: 2rem 0;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.title("📚 Document Q&A System")
    
    # Authentication
    if not st.session_state.authenticated:
        tab1, tab2 = st.tabs(["Login", "Sign Up"])
        
        with tab1:
            with st.form("login_form"):
                st.subheader("🔐 Login")
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Login")
                
                if submitted:
                    if authenticate(conn, username, password):
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.session_state.session_id = os.urandom(16).hex()
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid credentials")
        
        with tab2:
            with st.form("signup_form"):
                st.subheader("📝 Sign Up")
                new_username = st.text_input("New Username")
                new_password = st.text_input("New Password", type="password")
                confirm_password = st.text_input("Confirm Password", type="password")
                submitted = st.form_submit_button("Sign Up")
                
                if submitted:
                    if new_password != confirm_password:
                        st.error("❌ Passwords do not match")
                    elif register_user(conn, new_username, new_password):
                        st.success("✅ Registration successful! Please login.")
                    else:
                        st.error("❌ Username already exists")
    
    else:
        # Main application
        with st.sidebar:
            st.markdown(f"### 👤 Welcome, {st.session_state.username}!")
            st.divider()
            
            # File upload section
            st.subheader("📤 Upload Documents")
            stage_name = "DOCS"  # Using fixed stage
            
            uploaded_files = st.file_uploader(
                "Choose files to upload",
                accept_multiple_files=True,
                key="file_uploader"
            )
            
            if uploaded_files:
                with st.spinner("📤 Uploading files..."):
                    upload_files_to_stage(conn, stage_name, uploaded_files)
            
            if st.button("🚪 Logout"):
                st.session_state.clear()
                st.rerun()
        
        # Main content
        if 'uploaded_files' in st.session_state and st.session_state.uploaded_files:
            st.subheader("🔍 Ask Questions")
            question = st.text_area("Enter your question about the documents:")
            
            if st.button("Search", use_container_width=True):
                if question:
                    with st.spinner("🔍 Searching..."):
                        results = search_documents(conn, question, st.session_state.username)
                        
                        if results:
                            for result in results:
                                with st.expander(f"Answer from {result['file']}"):
                                    st.write(result['answer'])
                        else:
                            st.info("No relevant answers found")
                else:
                    st.warning("Please enter a question")
        else:
            st.info("👈 Please upload some documents using the sidebar!")

if __name__ == "__main__":
    main()