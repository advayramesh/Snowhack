import streamlit as st
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
import os
import yaml
from hashlib import sha256

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
        return False
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
            
            # Use PUT command to upload the file to the stage
            put_command = f"PUT 'file://{file_path}' @{stage_name} AUTO_COMPRESS=FALSE"
            cursor.execute(put_command)
            
            # Add file to session state
            if "uploaded_files" not in st.session_state:
                st.session_state.uploaded_files = []
            st.session_state.uploaded_files.append({
                "name": file.name,
                "stage": stage_name,
                "path": f"@{stage_name}/{file.name}"
            })
            
            uploaded_files.append(file.name)
            st.success(f"✅ {file.name} uploaded successfully")
            
            # Clean up local file
            os.remove(file_path)
        
        except ProgrammingError as e:
            st.error(f"❌ Error uploading {file.name}: {str(e)}")
        finally:
            cursor.close()
    return uploaded_files

def process_documents(conn, stage_name, file_path):
    """Process a document using Snowflake Cortex"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
            '@{stage_name}',
            '{file_path}'
        )
        """)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        st.error(f"Error processing document: {str(e)}")
        return None
    finally:
        cursor.close()

def extract_answer(conn, content, question):
    """Extract answer using Snowflake Cortex"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(%s, %s)
        """, (content, question))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        st.error(f"Error extracting answer: {str(e)}")
        return None
    finally:
        cursor.close()

# --- Main App ---
def main():
    st.set_page_config(page_title="Document Q&A System", layout="wide")
    
    # Custom CSS
    st.markdown("""
        <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        .stButton>button {
            width: 100%;
            background-color: #4CAF50;
            color: white;
            border: none;
            padding: 0.5rem 1rem;
            border-radius: 0.3rem;
        }
        .stButton>button:hover {
            background-color: #45a049;
        }
        .auth-form {
            background-color: #ffffff;
            padding: 2rem;
            border-radius: 1rem;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
            margin: 2rem auto;
            max-width: 400px;
        }
        .success-message {
            padding: 1rem;
            border-radius: 0.5rem;
            background-color: #d4edda;
            color: #155724;
            margin: 1rem 0;
        }
        .file-list {
            margin: 1rem 0;
            padding: 1rem;
            border-radius: 0.5rem;
            background-color: #f8f9fa;
        }
        .question-box {
            background-color: #ffffff;
            padding: 2rem;
            border-radius: 1rem;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
            margin: 1rem 0;
        }
        .uploaded-file {
            padding: 0.5rem;
            background-color: #e9ecef;
            border-radius: 0.3rem;
            margin: 0.5rem 0;
        }
        .centered-tabs {
            display: flex;
            justify-content: center;
            gap: 1rem;
            margin-bottom: 2rem;
        }
        .tab-button {
            padding: 0.5rem 2rem;
            border: none;
            background-color: #f8f9fa;
            cursor: pointer;
            border-radius: 0.3rem;
        }
        .tab-button.active {
            background-color: #4CAF50;
            color: white;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.title("📚 Document Q&A System")
    
    # Initialize session state
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False
        st.session_state["show_signup"] = False
    
    # Authentication
    if not st.session_state["authenticated"]:
        # Center the app title
        st.markdown("<h1 style='text-align: center;'>📚 Document Q&A System</h1>", unsafe_allow_html=True)
        
        # Add tabs for login/signup
        col1, col2, col3 = st.columns([1,2,1])
        with col2:
            tab1, tab2 = st.tabs(["Login", "Sign Up"])
            
            with tab1:
                st.markdown("<div class='auth-form'>", unsafe_allow_html=True)
                st.subheader("🔐 Login")
                username = st.text_input("Username", key="login_username")
                password = st.text_input("Password", type="password", key="login_password")
                if st.button("Login", key="login_button"):
                    if authenticate(username, password):
                        st.session_state["authenticated"] = True
                        st.session_state["username"] = username
                        st.session_state["session_id"] = os.urandom(16).hex()
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid credentials")
                st.markdown("</div>", unsafe_allow_html=True)
            
            with tab2:
                st.markdown("<div class='auth-form'>", unsafe_allow_html=True)
                st.subheader("📝 Sign Up")
                new_username = st.text_input("New Username", key="signup_username")
                new_password = st.text_input("New Password", type="password", key="signup_password")
                confirm_password = st.text_input("Confirm Password", type="password", key="signup_confirm")
                if st.button("Sign Up", key="signup_button"):
                    if new_password != confirm_password:
                        st.error("❌ Passwords do not match")
                    elif register_user(new_username, new_password):
                        st.success("✅ Registration successful! Please login.")
                    else:
                        st.error("❌ Username already exists")
                st.markdown("</div>", unsafe_allow_html=True)
    
    else:
        # Initialize Snowflake connection
        if "snowflake_connection" not in st.session_state:
            st.session_state.snowflake_connection = init_snowflake_connection()
        
        conn = st.session_state.snowflake_connection
        
        if conn:
            # Sidebar for file upload
            with st.sidebar:
                st.markdown("### 👤 User Profile")
                st.info(f"Logged in as: {st.session_state.username}")
                st.markdown("---")
                
                st.markdown("### 📤 Upload Files")
                stages = list_stages(conn)
                
                if stages:
                    stage_names = [stage[1] for stage in stages]
                    selected_stage = st.selectbox("Select Stage:", stage_names)
                    
                    uploaded_files = st.file_uploader(
                        "Choose files to upload",
                        accept_multiple_files=True,
                        key="file_uploader"
                    )
                    
                    if uploaded_files:
                        with st.spinner("📤 Uploading files..."):
                            successful_uploads = upload_files_to_stage(
                                conn, selected_stage, uploaded_files
                            )
                
                # Show uploaded files
                if "uploaded_files" in st.session_state:
                    st.markdown("### 📁 Your Uploaded Files")
                    for file in st.session_state.uploaded_files:
                        st.markdown(f"""
                            <div class='uploaded-file'>
                                📄 {file['name']}
                            </div>
                        """, unsafe_allow_html=True)
                
                st.markdown("---")
                if st.button("🚪 Logout", key="logout"):
                    st.session_state.clear()
                    st.rerun()
            
            # Main content area
            st.markdown("## 🤖 Ask Questions About Your Documents")
            
            # Display upload prompt if no files
            if "uploaded_files" not in st.session_state or not st.session_state.uploaded_files:
                st.warning("👈 Please upload some documents using the sidebar first!")
            else:
                st.markdown("""
                    <div class='question-box'>
                        <p>Ask any question about your uploaded documents:</p>
                    </div>
                """, unsafe_allow_html=True)
                
                question = st.text_area("Your Question:", height=100, 
                                      placeholder="Enter your question here...")
                
                col1, col2, col3 = st.columns([1,2,1])
                with col2:
                    if st.button("🔍 Get Answer", use_container_width=True):
                        if question:
                            with st.spinner("🤔 Analyzing documents..."):
                                all_content = []
                                
                                # Process each uploaded document
                                for file in st.session_state.uploaded_files:
                                    content = process_documents(
                                        conn, file['stage'], file['name']
                                    )
                                    if content:
                                        all_content.append(content)
                                
                                if all_content:
                                    # Extract answer from all documents
                                    combined_content = " ".join(all_content)
                                    answer = extract_answer(conn, combined_content, question)
                                    
                                    if answer:
                                        st.markdown("""
                                            <div style='background-color: #f8f9fa; padding: 1rem; 
                                                        border-radius: 0.5rem; margin-top: 1rem;'>
                                                <h3 style='color: #4CAF50;'>💡 Answer</h3>
                                                <p style='margin-top: 0.5rem;'>{}</p>
                                            </div>
                                        """.format(answer), unsafe_allow_html=True)
                                    else:
                                        st.info("🤔 No relevant answer found in the documents")
                                else:
                                    st.warning("⚠️ No document content available")
                        else:
                            st.warning("⚠️ Please enter a question first") in st.session_state:
                        with st.spinner("🤔 Analyzing documents..."):
                            all_content = []
                            
                            # Process each uploaded document
                            for file in st.session_state.uploaded_files:
                                content = process_documents(
                                    conn, file['stage'], file['name']
                                )
                                if content:
                                    all_content.append(content)
                            
                            if all_content:
                                # Extract answer from all documents
                                combined_content = " ".join(all_content)
                                answer = extract_answer(conn, combined_content, question)
                                
                                if answer:
                                    st.markdown("### 💡 Answer")
                                    st.write(answer)
                                else:
                                    st.info("No relevant answer found in the documents")
                            else:
                                st.warning("No document content available")
                    else:
                        st.warning("Please upload some documents and ask a question")
            
            with col2:
                st.subheader("📊 Session Info")
                st.markdown(f"**User:** {st.session_state.username}")
                st.markdown(f"**Files uploaded:** {len(st.session_state.uploaded_files) if 'uploaded_files' in st.session_state else 0}")

if __name__ == "__main__":
    main()