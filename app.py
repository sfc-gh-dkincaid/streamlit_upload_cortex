import os
import io
import pandas as pd
import streamlit as st
import json
from snowflake.snowpark.context import get_active_session
from typing import Optional, Dict, Any

# -------------------------------------
# Get Snowflake session
# -------------------------------------
session = get_active_session()

# -------------------------------------
# Stage existence check and creation
# -------------------------------------
def ensure_stage_exists(stage_name_no_at: str):
    """
    Creates a stage if it doesn't exist. Does nothing if it already exists.
    """
    try:
        # Check if stage exists
        session.sql(f"DESC STAGE {stage_name_no_at}").collect()
    except:
        # Create stage if it doesn't exist
        try:
            session.sql(f"""
                CREATE STAGE {stage_name_no_at}
                ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
            """).collect()
            st.sidebar.success(f"Stage @{stage_name_no_at} has been created.")
        except Exception as e:
            st.sidebar.error(f"Failed to create stage: {str(e)}")
            st.stop()

def parse_document(stage_name: str, file_name: str) -> Optional[str]:
        """Parse document using Snowflake's parse_document function."""
        try:
                
            # Construct the file path in the stage
            file_path = f"@{stage_name}/{file_name}.pdf"
            
            parse_sql = f"""
            SELECT snowflake.cortex.PARSE_DOCUMENT(
                {stage_name}, '{file_name}',{{'mode': 'OCR'}}
            ):content::varchar as parsed_content
            """
            
            result = session.sql(parse_sql).collect()
            
            if result and result[0]:                
                return result[0][0]
            else:
                st.error("No content extracted from document")
                return None
                
        except Exception as e:
            st.error(f"Failed to parse document: {str(e)}")
            return None

def ai_complete(document_content: str, question: str) -> Optional[str]:
        """Use Snowflake's ai_complete function to answer questions about the document."""
        try:
            # Construct the prompt
            prompt = f"""
            Based on the following document content, please answer the question:
            
            Document Content:
            {document_content}
            
            Question: {question}
            
            Please provide a comprehensive answer based only on the information in the document.
            """
            
            # Use AI_COMPLETE function
            ai_sql = f"""
            SELECT AI_COMPLETE(
                'openai-gpt-4.1',
                '{prompt}'
            )::varchar as response
            """
            
            result = session.sql(ai_sql).collect()
            
            if result:
                return result[0][0]
            else:
                return "No response generated"
                
        except Exception as e:
            st.error(f"Failed to get AI completion: {str(e)}")
            return None

# -------------------------------------
# Main Streamlit app
# -------------------------------------
def main():
    st.title("Snowflake Chat With Your Files")

    # -------------------------
    # Stage settings
    # -------------------------
    st.sidebar.header("Stage Settings")
    stage_name_no_at = st.sidebar.text_input(
        "Enter stage name (e.g., MY_INT_STAGE)",
        "MY_INT_STAGE"
    )
    stage_name = f"@{stage_name_no_at}"

    # Create stage if it doesn't exist
    ensure_stage_exists(stage_name_no_at)

    # -------------------------
    # Create tabs
    # -------------------------
    tab_upload, tab_url, tab_download = st.tabs([
        "File Upload",
        "Generate Presigned URL",
        "File Download"
    ])

    # -------------------------
    # File upload tab
    # -------------------------
    with tab_upload:
        st.header("File Upload")
        st.write("Upload files to Snowflake stage.")

        uploaded_file = st.file_uploader("Choose a file")

        if uploaded_file:
            file_extension = os.path.splitext(uploaded_file.name)[1].lower()
            try:
                # Create file stream using BytesIO and upload
                file_stream = io.BytesIO(uploaded_file.getvalue())
                session.file.put_stream(
                    file_stream,
                    f"{stage_name}/{uploaded_file.name}",
                    auto_compress=False,
                    overwrite=True
                )
                st.success(f"File '{uploaded_file.name}' has been uploaded successfully!")

                st.subheader("❓ Ask Questions")

                question = st.text_area(
                    "Ask a question about the document:",
                    placeholder="e.g., What are the main topics discussed in this document?",
                    height=100
                )


                if question:
                    parsed_content = parse_document(stage_name,uploaded_file.name)
                    answer = ai_complete(parsed_content,question)

                    if answer:
                        st.subheader("🤖 AI Response")
                        st.write(answer)

            except Exception as e:
                st.error(f"Error occurred while uploading file: {str(e)}")

    # -------------------------
    # Presigned URL generation tab
    # -------------------------
    with tab_url:
        st.header("Generate Presigned URL")
        st.write("Generate presigned URLs for files in the stage.")

        # Get list of files in stage
        stage_files = session.sql(f"LIST {stage_name}").collect()
        if stage_files:
            file_names = [
                row['name'].split('/', 1)[1] if '/' in row['name'] else row['name']
                for row in stage_files
            ]

            with st.form("url_generation_form"):
                selected_file = st.selectbox(
                    "Select a file to generate URL",
                    file_names
                )
                expiration_days = st.slider(
                    "Select expiration period (days)",
                    min_value=1,
                    max_value=7,
                    value=1,
                    help="Choose between 1 to 7 days"
                )

                submitted = st.form_submit_button("Generate URL")
                if submitted:
                    try:
                        expiration_seconds = expiration_days * 24 * 60 * 60
                        url_statement = f"""
                            SELECT GET_PRESIGNED_URL(
                                '@{stage_name_no_at}',
                                '{selected_file}',
                                {expiration_seconds}
                            )
                        """
                        result = session.sql(url_statement).collect()
                        signed_url = result[0][0]

                        st.success("URL generated successfully!")
                        st.write(f"Presigned URL (valid for {expiration_days} days):")
                        st.code(signed_url)
                    except Exception as e:
                        st.error(f"An error occurred: {str(e)}")
        else:
            st.warning("No files found in stage.")

    # -------------------------
    # File download tab
    # -------------------------
    with tab_download:
        st.header("File Download")
        st.write("Download files from stage.")

        # Get list of files in stage
        stage_files = session.sql(f"LIST {stage_name}").collect()
        if stage_files:
            file_names = [
                row['name'].split('/', 1)[1] if '/' in row['name'] else row['name']
                for row in stage_files
            ]
            selected_file = st.selectbox(
                "Select a file to download",
                file_names
            )

            if st.button("Download"):
                try:
                    with session.file.get_stream(f"{stage_name}/{selected_file}") as file_stream:
                        file_content = file_stream.read()
                        st.download_button(
                            label="Download File",
                            data=file_content,
                            file_name=selected_file,
                            mime="application/octet-stream"
                        )
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")
        else:
            st.warning("No files found in stage.")

# -------------------------------------
# Launch app
# -------------------------------------
if __name__ == "__main__":
    main() 
