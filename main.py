import os
import json
import logging
from dotenv import load_dotenv
from upload_data import get_or_create_data_store, convert_posts_to_documents, upload_documents_inline

# Configure loggings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Load environment variables from .env file
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    location = os.getenv("GCP_LOCATION")
    data_store_id = os.getenv("DATA_STORE_ID")
    branch_id = os.getenv("BRANCH_ID")
    tasks_file = "tasks.jsonl" # Your input data file

    # Validate environment variables
    if not all([project_id, location, data_store_id, branch_id]):
        logging.error("Missing one or more required environment variables. Please check your .env file.")
        return

    logging.info(f"Starting data upload process for project: {project_id}, data store: {data_store_id}")

    # 1. Read tasks from tasks.jsonl
    tasks_data = []
    try:
        with open(tasks_file, 'r', encoding='utf-8') as f:
            for line in f:
                tasks_data.append(json.loads(line))
        logging.info(f"Successfully loaded {len(tasks_data)} tasks from {tasks_file}")
    except FileNotFoundError:
        logging.error(f"Error: {tasks_file} not found.")
        return
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {tasks_file}: {e}")
        return

    # 2. Get or Create DataStore
    try:
        data_store = get_or_create_data_store(project_id, location, data_store_id)
        logging.info(f"DataStore '{data_store.display_name}' (ID: {data_store_id}) is ready.")
    except Exception as e:
        logging.error(f"Failed to get or create data store: {e}")
        return

    # 3. Convert tasks to Discovery Engine Documents
    try:
        documents = convert_posts_to_documents(tasks_data)
        logging.info(f"Converted {len(documents)} tasks into Discovery Engine documents.")
    except Exception as e:
        logging.error(f"Failed to convert tasks to documents: {e}")
        return

    # 4. Upload documents inline
    try:
        metadata = upload_documents_inline(project_id, location, data_store_id, branch_id, documents)
        logging.info(f"Document upload completed. Success count: {metadata.success_count}, Failure count: {metadata.failure_count}")
        if metadata.failure_count > 0:
            logging.warning("Some documents failed to upload. Check Discovery Engine logs for details.")
    except Exception as e:
        logging.error(f"Failed to upload documents: {e}")
        return

    logging.info("Data upload process finished.")

if __name__ == "__main__":
    main()
