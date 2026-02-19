import os
import json
import re
import logging
from dotenv import load_dotenv
from upload_data import get_or_create_data_store, upload_to_gcs, import_documents_with_metadata

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    load_dotenv()
    project_id = os.getenv("GCP_PROJECT_ID")
    location = os.getenv("GCP_LOCATION")
    data_store_id = os.getenv("DATA_STORE_ID")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    branch_id = "0" 

    # 1. Initialize
    get_or_create_data_store(project_id, location, data_store_id)

    # 2. Files to process
    files_to_process = [
        {
            "local_path": "goog-10-q-q1-2025.pdf", 
            "title": "Q1 2025 10K Google", 
            "url": "https://www.google.com"
        },
        {
            "local_path": "goog-10-q-q2-2025.pdf", 
            "title": "Q2 2025 10K Google", 
            "url": "https://www.wikipedia.org"
        },
        {
            "local_path": "GOOG-10-Q-Q3-2025.pdf", 
            "title": "Q3 2025 10K Google", 
            "url": "https://princexml.com/samples/journal"
        },
        {
            "local_path": "GOOG-10-Q-Q4-2025.pdf", 
            "title": "Q4 2025 10K Google", 
            "url": "https://abc.xyz/investor/"
        }
    ]

    manifest_entries = []

    for item in files_to_process:
        if not os.path.exists(item["local_path"]):
            logging.warning(f"File {item['local_path']} not found locally.")
            continue

        gcs_pdf_uri = upload_to_gcs(bucket_name, item["local_path"], item["local_path"])
    
        # ID must be alphanumeric/hyphens/underscores
        clean_id = item["local_path"].replace(".", "-").replace(" ", "-")

        # This follows the documentation's 'Using structData' example EXACTLY
        entry = {
            "id": clean_id, # Use 'id' for the 'document' schema
            "content": {
                "mimeType": "application/pdf",
                "uri": gcs_pdf_uri
            },
            "structData": {
                "title": item["title"],
                "url": item["url"]
            }
        }
        manifest_entries.append(json.dumps(entry))

    if not manifest_entries:
        logging.error("No documents were prepared. Check your local filenames.")
        return

    # 3. Create the local JSONL manifest and upload it
    manifest_file = "metadata_manifest.jsonl"
    with open(manifest_file, "w") as f:
        f.write("\n".join(manifest_entries))
    
    logging.info(f"Uploading manifest {manifest_file} to GCS...")
    gcs_manifest_uri = upload_to_gcs(bucket_name, manifest_file, manifest_file)

    # 4. Final Step: Import the manifest using 'custom' schema logic
    logging.info(f"Importing metadata from {gcs_manifest_uri}...")
    try:
        operation = import_documents_with_metadata(
            project_id, location, data_store_id, branch_id, gcs_manifest_uri
        )
        logging.info(f"Started import operation: {operation.operation.name}")
        logging.info("This may take a few minutes. Check the console for completion.")
    except Exception as e:
        logging.error(f"Error during import: {e}")

if __name__ == "__main__":
    main()