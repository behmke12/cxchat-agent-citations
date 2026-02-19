### Functions pulled from Olejniczak Lukasz Medium Post below:
### Title Integrating knowledge systems with Google Agentspace — custom Connectors/Datastores
### URL https://medium.com/google-cloud/integrating-knowledge-systems-with-google-agentspace-custom-connectors-datastores-3a9d2750f0e1
import logging
import os
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud import discoveryengine_v1alpha as discoveryengine

def get_or_create_data_store(project_id, location, data_store_id):
    client_options = ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
    client = discoveryengine.DataStoreServiceClient(client_options=client_options)
    ds_name = client.data_store_path(project_id, location, data_store_id)
    
    try:
        return client.get_data_store(request={"name": ds_name})
    except NotFound:
        parent = client.collection_path(project_id, location, "default_collection")
        
        # Create a "Clean" Data Store with NO pre-defined solution type
        data_store = discoveryengine.DataStore(
            display_name=data_store_id,
            industry_vertical=discoveryengine.IndustryVertical.GENERIC,
            content_config=discoveryengine.DataStore.ContentConfig.CONTENT_REQUIRED,
            # DO NOT add solution_types here; let Dialogflow CX do it
        )

        operation = client.create_data_store(
            request={
                "parent": parent,
                "data_store": data_store,
                "data_store_id": data_store_id,
            }
        )
        return operation.result()

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    return f"gs://{bucket_name}/{destination_blob_name}"

def import_documents_with_metadata(
    project_id: str,
    location: str,
    data_store_id: str,
    branch_id: str,
    gcs_metadata_jsonl_uri: str,
):
    client_options = ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
    client = discoveryengine.DocumentServiceClient(client_options=client_options)
    
    # In CX/Discovery Engine, branch is usually '0' or 'default_branch'
    parent = client.branch_path(project_id, location, data_store_id, branch_id)

    # Use 'document' schema to link JSONL manifest entries (metadata) to PDF content
    request = discoveryengine.ImportDocumentsRequest(
        parent=parent,
        gcs_source=discoveryengine.GcsSource(
            input_uris=[gcs_metadata_jsonl_uri],
            # 'document' is required for manifests containing content URIs + structData
            data_schema="document", 
        ),
        # ReconciliationMode.INCREMENTAL adds new docs without deleting existing ones
        reconciliation_mode=discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL,
    )

    logging.info(f"Triggering import for {data_store_id} using 'document' schema...")
    operation = client.import_documents(request=request)
    return operation