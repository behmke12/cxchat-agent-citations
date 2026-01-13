from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound
from google.cloud import discoveryengine_v1alpha as discoveryengine
import json
from typing import List

def get_or_create_data_store(
    project_id: str,
    location: str,
    data_store_id: str,
) -> discoveryengine.DataStore:
    """Get or create a DataStore."""

    client_options = (
        ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
        if location != "global"
        else None
    )
    client = discoveryengine.DataStoreServiceClient(client_options=client_options)
    ds_name = client.data_store_path(project_id, location, data_store_id)
    try:
        return client.get_data_store(request={"name": ds_name})
    except NotFound:
        parent = client.collection_path(project_id, location, "default_collection")
        operation = client.create_data_store(
            request={
                "parent": parent,
                "data_store": discoveryengine.DataStore(
                    display_name=data_store_id,
                    industry_vertical=discoveryengine.IndustryVertical.GENERIC,
                    acl_enabled=True, ########## we need authorized datastores
                ),
                "data_store_id": data_store_id,
            }
        )
        return operation.result()

def convert_posts_to_documents(tasks: List[dict]) -> List[discoveryengine.Document]:
    """Convert tasks into Discovery Engine Document messages."""
    docs: List[discoveryengine.Document] = []
    for task in tasks:
        payload = {
            "title": task.get("task_title", {}),
            "body": {
                "description": task.get("task_description", {}),
                "assigned_to": task.get("employee", {}),
                "assigned_to_email": task.get("employee_email", {}),
                "priority": task.get("priority", {}),
                "status": task.get("status", {}),
                "creation_date": task.get("creation_date", {}),
                "task_id": task.get("task_id")

            },
            "url": "http://localhost",
            "author": task.get("author"),
            "categories": [task.get("category")],
            "tags": task.get("tags"),
            "date": task.get("creation_date"),
        }
        doc = discoveryengine.Document(
            id=str(task.get("task_id")),
            json_data=json.dumps(payload),
            acl_info=discoveryengine.Document.AclInfo(
                readers=[{
                    "principals": [
                        {"user_id": task.get("employee_email", {})}
                    ]
                }]
            ),
        )
        docs.append(doc)
    return docs

def upload_documents_inline(
    project_id: str,
    location: str,
    data_store_id: str,
    branch_id: str,
    documents: List[discoveryengine.Document],
) -> discoveryengine.ImportDocumentsMetadata:
    """Inline import of Document messages."""

    client_options = (
        ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
        if location != "global"
        else None
    )

    client = discoveryengine.DocumentServiceClient(client_options=client_options)
    parent = client.branch_path(
        project=project_id,
        location=location,
        data_store=data_store_id,
        branch=branch_id,
    )
    request = discoveryengine.ImportDocumentsRequest(
        parent=parent,
        inline_source=discoveryengine.ImportDocumentsRequest.InlineSource(
            documents=documents,
        ),
    )
    operation = client.import_documents(request=request)
    operation.result()
    return operation.metadata