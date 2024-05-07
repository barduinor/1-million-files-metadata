from box_sdk_gen import BoxClient, UploadFileAttributes, File, UploadFileAttributesParentField
from box_client_ccg import ConfigCCG


def upload_file(client: BoxClient, folder_id: str, file_path: str) -> File:
    """Uploads a file to Box"""
    file_name = file_path.split("/")[-1]
    file_attributes = UploadFileAttributes(
        name=file_name, parent=UploadFileAttributesParentField(id=folder_id)
    )
    with open(file_path, "rb") as file:
        box_file = client.uploads.upload_file(file_attributes, file)
    return box_file
