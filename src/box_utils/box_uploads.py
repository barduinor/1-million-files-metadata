from box_sdk_gen import BoxClient, UploadFileAttributes, File, UploadFileAttributesParentField


def box_upload_file(client: BoxClient, folder_id: str, file_path: str) -> File:
    """Uploads a file to Box"""
    file_name = file_path.split("/")[-1]
    file_attributes = UploadFileAttributes(
        name=file_name, parent=UploadFileAttributesParentField(id=folder_id)
    )
    with open(file_path, "rb") as file:
        box_file = client.uploads.upload_file(file_attributes, file)
    return box_file.entries[0]


async def box_upload_file_async(client: BoxClient, folder_id: str, file_path: str) -> File:
    return box_upload_file(client, folder_id, file_path)


def box_delete_file(client: BoxClient, file_id: str):
    client.files.delete_file_by_id(file_id)


async def box_delete_file_async(client: BoxClient, file_id: str):
    box_delete_file(client, file_id)
