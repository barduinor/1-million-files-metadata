import os

from src.box_utils.box_uploads import upload_file
from src.box_utils.box_client_ccg import ConfigCCG, get_ccg_user_client


def test_upload_file():
    config = ConfigCCG()
    client = get_ccg_user_client(config, config.ccg_user_id)
    folder_id = config.folder_id

    # create test data folder if not exists
    test_data_folder = "tests/test_data"
    if not os.path.exists(test_data_folder):
        os.makedirs(test_data_folder)

    # create a test file with 10 random bytes
    test_file = os.path.join(test_data_folder, "test_file.txt")
    with open(test_file, "wb") as file:
        file.write(os.urandom(10))

    # upload the test file to Box
    box_file = upload_file(client, folder_id, test_file).entries[0]

    # check if the file was uploaded successfully
    assert box_file.name == "test_file.txt"
    assert box_file.size == 10
    assert box_file.parent.id == folder_id

    # delete the test file
    client.files.delete_file_by_id(box_file.id)
    os.remove(test_file)
