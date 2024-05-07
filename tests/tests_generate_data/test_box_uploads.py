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
    test_file = "tests/test_data/test_file.txt"
    with open(test_file, "wb") as file:
        file.write(os.urandom(10))

    # upload the test file to Box
    file = upload_file(client, folder_id, test_file)

    # check if the file was uploaded successfully
    assert file.name == "test_file.txt"
    assert file.size == 10
    assert file.parent.id == folder_id

    # delete the test file
    os.remove(test_file)
