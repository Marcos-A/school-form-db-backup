import sys
import dropbox
from dropbox.files import WriteMode, FileMetadata
from dropbox.exceptions import ApiError
from config import config_dbx


def upload_file(dropbox_path, local_file_path):
    token = config_dbx()
    dbx = dropbox.Dropbox(token)

    with open(local_file_path, 'rb') as local_file:
        try:
            dbx.files_upload(local_file.read(), dropbox_path,
                                mode=WriteMode('overwrite'))
        except ApiError as err:
            if(err.error.is_path() and
                err.error.get_path().error.is_insufficient_space()):
                sys.exit("ERROR: Cannot backup, insufficient space.")
            elif err.user_message_text:
                print(err.user_message_text)
            else:
                print(err)
                sys.exit()


def download_file(dropbox_path, local_file_path):
    token = config_dbx()
    dbx = dropbox.Dropbox(token)
    try:
        dbx.files_download_to_file(local_file_path, dropbox_path)
    # Do nothing if Dropbox file doesn't exist
    except ApiError:
        pass


def get_list_of_files(dropbox_dir_path):
    token = config_dbx()
    dbx = dropbox.Dropbox(token)

    try:
        response = dbx.files_list_folder(dropbox_dir_path)

        list_of_files = [file.name for file in response.entries
                        if isinstance(file, FileMetadata) and file.name is not None]

        return list_of_files
    # Do nothing if Dropbox dir doesn't exist
    except ApiError:
        pass
