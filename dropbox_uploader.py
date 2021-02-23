import sys
import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError
from config import config_dbx


def upload_file(backup_path, local_filename):
    token = config_dbx()
    dbx = dropbox.Dropbox(token)

    with open(local_filename, 'rb') as local_file:
        try:
            dbx.files_upload(local_file.read(), backup_path,
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
