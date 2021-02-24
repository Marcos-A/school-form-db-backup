# -*- coding: UTF-8 -*-
from configparser import ConfigParser
import os


# Configure PostgreSQL database parameters
def config_db(filename=os.path.join(os.getcwd(),'database.ini'), section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1] 
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


# Configure Dropbox API v2 token
def config_dbx(filename=os.path.join(os.getcwd(),'dropbox.ini'), section='dropbox'):
    parser = ConfigParser()
    parser.read(filename)

    if parser.has_section(section):
        params = parser.items(section)
        token = params[0][1]
        return token
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
