import csv
from datetime import datetime
import os
import psycopg2
from time import sleep
from config import config_db
from dropbox_uploader import download_file, get_list_of_files, upload_file


EVALUATION_LAST_RECORD_QUERY = 'SELECT * FROM forms_evaluation ORDER BY id DESC;'
STUDENT_EVALUATION_LAST_RECORD_QUERY = 'SELECT * FROM forms_studentevaluation ORDER BY id DESC;'
EVALUATION_COMPLETE_RESULTS_QUERY = 'SELECT * FROM forms_evaluation ORDER BY id ASC;'
STUDENT_EVALUATION_COMPLETE_RESULTS_QUERY = 'SELECT * FROM forms_studentevaluation ORDER BY id ASC;'

EVENTS_LOG = 'events_log.csv'
EVALUATION_LOG = 'evaluation_'
STUDENT_EVALUATION_LOG = 'studentevaluation_'


def get_last_log_record(dirname, log_filename):
    for filename in os.listdir(dirname):
        if filename.startswith(log_filename):
            with open(os.path.join(os.getcwd(), dirname, filename), 'r', encoding='utf-8') as log:
                log_reader = log.readlines()
                last_log_record_id = log_reader[-1].split(',')[0]

                return last_log_record_id
    else:
        return None


def get_last_db_record(sql_query):
    conn = None
    try:
        params = config_db()

        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        cursor.execute(sql_query)
        last_db_record_id = cursor.fetchall()[0][0]

        cursor.close()
        conn.commit()

        return last_db_record_id
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_complete_db_records(sql_query):
    conn = None
    try:
        params = config_db()

        conn = psycopg2.connect(**params)
        cursor = conn.cursor()

        cursor.execute(sql_query)
        complete_db_records = cursor.fetchall()

        cursor.close()
        conn.commit()

        return complete_db_records
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def record_log(data, log_filename):
    with open(log_filename, 'w', encoding='utf-8', newline='') as log:
        log_writer = csv.writer(log)

        for record in data:
            log_writer.writerow(record)


def empty_folder(dirname):
    for filename in os.listdir(dirname):
        file_path = os.path.join(dirname, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)


def log_changes(log_filename, timestamp, message):
    # Download latest Dropbox events log file if it exists
    download_file('/logs/'+EVENTS_LOG, os.path.join(os.getcwd(), EVENTS_LOG))

    # Update events log file or create a new one if there wasn't one in Dropbox
    with open(os.path.join(os.getcwd(), log_filename), 'a', encoding='utf-8', newline='') as log_file:
        log_writer = csv.writer(log_file)
        log_writer.writerow([timestamp, message])


def get_local_log_file(dirname, log_filename):
    if not os.listdir(dirname):
        return None
    else:
        for filename in os.listdir(dirname):
            if filename.startswith(log_filename):
                return filename
        return None


if __name__ == '__main__':
    while True:
        # Compare local student evaluation log file with latest student evaluation log files at Dropbox
        student_evaluation_current_log_file = get_local_log_file('last_db_logs', STUDENT_EVALUATION_LOG)
        # Check if Dropbox student evaluation log file exists
        if get_list_of_files('/logs/studentevaluation'):
            student_evaluation_latest_backup_file = get_list_of_files('/logs/studentevaluation')[-1]
        else:
            student_evaluation_latest_backup_file = None

        # If local latest student evaluation log files is outdated or doesn't exist,
        # replace them with the most recent Dropbox log files
        if (student_evaluation_current_log_file is None or
            (student_evaluation_latest_backup_file is not None and
             student_evaluation_current_log_file < student_evaluation_latest_backup_file)):
            empty_folder(os.path.join(os.getcwd(), 'last_db_logs/'))
            download_file('/logs/studentevaluation/'+student_evaluation_latest_backup_file,
                          'last_db_logs/'+student_evaluation_latest_backup_file)
            evaluation_latest_backup_file = get_list_of_files('/logs/evaluation')[-1]
            download_file('/logs/evaluation/'+evaluation_latest_backup_file,
                           'last_db_logs/'+evaluation_latest_backup_file)        

        # Compare last logged student evaluation id with last database student evaluation id
        last_db_student_evaluation_id = get_last_db_record(STUDENT_EVALUATION_LAST_RECORD_QUERY)
        last_log_student_evaluation_id = get_last_log_record(os.path.join(os.getcwd(),'last_db_logs/'),
                                                            STUDENT_EVALUATION_LOG)

        # If databzse last logged student evaluation id doesn't match last database student evaluation id
        # or doesn't exist
        if (last_log_student_evaluation_id is None or
            int(last_db_student_evaluation_id) != int(last_log_student_evaluation_id)):
            # Delete local evaluation and student evaluation log files
            empty_folder(os.path.join(os.getcwd(), 'last_db_logs/'))

            # Get evaluation and evaluation records from database and save both to CSV log files
            evaluation_records = get_complete_db_records(EVALUATION_COMPLETE_RESULTS_QUERY)
            student_evaluation_records = get_complete_db_records(STUDENT_EVALUATION_COMPLETE_RESULTS_QUERY)

            current_datetime = datetime.now()
            timestamp_for_filename = str(current_datetime.strftime("%Y%m%dT%H%M%S"))
            evaluation_log = EVALUATION_LOG + timestamp_for_filename + '.csv'
            student_evaluation_log = STUDENT_EVALUATION_LOG + timestamp_for_filename + '.csv'

            record_log(evaluation_records, 'last_db_logs/'+evaluation_log)
            record_log(student_evaluation_records, 'last_db_logs/'+student_evaluation_log)

            # Upload database log files to Dropbox
            upload_file('/logs/evaluation/'+evaluation_log, os.path.join(os.getcwd(),
                                                                        'last_db_logs/',
                                                                        evaluation_log))
            upload_file('/logs/studentevaluation/'+student_evaluation_log, os.path.join(os.getcwd(),
                                                                                'last_db_logs/',
                                                                                student_evaluation_log))

            # Update events log file
            timestamp_for_log = str(current_datetime.strftime("%Y-%m-%d %H:%M:%S"))
            if last_log_student_evaluation_id is None:
                log_changes(EVENTS_LOG, timestamp_for_log, 'first local logs created, ' +\
                                                                   'new logs, ' +\
                                                                   'uploaded logs not necessarily recorded ' +\
                                                                   'real changes in the database')
            else:
                log_changes(EVENTS_LOG, timestamp_for_log, 'new logs')

        else:
            # If Dropbox student evaluation log file doesn't exist, upload local database log files to Dropbox
            if not get_list_of_files('/logs/studentevaluation'):
                evaluation_log = get_local_log_file('last_db_logs/', EVALUATION_LOG)
                upload_file('/logs/evaluation/'+evaluation_log, os.path.join(os.getcwd(),
                                                                        'last_db_logs/',
                                                                        evaluation_log))
                student_evaluation_log = get_local_log_file('last_db_logs/', STUDENT_EVALUATION_LOG)
                upload_file('/logs/studentevaluation/'+student_evaluation_log, os.path.join(os.getcwd(),
                                                                                'last_db_logs/',
                                                                                student_evaluation_log))


            timestamp = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log_changes(EVENTS_LOG, timestamp, 'no changes')

        # Upload events log file to Dropbox
        upload_file('/logs/'+EVENTS_LOG, os.path.join(os.getcwd(), EVENTS_LOG))

        # Sleep for N seconds given until next check
        sleep(900)
        