import csv
from datetime import datetime
import os
import psycopg2
from time import sleep

from config import config
from dropbox_uploader import upload_file


EVALUATION_LAST_RECORD_QUERY = 'SELECT * FROM forms_evaluation ORDER BY id DESC;'
STUDENT_EVALUATION_LAST_RECORD_QUERY = 'SELECT * FROM forms_studentevaluation ORDER BY id DESC;'
EVALUATION_COMPLETE_RESULTS_QUERY = 'SELECT * FROM forms_evaluation ORDER BY id ASC;'
STUDENT_EVALUATION_COMPLETE_RESULTS_QUERY = 'SELECT * FROM forms_studentevaluation ORDER BY id ASC;'

EVENTS_CHECKER_LOG = 'log.csv'
EVALUATION_LOG = 'evaluation_'
STUDENT_EVALUATION_LOG = 'studentevaluation_'


def get_last_log_record(dirname, log_filename):
    for filename in os.listdir(dirname):
        if log_filename in filename:
            with open(os.path.join(os.getcwd(), dirname, filename), 'r', encoding='utf-8') as log:
                log_reader = log.readlines()
                last_log_record_id = log_reader[-1].split(',')[0]

                return last_log_record_id
    else:
        return 0


def get_last_db_record(sql_query):
    conn = None
    try:
        params = config()

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
        params = config()

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
    with open(os.path.join(os.getcwd(), log_filename), 'a', encoding='utf-8', newline='') as log_file:
        log_writer = csv.writer(log_file)
        log_writer.writerow([timestamp, message])


if __name__ == '__main__':
    while True:
        last_db_student_evaluation_id = get_last_db_record(STUDENT_EVALUATION_LAST_RECORD_QUERY)
        last_log_student_evaluation_id = get_last_log_record(os.path.join(os.getcwd(),'last_logs/'),
                                                            STUDENT_EVALUATION_LOG)

        if int(last_db_student_evaluation_id) != int(last_log_student_evaluation_id):
            empty_folder(os.path.join(os.getcwd(), 'last_logs/'))

            evaluation_records = get_complete_db_records(EVALUATION_COMPLETE_RESULTS_QUERY)
            student_evaluation_records = get_complete_db_records(STUDENT_EVALUATION_COMPLETE_RESULTS_QUERY)

            current_datetime = str(datetime.now().strftime("%Y%m%dT%H%M%S"))
            evaluation_log = EVALUATION_LOG + current_datetime + '.csv'
            student_evaluation_log = STUDENT_EVALUATION_LOG + current_datetime + '.csv'

            record_log(evaluation_records, 'last_logs/'+evaluation_log)
            record_log(student_evaluation_records, 'last_logs/'+student_evaluation_log)

            upload_file('/logs/evaluation/'+evaluation_log, os.path.join(os.getcwd(),
                                                                        'last_logs/',
                                                                        evaluation_log))
            upload_file('/logs/studentevaluation/'+student_evaluation_log, os.path.join(os.getcwd(),
                                                                                'last_logs/',
                                                                                student_evaluation_log))

            timestamp = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log_changes(EVENTS_CHECKER_LOG, timestamp, 'new logs')

        else:
            timestamp = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log_changes(EVENTS_CHECKER_LOG, timestamp, 'no changes')

        upload_file('/logs/'+EVENTS_CHECKER_LOG, os.path.join(os.getcwd(), EVENTS_CHECKER_LOG))

        sleep(900)
