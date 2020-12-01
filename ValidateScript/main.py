import csv
import json
import os
from datetime import datetime

import boto3
import mysql.connector
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from exception import WrongExtError, MissingS3FileError, MissingDataError, ZeroSizeError

load_dotenv()

s3Resource = boto3.resource('s3')
s3 = boto3.client('s3')
account_id = os.environ.get('BC_ACCOUNT')

config = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_DATABASE'),
    'raise_on_warnings': True
}


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


def validate_file(key, file_type):
    # Check if video has information about file path
    if not key or key == '' or key == 'not_found':
        raise MissingDataError(f"Path for {file_type} in DB is missing. ID:{row['id']}; Value:{key}")

    # Check if file exist in S3
    try:
        response = s3.head_object(Bucket='brightcove-archive', Key=key)
    except ClientError as e:
        print(e)
        print(f"Object doesn\'t exist ID:{row['id']} {key}")
        raise MissingS3FileError(f"ID:{row['id']} Object doesn\'t exist: {key}")

    # Check if file size is not 0
    min_siz = 1024 if file_type == 'video' else 1
    ext = os.path.splitext(key)[-1].lower()

    if file_type == 'video' and ext not in video_ext():
        raise WrongExtError(f"Wrong extension {ext}; Type: {file_type}; File:{key}")

    if file_type in ('thumbnail_path', 'poster_path') and (ext not in image_ext()):
        raise WrongExtError(f"Wrong extension {ext}; Type: {file_type}; File:{key}")

    if response['ContentLength'] <= min_siz:
        raise ZeroSizeError(f"File is smaller t4010578267001han 1024 bytes File:{key}")

    return response


def validate_project(video):
    status = {
        'account_id': video['account_id'],
        'id': video['id'],
        'master_path': video['master_path'],
        'master': None,
        'poster': None,
        'thumb': None
    }
    # Master file
    try:
        s3_response = validate_file(video['master_path'], 'video')
        # print(f"Object exist {row['master_path']}")
        # if video['master_size'] != s3_response['ContentLength']:
        #     status['master'] = 'Wrong size'

    except MissingDataError:
        status['master'] = 'Data missing'

    except WrongExtError:
        status['master'] = 'Wrong ext'

    except MissingS3FileError:
        status['master'] = 'S3 missing'

    except Exception as e:
        print(f"ID:{video['id']} Error: {e}")
        # errors[row['id']].append(f'{e}; master_path:{row["master_path"]}')

    # Thumbnail
    try:
        validate_file(video['thumbnail_path'], 'thumbnail_path')
    except MissingDataError:
        status['thumb'] = 'Data missing'

    except WrongExtError:
        status['thumb'] = 'Wrong ext'

    except MissingS3FileError:
        status['thumb'] = 'S3 missing'

    except Exception as e:
        print(f"ID:{video['id']} Error: {e}")
        # errors[row['id']].append(f'{e}; thumbnail_path:{row["thumbnail_path"]}')

    # Poster
    try:
        validate_file(video['poster_path'], 'poster_path')

    except MissingDataError:
        status['poster'] = 'Data missing'

    except MissingS3FileError:
        status['poster'] = 'S3 missing'

    except WrongExtError:
        status['poster'] = 'Wrong ext'


    except Exception as e:
        print(f"ID:{video['id']} Error: {e}")
        # errors[row['id']].append(f'{e}; poster_path: {row["poster_path"]}')

    if not status['master'] and not status['poster'] and not status['thumb']:
        return False

    return status


def generate_csv(dict_data):
    csv_columns = ['account_id', 'id', 'master_path', 'master', 'poster', 'thumb']
    csv_file = f"csv/{account_id}_{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError:
        print("I/O error")


def video_ext():
    return '.mp4', '.mpeg', '.m4v', '.mov', '.wmv', '.flv', '.mpg', '.mxf', '.3gp', '.m2t', '.f4v', '.avi', '.vob', '.m4v', '.webm', '.mts', '.mp3', '.m4a'


def image_ext():
    return '.jpeg', '.jpg', '.gif', '.png',


def get_items(account_id):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(dictionary=True)

    # query = "SELECT * from videos where (master_path != '' and master_path IS NOT NULL and master_path != 'not_found') and account_id=%s"
    query = "SELECT * from videos_with_size where account_id=%s LIMIT %d OFFSET %d"
    # query = "SELECT * from videos where account_id=%s"
    limit = os.environ.get('DB_LIMIT')
    offset = limit * int(os.environ.get('DB_OFFSET_ITERATION'))
    adr = (account_id, limit, offset)

    cursor.execute(query, adr)
    results = cursor.fetchall()
    cursor.close()
    cnx.close()
    return results


if __name__ == '__main__':
    begin_time = datetime.now()
    print(begin_time)
    print_hi('PyCharm')

    items = get_items(account_id)

    total = 0
    video_count = 0
    errors = []
    for row in items:
        total += 1
        processed = validate_project(row)

        if processed:
            errors.append(processed)

    generate_csv(errors)
    print(f"total processed: {total};")
    print(json.dumps(errors, sort_keys=True, indent=4))
    print(datetime.now()-begin_time)
