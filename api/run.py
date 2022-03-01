import botocore.exceptions
from flask import Flask, request
import boto3
import json
import pymysql

# ToDo
AWS_ACCESS_KEY = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_BUCKET_NAME = "esgi.iabd.bucket.s3"
AWS_BUCKET_KEY = "school_subjects.json"

AWS_DB_ENDPOINT = ""
AWS_DB_NAME = "db_aws_project"

app = Flask(__name__)


class RDS:
    def __init__(self):
        self.rds = pymysql.connect(
            host=AWS_DB_ENDPOINT,
            db=AWS_DB_NAME,
            user='tabernaque',
            password='michmich',
            port=3306
        )

    def insert_data(self, data):
        cur = self.rds.cursor()
        if cur.execute("SELECT * FROM school_subjects WHERE name = %s", data['name']) == 0:
            try:
                cur.execute("INSERT INTO school_subjects (name, description, hours) VALUES (%s,%s,%s)",
                            (data['name'], data['description'], [int(data['hours'])]))
                self.rds.commit()
            except pymysql.Error as e:
                return '{{"message": "{error}", "category": "danger"}}'.format(error=e)
            else:
                return '{{"message": "{message}", "category": "success"}}'.format(
                    message="La matière {subject}, a bien été ajouté dans la table "
                            "{endpoint} -> {db_name} -> {table_name}".format(
                             subject=data['name'],
                             endpoint=AWS_DB_ENDPOINT,
                             db_name=AWS_DB_NAME,
                             table_name="school_subjects"
                             )
                )
        else:
            return '{{"message": "La matière {subject}, existe déjà dans la table ' \
                   '{endpoint} -> {db_name} -> {table_name}", "category": "warning"}}'.format(
                    subject=data['name'],
                    endpoint=AWS_DB_ENDPOINT,
                    db_name=AWS_DB_NAME,
                    table_name="school_subjects"
                    )


class S3:
    def __init__(self):
        self.s3 = boto3.client('s3',
                               aws_access_key_id=AWS_ACCESS_KEY,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    def upload(self, data, way):
        try:
            self.s3.put_object(Body=json.dumps(data), Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
        except botocore.exceptions.ClientError as e:
            return '{{"message": "{error}", "category": "danger"}}'.format(error=e)
        else:
            return '{{"message": "{message}", "category": "success"}}'.format(
                message="La matière {subject}, a bien été ajouté au fichier {key} du S3 {bucket}".format(
                    subject=data[-1]['name'],
                    key=AWS_BUCKET_KEY,
                    bucket=AWS_BUCKET_NAME
                ) if way == "S3" else
                "Les données de la base de données RDS {endpoint} -> {db_name} "
                "ont bien été ajouté au fichier {key} du S3 {bucket}".format(
                    endpoint=AWS_DB_ENDPOINT,
                    db_name=AWS_DB_NAME,
                    key=AWS_BUCKET_KEY,
                    bucket=AWS_BUCKET_NAME
                )
            )


def rds_to_s3(way):
    rds = RDS()
    s3 = S3()
    cursor = rds.rds.cursor()
    cursor.execute("SELECT * FROM school_subjects")
    data_from_rds = cursor.fetchall()
    try:
        data_from_s3 = s3.s3.get_object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
    except botocore.exceptions.ClientError as e:
        data_to_s3 = [{'name': data_from_rds[0][0],
                       'description': data_from_rds[0][1],
                       'hours': data_from_rds[0][2]}]
    else:
        data_to_s3 = json.loads(data_from_s3['Body'].read().decode())
    for row in data_from_rds[1:]:
        check_duplicate = False
        for subject in data_to_s3:
            if subject['name'] == row[0]:
                check_duplicate = True
        if not check_duplicate:
            data_to_s3.append({'name': row[0], 'description': row[1], 'hours': row[2]})
    response = s3.upload(data_to_s3, way)
    return response


def s3_to_rds():
    s3 = S3()
    rds = RDS()
    data_from_s3 = s3.s3.get_object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
    data_to_rds = json.loads(data_from_s3['Body'].read().decode())
    for subject in data_to_rds:
        response = rds.insert_data(subject)
        if json.loads(response)['category'] == 'danger':
            return response
    return '{{"message": "{message}", "category": "success"}}'.format(
        message="Les données du fichier {key} du S3 {bucket} ont bien été ajouté à la base de données RDS "
                "{endpoint} -> {db_name}".format(key=AWS_BUCKET_KEY,
                                                 bucket=AWS_BUCKET_NAME,
                                                 endpoint=AWS_DB_ENDPOINT,
                                                 db_name=AWS_DB_NAME
                                                 )
    )


@app.route("/transfers", methods=['GET', 'POST'])
def transfers():
    record = json.loads(request.data)
    if record['way'] == "RDS":
        rds = RDS()
        res = rds.insert_data(record)
    elif record['way'] == "S3":
        s3 = S3()
        try:
            data_from_s3 = s3.s3.get_object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
        except botocore.exceptions.ClientError as e:
            res = s3.upload([{'name': record['name'], 'description': record['description'], 'hours': record['hours']}],
                            record['way'])
        else:
            data_to_s3 = json.loads(data_from_s3['Body'].read().decode())
            for subject in data_to_s3:
                if subject['name'] == record['name']:
                    return '{{"message": "La matière {subject}, existe déjà dans le fichier ' \
                           '{key} du S3 {bucket}", "category": "warning"}}'.format(
                            subject=record['name'],
                            key=AWS_BUCKET_KEY,
                            bucket=AWS_BUCKET_NAME
                            )
            data_to_s3.append({'name': record['name'], 'description': record['description'], 'hours': record['hours']})
            res = s3.upload(data_to_s3, record['way'])
    elif record['way'] == "RDStoS3":
        res = rds_to_s3(record['way'])
    else:
        res = s3_to_rds()
    return res


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=80)
