import botocore.exceptions
from flask import Flask, request
import boto3
import json
import pymysql

# ToDo
AWS_BUCKET_NAME = "esgi.iabd.bucket.s3"
AWS_ACCESS_KEY = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_BUCKET_KEY = "school_subjects.json"

app = Flask(__name__)


@app.route("/transfers", methods=['GET', 'POST'])
def transfers():
    record = json.loads(request.data)
    if record['way'] is "RDS":
        rds = RDS()
        rds.insert_data(record)
    elif record['way'] is "S3":
        s3 = S3()
        try:
            s3.s3.Object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY).load()
        except botocore.exceptions.ClientError as e:
            s3.upload([{'name': record['name'], 'description': record['description'], 'hours': record['hours']}])
        else:
            data_from_s3 = s3.s3.get_object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
            data_to_s3 = json.loads(data_from_s3['Body'].read().decode())
            data_to_s3[0].append({'name': record['name'], 'description': record['description'], 'hours': record['hours']})
            s3.upload(data_to_s3)
    elif record['way'] is "RDStoS3":
        rds = RDS()
        s3 = S3()
        cursor = rds.rds.cursor()
        cursor.execute("SELECT * FROM school_subjects")
        data_from_rds = cursor.fetchall()
        try:
            s3.s3.Object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY).load()
        except botocore.exceptions.ClientError as e:
            data_to_s3 = [{'name': data_from_rds[0][0],
                           'description': data_from_rds[0][1],
                           'hours': data_from_rds[0][2]}]
        else:
            data_from_s3 = s3.s3.get_object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
            data_to_s3 = json.loads(data_from_s3['Body'].read().decode())
        for row in data_from_rds[1:]:
            data_to_s3.append({'name': row[0], 'description': row[1], 'hours': row[2]})
        s3.upload(data_to_s3)
    else:
        s3 = S3()
        rds = RDS()
        data_from_s3 = s3.s3.get_object(Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
        data_to_rds = json.loads(data_from_s3['Body'].read().decode())
        for subject in data_to_rds:
            rds.insert_data(subject)


    return "Connexion à l'API OK"


class RDS:
    def __init__(self):
        self.rds = pymysql.connect(
            # ToDo
            host='',
            db='db_aws_project',
            user='tabernaque',
            password='michmich',
            port=3306
        )

    def insert_data(self, data):
        cur = self.rds.cursor()
        cur.execute("INSERT INTO school_subjects (name, description, hours) VALUES (%s,%s,%s)",
                    (data['name'], data['description'], [int(data['hours'])]))
        self.rds.commit()


class S3:
    def __init__(self):
        self.s3 = boto3.client('s3',
                               aws_access_key_id=AWS_ACCESS_KEY,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    def upload(self, data):
        result = self.s3.put_object(Body=json.dumps(data), Bucket=AWS_BUCKET_NAME, Key=AWS_BUCKET_KEY)
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:
            print('Fichier Upload')
        else:
            print('Fichier non Upload')


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=80)
