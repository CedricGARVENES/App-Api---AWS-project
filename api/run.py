from flask import Flask, request
import boto3
import json
import pymysql

# ToDo
AWS_BUCKET_NAME = "esgi.iabd.bucket.s3"
AWS_ACCESS_KEY = ""
AWS_SECRET_ACCESS_KEY = ""

app = Flask(__name__)


@app.route("/transfers", methods=['GET', 'POST'])
def transfers():
    record = json.loads(request.data)
    if record['way'] == "RDS":
        rds = RDS()
        rds.insert_data(record)
    elif record['way'] == "S3":
        s3 = S3()
        s3.upload(record)
    elif record['way'] == "RDStoS3":
        rds = RDS()
        s3 = S3()
        cursor = rds.rds.cursor()
        cursor.execute("SELECT * FROM school_subjects")
        data_from_rds = cursor.fetchall()
        data_to_s3 = json.loads(json.dumps(
            {'name': data_from_rds[0]['name'],
             'description': data_from_rds[0]['description'],
             'hours': data_from_rds[0]['hours']}
        ))
        for row in data_from_rds[1:]:
            data_to_s3.append(json.loads(json.dumps(
                {'name': row['name'], 'description': row['description'], 'hours': row['hours']}
            )))
        s3.upload(data_to_s3)
    else:
        s3 = S3()
        rds = RDS()

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
        body = json.dumps({'name': data['name'], 'description': data['description'], 'hours': data['hours']})
        result = self.s3.put_object(Body=body, Bucket=AWS_BUCKET_NAME, Key="school_subjects.json")
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:
            print('Fichier Upload')
        else:
            print('Fichier non Upload')


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=80)
