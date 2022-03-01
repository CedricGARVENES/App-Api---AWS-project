import pymysql


def run():
    db = pymysql.connect(
        # ToDo
        host='',
        user='tabernaque',
        password='michmich',
        port=3306
    )
    cursor = db.cursor()

    sql = '''create database db_aws_project'''
    cursor.execute(sql)
    cursor.connection.commit()

    sql = '''use db_aws_project'''
    cursor.execute(sql)

    sql = '''CREATE TABLE school_subjects ( name varchar(30), 
                                            description varchar(500), 
                                            hours integer), 
                                            primary key (name)
                                            )'''
    cursor.execute(sql)


if __name__ == '__main__':
    run()
