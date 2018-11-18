import pymysql
from helpers.YamlReader import YamlReader

class MariaDb:
    def dbExecute(func):
        def dbExec(*args, **kwargs):
            config = YamlReader.ymlConfig("conf/config.yml")
            connectionString = config["db"]["connectionstring"]
            dbUsername = config["db"]["username"]
            dbPassword = config["db"]["password"]
            dbName = config["db"]["dbname"]
            try:
                db = pymysql.connect(connectionString, dbUsername, dbPassword, dbName)
                cursor = db.cursor()
                query = func(*args, **kwargs)
                cursor.execute(query)
                result = cursor.fetchall()
                db.commit()
                db.close()
                return result
            except pymysql.Error as error:
                print("Error: {}".format(error))
        return dbExec

    # Create table
    @dbExecute
    def createTable(tablename, fields):
        query = "create table if not exists %s (%s);" % (tablename, fields)
        return (query)

    # Drop table
    @dbExecute
    def dropTable(tablename):
        query = "drop table if exists %s;" % (tablename)
        return (query)

    # Insert record
    @dbExecute
    def insertRecord(tablename, fields, values):
        query = "insert into %s (%s) values %s;" % (tablename, fields, values)
        # print(query)
        return (query)

    # Select query
    @dbExecute
    def selectRecord(tablename, fields, predicate=""):
        query = "select %s from %s;" % (fields, tablename)
        return (query)






