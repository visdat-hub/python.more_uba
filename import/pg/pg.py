# postgresql connection
from __future__ import unicode_literals
import sys
sys.path.append('./classes')
import psycopg2

class db_connector():

    def __init__(self):
        self.dbConnector = {"error" : None,  "connection" : None,  "query_result" : None}

    def dbConfig(self,  db_config):
        """db configuration"""
        self.db_password = db_config['db_password']
        self.db_user = db_config['db_user']
        self.db_port = db_config['db_port']
        self.db_name = db_config['db_name']
        self.db_host = db_config['db_host']

    def dbConnect(self):
        """db connection string"""
        try:
            connector = psycopg2.connect("dbname='"+self.db_name+"' user='"+self.db_user+"' host='"+self.db_host+"' port='"+self.db_port+"' password='"+self.db_password+"'")
            self.dbConnector["connection"] = connector
            print ("OK -> database connection established...")
        except:
            self.dbConnector["error"] = sys.exc_info()
        return self.dbConnector

    def dbClose(self):
        """close connection"""
        try:
            self.dbConnector["connection"].commit()
            self.dbConnector["connection"].close()
            print("OK -> database connection closed...")
        except:
            self.dbConnector["error"] = sys.exc_info()

    def dbCopyFromCsv(self, csvFile, table):
        """copy from csv to db"""
        cur = self.dbConnector["connection"].cursor()
        with open(csvFile, 'r') as f:
            # next(f) Skip the header row.
            try:
                cur.copy_from(f, table, sep=',')
            except:
                self.dbConnector["error"] = sys.exc_info()
                print (sys.exc_info())
        cur.close()
        self.dbConnector["connection"].commit()

    def tblExecute(self, sql):
        """execute sql string"""
        cur = self.dbConnector["connection"].cursor()
        try:
            cur.execute(sql)
        except:
            self.dbConnector["error"] = sys.exc_info()
            print (sys.exc_info())
        cur.close()
        self.dbConnector["connection"].commit()

    def tblInsert(self, table,  columns,  insertPlaceholder,  valuesAsJson,  returnParam):
        """insert values into a table"""
        returnValue =  None
        if returnParam != None:
            sql = "INSERT INTO " + table + "(" + columns + ") " + \
                "VALUES(" + insertPlaceholder + ") RETURNING " + returnParam
        else:
            sql = "INSERT INTO " + table + "(" + columns + ") " + \
                "VALUES(" + insertPlaceholder + ")"
        print(sql)
        print(valuesAsJson)
        cur = self.dbConnector["connection"].cursor()
        try:
            cur.execute(sql,  valuesAsJson)
            if returnParam != None:
                returnValue = cur.fetchone()[0]
        except:
            self.dbConnector["error"] = sys.exc_info()
            print ("DATABASE ERROR")
            print (sys.exc_info())
            sys.exit()
        cur.close()
        self.dbConnector["connection"].commit()

        return returnValue

    def tblDeleteRows(self,  tblName, whereCondition):
        """delete rows from a table"""
        if whereCondition != None:
            sql = "DELETE FROM " + tblName + " WHERE " + whereCondition
        else:
            sql =  "DELETE testerror FROM " + tblName
        print (sql)
        cur = self.dbConnector["connection"].cursor()
        try:
            cur.execute(sql)
        except:
            self.dbConnector["error"] = sys.exc_info()
            print ("DATABASE ERROR")
            print (sys.exc_info())
        cur.close()
        self.dbConnector["connection"].commit()

    def tblSelect(self, sql):
        """select data from table by using a sql statement"""
        print("--> query data from database... ")
        print (sql)
        queryResult = None
        rowcount = 0
        cur = self.dbConnector["connection"].cursor()
        try:
            cur.execute(sql)
            queryResult = cur.fetchall()
            rowcount = cur.rowcount
        except:
            self.dbConnector["error"] = sys.exc_info()
            print (sys.exc_info())
        cur.close()
        print("--> row count of query result... " + str(rowcount))
        return queryResult,  rowcount
