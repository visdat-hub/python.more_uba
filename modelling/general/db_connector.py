# -*- coding: UTF-8 -*-
import psycopg2

class db_connector():
    def __init__(self):
        self.__returnVal = 'no_data'

    ## Definition of connectionparams to the oekoservice database
    #  the connection is realised with psycopg2

    def db_connect(self):
        #try:
        connector = psycopg2.connect("dbname='more_uba' user='visdat' host='192.168.0.194' port='9991' password='9Leravu6'")
        return connector
        #except Exception, e:
        #    print (e)
