import argparse
import json
import os
import csv
import argparse
import httplib2
import sys
import time
import datetime
import io
import hashlib
import logging
import cloudstorage as gcs
import webapp2
# Google apliclient (Google App Engine specific) libraries
from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
from google.appengine.api import app_identity
from StringIO import StringIO
import googleapiclient.http

import MySQLdb

# Google apliclient (Google App Engine specific) libraries
from oauth2client import client
from oauth2client import tools

from bottle import Bottle
import time

bottle = Bottle()

my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=15)
# All requests to GCS using the GCS client within current GAE request and
# current thread will use this retry params as default. If a default is not
# set via this mechanism, the library's built-in default will be used.
# Any GCS client function can also be given a more specific retry params
# that overrides the default.
# Note: the built-in default is good enough for most cases. We override
# retry_params here only for demo purposes.
gcs.set_default_retry_params(my_default_retry_params)

bucket_name = os.environ.get('csecloud-971.appspot.com',
                                 app_identity.get_default_gcs_bucket_name())
bucket = '/' + bucket_name
filename = bucket + '/earthquake.csv'

# Note: We don't need to call run() since our application is embedded within
# the App Engine WSGI application server.
def read_file(filename,cursor):
    with gcs.open(filename,'r') as gcs_file:
        csv_data = csv.reader(StringIO(gcs_file.read()),delimiter=',',quotechar = '"')
    print "file read succes"
    result = insert(csv_data,cursor)
    gcs_file.close()

    return result

def insert(csv_data,cursor):
   csv_data.next()
   try:
       for row in csv_data:
           row[0] = slicing(row[0])
           row[12] = slicing(row[12])
           for i in range(0,13):
               if row[i] == ' ':
                   row[i] = "''"
           place = str(row[13])
           place.replace("'"," ")
           qry = "INSERT INTO earthquake(time,latitude,longitude,depth,mag,magType,nst) VALUES('"+row[0]+"',"+row[1]+","+row[2]+","+row[3]+","+row[4]+",'"+row[5]+"',"+row[6]+","+row[7]+","+row[8]+","+row[9]+",'"+row[10]+"','"+row[11]+"','"+row[12]+"','"+place+"','"+row[14]+"',')"
           print qry
           cursor.execute(qry)
       return "success"
   except Exception as e:
       return str(e)

def slicing(str1):
    ans1 = str1[:10]+' '+str1[11:19]
    return ans1

@bottle.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    try:
        conobj = MySQLdb.connect(unix_socket='/cloudsql/csecloud-971:cloudsql',user='root')
        cursor = conobj.cursor()
        sql = "CREATE DATABASE IF NOT EXISTS myclouddata"
        print "created database"
        cursor.execute(sql)
        sql = "USE myclouddata"
        cursor.execute(sql)
        print "connected to database"
        table = "CREATE TABLE IF NOT EXISTS earthquake (time TIMESTAMP, latitude DOUBLE, longitude DOUBLE, depth DOUBLE, mag DOUBLE, magType VARCHAR(100), nst DOUBLE, gap DOUBLE, dmin DOUBLE, rms DOUBLE, net VARCHAR(500), id VARCHAR(80), updated TIMESTAMP, place VARCHAR(500), type VARCHAR(80))"
        cursor.execute(table)
        print "created table"
        ea = read_file(filename,cursor)
        conobj.commit()
        trunc = "DROP TABLE earthquake"
        cursor.execute(trunc)
        print type(ea)
        return ea
    except Exception as e :
        print str(e)
        return e

    # sample.dat file stores the short lived access tokens,which your application requests user data, attaching the access token to the request.
    # so that user need tnot validate through the browser everytime.This is optional.If the credentials don't exist
    # or are invalid run through the native client flow.The storage object will ensure that if successfull the good
    # credentials will get written back to the file(sample.dat in     is case"hurray"
# Define an handler for 404 errors.
@bottle.error(404)
def error_404(error):
    """Return a custom 404 error."""
    return 'Sorry, nothing at this URL.'
