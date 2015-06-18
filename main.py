#NAME: NISPAND MEHTA
#ID_NUMBER: 1001163146
#BATCH TIME: 3.30 to 5.30 p.m.
"""Links Referred::
https://www.facebook.com/l.php?u=https%3A%2F%2Fconsole.developers.google.com%2Fstart%2Fappengine%3F_ga%3D1.237094793.840266212.1433886398&h=CAQFXCrYN
http://stackoverflow.com/questions/3215140/google-app-engine-appcfg-py-rollback
https://cloud.google.com/appengine/docs/python/googlecloudstorageclient/
http://nealbuerger.com/2013/12/google-app-engine-import-csv-to-datastore/
https://www.daniweb.com/software-development/python/threads/228057/insert-file-data-in-mysql-using-python
http://stackoverflow.com/questions/15064376/python-read-a-csv-and-place-values-in-mysql-database
"""
import cgi
import os
import csv
import cloudstorage as gcs


# Google apliclient (Google App Engine specific) libraries
from google.appengine.api import app_identity
from StringIO import StringIO

from bottle import route, request, response, template, HTTPResponse

import MySQLdb

# Google apliclient (Google App Engine specific) libraries

from bottle import Bottle
import time

bottle = Bottle()
conobj = MySQLdb.connect(unix_socket='/cloudsql/csecloud-971:cloudsql',user='root')
cursor = conobj.cursor()
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
#set bucket name and connect it to default bucket
bucket_name = os.environ.get('csecloud-971.appspot.com',
                                 app_identity.get_default_gcs_bucket_name())
bucket = '/' + bucket_name
filename = bucket + '/all_month.csv'
#Route to get CSV file from User
#It will return a template defined in views folder
@bottle.route('/route_to_upload_file')
def route_to_upload_file():
    return template('get_file',name="hello")

@bottle.route('/web_interface')
def web_interface():
    return  template('Query_page',name="hello")

@bottle.route('/process_query', method = 'GET')
def process_query():
    mag = request.GET.get('mag')
    par1 = request.GET.get('par1')
    par2 = request.GET.get('par2')
    location = request.GET.get('loc')
    print mag
    print par1
    print par2
    print location
    sql = "USE myclouddata"
    cursor.execute(sql)
    def_query = "SELECT * from earthquake WHERE "
    if location == None:
        data = fun_for_mag_only(def_query,mag,par1)

    elif mag == None :
        fun_for_location_only(def_query,location)

    else:
        fun_for_macandloc(def_query,mag,par1,par2,location)
    resp = HTTPResponse(body = data,status = 200)
    print data
    return data

def fun_for_mag_only(query,mag,par1):
    if par1 == 'grt':
        query = query + " mag > "+ mag
        print query
    elif par1 == 'less':
        query = query + " mag < " + mag
        print query
    elif par1 == 'equal':
        query = query + "mag = " + mag
        print query
    elif par1 == 'lte':
        query = query + "mag < = " + mag
        print query
    elif par1 == 'gte':
        query == query + "mag > =" +mag
        print query
    cursor.execute(query)
    data = cursor.fetchall()
    ans = " "
    for x in data:
        ans = ans + str(x) + "<br>"
        ans = ans + "=========================================================================================================================<br>"

    return ans

def fun_for_location_only(query,location):

    return "Done"

def fun_for_macandloc(query,mag,par1,par2,location):
    return "Done"


#function to upload file on google bucket
@bottle.route('/upload_file',method='POST')
def upload_file():
    start_time = time.time()
    bucket_name = os.environ.get('BUCKET_NAME',app_identity.get_default_gcs_bucket_name())
    filetoupload = "/"+bucket_name+"/all_months.csv"
    contents = request.files.get('contents')
    raw = contents.file.read()
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filetoupload,'w',content_type='text/plain',options={'x-goog-meta-foo': 'foo','x-goog-meta-bar': 'bar'},retry_params=write_retry_params)
    gcs_file.write(raw)
    gcs_file.close()
    end_time = time.time()
    time_taken = end_time-start_time
    return template('time_taken_to_upload',time_taken=time_taken)

#Read File from bucket
def read_file(filename,cursor):
    with gcs.open(filename,'r') as gcs_file:
        csv_data = csv.reader(StringIO(gcs_file.read()),delimiter=',',quotechar = '"')
    print csv_data
    print "file read successfull"
    result = insert(csv_data,cursor)
    gcs_file.close()

    return result

#Insert into Google MySQLdb
def insert(csv_data,cursor):
   print csv_data
   csv_data.next()
   print csv_data
   try:
       for row in csv_data:
           row[0] = slicing(row[0])
           row[12] = slicing(row[12])
           for i in range(0,14):
               if row[i] == '':
                   row[i] = "''"
           place = str(row[13])
           place = place.replace("'","")
           qry = "INSERT INTO earthquake(time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type) VALUES('"+row[0]+"',"+row[1]+","+row[2]+","+row[3]+","+row[4]+",'"+row[5]+"',"+row[6]+","+row[7]+","+row[8]+","+row[9]+",'"+row[10]+"','"+row[11]+"','"+row[12]+"','"+place+"','"+row[14]+"')"
           cursor.execute(qry)
       return "success"
   except Exception as e:
       return str(e)

def slicing(str1):
    ans1 = str1[:10]+' '+str1[11:19]
    return ans1

@bottle.route('/')
def main_fun():
    try:
        sql = "CREATE DATABASE IF NOT EXISTS myclouddata"
        print "created database"
        cursor.execute(sql)
        sql = "USE myclouddata"
        cursor.execute(sql)
        print "connected to database"
        table = "CREATE TABLE IF NOT EXISTS earthquake (time TIMESTAMP, latitude DOUBLE, longitude DOUBLE, depth DOUBLE, mag DOUBLE, magType VARCHAR(100), nst DOUBLE, gap DOUBLE, dmin DOUBLE, rms DOUBLE, net VARCHAR(500), id VARCHAR(80), updated TIMESTAMP, place VARCHAR(500), type VARCHAR(80))"
        cursor.execute(table)
        print "created table"
        start_time = time.time()
        ea = read_file(filename,cursor)
        timetaken = time.time() - start_time
        conobj.commit()
        extract = "select week(time),mag,count(id) from earthquake group by mag,week(time) having mag in (2,3,4,5) or mag>5"
        cursor.execute(extract)
        ans = "<table><tr><th>Week</th><th>mag</th><th>Number of quakes</th></tr>"
        data = cursor.fetchall()
        for x in data:
            ans = ans +"<tr><td>" + str(x[0]) + "</td><td>" + str(x[1]) + "</td><td>" + str(x[2]) +"</td></tr>"
        ans = ans + "</table>"
        qry = "Select count(*) from earthquake"
        cursor.execute(qry)
        count = cursor.fetchall()
        print count
        trunc = "DROP TABLE earthquake"
        cursor.execute(trunc)
        return "Time taken to insert into database MySql ="+str(timetaken)+"<br>Number of Earthquake Greater than 2,3,4,5::<br>"+ ans

    except Exception as e :
        print str(e)
        return e

# Define an handler for 404 errors.
@bottle.error(404)
def error_404(error):
    """Return a custom 404 error."""
    return 'Sorry, nothing at this URL.'
