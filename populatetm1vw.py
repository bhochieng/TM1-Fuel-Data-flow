#!/usr/bin/python
import cx_Oracle
import csv
import os
import json
import datetime
import platform
from datetime import  timedelta
from datetime import datetime
now = datetime.now()
dt_string = now.strftime("%d%m%Y%H:%M:%S.%f")
with open('passwords/credentials.json') as json_data:
    credentials = json.load(json_data)
base_path = credentials['authentication']['dwh_credentials']['project_base_path']

#Authencticate to Oracle DB
db_host = credentials['authentication']['dwh_credentials']['host_addr']
db_username = credentials['authentication']['dwh_credentials']['db_user']
db_password = credentials['authentication']['dwh_credentials']['db_password']
db_port = credentials['authentication']['dwh_credentials']['db_port']
oracle_sid = credentials['authentication']['dwh_credentials']['oracle_sid']
#check plafrom
if platform.system().upper() == 'LINUX':
    os.environ['LD_LIBRARY_PATH'] = credentials['authentication']['dwh_credentials']['ld_library_path']
    os.environ['ORACLE_HOME'] = credentials['authentication']['dwh_credentials']['oracle_home']
    os.environ['ORACLE_SID'] = credentials['authentication']['dwh_credentials']['oracle_sid']
    dsn_tns = cx_Oracle.makedsn(db_host, db_port, oracle_sid)
    dbCon = cx_Oracle.connect(db_username, db_password, dsn_tns)
else:
    lib_dir = (credentials['authentication']['dwh_credentials']['windows_lib_dir'])
    cx_Oracle.init_oracle_client(lib_dir)
    dsn_tns = cx_Oracle.makedsn(db_host, db_port, oracle_sid)
    dbCon = cx_Oracle.connect(db_username, db_password, dsn_tns)
cursor = cx_Oracle.Cursor(dbCon) 

#truncate table before load
truncate_tbl = """ delete  from t_sabre_mc_aims_merge where numericflightdate >= to_date('12-JUL-2021') """
cursor.execute(truncate_tbl )
dbCon.commit()

print("done deleting")
print ("inserting")
_mstr = """ insert into t_sabre_mc_aims_merge
(select DISTINCT
                                                 numericflightdate,
                                                 flightnumber,
                                                 origin,
                                                 destination,
                                                 tailnumber,
                                                 prelocal,
                                                 pretransit,
                                                 pretotal,
                                                 infantlocal,
                                                 infanttransit,
                                                 infanttotal,
                                                 ecolocal,
                                                 ecotransit,
                                                 ecototal,
                                                 actualpax02localpad,
                                                 actualpax02thrupad,
                                                 actualpax02totalpad,
                                                 blockhours,
                                                 actype,
                                                 men,
                                                 women,
                                                 CHILD,
                                                 infant,
                                                 flttime,
                                                 '' as depdly  from vw_aims_ma_report_daily_24072021_back) """
cursor.execute(_mstr )
dbCon.commit()

cursor.close()
dbCon.close()

#subprocess.call(['./mv_oases_prod.sh']) 
#./mv_oases.sh
execfile(base_path+"TM1_Fuelplus_Projects/scripts/populate_gmt_var.py")

