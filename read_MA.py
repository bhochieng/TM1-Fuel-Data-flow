import json
import platform
import os
import cx_Oracle
import glob

with open('passwords/credentials.json') as json_data:
    credentials = json.load(json_data)

#check plafrom
db_host = credentials['authentication']['dwh_credentials']['host_addr']
db_username = credentials['authentication']['dwh_credentials']['db_user']
db_password = credentials['authentication']['dwh_credentials']['db_password']
db_port = credentials['authentication']['dwh_credentials']['db_port']
oracle_sid = credentials['authentication']['dwh_credentials']['oracle_sid']

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

def main ():
    print("HERE")