import json
import platform
import os
import cx_Oracle
import glob
import subprocess

with open('passwords/credentials.json') as json_data:
    credentials = json.load(json_data)

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
cursor1 = cx_Oracle.Cursor(dbCon)
cursor2 = cx_Oracle.Cursor(dbCon)

#Set the querry to check missing records in fuelplus view but present in TM1 table. These are the records affected by GMT VAR.
_mstr = """ select * from table """
cursor.execute(_mstr,date_start='12-JUL-2021')

rows=[]
_monce = 0
_n = 0
for row in cursor:
    _mrow = [] 
    #get the gmt variance for each row.
    id =str(row[2]).strip()
    _flt_num = str(row[2]).strip()
    _flt_dep = str(row[6]).strip()
    _flt_dst = str(row[7]).strip()

    _gmtvar = """ select TIMEZONE_OFFSET from T_TIMEZONE_OFFSET where TIMEZONE = (select trim(TIMEZONE) from T_AIRPOTRS_CITIES where CITY_CODE = :city_code) GROUP BY TIMEZONE_OFFSET """
    cursor1.execute(_gmtvar,city_code=_flt_dep)

    _gmtvar = """ select TIMEZONE_OFFSET from T_TIMEZONE_OFFSET where TIMEZONE = (select trim(TIMEZONE) from T_AIRPOTRS_CITIES where CITY_CODE = :city_code) GROUP BY TIMEZONE_OFFSET """
    cursor2.execute(_gmtvar,city_code=_flt_dst)

    for row in cursor1:
        _flt_dep_gmt_var = str(row[0]).replace(':','').strip()
        # print(_flt_dep_gmt_var)

    for row in cursor2:
        _flt_dst_gmt_var = str(row[0]).replace(':','').strip()
        # print(_flt_dst_gmt_var)

    #insert into the gmt variance table for the missing records.
    print(_flt_num+'%'+_flt_dep+'%'+_flt_dst+'%'+_flt_dep_gmt_var+'%'+_flt_dst_gmt_var)
    _mrow= [_flt_num,_flt_dep,_flt_dst,_flt_dep_gmt_var,_flt_dst_gmt_var]
    rows.append(_mrow)
    _monce ==1
    _n = _n + 1
if _n>0 :
    cursor.prepare ('insert into DWH_FOUNDATION.t_aims_gmt_variances_test(FLT_NUMBER, DEP_, DST_, STD_GMTVR, STAGMTVR) values(:1, :2, :3, :4, :5)')                
    cursor.executemany(None, rows)
    rows=[]
    _n = 0
    dbCon.commit() 
    cursor1.close()
    cursor2.close()
cursor.close()
dbCon.close()
