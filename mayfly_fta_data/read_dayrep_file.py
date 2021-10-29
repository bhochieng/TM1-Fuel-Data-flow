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

def main():
    _mcount = 0
    while _mcount <1:
        if platform.system().upper() == 'LINUX':
            _mtype = '/u01/projects/AIMS_Files/attachments/*DAYREP1.txt'
            _msoucefolder = '/u01/projects/MA_project/data/'
        else:
            _msoucefolder = 'C:\\Files\\*DAYRE*.txt'
            _mtype = _msoucefolder
        rows=[]
        for filename in glob.glob(_mtype):
            mfilename =  os.path.basename(filename )
            mfilename_db = mfilename[:19]
            print(mfilename_db)
            _monce = 0
            fh  = open(filename,'r')
            _n = 0
           
            for lines in fh:
                [_mdate_,_mflt,_mcarrier,_mtype,_mreg,_mac,_mdep,_marr,_mstd_,_msta_,_metd,_meta,_mtkof,_mtdown,_matd,_mata,_mst_,_mstd_utc,_msta_utc,_metd_utc,_meta_utc,_mtkof_utc,_mtdown_utc,_matd_utc,_mata_utc,_morign_arr_ston,_morigin_rou,_mact_blokof_date,_mact_tkof_date,_mact_tdown_date,_mact_blockon_date,_morg_date,_morg_std,_morg_sta] =['','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','']
                mlines = lines.split("\t")
                _mrow = []

                if len(mlines) >= 34 and len(mlines[0].strip()) > 5:  
                    _mdate_   = mlines[0]
                    # print(_mdate_)
                    _mflt   = mlines[1].replace('PW','').replace('*','').strip()
                    # print(_mflt)
                    _mcarrier   = mlines[2].strip()
                    _mtype   = mlines[3].strip()
                    _mreg   = mlines[4].strip()
                    _mac   = mlines[5].strip()
                    _mdep   = mlines[6].strip()
                    _marr   = mlines[7].strip()
                    _mstd_   = mlines[8].strip()
                    _msta_   = mlines[9].strip()
                    _metd   = mlines[10].strip()
                    _meta   = mlines[11].strip()
                    _mtkof   = mlines[12].strip()
                    _mtdown   = mlines[13].strip()
                    _matd   = mlines[14].strip()
                    _mata   = mlines[15].strip()
                    _mst_   = mlines[16].strip()
                    _mstd_utc   = mlines[17].strip()
                    _msta_utc   = mlines[18].strip()
                    _metd_utc   = mlines[19].strip()
                    _meta_utc   = mlines[20].strip()
                    _mtkof_utc   = mlines[21].strip()
                    _mtdown_utc   = mlines[22].strip()
                    _matd_utc   = mlines[23].strip()
                    _mata_utc   = mlines[24].strip()
                    _morign_arr_ston   = mlines[25].strip()
                    _morigin_rou   = mlines[26].strip()
                    _mact_blokof_date   = mlines[27].strip()
                    _mact_tkof_date   = mlines[28].strip()
                    _mact_tdown_date   = mlines[29].strip()
                    _mact_blockon_date   = mlines[30].strip()
                    _morg_date   = mlines[31].strip()
                    _morg_std   = mlines[32].strip()
                    _morg_sta  = mlines[33].strip()
                    _mfiledatetime = mfilename_db

                    _mrow= [_mdate_,_mflt,_mcarrier,_mtype,_mreg,_mac,_mdep,_marr,_mstd_,_msta_,_metd,_meta,_mtkof,_mtdown,_matd,_mata,_mst_,_mstd_utc,_msta_utc,_metd_utc,_meta_utc,_mtkof_utc,_mtdown_utc,_matd_utc,_mata_utc,_morign_arr_ston,_morigin_rou,_mact_blokof_date,_mact_tkof_date,_mact_tdown_date,_mact_blockon_date,_morg_date,_morg_std,_morg_sta,_mfiledatetime]
                    rows.append(_mrow)
                    _monce ==1
                    _n = _n + 1
            if _n>0 :
                cursor.prepare('insert into dwh_foundation.T_AIMS_MAYFLY_FTA_tmp(FLIGHT_DATE,FLIGHT_NUMBER,CARRIER,FLIGHT_TYPE,AIRCRAFT_REGISTRATION,AIRCRAFT_TYPE,DEPARTURE,ARRIVAL,STD,STA,ETD,ETA,TAKE_OFF,TOUCH_DOWN,ATD,ATA,LEG_STATUS,STD_UTC,STA_UTC,ETD_UTC,ETA_UTC,TKOF_UTC,TDWN_UTC,ATD_UTC,ATA_UTC,ORIGN_ARR_STON,ORIGIN_ROU,ACT_BLOKOF_DATE,ACT_TKOF_DATE,ACT_TDOWN_DATE,ACT_BLOCKON_DATE,ORG_DATE,ORG_STD ,ORG_STA ,EMAIL_FILE_DATE) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35)')
                cursor.executemany(None, rows)
                rows=[]
                _n = 0
                dbCon.commit()
        _mcount = _mcount + 1    
if __name__ == "__main__" : main()
