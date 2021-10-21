import json
import platform
import os
import cx_Oracle
import glob
import subprocess

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
    _mcount = 0
    while _mcount < 1:
        if platform.system().upper() == 'LINUX':
            _msoucefolder = '/u01/projects/AIMS_Files/attachments/'
        else:
            _msoucefolder = 'C:\\Files\\'
        rows=[]
        _mtype = _msoucefolder+'*109905.txt'
        print(_mtype)
        #check if the file exists in the folder
        for filename in glob.glob(_mtype):
            mfilename =  os.path.basename(filename )
            mfilename_db = mfilename[:19]
            print(mfilename_db)
            _monce = 0
            fh  = open(filename,'r')
            _n = 0
            for lines in fh:
                [_mday_flt,_mflt,_mdep,_marr,_mreg,_mexppas,_mtransit,_mpason,_mpasoff,_mtotal_pax,_mac,_mblof,_mblon,_mtkof,_matd_local,_mata_local,_mtdown,_mlegstatus,_mactual_block_time,_mflight_time_mvt,_mmvt ] =['','','','','','','','','','','','','','','','','','','','','']
                mlines = lines.split(",")
                _mrow = []                
                if mlines[0].strip() != 'DAY':
                    _mday_flt     =  mlines[0].strip()
                    _mflt     = mlines[1].strip()
                    _mdep     = mlines[2].replace('"','').strip()
                    _marr     = mlines[3].replace('"','').strip()
                    _mreg     = mlines[4].replace('"','').strip()
                    _mexppas_temp = mlines[5].replace('"','').split()
                    _mexppas  = _mexppas_temp[0].strip() #ingle input to 
                    _mexppas_1  = _mexppas_temp[1].strip() #ingle input to exppas
                    _mexppas_2  = _mexppas_temp[2].strip() #ingle input to exppas
                    _mexppas_3  = _mexppas_temp[3].strip() #ingle input to exppas
                    _mexppas_4  = _mexppas_temp[4].strip() #ingle input to exppas
                    _mexppas_5  = _mexppas_temp[5].strip() #ingle input to exppas
                    _mexppas_6  = _mexppas_temp[6].strip() #ingle input to exppas
                    _mexppas_7  = _mexppas_temp[7].strip() #ingle input to exppas
                    _mexppas_8  = _mexppas_temp[8].strip() #ingle input to exppas
                    _mexppas_9  = _mexppas_temp[9].strip() #ingle input to exppas
                    _mtransit_temp = mlines[6].replace('"','').split()
                    _mtransit    = _mtransit_temp[0].strip() #ingle input to 
                    _mtransit_1  = _mtransit_temp[1].strip() #ingle input to exppas
                    _mtransit_2  = _mtransit_temp[2].strip() #ingle input to exppas
                    _mtransit_3  = _mtransit_temp[3].strip() #ingle input to exppas
                    _mtransit_4  = _mtransit_temp[4].strip() #ingle input to exppas
                    _mtransit_5  = _mtransit_temp[5].strip() #ingle input to exppas
                    _mtransit_6  = _mtransit_temp[6].strip() #ingle input to exppas
                    _mtransit_7  = _mtransit_temp[7].strip() #ingle input to exppas
                    _mtransit_8  = _mtransit_temp[8].strip() #ingle input to exppas
                    _mtransit_9  = _mtransit_temp[9].strip() #ingle input to exppas
                    _mpason_temp = mlines[7].replace('"','').split()
                    _mpason    = _mpason_temp[0].strip() #ingle input to 
                    _mpason_1  = _mpason_temp[1].strip() #ingle input to exppas
                    _mpason_2  = _mpason_temp[2].strip() #ingle input to exppas
                    _mpason_3  = _mpason_temp[3].strip() #ingle input to exppas
                    _mpason_4  = _mpason_temp[4].strip() #ingle input to exppas
                    _mpason_5  = _mpason_temp[5].strip() #ingle input to exppas
                    _mpason_6  = _mpason_temp[6].strip() #ingle input to exppas
                    _mpason_7  = _mpason_temp[7].strip() #ingle input to exppas
                    _mpason_8  = _mpason_temp[8].strip() #ingle input to exppas
                    _mpason_9  = _mpason_temp[9].strip() #ingle input to exppas
                    _mpasoff_temp = mlines[8].replace('"','').split()
                    _mpasoff    = _mpasoff_temp[0].strip() #ingle input to 
                    _mpasoff_1  = _mpasoff_temp[1].strip() #ingle input to _mpasoff
                    _mpasoff_2  = _mpasoff_temp[2].strip() #ingle input to _mpasoff
                    _mpasoff_3  = _mpasoff_temp[3].strip() #ingle input to _mpasoff
                    _mpasoff_4  = _mpasoff_temp[4].strip() #ingle input to _mpasoff
                    _mpasoff_5  = _mpasoff_temp[5].strip() #ingle input to _mpasoff
                    _mpasoff_6  = _mpasoff_temp[6].strip() #ingle input to _mpasoff
                    _mpasoff_7  = _mpasoff_temp[7].strip() #ingle input to _mpasoff
                    _mpasoff_8  = _mpasoff_temp[8].strip() #ingle input to _mpasoff
                    _mpasoff_9  = _mpasoff_temp[9].strip() #ingle input to _mpasoff
                    _mtotal_pax     = mlines[9]
                    _mac     = mlines[10]
                    _mblof     = mlines[11]
                    _mblon     = mlines[12]
                    _mtkof     = mlines[13]
                    _matd_local     = mlines[14]
                    _mata_local     = mlines[15]
                    _mtdown     = mlines[16]
                    _mlegstatus     = mlines[17]
                    _mactual_block_time     = mlines[18]
                    _mflight_time_mvt     = mlines[19]
                    _mmvt_temp = mlines[20].replace('"','').split()
                    _mmvt    = _mmvt_temp[0] #ingle input to 
                    _mmvt_1  = _mmvt_temp[1] #ingle input to mvt
                    _mmvt_2  = _mmvt_temp[2] #ingle input to mvt
                    _mmvt_3  = _mmvt_temp[3] #ingle input to mvt
                    _mmvt_4  = _mmvt_temp[4] #ingle input to mvt
                    _mmvt_5  = _mmvt_temp[5] #ingle input to mvt
                    _mmvt_6  = _mmvt_temp[6] #ingle input to mvt
                    _mmvt_7  = _mmvt_temp[7] #ingle input to mvt
                    _mmvt_8  = _mmvt_temp[8] #ingle input to mvt
                    _mmvt_9  = _mmvt_temp[9] #ingle input to mvt
                    _mmvt_10  = _mmvt_temp[10] #ingle input to mvt
                    _mmvt_11  = _mmvt_temp[11] #ingle input to mvt
                    _mmvt_12  = _mmvt_temp[12] #ingle input to mvt
                    _mmvt_13  = _mmvt_temp[13] #ingle input to mvt
                    _mmvt_14  = _mmvt_temp[14] #ingle input to mvt
                    _mmvt_15  = _mmvt_temp[15] #ingle input to mvt
                    _mmvt_16  = _mmvt_temp[16] #ingle input to mvt
                    _mmvt_17  = _mmvt_temp[17] #ingle input to mvt
                    _mmvt_18  = _mmvt_temp[18] #ingle input to mvt
                    _mmvt_19  = _mmvt_temp[19] #ingle input to mvt
                    _mmvt_20  = _mmvt_temp[20] #ingle input to mvt
                    _mmvt_21  = _mmvt_temp[21] #ingle input to mvt
                    _mmvt_22  = _mmvt_temp[22] #ingle input to mvt
                    _mmvt_23  = _mmvt_temp[23] #ingle input to mvt
                    _mmvt_24  = _mmvt_temp[24] #ingle input to mvt
                    _mmvt_25  = _mmvt_temp[25] #ingle input to mvt
                    _mmvt_26  = _mmvt_temp[26] #ingle input to mvt
                    _mfiledatetime = mfilename_db.strip()
                    _mrow= [_mday_flt,_mflt,_mdep,_marr,_mreg,_mexppas,_mexppas_1,_mexppas_2,_mexppas_3,_mexppas_4,_mexppas_5,_mexppas_6,_mexppas_7,_mexppas_8,_mexppas_9,_mtransit,_mtransit_1,_mtransit_2,_mtransit_3,_mtransit_4,_mtransit_5,_mtransit_6,_mtransit_7,_mtransit_8,_mtransit_9,_mpason,_mpason_1,_mpason_2,_mpason_3,_mpason_4,_mpason_5,_mpason_6,_mpason_7,_mpason_8,_mpason_9,_mpasoff,_mpasoff_1,_mpasoff_2,_mpasoff_3,_mpasoff_4,_mpasoff_5,_mpasoff_6,_mpasoff_7,_mpasoff_8,_mpasoff_9,_mtotal_pax,_mac,_mblof,_mblon,_mtkof,_matd_local,_mata_local,_mtdown,_mlegstatus,_mactual_block_time,_mflight_time_mvt,_mmvt,_mmvt_1,_mmvt_2,_mmvt_3,_mmvt_4,_mmvt_5,_mmvt_6,_mmvt_7,_mmvt_8,_mmvt_9,_mmvt_10,_mmvt_11,_mmvt_12,_mmvt_13,_mmvt_14,_mmvt_15,_mmvt_16,_mmvt_17,_mmvt_18,_mmvt_19,_mmvt_20,_mmvt_21,_mmvt_22,_mmvt_23,_mmvt_24,_mmvt_25,_mmvt_26,_mfiledatetime]
                    rows.append(_mrow)
                    _monce ==1
                    _n = _n + 1
                    print(_mrow)
            if _n>0 :
                cursor.prepare(
                    'insert into DWH_FOUNDATION.T_MA_1_LOAD(DAY_FLT,FLT,DEP,ARR,REG,EXPPAS,EXPPAS_1,EXPPAS_2,EXPPAS_3,EXPPAS_4,EXPPAS_5,EXPPAS_6,EXPPAS_7,EXPPAS_8,EXPPAS_9,TRANSIT,TRANSIT_1,TRANSIT_2,TRANSIT_3,TRANSIT_4,TRANSIT_5,TRANSIT_6,TRANSIT_7,TRANSIT_8,TRANSIT_9,PASON,PASON_1,PASON_2,PASON_3,PASON_4,PASON_5,PASON_6,PASON_7,PASON_8,PASON_9,PASOFF,PASOFF_1,PASOFF_2,PASOFF_3,PASOFF_4,PASOFF_5,PASOFF_6,PASOFF_7,PASOFF_8,PASOFF_9,TOTAL_PAX,AC,BLOF,BLON,TKOF,ATD_LOCAL,ATA_LOCAL,TDOWN,LEGSTATUS,ACTUAL_BLOCK_TIME,FLIGHT_TIME_MVT,AZFW,UPLFW,RAMP,STDN,BURN,TANK,ZFW,FPBURN,MILES,MVT_9,MVT_10,UPLIFV,MVT_12,MVT_13,MVT_14,MVT_15,MVT_16,CARGO,MAIL,PAYLD,BAGS,MVT_21,MVT_22,MVT_23,MVT_24,MVT_25,MVT_26,EMAIL_FILE_DATE) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58,:59,:60,:61,:62,:63,:64,:65,:66,:67,:68,:69,:70,:71,:72,:73,:74,:75,:76,:77,:78,:79,:80,:81,:82,:83,:84)'
                    )                
                cursor.executemany(None, rows)
                rows=[]
                _n = 0
                dbCon.commit()                              

        _mcount = _mcount + 1
        print("Yaaay!! ... Done Processing file") 
if __name__ == "__main__" : main()

if platform.system().upper() == 'LINUX':
    subprocess.call(['./mv_1005_files.sh']) 
    print('Done processing')
else:
    print('Done processing,')


