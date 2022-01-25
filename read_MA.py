import json
import platform
import os
import cx_Oracle
import glob
import re
import subprocess
from datetime import datetime

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


cursor = cx_Oracle.Cursor(dbCon)
cursor1 = cx_Oracle.Cursor(dbCon)
cursor2 = cx_Oracle.Cursor(dbCon)
cursor3 = cx_Oracle.Cursor(dbCon)
cursor4 = cx_Oracle.Cursor(dbCon)
cursor_select   = cx_Oracle.Cursor(dbCon)
_k = 1
digits = frozenset('0123456789-+')

lines =''
_mdate = ''
_mdoc = ''
_myesterday  = ''
today_date = datetime.datetime.today()
tomorrows_date = today_date + datetime.timedelta(0)
_mprocdate =  (today_date).strftime('%d-%b-%Y')
myesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
myesterday1 = datetime.datetime.now() - datetime.timedelta(days = 1)    
myesterday=  myesterday.strftime("%y%m")
myesterday1=  myesterday1.strftime("%y%m%d")

##dsn_tns = cx_Oracle.makedsn('10.2.155.78', 1521, 'warehouse')

_mopen=0
def main():
    _mcount = 0
    _monceerror = 0
    _mopen=0
    while _mcount <1:
        if platform.system().upper() == 'LINUX':
            #_mtype = '1004.TXT'
            _merrorlogpath = '/u01/projects/MA_project/data/'
            _msoucefolder = '/u01/projects/AIMS_Files/attachments/'
            _moutputfolder = '/u01/projects/MA_project/data/'
            _mbackupfolder = '/u01/projects/MA_project/data/'
	    print('HEREEEE')
        else:
            _msoucefolder = 'C:\\Files\\'
            _moutputfolder = 'C:\\Files\\Output\\'
            _merrorlogpath  = 'C:/Files/'
            _mbackupfolder = 'C:\\Files\\processed\\'
        _mopen = 0
        rows=[]
        #_moutfile = 'MA_NEW.txt'
        _mtype = _msoucefolder+'*MA*.txt'
        #_mtype = '/app/u02/businessobjects/dataservices/amadeus_hot/altea/PRD.TSR.KQ.AL.HOT.D'+_myesterday
        for filename in glob.glob(_mtype):
            filelogl=[]
	    print('Here 2',filename )
            head, filename1 = os.path.split(filename)
            mfilename =  os.path.basename(filename )
            mfilename_db = mfilename[:-4]
            mfilename_db = mfilename[:19]
            mfile_type = mfilename[20:-1]
            _merrorfile = _merrorlogpath+mfilename+'.log'

            _mfilename = filename
            _monce = 0
            _mnorecords = 0
            _mfileloaded = 0
            fh  = open(filename,'r')
            _n = 0
	    print('Here 3',filename )
            for lines in fh:
                #print(lines)
                #[_Mfltnum,_Mdept,_Mdest,_mtailNum,_MactEqpmt,_MblockTime,_MflightTime,_Mdepdate,_Mdeptime,_Moffdate,_Mofftime,_Mtakeoffdate,_Mtakeofftime,_Mtouchdowndate,_Mtouchdowntime,_Monblockdate,_Monblocktime,_MoutLT,_MoffLT,_MonLT,_MinLT,_Mstdgmtvr,_Mstagmtvr,_MactYLPax,_MactYLPad,_MactYLAwd,_MactYLInf,_MactYRPax,_MactYRPad,_MactYRAwd,_MactYRInf,_MactYTPax,_MactYTPad,_MactYTAwd,_MactYTInf,_McatAdult,_McatMen,_McatWomen,_McatChild,_McatInfant,_McatAll,_McatTurn,_MxAlc,_MdeptFuel,_MarrvFuel,_MonFuel,_Mmxconsumpt,_MupliftQty,_MdeFuelQty,_Mdenstity,_MtaxiInFuel,_MoffFuel,_MactJPax ,_MactJPad ,_MactJAwd,_McreateDate] =['','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','']
                [_mdate,_mflt,_mcarrier,_mtype,_mreg,_mdep,_marr,_mstd,_msta,_metd,_meta,_mtkof,_mtdwn,_matd,_mata,_mblock,_mflthr,_mst,_mstd_local,_msta_local,_metd_local,_meta_local,_mtkof_local,_mtdwn_local,_matd_local,_mata_local,_macversion,_macconfig,_mactpax,_mpax,_mactblockoff,_macttkoff,_macttdown,_mactblockon,_morgdate,_morgstd,_morgsta,_mdly1,_mdly1arr,_minitia,_muplift,_mtotal,_mremain,_maburn,_msg,_mcargo,_mmail,_mbags,_mcrew ,_McreateDate ] =['','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','']
                #lines = lines .replace(",", ";")
                mlines = lines.split("\t")
                _mrow = []
                print(len(mlines))
                # the data is 41
                if len(mlines) == 49 and not mlines[0].strip()=='DATE' and mlines[0].strip()  and not mlines[2].strip() == 'PW':   
                    #print(mlines[0].strip())
                    _mdate      =     mlines[0].strip()
                    _mflt      =     mlines[1].replace('*','').strip()
                    _mcarrier      =     mlines[2].strip()
                    _mtype      =     mlines[3].strip()
                    _mreg      =     mlines[4].strip()
                    _mdep      =     mlines[5].strip()
                    _marr      =     mlines[6].strip()
                    _mstd      =     mlines[7].strip()
                    _msta      =     mlines[8].strip()
                    _metd      =     mlines[9].strip()
                    _meta      =     mlines[10].strip()
                    _mtkof      =     mlines[11].strip()
                    _mtdwn      =     mlines[12].strip()
                    _matd      =     mlines[13].strip()
                    _mata      =     mlines[14].strip()
                    _mblock      =     mlines[15].strip()
                    _mflthr      =     mlines[16].strip()
                    _mst      =     mlines[17].strip()  # covers ac version.
                    _mstd_local =     mlines[18].strip()
                    _msta_local =     mlines[19].strip()
                    _metd_local =     mlines[20].strip()
                    _meta_local =     mlines[21].strip()
                    _mtkof_local =     mlines[22].strip()
                    _mtdwn_local =     mlines[23].strip()
                    _matd_local =     mlines[24].strip()
                    _mata_local =     mlines[25].strip()
                    _macversion = mlines[26].strip()
                    _macconfig_temp = re.split('\s+', mlines[27])
                    _macconfig    = _macconfig_temp[0]
                    _macconfig1    = _macconfig_temp[1] 
                    _mactpax_temp = re.split('\s+', mlines[28])
                    _mactpax    = _mactpax_temp[0].replace('|','')
                    _mactpax1    = _mactpax_temp[1].replace('|','')
                    _mpax      =     mlines[29]
                    _mactblockoff =     mlines[30]
                    _macttkoff      =     mlines[31]
                    _macttdown      =     mlines[32]
                    _mactblockon      =     mlines[33]
                    _morgdate      =     mlines[34]
                    _morgstd      =     mlines[35]
                    _morgsta      =     mlines[36]
                    _mdly1 = mlines[37]
                    _mdly1arr = mlines[38]
                    _minitia      =     mlines[39]
                    _muplift      =     mlines[40]
                    _mtotal      =     mlines[41]
                    _mremain      =     mlines[42]
                    _maburn      =     mlines[43]
                    _msg = mlines[44]
                    _mcargo      =     mlines[45]
                    _mmail      =     mlines[46]
                    _mbags      =     mlines[47]
                    _mcrew  =     mlines[48]
                    _McreateDate = _mprocdate     # TRANS_DATE
                    _mfiledatetime = mfilename_db.strip()
                    #extra fields.
                    if mfile_type.strip() == 'MA.tx':
                        _mfile_type = 'NORMAL'
                    elif mfile_type.strip() == 'MA_A.tx':
                        _mfile_type = 'ONDEMAND'
                    else:
                        _mfile_type = 'NORMAL FILE'
                    _utc_timezone = """ select TIMEZONE from AIRPORT_TZ_OFFSET where AIRPORT_CODE = :airport_code """
                    cursor1.execute(_utc_timezone,airport_code=_mdep)

                    _utc_timezone = """ select TIMEZONE from AIRPORT_TZ_OFFSET where AIRPORT_CODE = :airport_code """
                    cursor2.execute(_utc_timezone,airport_code=_marr)

                    for row in cursor1:
                        _flt_dep_gmt_time_zone = str(row[0]).strip()
                        # print(_flt_dep_gmt_time_zone)
                    for row in cursor2:
                        _flt_arr_gmt_time_zone = str(row[0]).strip()
                        # print(_flt_arr_gmt_time_zone)
                    #Get DEP GMT OFFSET BASED ON TIMEZONE above.
                    tz_dep_offset_querry = """ SELECT TZ_OFFSET( :time_zone ) FROM DUAL """
                    cursor3.execute(tz_dep_offset_querry, time_zone=_flt_dep_gmt_time_zone)
                    for row in cursor3:
                        _flt_dep_gmt_var = str(row[0]).replace(':','').strip()
                        # print(_flt_dep_gmt_var)
                    #Get ARR GMT OFFSET BASED ON TIMEZONE above.
                    tz_arr_offset_querry = """ SELECT TZ_OFFSET( :time_zone ) FROM DUAL """
                    cursor4.execute(tz_arr_offset_querry, time_zone=_flt_arr_gmt_time_zone)
                    for row in cursor4:
                        _flt_arr_gmt_var = str(row[0]).replace(':','').strip()
                        # print(_flt_dep_gmt_var)
                    # print("DEP",_flt_dep_gmt_time_zone ,_flt_dep_gmt_var, "|     ARR", _flt_arr_gmt_time_zone,_flt_arr_gmt_var )
                    
                    _mrow= [_mdate,_mflt,_mcarrier,_mtype,_mreg,_mdep,_marr,_mstd,_msta,_metd,_meta,_mtkof,_mtdwn,_matd,_mata,_mblock,_mflthr,_mst,_mstd_local,_msta_local,_metd_local,_meta_local,_mtkof_local,_mtdwn_local,_matd_local,_mata_local,_macversion,_macconfig,_macconfig1,_mactpax,_mactpax1,_mpax,_mactblockoff,_macttkoff,_macttdown,_mactblockon,_morgdate,_morgstd,_morgsta,_mdly1,_mdly1arr,_minitia,_muplift,_mtotal,_mremain,_maburn,_msg,_mcargo,_mmail,_mbags,_mcrew ,_McreateDate,_mfiledatetime,_flt_dep_gmt_var,_flt_arr_gmt_var,_flt_dep_gmt_time_zone,_flt_arr_gmt_time_zone,_mfile_type ]
                    
                    rows.append(_mrow)
                    print(_mrow)
                    _monce ==1
                    _n = _n + 1
            if _n>0 :
                cursor.prepare('insert into dwh_foundation.T_TM1_NEW1_1_LOAD(date_,flt,carrier,type,reg,dep,arr,std,sta,etd,eta,tkof,tdwn,atd,ata,block,flthr,st,std_local,sta_local,etd_local,eta_local,tkof_local,tdwn_local,atd_local,ata_local,acversion,BUSINESS_CLASS,ECONOMY,ACTPAX_BUSINESS,ACTPAX_ECONOMY,pax,actblockoff,acttkoff,acttdown,actblockon,orgdate,orgstd,orgsta,dly1,dly1arr,initia,uplift,total,remain,aburn,sg,cargo,mail,bags,crew,createdate,EMAIL_FILE_DATE,STD_GMTVR,STAGMTVR,DEP_TIMEZONE,ARR_TIMEZONE,FILE_TYPE) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,:15,:16,:17,:18,:19,:20,:21,:22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58)')
                #cursor.prepare('insert into DWH_FOUNDATION.T_MA(DAY_FLT,FLT,DEP,ARR,REG,EXPPAS,TRANSIT,PASON,PASOFF,TOTAL_PAX,AC,BLOF,BLON,TKOF,ATD_LOCAL,ATA_LOCAL,TDOWN,LEGSTATUS,ACTUAL_BLOCK_TIME,FLIGHT_TIME_MVT,MVT) values(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,:15,:16,:17,:18,:19,:20,:21)')
                cursor.executemany(None, rows)
                rows=[]
                _n = 0
                dbCon.commit()

        _mcount = _mcount + 1
        
if __name__ == "__main__" : main()
if platform.system().upper() == 'LINUX':
    subprocess.call(['./mv_ma_files.sh']) 
    execfile("/u01/projects/TM1_Fuelplus_Projects/scripts/insertintotm1new1.py")
    print('Done processing')
else:
    print('Done processing,')