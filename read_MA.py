import json
import platform
import os
import cx_Oracle
import glob
import re
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

def main():
    _mcount = 0
    while _mcount <1:
        if platform.system().upper() == 'LINUX':
            _msoucefolder = '/u01/projects/AIMS_Files/attachments/'
        else:
            _msoucefolder = 'C:\\Files\\'
        rows=[]
        _mtype = _msoucefolder+'*MA.txt'
        for filename in glob.glob(_mtype):
            mfilename =  os.path.basename(filename )
            mfilename_db = mfilename[:-4]
            mfilename_db = mfilename[:19]
            _monce = 0
            fh  = open(filename,'r')
            _n = 0
            for lines in fh:
                [_mdate,_mflt,_mcarrier,_mtype,_mreg,_mdep,_marr,_mstd,_msta,_metd,_meta,_mtkof,_mtdwn,_matd,_mata,_mblock,_mflthr,_mst,_mstd_local,_msta_local,_metd_local,_meta_local,_mtkof_local,_mtdwn_local,_matd_local,_mata_local,_macversion,_macconfig,_mactpax,_mpax,_mactblockoff,_macttkoff,_macttdown,_mactblockon,_morgdate,_morgstd,_morgsta,_mdly1,_mdly1arr,_minitia,_muplift,_mtotal,_mremain,_maburn,_msg,_mcargo,_mmail,_mbags,_mcrew ] =['','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','']
                mlines = lines.split("\t")
                _mrow = []
                if len(mlines) == 49 and not mlines[0].strip()=='DATE' and mlines[0].strip()  and not mlines[2].strip() == 'PW':   
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
                    _mfiledatetime = mfilename_db.strip()
                    _mrow= [_mdate,_mflt,_mcarrier,_mtype,_mreg,_mdep,_marr,_mstd,_msta,_metd,_meta,_mtkof,_mtdwn,_matd,_mata,_mblock,_mflthr,_mst,_mstd_local,_msta_local,_metd_local,_meta_local,_mtkof_local,_mtdwn_local,_matd_local,_mata_local,_macversion,_macconfig,_macconfig1,_mactpax,_mactpax1,_mpax,_mactblockoff,_macttkoff,_macttdown,_mactblockon,_morgdate,_morgstd,_morgsta,_mdly1,_mdly1arr,_minitia,_muplift,_mtotal,_mremain,_maburn,_msg,_mcargo,_mmail,_mbags,_mcrew,_mfiledatetime ]
                    rows.append(_mrow)
                    print(_mrow)
                    _monce ==1
                    _n = _n + 1
            if _n>0 :
                cursor.prepare('insert into dwh_foundation.T_TM1_NEW1_1_LOAD(date_,flt,carrier,type,reg,dep,arr,std,sta,etd,eta,tkof,tdwn,atd,ata,block,flthr,st,std_local,sta_local,etd_local,eta_local,tkof_local,tdwn_local,atd_local,ata_local,acversion,BUSINESS_CLASS,ECONOMY,ACTPAX_BUSINESS,ACTPAX_ECONOMY,pax,actblockoff,acttkoff,acttdown,actblockon,orgdate,orgstd,orgsta,dly1,dly1arr,initia,uplift,total,remain,aburn,sg,cargo,mail,bags,crew,EMAIL_FILE_DATE) values (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,:15,:16,:17,:18,:19,:20,:21,:22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52)')
                cursor.executemany(None, rows)
                rows=[]
                _n = 0
                dbCon.commit()
        _mcount = _mcount + 1        
if __name__ == "__main__" : main()
if platform.system().upper() == 'LINUX':
    subprocess.call(['./mv_ma_files.sh']) 
    print('Done processing')
else:
    print('Done processing,')