select * from T_TM1_NEW1_1
where not exists(select * from vw_mc_report_final_fp_aims
    where vw_mc_report_final_fp_aims.FLTNUM = trim(T_TM1_NEW1_1.FLT))
and trim(FLT) not in ('3','2')
and trim(FLT) not like '%PW%'
and trim(FLT) not like '%A%'
and trim(CARRIER) not like '%8Z%'
  and trim(DEP) = trim(DEP)
and to_date(to_char(trim(DATE_)),'DD-MM-yy hh24:mi:SS')  > to_date(:date_start)