select date(call_date)          as my_date,
       uniqueid,
       substring(dialog, 11, 4) as ochered,
       last_step,
       route,
       billsec,
       client_status,
       otkaz,
       directory,
       server_number,
       city_c,
       ptv_c,
       marker,
       was_repeat,
       phone
from suitecrm_robot.jc_robot_log
where date(call_date) between '2022-04-01' and '2022-04-12';
