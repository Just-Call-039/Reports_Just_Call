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
where (date(call_date) = date(now()))
  and (timediff(now(), call_date) <= str_to_date('04:00:00', '%H:%i:%S'));
