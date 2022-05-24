select call_date                           as my_date,
       now(),
       timediff(now(), call_date)          as r,
       str_to_date('04:00:00', '%H:%i:%S') as mark,
       uniqueid,
       substring(dialog, 11, 4)            as ochered,
       last_step,
       billsec,
       phone
from suitecrm_robot.jc_robot_log
where (date(call_date) = date(now()))
  and (timediff(now(), call_date) <= str_to_date('04:00:00', '%H:%i:%S'))
order by my_date desc;
