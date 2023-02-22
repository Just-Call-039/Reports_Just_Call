select phone,
       call_date,
       route,
       last_step,
       uniqueid,
       client_status,
       otkaz,
       was_repeat,
       substring(dialog, 11, 4) as queue,
       server_number,
       directory,
       city_c,
       region_c
from suitecrm_robot.jc_robot_log
where date(call_date) = '2023-02-20'
limit 1000;
