select uniqueid,
       phone,
       client_status,
       assigned_user_id,
       last_step,
       substring(dialog, 11, 4) as ochered,
       call_date,
       ptv_c,
       region_c
from suitecrm_robot.jc_robot_log
where month(call_date) = 1
  and year(call_date) = 2023
  and last_step not in ('0', '1', '111', '261', '262');
