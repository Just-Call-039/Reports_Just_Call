with temp_robot as (select *
                    from (select toDate(call_date)                                                      as call_date,
                                 uniqueid,
                                 substring(dialog, 11, 4)                                               as ochered,
                                 phone,
                                 row_number() over (partition by phone order by toDate(call_date) desc) as num
                          from suitecrm_robot_ch.jc_robot_log
                          where toMonth(call_date) between 10 and 12
                            and toYear(call_date) = 2022) as temp
                    where temp.num = 1),
     temp_requests as (select *
                       from suitecrm_robot_ch.all_requests
                       where toMonth(request_date) = 12
                         and toYear(request_date) = 2022)

select temp_requests.request_date,
       temp_requests.project,
       temp_requests.phone_request,
       temp_requests.user,
       temp_requests.super,
       temp_requests.status,
       temp_robot.uniqueid
from temp_requests
         left join temp_robot on temp_requests.phone_request = temp_robot.phone;
