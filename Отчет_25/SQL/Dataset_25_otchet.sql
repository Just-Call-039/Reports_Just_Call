select o25.my_date         as my_date,
       o25.uniqueid        as uniqueid,
       o25.ochered         as ochered,
       o25.last_step       as last_step,
       o25.route           as route,
       o25.billsec         as billsec,
       o25.client_status   as client_status,
       o25.otkaz           as otkaz,
       o25.directory       as directory,
       o25.server_number   as server_number,
       o25.city_c          as city_c,
       o25.ptv_c           as ptv_c,
       o25.marker          as marker,
       o25.was_repeat      as was_repeat,
       o25.phone           as phone,
       o25.teh_vozmozhnost as teh_vozmozhnost,
       o25.region          as region,
       o25.status          as status,
       o25.alive           as alive,
       r25.phone_number,
       r25.assigned_user_id,
       r25.status_request,
       r25.date_reguest,
       r25.uniqueid,
       r25.ochered,
       r25.project         as request_project,
       temp.group          as queue_group,
       temp.project           queue_project
from suitecrm_robot_ch.otchet_25 as o25
         left join suitecrm_robot_ch.request_25 as r25 on o25.uniqueid = r25.uniqueid
         left join (select t_1.queue, t_1.group, t_1.project
                    from (select *, row_number() over (partition by queue order by date_add desc) as num
                          from suitecrm_robot_ch.grouping_of_queues) as t_1
                    where t_1.num = 1) as temp on o25.ochered = temp.queue;
