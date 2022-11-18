# Все звонки по очередям для 5 проекта, которые перевели на любые проекты, кроме 5 и лиды 1.

with RTK (queue, project, lids) as (select queue, project, project_type
                                    from (select queue,
                                                 project,
                                                 date,
                                                 project_type,
                                                 row_number() over (partition by queue order by date desc) as now
                                          from suitecrm.queue_project) as temp
                                    where temp.now = 1),
     calls (call_id, queue_call, c_id) as (select rob_log.id, substring(dialog, 11, 4), uniqueid
                                           from suitecrm_robot.jc_robot_log as rob_log
                                           where date(call_date) = date(now()) - interval 1 day),
     transfer (from_d, to_d, tr_id) as (select dialog, destination_queue, uniqueid
                                        from suitecrm.transferred_to_other_queue
                                        where date(date) = date(now()) - interval 1 day)
select calls.call_id, RTK.queue, RTK.project, RTK.lids
from calls
         left join RTK on calls.queue_call = RTK.queue
         left join transfer on calls.call_id = transfer.tr_id
where RTK.project = 5
  and call_id not in (select call_id
                      from calls
                               left join transfer on calls.call_id = transfer.tr_id
                      where transfer.to_d not in (select RTK.queue where RTK.project = 5))
  and lids = 1;
