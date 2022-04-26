select phone_number,
       assigned_user_id,
       status as status_request,
       my_date,
       uniqueid,
       ochered,
       project
from (select my_phone_work                                                as phone_number,
             assigned_user_id,
             status,
             new_rob.my_date,
             new_rob.uniqueid,
             new_rob.ochered,
             new_rob.phone,
             row_number() over (partition by phone order by my_date desc) as num,
             project
      from (select 'RTK'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date(date_entered)                                                   as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_rostelecom
            where status != 'Error'
              and date(date_entered) between '2022-04-01' and '2022-04-08'
            union all
            select 'Beeline'                                                               project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date(date_entered)                                                   as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline
            where status != 'Error'
              and date(date_entered) between '2022-04-01' and '2022-04-08'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date(date_entered)                                                   as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_domru
            where status != 'Error'
              and date(date_entered) between '2022-04-01' and '2022-04-08'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date(date_entered)                                                   as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_ttk
            where status != 'Error'
              and date(date_entered) between '2022-04-01' and '2022-04-08'
            union all
            select 'NBN'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date(date_entered)                                                   as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_netbynet
            where status != 'Error'
              and date(date_entered) between '2022-04-01' and '2022-04-08'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date(date_entered)                                                   as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_mts jc_meetings_mts
            where status != 'Error'
              and date(date_entered) between '2022-04-01' and '2022-04-08'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date(date_entered)                                                   as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline_mnp
            where status != 'Error'
              and date(date_entered) between '2022-04-01' and '2022-04-08') as reguest
               left join
           (select call_date + interval 2 hour as my_date,
                   uniqueid,
                   substring(dialog, 11, 4)    as ochered,
                   phone
            from suitecrm_robot.jc_robot_log
            where date(call_date) between '2022-04-01' and '2022-04-08') as new_rob
           on reguest.my_phone_work = new_rob.phone) as total
where num = 1;
