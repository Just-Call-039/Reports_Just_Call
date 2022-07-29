select *
from (select 'id',
             'call_date',
             'name',
             'phone',
             'queue',
             'user_call',
             'super',
             'city',
             'call_sec',
             'short_calls',
             'project',
             'req_date',
             'user_req',
             'status'
      union all
      select cl.id,
             date(cl.date_entered)                                                                     as call_date,
             cl.name,
             cl_c.asterisk_caller_id_c                                                                 as phone,
#        cl_c.project_c,
             if((cl_c.queue_c in ('', ' ') or cl_c.queue_c is null), 'unknown_queue',
                cl_c.queue_c)                                                                          as queue,
             if((cl.assigned_user_id in ('', ' ') or cl.assigned_user_id is null), 'unknown_id',
                cl.assigned_user_id)                                                                   as user_call,
             if((cl_c.user_id_c in ('', ' ') or cl_c.user_id_c is null), 'unknown_id', cl_c.user_id_c) as super,
             case
                 when city_c is null then concat(town_c, 'OBL')
                 when city_c in ('', ' ') then concat(town_c, 'OBL')
                 else city_c
                 end                                                                                   as city,
             duration_minutes                                                                          as call_sec,
#        cl.duration_minutes div 60                                                                as call_min,
#        cl.duration_minutes % 60                                                                  as call_sec,
             if(cl.duration_minutes <= 10, 1, 0)                                                       as short_calls,
#        temp_req.phone,
             temp_req.project,
             temp_req.req_date,
             temp_req.assigned_user_id                                                                 as user_req,
             temp_req.status
      from suitecrm.calls as cl
               inner join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
               left join suitecrm.contacts on cl_c.asterisk_caller_id_c = contacts.phone_work
               left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
               left join (with phone_number as (select *
                                                from (select cl.id,
                                                             cl_c.asterisk_caller_id_c,
                                                             date(cl.date_entered)                                                  as call_date,
                                                             row_number()
                                                                     over (partition by asterisk_caller_id_c order by date_entered) as num
                                                      from suitecrm.calls as cl
                                                               left join suitecrm.calls_cstm as cl_c on cl.id = cl_c.id_c
                                                      where (date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day)
                                                        and cl_c.result_call_c = 'MeetingWait') as temp
                                                where num = 1),
                               request as (select 'RTK'                                                                project,
                                                  concat(8,
                                                         right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                                                  date(date_entered)                                                as date,
                                                  assigned_user_id,
                                                  status
                                           from suitecrm.jc_meetings_rostelecom
                                           where status != 'Error'
                                             and date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day
                                           union all
                                           select 'Beeline'                                                            project,
                                                  concat(8,
                                                         right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                                                  date(date_entered)                                                as date,
                                                  assigned_user_id,
                                                  status
                                           from suitecrm.jc_meetings_beeline
                                           where status != 'Error'
                                             and date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day
                                           union all
                                           select 'DOMRU'                                                              project,
                                                  concat(8,
                                                         right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                                                  date(date_entered)                                                as date,
                                                  assigned_user_id,
                                                  status
                                           from suitecrm.jc_meetings_domru
                                           where status != 'Error'
                                             and date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day
                                           union all
                                           select 'TTK'                                                                project,
                                                  concat(8,
                                                         right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                                                  date(date_entered)                                                as date,
                                                  assigned_user_id,
                                                  status
                                           from suitecrm.jc_meetings_ttk
                                           where status != 'Error'
                                             and date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day
                                           union all
                                           select 'NBN'                                                                project,
                                                  concat(8,
                                                         right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                                                  date(date_entered)                                                as date,
                                                  assigned_user_id,
                                                  status
                                           from suitecrm.jc_meetings_netbynet
                                           where status != 'Error'
                                             and date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day
                                           union all
                                           select 'MTS'                                                                project,
                                                  concat(8,
                                                         right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                                                  date(date_entered)                                                as date,
                                                  assigned_user_id,
                                                  status
                                           from suitecrm.jc_meetings_mts jc_meetings_mts
                                           where status != 'Error'
                                             and date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day)
#                                      union all
#                                      select project,
#                                             concat(8,
#                                                    right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
#                                             date(date_entered)                                                as date,
#                                             assigned_user_id,
#                                             status
#                                      from suitecrm.jc_meetings_beeline_mnp
#                                      where status != 'Error'
#                                        and date(date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day)
                          select phone_number.id,
#        phone_number.asterisk_caller_id_c as phone,
                                 phone_number.call_date,
                                 request.project,
                                 request.date as req_date,
                                 request.assigned_user_id,
                                 request.status
                          from request
                                   left join phone_number
                                             on phone_number.asterisk_caller_id_c = request.my_phone_work and
                                                date(phone_number.call_date) = date(request.date)) as temp_req
                         on cl.id = temp_req.id
      where date(cl.date_entered) between date(now()) - interval 70 day and date(now()) - interval 1 day) as t_1
into outfile '/var/lib/mysql-files/4_report/Main.csv'
    fields terminated by ';';
