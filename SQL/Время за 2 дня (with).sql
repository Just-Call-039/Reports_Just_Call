select queue,
       sum(inbound1)                          as inbound,
       sum(total_sec1)                        as t_sec,
       sum(inbound1) / sum(total_sec1)        as dolya_avtodozvon,
       sum(inbound1) * 0.65                   as eff_time_avtodozv,
       min(avg_dlit)                          as avg_sec,
       (sum(inbound1) * 0.65) / min(avg_dlit) as perevod_hour
from (select id_user,
             sum_call,
             queue,
             count_call,
             prop,
             talk_inbound,
             talk_inbound * prop as 'inbound1',
             total_sec,
             total_sec * prop    as 'total_sec1'
      from (select id_user,
                   talk_inbound,
                   (recall + sobranie + obuchenie + training + nastavnik + problems + fact -
                    progul_obrabotka) as total_sec
            from suitecrm.reports_cache
            where date = date(now()) - interval 2 day
              and id_user not in ('1', '')
              and id_user is not null) as sec
               left join
           (with t_1 (user, count_call) as
                     (select assigned_user_id, count(id)
                      from suitecrm.calls as cl
                               left join suitecrm.calls_cstm as clc on cl.id = clc.id_c
                      where date(date_entered) = date(now()) - interval 2 day
                        and cl.name = 'Входящий звонок'
                      group by assigned_user_id),
                 t_2 (user, queue, count_call_q) as
                     (select assigned_user_id, queue_c, count(id)
                      from suitecrm.calls as cl
                               left join suitecrm.calls_cstm as clc on cl.id = clc.id_c
                      where date(date_entered) = date(now()) - interval 2 day
                        and cl.name = 'Входящий звонок'
                      group by assigned_user_id, queue_c)
            select t_1.user,
                   t_2.queue,
                   t_2.count_call_q                  as 'count_call',
                   t_1.count_call                    as 'sum_call',
                   t_2.count_call_q / t_1.count_call as 'prop'
            from t_1
                     left join t_2 on t_1.user = t_2.user) as count_call on sec.id_user = count_call.user) as total
         left join (select queue_c, sum(duration_minutes) / count(id) as avg_dlit
                    from suitecrm.calls
                             left join suitecrm.calls_cstm on calls.id = calls_cstm.id_c
                    where date(date_entered) = date(now()) - interval 2 day
                      and name = 'Входящий звонок'
                    group by queue_c) as avg on avg.queue_c = total.queue
group by queue;
