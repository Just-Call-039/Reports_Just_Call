select distinct(project_c)
from suitecrm.users_cstm;

with t_1 (id_user, date, schedule_type, num) as (select id_user, date, schedule_type, num
                                                 from (select id_user,
                                                              date,
                                                              schedule_type,
                                                              row_number() over (partition by id_user order by date desc) as num
                                                       from suitecrm.worktime_log) as temp
                                                 where num = 1),
     t_2 (id, first_name, last_name, status, project_c, date_entered, last_login_c,
          experience, dismissal_date_c) as (select u.id,
                                                   first_name,
                                                   last_name,
                                                   if(first_name like 'я%', 'dismissed', 'working') as status,
                                                   project_c,
                                                   date_entered,
                                                   last_login_c,
                                                   if(dismissal_date_c is NULL, datediff(last_login_c, date_entered),
                                                      datediff(dismissal_date_c, date_entered))     as experience,
                                                   dismissal_date_c
                                            from suitecrm.users as u
                                                     left join suitecrm.users_cstm as uc on u.id = uc.id_c)
select id_user,
       date,
       schedule_type,
       case
           when schedule_type in ('5_2', '2_2') then 8
           when schedule_type in ('5_2_05', '2_2_05') then 4
           else 'unknown'
           end as work_time,
       id,
       first_name,
       last_name,
       status,
       project_c,
       date_entered,
       last_login_c,
       experience,
       dismissal_date_c
from t_1
         left join t_2 on t_1.id_user = t_2.id;

select *
from suitecrm.worktime_log
where date(date) >= '2022-06-02';

select distinct(schedule_type)
from suitecrm.worktime_log;

select id_user, date, schedule_type
from (select id_user, date, schedule_type, row_number() over (partition by id_user order by date desc) as num
      from suitecrm.worktime_log) t_1
where num = 1;


with t_1 (id_user, date, schedule_type, num) as (select id_user, date, schedule_type, num
                                                 from (select id_user,
                                                              date,
                                                              schedule_type,
                                                              row_number() over (partition by id_user order by date desc) as num
                                                       from suitecrm.worktime_log) as temp
                                                 where num = 1),
     t_2 (id, first_name, last_name, status, project_c, date_entered, last_login_c,
          experience, dismissal_date_c) as (select u.id,
                                                   first_name,
                                                   last_name,
                                                   if(first_name like 'я%', 'dismissed', 'working') as status,
                                                   project_c,
                                                   date_entered,
                                                   last_login_c,
                                                   if(dismissal_date_c is NULL, datediff(last_login_c, date_entered),
                                                      datediff(dismissal_date_c, date_entered))     as experience,
                                                   dismissal_date_c
                                            from suitecrm.users as u
                                                     left join suitecrm.users_cstm as uc on u.id = uc.id_c)
select id_user,
       date,
       date_entered,
       last_login_c,
       experience,
       dismissal_date_c,
#        schedule_type,
       case
           when schedule_type in ('5_2', '2_2') then 8
           when schedule_type in ('5_2_05', '2_2_05') then 4
           else 'unknown'
           end as work_time,
#        id,
#        first_name,
#        last_name,
#        status,
       project_c
from t_1
         left join t_2 on t_1.id_user = t_2.id
where date(t_1.date) = date(now());
