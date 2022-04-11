select 'RTK'                 project,
       phone_work,
       date(date_entered) as date,
       assigned_user_id,
       status
from suitecrm.jc_meetings_rostelecom
where status != 'Error'
# and date_entered <= now() - interval 12 month
union all
select 'Beeline'             project,
       phone_work,
       date(date_entered) as date,
       assigned_user_id,
       status
from suitecrm.jc_meetings_beeline
where status != 'Error'
# and date_entered <= now() - interval 12 month
union all
select project,
       phone_work,
       date(date_entered) as date,
       assigned_user_id,
       status
from suitecrm.jc_meetings_domru
where status != 'Error'
# and date_entered <= now() - interval 12 month
union all
select project,
       phone_work,
       date(date_entered) as date,
       assigned_user_id,
       status
from suitecrm.jc_meetings_ttk
where status != 'Error'
# and date_entered <= now() - interval 12 month
union all
select 'NBN'                 project,
       phone_work,
       date(date_entered) as date,
       assigned_user_id,
       status
from suitecrm.jc_meetings_netbynet
where status != 'Error'
# and date_entered <= now() - interval 12 month
union all
select project,
       phone_work,
       date(date_entered) as date,
       assigned_user_id,
       status
from suitecrm.jc_meetings_mts jc_meetings_mts
where status != 'Error'
# and date_entered <= now() - interval 12 month
union all
select project,
       phone_work,
       date(date_entered) as date,
       assigned_user_id,
       status
from suitecrm.jc_meetings_beeline_mnp
where status != 'Error'
# and date_entered <= now() - interval 12 month
