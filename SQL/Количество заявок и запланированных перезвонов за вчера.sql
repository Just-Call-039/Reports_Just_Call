with total(id) as
         (select distinct(contact_id_c)
          from suitecrm.jc_planned_calls
          where (date(date_entered) = date(now()) - interval 1 day)
            and status = '1'),
     zayavka(id) as (select distinct(contact_id_c)
                     from suitecrm.jc_planned_calls
                     where contacts_status = 'MeetingWait'
                       and (date(date_entered) = date(now()) - interval 1 day))
select count(zayavka.id), count(total.id)
from total
         left join zayavka on total.id = zayavka.id;
