select otkaz_c as otkaz, count(otkaz_c) as my_sum
from (select otkaz_c, date(date_entered)
      from suitecrm.calls_cstm as c_c
               left join suitecrm.calls as c on c_c.id_c = c.id
      where date(c.date_entered) = date(now()) - interval 1 day
        and right(otkaz_c, 2) != 23) as temp
group by otkaz_c;
