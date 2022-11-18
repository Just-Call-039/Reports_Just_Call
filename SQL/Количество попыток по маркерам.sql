with total (marker, count_good_calls_c, step_c, contacts_status_c)
         as (select marker_c, count_good_calls_c, step_c, contacts_status_c
             from suitecrm.contacts_cstm
             where date(last_call_c) = date(now()) - interval 2 day),
     t_1 (marker_c, my_c_one) as (select marker, count(*)
                                  from total
                                  where count_good_calls_c >= 1
                                  group by marker),
     t_2 (marker_c, my_c_two) as (select marker, count(*)
                                  from total
                                  where count_good_calls_c >= 2
                                  group by marker),
     t_3 (marker_c, my_c_three) as (select marker, count(*)
                                    from total
                                    where count_good_calls_c >= 3
                                    group by marker),
     t_4 (marker_c, one_dozvon) as (select marker, count(*)
                                    from total
                                    where count_good_calls_c = 1
                                      and step_c not in (0, 1, 111, 261, 262)
                                    group by marker),
     t_5 (marker_c, two_dozvon) as (select marker, count(*)
                                    from total
                                    where count_good_calls_c = 2
                                      and step_c not in (0, 1, 111, 261, 262)
                                    group by marker),
     t_6 (marker_c, three_dozvon) as (select marker, count(*)
                                      from total
                                      where count_good_calls_c = 3
                                        and step_c not in (0, 1, 111, 261, 262)
                                      group by marker),
     t_7 (marker_c, perevod) as (select marker, count(*)
                                 from total
                                 where count_good_calls_c >= 1
                                   and contacts_status_c in ('MeetingWait', 'CallWait', 'Wait', 'refusing')
                                 group by marker)
select t_1.marker_c,
       t_1.my_c_one,
       t_4.one_dozvon,
       t_2.my_c_two,
       t_5.two_dozvon,
       t_3.my_c_three,
       t_6.three_dozvon,
       t_7.perevod
from t_1
         left join t_2 on t_1.marker_c = t_2.marker_c
         left join t_3 on t_1.marker_c = t_3.marker_c
         left join t_4 on t_1.marker_c = t_4.marker_c
         left join t_5 on t_1.marker_c = t_5.marker_c
         left join t_6 on t_1.marker_c = t_6.marker_c
         left join t_7 on t_1.marker_c = t_7.marker_c;
