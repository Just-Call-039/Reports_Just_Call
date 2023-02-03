insert into suitecrm_robot_ch.phone_category_total_check

select rl.uniqueid,
       rl.phone,
       rl.client_status,
       if(client_status = 'MeetingWait'
              and step_translation != 0
              and ochered_translation != 0
              and assigned_user_id not in ('', ' ', '1'), 1, 0) as is_request,
       rl.assigned_user_id,
       rl.last_step,
       rl.ochered,
       rl.call_date,
       trs.step                                                 as step_translation,
       trs.ochered                                              as ochered_translation,
       pc.category,
       case
           when
                       rl.ptv_c like '%^3^%'
                   or rl.ptv_c like '%^5^%'
                   or rl.ptv_c like '%^6^%'
                   or rl.ptv_c like '%^10^%'
                   or rl.ptv_c like '%^11^%'
                   or rl.ptv_c like '%^19^%'
                   or rl.ptv_c like '%^14^%' then 'Разметка Наша'
           when
                       rl.ptv_c like '%^3_19^%'
                   or rl.ptv_c like '%^5_19^%'
                   or rl.ptv_c like '%^6_19^%'
                   or rl.ptv_c like '%^10_19^%'
                   or rl.ptv_c like '%^11_19^%'
                   or rl.ptv_c like '%^19_19^%'
                   or rl.ptv_c like '%^14_19^%' then 'Разметка не наша 50+'
           when
                       rl.ptv_c like '%^3_21^%'
                   or rl.ptv_c like '%^5_21^%'
                   or rl.ptv_c like '%^6_21^%'
                   or rl.ptv_c like '%^10_21^%'
                   or rl.ptv_c like '%^11_21^%'
                   or rl.ptv_c like '%^19_21^%'
                   or rl.ptv_c like '%^14_21^%' then 'Разметка не наша Телеком'
           when
                       rl.ptv_c like '%^3_18^%'
                   or rl.ptv_c like '%^5_18^%'
                   or rl.ptv_c like '%^6_18^%'
                   or rl.ptv_c like '%^10_18^%'
                   or rl.ptv_c like '%^11_18^%'
                   or rl.ptv_c like '%^19_18^%'
                   or rl.ptv_c like '%^14_18^%' then 'Разметка не наша 50-40'
           when
                       rl.ptv_c like '%^5_20^%'
                   or rl.ptv_c like '%^3_20^%'
                   or rl.ptv_c like '%^6_20^%'
                   or rl.ptv_c like '%^10_20^%'
                   or rl.ptv_c like '%^11_20^%'
                   or rl.ptv_c like '%^19_20^%'
                   or rl.ptv_c like '%^14_20^%' then 'Разметка не наша Спутник'
           when
                       rl.ptv_c like '%^3_17^%'
                   or rl.ptv_c like '%^5_17^%'
                   or rl.ptv_c like '%^6_17^%'
                   or rl.ptv_c like '%^10_17^%'
                   or rl.ptv_c like '%^11_17^%'
                   or rl.ptv_c like '%^19_17^%'
                   or rl.ptv_c like '%^14_17^%' then 'Разметка не наша 40-30'
           when
                       rl.ptv_c like '%^5_16^%'
                   or rl.ptv_c like '%^3_16^%'
                   or rl.ptv_c like '%^6_16^%'
                   or rl.ptv_c like '%^10_16^%'
                   or rl.ptv_c like '%^11_16^%'
                   or rl.ptv_c like '%^19_16^%'
                   or rl.ptv_c like '%^14_16^%' then 'Разметка не наша 30-20'
           when
                       rl.ptv_c like '%^5_15^%'
                   or rl.ptv_c like '%^3_15^%'
                   or rl.ptv_c like '%^6_15^%'
                   or rl.ptv_c like '%^10_15^%'
                   or rl.ptv_c like '%^11_15^%'
                   or rl.ptv_c like '%^19_15^%'
                   or rl.ptv_c like '%^14_15^%' then 'Разметка не наша 20-0'
           else ''
           end                                                     ptv,
       case
           when region_c = 1 then 'Наша полная'
           when region_c = 2 then 'Наша неполная'
           when region_c = 4 then 'Фиас из разных источников'
           when region_c = 5 then 'Фиас до города'
           when region_c = 6 then 'Старый town_c'
           when region_c = 7 then 'Def code'
           else ''
           end                                                     region
from suitecrm_robot_ch.jc_robot_log_january_only as rl
         left join suitecrm_robot_ch.translation_steps_for_test as trs
                   on rl.ochered = trs.ochered
                       and rl.last_step = trs.step
         left join suitecrm_robot_ch.phone_category as pc on rl.phone = toInt64(pc.phone);
