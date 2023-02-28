with calls as (select phone,
                      date(call_date)          as call_date,
                      route,
                      case
                          when
                                      ptv_c like '%^3^%'
                                  or ptv_c like '%^5^%'
                                  or ptv_c like '%^6^%'
                                  or ptv_c like '%^10^%'
                                  or ptv_c like '%^11^%'
                                  or ptv_c like '%^19^%'
                                  or ptv_c like '%^14^%' then 'Разметка Наша'
                          when
                                      ptv_c like '%^3_19^%'
                                  or ptv_c like '%^5_19^%'
                                  or ptv_c like '%^6_19^%'
                                  or ptv_c like '%^10_19^%'
                                  or ptv_c like '%^11_19^%'
                                  or ptv_c like '%^19_19^%'
                                  or ptv_c like '%^14_19^%' then 'Разметка не наша 50+'
                          when
                                      ptv_c like '%^3_21^%'
                                  or ptv_c like '%^5_21^%'
                                  or ptv_c like '%^6_21^%'
                                  or ptv_c like '%^10_21^%'
                                  or ptv_c like '%^11_21^%'
                                  or ptv_c like '%^19_21^%'
                                  or ptv_c like '%^14_21^%' then 'Разметка не наша Телеком'
                          when
                                      ptv_c like '%^3_18^%'
                                  or ptv_c like '%^5_18^%'
                                  or ptv_c like '%^6_18^%'
                                  or ptv_c like '%^10_18^%'
                                  or ptv_c like '%^11_18^%'
                                  or ptv_c like '%^19_18^%'
                                  or ptv_c like '%^14_18^%' then 'Разметка не наша 50-40'
                          when
                                      ptv_c like '%^5_20^%'
                                  or ptv_c like '%^3_20^%'
                                  or ptv_c like '%^6_20^%'
                                  or ptv_c like '%^10_20^%'
                                  or ptv_c like '%^11_20^%'
                                  or ptv_c like '%^19_20^%'
                                  or ptv_c like '%^14_20^%' then 'Разметка не наша Спутник'
                          when
                                      ptv_c like '%^3_17^%'
                                  or ptv_c like '%^5_17^%'
                                  or ptv_c like '%^6_17^%'
                                  or ptv_c like '%^10_17^%'
                                  or ptv_c like '%^11_17^%'
                                  or ptv_c like '%^19_17^%'
                                  or ptv_c like '%^14_17^%' then 'Разметка не наша 40-30'
                          when
                                      ptv_c like '%^5_16^%'
                                  or ptv_c like '%^3_16^%'
                                  or ptv_c like '%^6_16^%'
                                  or ptv_c like '%^10_16^%'
                                  or ptv_c like '%^11_16^%'
                                  or ptv_c like '%^19_16^%'
                                  or ptv_c like '%^14_16^%' then 'Разметка не наша 30-20'
                          when
                                      ptv_c like '%^5_15^%'
                                  or ptv_c like '%^3_15^%'
                                  or ptv_c like '%^6_15^%'
                                  or ptv_c like '%^10_15^%'
                                  or ptv_c like '%^11_15^%'
                                  or ptv_c like '%^19_15^%'
                                  or ptv_c like '%^14_15^%' then 'Разметка не наша 20-0'
                          else ''
                          end                  as ptv,
                      case
                          when region_c = 1 then 'Наша полная'
                          when region_c = 2 then 'Наша неполная'
                          when region_c = 4 then 'Фиас из разных источников'
                          when region_c = 5 then 'Фиас до города'
                          when region_c = 6 then 'Старый town_c'
                          when region_c = 7 then 'Def code'
                          else ''
                          end                  as region,
                      last_step,
                      uniqueid,
                      client_status,
                      otkaz,
                      was_repeat,
                      substring(dialog, 11, 4) as queue,
                      server_number,
                      directory,
                      city_c,
                      region_c
               from suitecrm_robot.jc_robot_log
               where date(call_date) = date(now()) - interval 1 day)

select phone,
       call_date,
       route,
       if(ptv not in ('', ' '), ptv, region) as quality,
       last_step,
       uniqueid,
       client_status,
       otkaz,
       was_repeat,
       queue,
       server_number,
       directory,
       city_c,
       region_c
from calls;
