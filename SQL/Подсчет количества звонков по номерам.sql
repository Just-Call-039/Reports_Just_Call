with current_robot as (select phone,
                              call_date,
                              last_step,
                              ptv_c,
                              region_c
                       from suitecrm_robot.jc_robot_log
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')),

     history_robot as (select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_10
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_09
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_08
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_07
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_06
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_05
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_04
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_03
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_02
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2022_01
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2021_12
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2021_11
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2021_10
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2021_09
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2021_08
                       where last_step not in (0, 1, 261, 262, 111, '', ' ')
                       union all
                       select phone,
                              call_date,
                              last_step
                       from suitecrm_robot.jc_robot_log_2021_07
                       where last_step not in (0, 1, 261, 262, 111, '', ' '))

select count(temp.phone), temp.phone
from (select current_robot.phone
      from current_robot
      union all
      select history_robot.phone
      from history_robot) as temp
group by temp.phone;
