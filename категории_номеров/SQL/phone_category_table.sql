insert into suitecrm_robot_ch.phone_category

select count(phone) over (partition by phone) as category, phone
from suitecrm_robot_ch.jc_robot_log;
