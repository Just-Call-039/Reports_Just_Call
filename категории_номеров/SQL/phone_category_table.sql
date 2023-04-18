insert into suitecrm_robot_ch.phone_category

select count(phone) as category, phone
from suitecrm_robot_ch.jc_robot_log
group by phone;
