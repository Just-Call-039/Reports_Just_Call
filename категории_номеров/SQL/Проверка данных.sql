select count(phone_request)
from suitecrm_robot_ch.phone_category_total
where phone_request not in ('', ' ');

select countIf(phone_request not in ('', ' '))
from suitecrm_robot_ch.phone_category_total;

select count(1)
from suitecrm_robot_ch.all_requests
where toDate(request_date) >= '2021-07-01';

select count(1)
from suitecrm_robot_ch.phone_category_total;
