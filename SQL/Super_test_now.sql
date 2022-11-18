select id, first_name, last_name, department_c
from suitecrm.users
         left join suitecrm.users_cstm on users.id = users_cstm.id_c
where id in (select distinct supervisor from suitecrm.worktime_supervisor)
into outfile 'sftp://root@10.88.22.250/root/airflow/dags' fields terminated by ';';
