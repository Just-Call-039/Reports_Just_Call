# Количество разметки МТС с отлежкой 100 дней.
# Дополнительно необходимо прописать все возможные варианты для МТС, а не только один.

select *
from suitecrm.contacts_cstm
where date(last_call_c) < date(now()) - interval 100 day
  and ptv_c like '%11%';
