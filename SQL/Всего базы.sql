select t1.queue_num,
       count(t1.id_c)  as kolichestvo
from (
         select queue_num,
                id_c
         from contacts_cstm
                  left join adial_campaign_contacts_c
                            ON adial_campaign_contacts_c.adial_campaign_contactscontacts_idb = contacts_cstm.id_c
                  left join adial_campaign
                            on adial_campaign.id = adial_campaign_contacts_c.adial_campaign_contactsadial_campaign_ida
         where adial_campaign_contacts_c.deleted = '0'
           and step_c is null
           and contacts_status_c = 'null_status'
           and (count_good_calls_c in (0, 1)
             or (count_good_calls_c = 1 and last_call_c < (now() - interval 3 hour)))
     ) as t1
group by 1;
