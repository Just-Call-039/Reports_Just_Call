CREATE TABLE suitecrm_robot_ch.request_25
(
    phone_number String,
    assigned_user_id String,
    status_request String,
    my_date DateTime,
    uniqueid String,
    ochered Int32,
    project String
) ENGINE = MergeTree()
    order by my_date;

DROP TABLE suitecrm_robot_ch.request_25;
