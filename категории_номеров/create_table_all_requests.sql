CREATE TABLE suitecrm_robot_ch.all_requests
(
    project       String,
    my_phone_work String,
    request_date  Date,
    user          String,
    super         String,
    status        String
) ENGINE = MergeTree()
      order by my_phone_work;

DROP TABLE suitecrm_robot_ch.all_requests;
