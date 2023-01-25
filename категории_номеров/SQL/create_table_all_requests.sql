CREATE TABLE suitecrm_robot_ch.all_requests
(
    phone_request String,
    user          String,
    status        String,
    request_date  Date,
    uniqueid      String,
    ochered       String,
    phone         String,
    project       String
) ENGINE = MergeTree()
      order by phone_request;

drop table suitecrm_robot_ch.all_requests;
