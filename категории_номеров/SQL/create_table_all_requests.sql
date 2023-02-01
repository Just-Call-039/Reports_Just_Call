CREATE TABLE suitecrm_robot_ch.all_requests
(
    project       String,
    phone_request String,
    request_date  Date,
    user          String,
    super         String,
    status        String
) ENGINE = MergeTree()
      order by phone_request;

-- drop table suitecrm_robot_ch.all_requests;

CREATE TABLE suitecrm_robot_ch.all_requests_id
(
    project       String,
    phone_request String,
    request_date  Date,
    user          String,
    super         String,
    status        String,
    uniqueid      String
) ENGINE = MergeTree()
      order by phone_request;

-- drop table suitecrm_robot_ch.all_requests_id;
