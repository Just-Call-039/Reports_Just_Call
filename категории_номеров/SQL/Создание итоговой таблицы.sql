CREATE TABLE suitecrm_robot_ch.phone_category_total
(
    call_date     Date,
    phone         String,
    category      Int32,
    ptv           String,
    region        String,
    request_date  Date,
    phone_request String
) ENGINE = MergeTree()
      order by category;

-- drop table suitecrm_robot_ch.phone_category_total;
