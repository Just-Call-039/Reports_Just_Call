CREATE TABLE suitecrm_robot_ch.phone_category_total_check
(
    uniqueid            String,
    phone               Int64,
    client_status       String,
    is_request          Int64,
    assigned_user_id    String,
    last_step           Int64,
    ochered             Int64,
    call_date           datetime,
    step_translation    Int64,
    ochered_translation Int64,
    category            Int64,
    ptv                 String,
    region              String
) ENGINE = MergeTree()
      order by phone;

-- drop table suitecrm_robot_ch.phone_category_total_check;
