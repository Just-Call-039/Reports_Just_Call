CREATE TABLE suitecrm_robot_ch.jc_robot_log_january_only
(
    uniqueid         String,
    phone            Int64,
    client_status    String,
    assigned_user_id String,
    last_step        Int64,
    ochered          Int64,
    call_date        datetime,
    ptv_c            String,
    region_c         Int64
) ENGINE = MergeTree()
      order by phone;

-- drop table suitecrm_robot_ch.jc_robot_log_january_only;

CREATE TABLE suitecrm_robot_ch.translation_steps_for_test
(
    step    Int64,
    ochered Int64
) ENGINE = MergeTree()
      order by ochered;

-- drop table suitecrm_robot_ch.translation_steps_for_test;
