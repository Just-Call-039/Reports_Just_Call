CREATE TABLE suitecrm_robot_ch.phone_category
(
    category Int32,
    phone    String
) ENGINE = MergeTree()
      order by category;

-- drop table suitecrm_robot_ch.phone_category;
