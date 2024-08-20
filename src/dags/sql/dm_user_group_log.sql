DROP TABLE IF EXISTS {{ user }}{{ dds_schema_postfix }}.dm_user_group_log;
CREATE TABLE {{ user }}{{ dds_schema_postfix }}.dm_user_group_log (hk_group_id int NOT NULL PRIMARY KEY, 
                                                           cnt_added_users int, 
                                                           cnt_users_in_group_with_messages int, 
                                                           group_conversion numeric(18, 6));
INSERT INTO {{ user }}{{ dds_schema_postfix }}.dm_user_group_log(hk_group_id,
                                                         cnt_added_users,
                                                         cnt_users_in_group_with_messages,
                                                         group_conversion)
WITH user_group_log AS
  (SELECT hg.hk_group_id,
          COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users,
          MAX(hg.registration_dt) AS max_registration_dt
   FROM {{ user }}{{ dds_schema_postfix }}.h_groups hg
   LEFT JOIN {{ user }}{{ dds_schema_postfix }}.l_user_group_activity luga ON hg.hk_group_id = luga.hk_group_id
   INNER JOIN {{ user }}{{ dds_schema_postfix }}.s_auth_history sah 
    ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
   WHERE sah.event = 'add'
   GROUP BY hg.hk_group_id),
     user_group_messages AS
  (SELECT hg.hk_group_id,
          COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
   FROM {{ user }}{{ dds_schema_postfix }}.h_groups hg
   LEFT JOIN {{ user }}{{ dds_schema_postfix }}.l_groups_dialogs lgd ON hg.hk_group_id = lgd.hk_group_id
   INNER JOIN {{ user }}{{ dds_schema_postfix }}.l_user_message lum ON lgd.hk_message_id = lum.hk_message_id
   GROUP BY hg.hk_group_id)
SELECT ugl.hk_group_id,
       ugl.cnt_added_users,
       ugm.cnt_users_in_group_with_messages,
       ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY ugl.cnt_added_users DESC
LIMIT 10;