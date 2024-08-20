CREATE TABLE IF NOT EXISTS {{ user }}{{ dds_schema_postfix }}.s_auth_history (
	hk_l_user_group_activity int NOT NULL 
        REFERENCES {{ user }}{{ dds_schema_postfix }}.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(6),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
);

INSERT INTO {{ user }}{{ dds_schema_postfix }}.s_auth_history(hk_l_user_group_activity, 
                                              user_id_from, 
                                              event, 
                                              event_dt, 
                                              load_dt, 
                                              load_src)
SELECT
	uga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.date_time AS event_dt,
	NOW() AS load_dt,
	'S3' AS load_src
FROM {{ user }}{{ stage_schema_postfix }}.group_log AS gl
LEFT JOIN {{ user }}{{ dds_schema_postfix }}.h_groups AS hg 
    ON gl.group_id = hg.group_id
LEFT JOIN {{ user }}{{ dds_schema_postfix }}.h_users AS hu 
    ON gl.user_id = hu.user_id
LEFT JOIN {{ user }}{{ dds_schema_postfix }}.l_user_group_activity AS uga 
    ON hg.hk_group_id = uga.hk_group_id AND hu.hk_user_id = uga.hk_user_id
WHERE hk_l_user_group_activity 
    NOT IN (SELECT hk_l_user_group_activity FROM {{ user }}{{ dds_schema_postfix }}.s_auth_history); 
;