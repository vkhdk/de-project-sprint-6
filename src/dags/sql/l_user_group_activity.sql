CREATE TABLE IF NOT EXISTS {{ user }}{{ dds_schema_postfix }}.l_user_group_activity (
	hk_l_user_group_activity int PRIMARY KEY,
	hk_user_id int NOT NULL REFERENCES {{ user }}{{ dds_schema_postfix }}.h_users (hk_user_id),
	hk_group_id int NOT NULL REFERENCES {{ user }}{{ dds_schema_postfix }}.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
);

INSERT INTO {{ user }}{{ dds_schema_postfix }}.l_user_group_activity(hk_l_user_group_activity, 
                                                             hk_user_id, 
                                                             hk_group_id, 
                                                             load_dt, 
                                                             load_src)
SELECT DISTINCT
	hash(gl.group_id, gl.user_id) AS hk_l_user_group_activity,
	hu.hk_user_id,
	hg.hk_group_id,
	NOW() AS load_dt,
	'S3' AS load_src
FROM {{ user }}{{ stage_schema_postfix }}.group_log AS gl
LEFT JOIN {{ user }}{{ dds_schema_postfix }}.h_users AS hu ON gl.user_id = hu.user_id 
LEFT JOIN {{ user }}{{ dds_schema_postfix }}.h_groups AS hg ON gl.group_id = hg.group_id 
WHERE hash(gl.group_id, gl.user_id) 
    NOT IN (SELECT hk_l_user_group_activity FROM {{ user }}{{ dds_schema_postfix }}.l_user_group_activity)
;