COPY {{ user }}{{ stage_schema_postfix }}.{{ table_name }}
    FROM LOCAL '{{ local_file_path }}'
    ENCLOSED BY '"'
    DELIMITER ','
    REJECTED DATA AS TABLE {{ table_name }}_rej
    SKIP 1