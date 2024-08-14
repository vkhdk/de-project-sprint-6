from airflow import DAG
import datetime as dt
from datetime import datetime

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': dt.timedelta(seconds=5),
        'start_date': dt.datetime(2020, 2, 2)
    }

with DAG(
    'dag_group_conversion',
    default_args = default_args,
    schedule_interval = '0/15 * * * *',
    max_active_runs = 1,
    catchup = False) as dag:

    from airflow.operators.dummy_operator import DummyOperator
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup
    from secrets_p6 import vertica_conn_info, s3_conn_info
    from dag_group_conversion_scripts import stg_group_log, \
                                             load_to_stg, \
                                             l_user_group_activity, \
                                             s_auth_history, \
                                             dm_user_group_log
    import boto3
    import vertica_python
    import os

    def get_data_from_s3(**kwargs):
        AWS_ACCESS_KEY_ID = s3_conn_info['aws_access_key_id']
        AWS_SECRET_ACCESS_KEY = s3_conn_info['aws_secret_access_key']
        service_name = s3_conn_info['service_name']
        endpoint_url = s3_conn_info['endpoint_url']
        bucket = kwargs['bucket']
        keys = kwargs['keys']
        file_path = kwargs['file_path']
        session = boto3.session.Session()
        s3_client = session.client(
            service_name = service_name,
            endpoint_url = endpoint_url,
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
            )
        for key in keys:
            dest_pathname = os.path.join(file_path, key)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            s3_client.download_file(
                Bucket = bucket,
                Key = key,
                Filename = file_path + key
        )
            
    def vertica_execute(**kwargs):
        sql = kwargs['sql']
        print(sql)
        vertica_conn_info = kwargs['vertica_conn_info']
        conn_params = {
            'host': vertica_conn_info['host'],
            'port': vertica_conn_info['port'],
            'user': vertica_conn_info['user'],
            'password': vertica_conn_info['password'],
            'ssl': False,
            'autocommit': True,
            'connection_timeout': 5
            }
        print(conn_params)
        with vertica_python.connect(**conn_params) as connection:
            with connection.cursor() as cur:
                cur.execute(sql)
    
    with TaskGroup(group_id='tg_etl') as tg_etl:
        get_data_from_s3_t = PythonOperator(
            task_id = 'get_data_from_s3_t',
            python_callable = get_data_from_s3,
            op_kwargs = {'bucket': 'sprint6', 'keys': ['group_log.csv'], 'file_path': '../raw_from_s3/sprint6/'},
        )

        create_stg_group_log = PythonOperator(
            task_id = 'create_stg_group_log',
            python_callable = vertica_execute,
            op_kwargs = {'sql': stg_group_log.format(user = vertica_conn_info['user'], 
                                                    stage_schema_postfix = vertica_conn_info['stage_schema_postfix']),
                        'vertica_conn_info': vertica_conn_info},
        )

        group_log_load_to_stg = PythonOperator(
            task_id = 'group_log_load_to_stg',
            python_callable = vertica_execute,
            op_kwargs = {'sql': load_to_stg.format(user = vertica_conn_info['user'], 
                                                    stage_schema_postfix = vertica_conn_info['stage_schema_postfix'],
                                                    table_name = 'group_log',
                                                    file_path = '../raw_from_s3/sprint6/group_log.csv'),
                        'vertica_conn_info': vertica_conn_info},
        )

        fill_l_user_group_activity = PythonOperator(
            task_id = 'fill_l_user_group_activity',
            python_callable = vertica_execute,
            op_kwargs = {'sql': l_user_group_activity.format(user = vertica_conn_info['user'], 
                                                    dds_schema_postfix = vertica_conn_info['dds_schema_postfix'],
                                                    stage_schema_postfix = vertica_conn_info['stage_schema_postfix']),
                        'vertica_conn_info': vertica_conn_info},
        )

        fill_s_auth_history = PythonOperator(
            task_id = 'fill_s_auth_history',
            python_callable = vertica_execute,
            op_kwargs = {'sql': s_auth_history.format(user = vertica_conn_info['user'], 
                                                    dds_schema_postfix = vertica_conn_info['dds_schema_postfix'],
                                                    stage_schema_postfix = vertica_conn_info['stage_schema_postfix']),
                        'vertica_conn_info': vertica_conn_info},
        )

        get_data_from_s3_t >> create_stg_group_log >> group_log_load_to_stg >> fill_l_user_group_activity >> fill_s_auth_history


    
    fill_dm_user_group_log = PythonOperator(
        task_id = 'fill_dm_user_group_log',
        python_callable = vertica_execute,
        op_kwargs = {'sql': dm_user_group_log.format(user = vertica_conn_info['user'], 
                                                dds_schema_postfix = vertica_conn_info['dds_schema_postfix']),
                    'vertica_conn_info': vertica_conn_info},
    )

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> tg_etl >> fill_dm_user_group_log >> end