# Import library
import pendulum
import pandas as pd
from airflow import DAG
from datetime import timedelta
from airflow.models import XCom
from airflow.hooks.base_hook import BaseHook
from airflow.utils.db import provide_session
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
# from great_expectations_provider.operators.great_expectations import (
#     GreatExpectationsOperator
# )
from include.great_expectations.suite_fact_order_accumulating import get_data_suite
from include.great_expectations.fact_order_acumulating_datasource import get_data_context
from include.great_expectations.checkpoint_fact_order_accumulating import get_checkpoint



# Write down all information about this DAG in doc_info
doc_info = """
### FACT_ORDER_ACCUMULATING

### PURPOSE
<describe main purpose of this DAG>

### NOTES
<insert link documentation of this DAG>
"""

default_args = {
    'owner': 'Dilla',
    'retries': 0,
    # 'on_failure_callback': task_fail_slack_alert,
    'execution_timeout': timedelta(minutes=30)
}

def get_data_load():
    src = PostgresHook(postgres_conn_id='database')
    dest = PostgresHook(postgres_conn_id='database')
    dest_engine = dest.get_sqlalchemy_engine()
    src_conn = src.get_conn()
    scr_cursor = src_conn.cursor()

    fd = open('/opt/airflow/dags/utils/sql/fact_order_accumulating_1.sql', 'r')
    sqlFile = fd.read()
    scr_cursor.execute(sqlFile)
    rows = scr_cursor.fetchall()
    fd.close()
    col_names = []
    for names in scr_cursor.description:
        col_names.append(names[0])
    new = pd.DataFrame(rows, columns=col_names)
    new["data_updated_at"] = pd.to_datetime(pendulum.now(tz = "Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S"))

    new.to_sql(
        'fact_order_accumulating',
        dest_engine, 
        if_exists='append', 
        index=False, 
        schema='datawarehouse')
    src_conn.close()
    print('success')

def get_data_quality():
    get_data_suite()
    get_data_context()
    get_checkpoint()

# Clear xcom
@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

with DAG(
        dag_id='fact_order_accumulating',
        schedule_interval='0 1 * * *',
        start_date=pendulum.datetime(2021, 1, 1, tz='Asia/Bangkok'), 
        doc_md = doc_info,
        catchup=False,
        template_searchpath = ['/opt/airflow/dags/utils/sql'],
        default_args = default_args,
        tags=['datawarehouse']
) as dag:
    start_task = DummyOperator(
        task_id = "start",
        trigger_rule = "all_done"
    )

    create_table = PostgresOperator(      
                task_id = "create_table_task",
                postgres_conn_id = "database",
                sql =['create_table_fact_order_accumulating.sql'],
                dag = dag
                )
    
    get_update_load_data = PythonOperator(
            task_id = 'get_update_load',
            python_callable = get_data_load,
            )

    get_quality_data = PythonOperator(
            task_id = 'get_data_quality',
            python_callable = get_data_quality,
            )
    # ge_data_context_config_pass = GreatExpectationsOperator(
    #     task_id="ge_data_context_config_with_checkpoint_config_pass",
    #     data_context_config=get_data_context,
    #     checkpoint_name = get_checkpoint,
    #     expectation_suite_name="suite_fact_order_accumulating",
    #     data_asset_name="fact_order_accumulating",)

    delete_xcom = PythonOperator(
        task_id = "delete_xcom",
        python_callable = cleanup_xcom,
        provide_context = True
    )

    end_task = DummyOperator(
            task_id = "finish",
            trigger_rule = "all_done"
        )

    start_task >> create_table >> get_update_load_data >> get_quality_data >> delete_xcom >> end_task