from datetime import date, datetime, timedelta
import os
import pendulum

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from common.utils.gmail import send_email
from common.utils.sms import send_sms
from dags.services.momentum.run import run


local_tz = pendulum.timezone('Europe/Paris')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['edouard.darchimbaud@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    os.path.basename(__file__).split('.')[0],
    catchup=False,
    default_args=default_args,
    description='A DAG to execute Momentum strategy',
    schedule_interval='6 0 * * *',
)


def execute_momentum(**kwargs):
    run(mode='live', stems='ES,GC,NQ,ZF,ZN,ZT', leverage=2)


def notify_failure(**kwargs):
    send_sms('[ERR] Execute Momentum')
    send_email('[ERR] Execute Momentum')


t1 = PythonOperator(
    dag=dag,
    provide_context=True,
    python_callable=execute_momentum,
    task_id='execute_momentum',
)

t2 = PythonOperator(
    dag=dag,
    provide_context=True,
    python_callable=notify_failure,
    task_id='notify_failure',
    trigger_rule=TriggerRule.ONE_FAILED
)


t1.doc_md = """\
#### Task documentation

Execute Momentum strategy
"""

t1 >> t2
