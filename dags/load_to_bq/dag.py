from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from dags.load_to_bq.flt_bl import FLT_BLTask

default_args = {
    "owner": "kien tran",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 19),
    "email": ["kientd.aits@vietnamairlines.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2019, 1, 1),
}

dag = DAG("load_avro_to_bq", default_args=default_args, catchup=False,
          max_active_runs=1, schedule_interval=timedelta(days=1))

with dag:
    start = DummyOperator(
        task_id='start'
    )

    end = DummyOperator(
        task_id='end'
    )

    flt_blTask = FLT_BLTask(dag).tasks()
    start >> flt_blTask[0]
    flt_blTask[1] >> end
