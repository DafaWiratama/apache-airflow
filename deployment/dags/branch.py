from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

args = {
    "default_args": {
        "depends_on_past": False,
        "start_date": days_ago(1),
        'retries': 0
    },
    "schedule_interval": None,
    "catchup": False,
    "max_active_runs": 1
}


def branch_fn():
    return "t5"  # id of the next task


dag = DAG("branch_sample", **args)
with dag:
    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = BranchPythonOperator(task_id="t3", python_callable=branch_fn)
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")
    t7 = DummyOperator(task_id="t7", trigger_rule=TriggerRule.ONE_SUCCESS)
    t8 = DummyOperator(task_id="t8")
    t9 = DummyOperator(task_id="t9")

    t1 >> t2 >> t3
    t3 >> t4
    t3 >> t5 >> t6
    t4 >> t7
    t6 >> t7
    t7 >> t8 >> t9