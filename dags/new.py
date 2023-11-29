from datetime import datetime, timedelta
from airflow.models import DagRun
from airflow.settings import Session

def get_last_exec_date(dag_id: str) -> dict:
    """
    Using the dag id, retrieve the latest execution datetime of a dag which was successfully ran"""
    dag_runs = DagRun.find(dag_id=dag_id)
    dags = []
    for dag in dag_runs:
        if dag.state == 'success':
            dags.append(dag)

    dags.sort(key=lambda x: x.execution_date, reverse=False)

    return dags[0] if dags != [] else None


if __name__ == "__main__":
    dag_runs = DagRun.find(dag_id="iris_with_params")
    last_run = dag_runs[-1] 
    last_from_date = last_run.conf.get("from_date")
    date_obj = datetime.strptime(last_from_date, '%Y-%m-%d')
    new_date_obj = date_obj + timedelta(days=1)
    new_from_date = new_date_obj.strftime('%Y-%m-%d')

    print(f"{last_from_date}")
    print(f"{new_from_date}")










