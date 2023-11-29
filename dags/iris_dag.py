from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

import os
from pathlib import Path

current_dir = "/Users/marc/DODOBIRD/DODO_CODE/git-worktree-test/kedro-airflow-docker-operator.git/marc-branch"

# DockerOperator settings
docker_image = "test-iris:latest"
network_mode = "bridge"
user = "kedro_docker"
mount_tmp_dir = False
mounts = [
    Mount(source=f"{current_dir}/src", target="/home/kedro_docker/src", type='bind'),
    Mount(source=f"{current_dir}/data", target="/home/kedro_docker/data", type='bind'),
    Mount(source=f"{current_dir}/conf", target="/home/kedro_docker/conf", type='bind'),

    # Mount(source="", target="/opt/airflow/", type='bind'),
    #Mount(source="", target="", type='bind')
]

# DAG and task settings
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "iris"


def determine_dates(**kwargs):

    params = kwargs['params']
    from_date = params.get('from_date')
    to_date = params.get('to_date')

    if from_date is None or to_date is None:
        # WARNING: dag_runs are not properly sorted. 
        default_to_date = "1000-01-01"
        dag_runs = DagRun.find(dag_id="iris")
        last_to_date = dag_runs[-1].conf.get("to_date", default_to_date)
        try:
            last_to_date = datetime.strptime(last_to_date, '%Y-%m-%d')
        except Exception as err: 
            print("#"*50)
            print(f"{last_to_date=}")
            print("#"*50)
            last_to_date = datetime.strptime(default_to_date,'%Y-%m-%d')

        from_date = last_to_date
        to_date = from_date + timedelta(days=1)
    
    kwargs['ti'].xcom_push(key='from_date', value=from_date)
    kwargs['ti'].xcom_push(key='to_date', value=to_date)

with DAG(
    dag_id="iris",
    start_date=datetime(2023,1,1),
    max_active_runs=3,
    schedule_interval="@once",
    catchup=False,
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5), 
        params = {"from_date" : Param(None, type=["null", "string"]),
                  "to_date" : Param(None, type=["null", "string"]),
                  } ,

        render_template_as_native_obj=True

    )

) as dag:
    

    var = "{{ task_instance.xcom_pull(task_ids='determine_dates', key='from_date') }}" 

    tasks = {

        "determine_dates" : PythonOperator(
            task_id='determine_dates',
            python_callable=determine_dates,
            provide_context=True,
            dag=dag),

        "check_date": DockerOperator(
            api_version='1.37',
            docker_url='TCP://docker-socket-proxy:2375',  # Adjust as needed
            command=(
                f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} "
                "--nodes check_date --env local "
                f"--params \'from_date={var}\' ' "
            ),
            image=docker_image,
            network_mode=network_mode,
            user=user,
            mount_tmp_dir=mount_tmp_dir,
            mounts=mounts,
            task_id="check_date",
        ),

        "split": DockerOperator(
            api_version='1.37',
            docker_url='TCP://docker-socket-proxy:2375',  # Adjust as needed
            command=(
                f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} "
                "--nodes split --env local "
                f"--params from_date={var} ' "
            ),
            image=docker_image,
            network_mode=network_mode,
            user=user,
            mount_tmp_dir=mount_tmp_dir,
            mounts=mounts,
            task_id="split_task",
        ),


        "make_predictions": DockerOperator(
            api_version='1.37',
            docker_url='TCP://docker-socket-proxy:2375',  # Adjust as needed
            command=(
                f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} "
                "--nodes split --env local "
                "--params from_date='"
            ),
            image=docker_image,
            network_mode=network_mode,
            user=user,
            mount_tmp_dir=mount_tmp_dir,
            mounts=mounts,
            task_id="make_predictions_task",
        ),

        "report_accuracy": DockerOperator(
            api_version='1.37',
            docker_url='TCP://docker-socket-proxy:2375',  # Adjust as needed
            command=f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} --nodes report_accuracy --env local'",
            image=docker_image,
            network_mode=network_mode,
            user=user,
            mount_tmp_dir=mount_tmp_dir,
            mounts=mounts,
            task_id="report_accuracy_task",
        ),

    }
    tasks['determine_dates'] >> tasks['check_date']
    tasks['check_date'] >> tasks['split']
    tasks["split"] >> tasks["make_predictions"]
    tasks["split"] >> tasks["report_accuracy"]
    tasks["make_predictions"] >> tasks["report_accuracy"]


