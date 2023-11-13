from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

import os
from pathlib import Path

current_dir = "/Users/emmanuelthierrylincoln/Desktop/dodobird/Engineering/DataEngineering/iris/"
# DockerOperator settings
docker_image = "test-iris:latest"
network_mode = "bridge"
user = "kedro_docker"
mount_tmp_dir = False
mounts = [
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

with DAG(
    dag_id="iris_2",
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
        retry_delay=timedelta(minutes=5)
    )
) as dag:
    
    tasks = {

        "split": DockerOperator(
            api_version='1.37',
            docker_url='TCP://docker-socket-proxy:2375',  # Adjust as needed
            command=f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} --nodes split --env local'",
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
            command=f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} --nodes make_predictions --env local'",
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

    tasks["split"] >> tasks["make_predictions"]
    tasks["split"] >> tasks["report_accuracy"]
    tasks["make_predictions"] >> tasks["report_accuracy"]


