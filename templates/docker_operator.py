from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

import os
from pathlib import Path

current_dir = "{{ current_dir }}"
# DockerOperator settings
docker_image = "{{ docker_image }}"
network_mode = "{{ network_mode | default('bridge') }}"
user = "{{ user | default('default') }}"
mount_tmp_dir = {{ mount_tmp_dir | default(False) }}
mounts = [
    Mount(source=f"{current_dir}/data", target="/home/kedro_docker/data", type='bind'),
    Mount(source=f"{current_dir}/conf", target="/home/kedro_docker/conf", type='bind'),

    # Mount(source="{{ dirty_repo_mount }}", target="/opt/airflow/", type='bind'),
    #Mount(source="{{ dirty_aws_cred_mount }}", target="{{ dirty_aws_cred_mount }}", type='bind')
]

# DAG and task settings
env = "{{ env }}"
pipeline_name = "{{ pipeline_name }}"
project_path = Path.cwd()
package_name = "{{ package_name }}"

with DAG(
    dag_id="{{ dag_name | safe }}",
    start_date=datetime({{ start_date | default([2023, 1, 1]) | join(",")}}),
    max_active_runs={{ max_active_runs | default(3) }},
    schedule_interval="{{ schedule_interval | default('@once') }}",
    catchup={{ catchup | default(False) }},
    default_args=dict(
        owner="{{ owner | default('airflow') }}",
        depends_on_past={{ depends_on_past | default(False) }},
        email_on_failure={{ email_on_failure | default(False) }},
        email_on_retry={{ email_on_retry | default(False) }},
        retries={{ retries | default(1) }},
        retry_delay=timedelta(minutes={{ retry_delay | default(5) }})
    )
) as dag:
    
    tasks = {
    {% for node in pipeline.nodes %}
        "{{ node.name | safe }}": DockerOperator(
            api_version='1.37',
            docker_url='TCP://docker-socket-proxy:2375',  # Adjust as needed
            command=f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} --nodes {{ node.name | safe }} --env {{ env | default(local) }}'",
            image=docker_image,
            network_mode=network_mode,
            user=user,
            mount_tmp_dir=mount_tmp_dir,
            mounts=mounts,
            task_id="{{ node.name | safe }}_task",
        ),
    {% endfor %}
    }

    {% for parent_node, child_nodes in dependencies.items() -%}
    {% for child in child_nodes %}    tasks["{{ parent_node.name | safe }}"] >> tasks["{{ child.name | safe }}"]
    {% endfor %}
    {%- endfor %}


