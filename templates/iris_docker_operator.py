from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import DagRun
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
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

def determine_dates(**kwargs):

    params = kwargs['params']
    from_date = params.get('from_date')
    to_date = params.get('to_date')
    from_date = datetime.strptime(from_date, '%Y-%m-%d')
    to_date = datetime.strptime(to_date, '%Y-%m-%d')

    if from_date is None or to_date is None:
        # WARNING: dag_runs are not properly sorted. 
        default_to_date = "1000-01-01"
        # TODO: we need to sort dag_runs by execution date from oldest to newest
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

    from_date_str = from_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    to_date_str = to_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    kwargs['ti'].xcom_push(key='from_date', value=from_date_str)
    kwargs['ti'].xcom_push(key='to_date', value=to_date_str)

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
        retry_delay=timedelta(minutes={{ retry_delay | default(5) }}),
        params = {"from_date" : Param(None, type=["null", "string"], format="date"),
                "to_date" : Param(None, type=["null", "string"], format="date"),
                } ,

        render_template_as_native_obj=True
    )
) as dag:
    
    var = "{% raw %}{{ task_instance.xcom_pull(task_ids='determine_dates', key='from_date') }}{% endraw %}"

    tasks = {

        "determine_dates" : PythonOperator(
            task_id='determine_dates',
            python_callable=determine_dates,
            provide_context=True,
            dag=dag),

    {% for node in pipeline.nodes %}
        "{{ node.name | safe }}": DockerOperator(
            api_version='1.37',
            docker_url='TCP://docker-socket-proxy:2375',  # Adjust as needed
            command=f"/bin/bash -c 'python -m kedro run --pipeline {pipeline_name} "
                    " --nodes {{ node.name | safe }} --env {{ env | default(local) }} "
                    f"--params from_date=\"{var}\" ' ",
            image=docker_image,
            network_mode=network_mode,
            user=user,
            mount_tmp_dir=mount_tmp_dir,
            mounts=mounts,
            task_id="{{ node.name | safe }}_task",
        ),
    {% endfor %}
    }
    tasks['determine_dates'] >> tasks['check_date']
    {%- for parent_node, child_nodes in [dependencies.items()|first] %}
    tasks['check_date'] >> tasks['{{ parent_node.name | safe }}']
    {%- endfor %}
    {% for parent_node, child_nodes in dependencies.items() -%}
    {% for child in child_nodes %}    tasks["{{ parent_node.name | safe }}"] >> tasks["{{ child.name | safe }}"]
    {% endfor %}
    {%- endfor %}


