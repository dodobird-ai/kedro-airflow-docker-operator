# Iris

Compile the docker image 

```docker build .  -t test-iris```

Then using kedro-airflow ( install in a clean venv using pip install kedro-airflow) run the command: 

```
kedro airflow create --params "docker_image=test-iris:latest,user=kedro_docker,current_dir=/Users/emmanuelthierrylincoln/Desktop/dodobird/Engineering/DataEngineering/iris/" -j templates/docker_operator.py

```

This will take the jinga template at templates/docker_operator.py, compile it as a dag file inside airflow_dags/ folder