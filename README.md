# Iris - Marc Change

Compile the docker image 

```docker build .  -t test-iris```

```echo -e "AIRFLOW_UID=$(id -u)" > .env```

```docker-compose up``` 



Then using kedro-airflow ( install in a clean venv using pip install kedro-airflow) run the command: 

```
kedro airflow create --params "docker_image=test-iris:latest,user=kedro_docker,current_dir=$(pwd)" -j templates/docker_operator.py
```

This will take the jinga template at templates/docker_operator.py, compile it as a dag file inside airflow_dags/ folder
