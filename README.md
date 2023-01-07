# Data Engineering Project
Project repository for Data Engineering course.

## Project Setup
1. Install Docker and docker-compose.

## Apache Airflow
Username and password are `airflow`. The UI can accessed at http://localhost:8080.

## Apache Spark
Can be accessed at http://localhost:8081.
1. Setup master and worker nodes using `docker compose up`.
2. Connect to master node with `docker exec -it <master-container> bash`. Replace `<master-container>` with container's ID.
3. When you are in bash shell you can run a job using `bin/spark-submit ./work/job.py`.

## TODO
Stopped at `spark-submit --master spark://spark-master:7077 --name arrow-spark ./job.py` tried to run this inside of a airflow-scheduler container. It runs but it comes to an error. Also airflow path towards the scripts is not in dag folder.