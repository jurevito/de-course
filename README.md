# Data Engineering Project
Project repository for Data Engineering course.

## Project Setup
1. Make sure you installed `docker` and `docker-compose`.
2. Run `docker-compose up` to prepare docker containers.

## Information
- **Apache Airflow**
    - `port` is `8080:8080`
    - `username` is `airflow`
    - `password` is `airflow`

## Apache Spark
1. Setup master and worker nodes using `docker compose up`.
2. Connect to master node with `docker exec -it <master-container> bash`. Replace `<master-container>` with container's ID.
3. When you are in bash shell you can run a job using `bin/spark-submit ./work/target/scala-2.12/spark-job_2.12-1.0.jar --class "Job"`.