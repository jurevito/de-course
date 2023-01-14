# Data Engineering Project
Project repository for Data Engineering course.

## Project Setup
1. Install Docker and docker-compose.
2. Run `docker compose up` to setup containers. First run add `--build` to build airflow image with all dependencies.
3. Run `pip install -r requirements.txt` to install dependencies locally.
4. Open Apache Airflow UI at http://localhost:8080 and login with username and password `airflow`.
5. Add all necessary connections in `Admin -> Connections`.
    - Add connection for Apache Spark. Host should be `spark://spark-master`, port is `7077` and connection ID is `spark_container`.
6. Download [dataset](https://www.kaggle.com/datasets/Cornell-University/arxiv?resource=download) and save it as `./data/ingestion/publications.json`. Run `setup.py` script to divide data into chunks.

## Apache Spark
Can be accessed at http://localhost:8081.
1. Setup master and worker nodes using `docker compose up`.
2. Connect to master node with `docker exec -it <master-container> bash`. Replace `<master-container>` with container's ID.
3. When you are in bash shell you can run a job using `bin/spark-submit ./work/job.py`.