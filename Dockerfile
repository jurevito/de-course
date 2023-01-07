FROM apache/airflow:2.4.3


USER root

# Install ps
RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

WORKDIR /app

#COPY requirements.txt /app

#RUN pip install --trusted-host pypi.python.org -r requirements.txt