FROM apache/airflow:latest

# Switch to root user to install packages
USER root

# Install necessary packages including Java
RUN apt-get update && \
    apt-get -y install git openjdk-17-jdk-headless && \
    apt-get clean

# Switch to airflow user to install Python packages
USER airflow

# Install PySpark
RUN pip install pyspark
