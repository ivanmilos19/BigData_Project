FROM apache/airflow:2.7.1
USER root
 
# Install OpenJDK-11
RUN apt update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean
 
# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
 
USER airflow
 
RUN curl -O 'https://bootstrap.pypa.io/get-pip.py' && \
    python3 get-pip.py --user
 
RUN pip install pyspark
