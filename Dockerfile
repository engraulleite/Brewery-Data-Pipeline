FROM jupyter/pyspark-notebook:spark-3.4.1

# Switch to root for system packages
USER root

# Basic installations
RUN apt-get update && \
    apt-get install -y curl wget gnupg software-properties-common python3 python3-pip bash procps && \
    pip3 install --upgrade pip

# Environment variables
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copy the .tgz file into the image
COPY aux/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz

# Extract locally without downloading
RUN tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Copy requirements and install dependencies
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# Install project dependencies
RUN pip install --no-cache-dir pyspark==3.4.1 delta-spark==2.4.0

# Expose Spark UI and Jupyter Notebook ports (if they are used)
EXPOSE 4040
EXPOSE 8888

# Set default environment variable for Python code location
ENV PYTHONPATH=/home/project

# Copy the project into the container
COPY . /home/project

# Working directory
WORKDIR /home/project

# Create Hive warehouse directory
RUN mkdir -p /opt/spark-warehouse

# Switch back to default user (jovyan)
USER ${NB_UID}
