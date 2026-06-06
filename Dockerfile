# Base image with Python 3.11
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        curl wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Application source
WORKDIR /app
COPY src/ /app/src/

# Data and output directories
VOLUME ["/data", "/output"]


# JAVA

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk \
    bash \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Entrypoints to run two scrips (detector and visualizer) 
#sed used to make windows->linux compatable
COPY entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh 
ENTRYPOINT ["/app/entrypoint.sh"]
