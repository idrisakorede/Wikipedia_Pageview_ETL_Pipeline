# Define build-time variables for Airflow and Python versions
ARG AIRFLOW_VERSION=3.1.0
ARG PYTHON_VERSION=3.12

# Use the specified Airflow image with the chosen Python version
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# # Set working directory to Airflow home
# WORKDIR /opt/airflow

# Copy dependency list into the image
COPY requirements.txt /requirements.txt 

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

