# Airflow runtime for the trade pipeline.
#
# The build context is ./airflow (see docker-compose.yml), so COPY paths below
# are relative to that directory.
#
# WHY AN ISOLATED VIRTUAL ENVIRONMENT
# -----------------------------------
# dbt-core and the Snowflake connector are installed into /opt/pipeline_venv,
# NOT into Airflow's own Python environment. Airflow and dbt-core both pin
# Jinja2 and click to overlapping-but-different ranges, and installing them
# together is the classic way to end up with an image that will not resolve.
#
# Two extra lines buy complete isolation. The DAG calls the interpreters in this
# venv directly, so Airflow's environment stays exactly as the base image ships it.

# 1. Base Airflow image. Python 3.12 matches the dependency set already proven
#    in the project's local development virtual environment.
FROM apache/airflow:2.11.2-python3.12

# 2. Create the venv directory. Root is needed because it lives outside
#    Airflow's home, then ownership is handed to the airflow user.
USER root
RUN mkdir -p /opt/pipeline_venv && chown airflow: /opt/pipeline_venv
USER airflow

# 3. Copy the pipeline's requirements file into the image.
COPY requirements.txt /tmp/pipeline-requirements.txt

# 4. Build the isolated virtual environment and install the dependencies.
RUN python -m venv /opt/pipeline_venv \
 && /opt/pipeline_venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/pipeline_venv/bin/pip install --no-cache-dir -r /tmp/pipeline-requirements.txt

# 5. Smoke-test the runtime so a broken image fails at build time, not at DAG run time.
RUN /opt/pipeline_venv/bin/dbt --version \
 && /opt/pipeline_venv/bin/python -c "import snowflake.connector, faker; print('pipeline venv OK')"
