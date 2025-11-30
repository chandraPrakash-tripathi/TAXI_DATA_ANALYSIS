FROM astrocrpublic.azurecr.io/runtime:3.1-5

RUN python -m venv venv && source venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate