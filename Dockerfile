FROM quay.io/astronomer/astro-runtime:12.8.0

ENV AIRFLOW_VAR_MY_DAG_PARTNER_1='{"name":"partner_a","api_secret":"mysecret","path":"/tmp/partner_a"}'
