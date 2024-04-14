# Start Airflow webserver and scheduler
airflow webserver --daemon &
airflow scheduler --daemon &

# Trigger the DAG run
airflow dags trigger -r upload_congress_data

# Keep the container running
exec "$@"
