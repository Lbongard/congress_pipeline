airflow-up:
	cd ./airflow && docker compose build && docker compose up airflow-init && docker compose up -d && cd ../

trigger-dags:
	cd ./airflow && docker exec -it airflow-airflow-triggerer1 bash && docker airflow dags trigger upload_congress_data && exit && cd ../

airflow-reset:
	cd ./airflow && docker compose down && docker volume rm airflow_postgres-db-volume && cd ../ || cd ./airflow && docker volume rm airflow_postgres-db-volume && cd ../