airflow-up:
	cd ./airflow && docker compose build && docker compose up airflow-init && docker compose up -d && $(MAKE) trigger-dags && cd ../

trigger-dags:
	docker exec airflow-airflow-triggerer-1 airflow dags trigger upload_congress_data && cd ../

airflow-reset:
	cd ./airflow && docker compose down && docker volume rm airflow_postgres-db-volume && cd ../ || cd ./airflow && docker volume rm airflow_postgres-db-volume && cd ../

set-gcp-project:
    @echo "GCP_PROJECT=$(PROJECT_ID)" > airflow/.env
    @echo "GCP_PROJECT set to $(PROJECT_ID) in airflow .env file."

set-congress-api-key:
	@echo "congress_api_key=$(API_KEY)" > airflow/.env
    @echo "Congress API Key set to $(API_KEY) in airflow .env file."