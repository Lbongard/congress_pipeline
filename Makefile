airflow-up:
	cd ./airflow && \
	docker compose build && \
	docker compose up airflow-init && \
	docker compose up -d && \
	cd ../

trigger-dags:
	cd ./airflow && \
	docker exec airflow-airflow-triggerer-1 airflow dags trigger upload_congress_data && \
	cd ../

airflow-reset:
	cd ./airflow && \
	docker compose down && \
	docker volume rm airflow_postgres-db-volume && \
	cd ../ || \
	cd ./airflow && \
	docker volume rm airflow_postgres-db-volume && \
	cd ../

set-gcp-project:
	@echo "GCP_PROJECT=$(GCP_PROJECT)" >> airflow/.env
	@echo "GCP_PROJECT=$(GCP_PROJECT)" >> dbt/.env
	@echo "GCP_PROJECT=$(GCP_PROJECT)" >> dbt/models/staging/.env
	@echo "GCP_PROJECT set to $(GCP_PROJECT)."

set-congress-api-key:
	@echo "congress_api_key=$(congress-api-key)" >> airflow/.env
	@echo "Congress API Key set to $(congress-api-key)."

install-dbt:
	brew untap dbt-labs/dbt
	brew tap dbt-labs/dbt-cli
	brew install dbt