init-airflow: # Initialize Airflow database and create admin user
	docker-compose -f docker/docker-compose.yml up airflow-init

run-airflow: # Start Airflow services
	docker-compose -f docker/docker-compose.yml up -d

stop: # Stop Airflow services
	docker-compose -f docker/docker-compose.yml down

stop-with-volumes: # Stop Airflow services and remove volumes
	docker-compose -f docker/docker-compose.yml down -v

clean: # Remove staging and curated data
	rm -rf data/raw/* data/staging/* data/curated/* logs/* __pycache__

run-etl: # Run the ETL process
	python src/etl/test_etl.py

run-app: # Run the Streamlit application 
	streamlit run src/app/main.py

check-airflow: # Check the status of Airflow containers
	docker ps --filter "name=atay" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
