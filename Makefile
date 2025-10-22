init-airflow:
	docker-compose -f docker/docker-compose.yml up airflow-init

run-airflow:
	docker-compose -f docker/docker-compose.yml up -d

stop:
	docker-compose -f docker/docker-compose.yml down

stop-with-volumes:
	docker-compose -f docker/docker-compose.yml down -v

clean:
	rm -rf data/staging/* data/curated/*

run-etl:
	python src/etl/test_etl.py

run-app:
	streamlit run src/app/main.py

check-airflow:
	docker ps --filter "name=atay" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
