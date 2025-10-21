run-airflow:
	docker-compose -f docker/docker-compose.yml up --build

stop:
	docker-compose -f docker/docker-compose.yml down

clean:
	rm -rf data/staging/* data/curated/*

run-etl:
	python src/etl/test_etl.py

run-app:
	streamlit run src/app/main.py
