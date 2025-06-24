all:
	docker compose up -d

re:
	docker compose down
	docker compose up --build -d

down:
	docker compose down

clean:
	docker compose down --rmi all
	docker volume rm $$(docker volume ls -q)

airflow:
	docker exec -it project2-airflow-apiserver-1 bash

kafka:
	docker exec -it kafka bash