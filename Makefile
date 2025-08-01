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

super_clean: clean
	docker system prune -a

airflow:
	docker exec -it api-server bash

kafka:
	docker exec -it kafka bash

spark:
	docker exec -it spark-master bash

psql:
	docker exec -it postgres bash

build:
	docker compose up -d --build

.PHONY: all re down clean airflow kafka spark psql
