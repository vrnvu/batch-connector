.PHONY = help postgres it

.DEFAULT_GOAL = help

help:
	@echo "---------------HELP-----------------"
	@echo "postgres: run docker container"
	@echo "it: shell into postgres container and db"
	@echo "------------------------------------"

postgres:
	docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=dbname -p 5432:5432 --name postgres postgres:12.3-alpine
it:
	docker exec -it postgres psql -U postgres -d dbname
