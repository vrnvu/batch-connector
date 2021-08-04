# batch-connector

## Getting started


Run a local docker container with psql:

```
docker run -d --rm \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=dbname \
  -p 5432:5432 --name postgres postgres:12.3-alpine
```

Connect to the running container and create a table manually:

```
docker exec -it postgres psql -U postgres -d dbname
```
