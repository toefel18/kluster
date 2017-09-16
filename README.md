# kluster
cluster of N postgres instances using Kafka as an in-sync mechanism


Spin up kafka and postgres
```sh
docker run --name kluster-kafka -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 --env TOPICS=kluster-mutation,kluster-response spotify/kafka
            
docker run --name kluster-postgres-1 -d -p 20000:5432 -e POSTGRES_USER=kluster -e POSTGRES_PASSWORD=kluster -d postgres
docker run --name kluster-postgres-2 -d -p 20001:5432 -e POSTGRES_USER=kluster -e POSTGRES_PASSWORD=kluster -d postgres


```
