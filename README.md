# kluster
cluster of N postgres instances using Kafka as a mechanism to keep the instances in-sync.

Spin up kafka and postgres
```sh
docker run --name kluster-kafka -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 --env TOPICS=kluster-mutation,kluster-response spotify/kafka
            
docker run --name kluster-postgres-1 -d -p 20000:5432 -e POSTGRES_USER=kluster -e POSTGRES_PASSWORD=kluster -d postgres
docker run --name kluster-postgres-2 -d -p 20001:5432 -e POSTGRES_USER=kluster -e POSTGRES_PASSWORD=kluster -d postgres
```

All writes are published as queries on a Kafka topic with one partition. 
Daemons read from this topic, execute the queries and write the response to the
response topic, using a correlation ID provided.

TODO:
 * provide docker compose file that spins it all up.
 * allow messages to be a set of statements forming a transaction
 * implement a client that listens on the response topic and requires at least 1 result, or a quorum of x results to be sure.
 * analyse the consequences of allowing multiple write partitions, but force each producer to produce to the same partition during it's runtime.
  