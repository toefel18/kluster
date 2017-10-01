package main

import (
	"log"
	"os"
	"github.com/toefel18/kluster/kluster-client-golang/kluster"
	"time"
)

func main() {
	log.Println("Starting a kluster client - golang")

	kafkaBootstrapServers := mustHaveEnvironment("KAFKA_BOOTSTRAP_SERVERS")
	kafkaMutationTopic := mustHaveEnvironment("KAFKA_MUTATION_TOPIC")
	kafkaResponseTopic := mustHaveEnvironment("KAFKA_RESPONSE_TOPIC")

	log.Println("creating kafka client")

	client := kluster.NewKafkaClient(kafkaBootstrapServers, kafkaMutationTopic, kafkaResponseTopic)

	log.Println("client created")
	defer client.Close()

	for ; ; {
		queryPromise := client.Exec("SELECT 2+2 as Count", 5*time.Second)
		_, err := queryPromise.WaitForSingle()
		if err != nil {
			log.Fatalf("error while executing query %v", err.Error())
		}
		time.Sleep(5*time.Second)
	}

	log.Println("Exiting")
}

func mustHaveEnvironment(name string) string {
	val, set := os.LookupEnv(name)
	if !set {
		log.Fatalf("could not find required environment variable %v", name)
	}
	return val
}
