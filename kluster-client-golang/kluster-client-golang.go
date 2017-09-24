package main

import (
	"log"
	"os"
	"github.com/Shopify/sarama"
	"github.com/toefel18/kluster/kluster-client-golang/kluster"
)

func main() {
	log.Println("Starting a kluster client - golang")

	kafkaBootstrapServers := mustHaveEnvironment("KAFKA_BOOTSTRAP_SERVERS")
	kafkaMutationTopic := mustHaveEnvironment("KAFKA_MUTATION_TOPIC")
	kafkaResponseTopic := mustHaveEnvironment("KAFKA_RESPONSE_TOPIC")

	kluster.NewFutureResult()

}

func mustHaveEnvironment(name string) string {
	val, set := os.LookupEnv(name)
	if !set {
		log.Fatalf("could not find required environment variable %v", name)
	}
	return val
}
