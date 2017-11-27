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
	kafkaAllTopic := mustHaveEnvironment("KAFKA_ALL_TOPIC")
	kafkaOneTopic := mustHaveEnvironment("KAFKA_ONE_TOPIC")
	kafkaResponseTopic := mustHaveEnvironment("KAFKA_RESPONSE_TOPIC")

	log.Println("creating kafka client")

	client := kluster.NewKafkaClient(kafkaBootstrapServers, kafkaAllTopic, kafkaOneTopic, kafkaResponseTopic)

	log.Println("client created")
	defer client.Close()

	for ; ; {
		allQueryPromise, oneQueryPromise, err := client.Exec("SELECT 2+2 as Count", 5*time.Second)
		if err != nil {
			log.Fatalf("error while executing query %v", err.Error())
		}
		_, errAll := allQueryPromise.WaitForSingle()
		_, errOne := oneQueryPromise.WaitForSingle()
		if errAll != nil || errOne != nil {
			log.Fatalf("error while executing query all=%v, one=%v", errAll.Error(), errOne.Error())
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
