package main

import (
	"database/sql"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	_ "github.com/lib/pq"
	"github.com/satori/go.uuid"
)

func main() {
	log.Println("Starting Kafka -> Postgres adapter")
	pgConnectionString := mustHaveEnvironment("DATABASE_ADDRESS")
	kafkaBootstrapServers := mustHaveEnvironment("KAFKA_BOOTSTRAP_SERVERS")
	kafkaMutationTopic := mustHaveEnvironment("KAFKA_MUTATION_TOPIC")
	kafkaResponseTopic := mustHaveEnvironment("KAFKA_RESPONSE_TOPIC")

	db, err := sql.Open("postgres", pgConnectionString)
	if err != nil {
		log.Fatalf("Error connecting to postgres: %v", err.Error())
	}
	log.Printf("Connected to DB %v", pgConnectionString)

	if err := consumeMutations(kafkaBootstrapServers, kafkaMutationTopic, kafkaResponseTopic, db); err != nil {
		log.Fatalf("Error with Kafka %v", err.Error())
	}

}

func consumeMutations(bootstrapServers string, mutationTopic string, responseTopic string, db *sql.DB) error {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	brokers := []string{bootstrapServers}
	mutationConsumer, err := cluster.NewConsumer(brokers, uuid.NewV4().String(), []string{mutationTopic}, config)
	if err != nil {
		return err
	}
	defer mutationConsumer.Close()

	programInterrupted := make(chan os.Signal, 1)
	signal.Notify(programInterrupted, os.Interrupt)

	log.Println("Listening for kafka messages")

	for {
		select {
		case msg, ok := <-mutationConsumer.Messages():
			if ok {
				log.Printf("Received kafka message, key=%v val=%v", string(msg.Key), string(msg.Value))
				executeDatabaseMsg(msg, db)
			}
		case msg, ok := <-mutationConsumer.Notifications():
			if ok {
				log.Printf("Received kafka balancing notification claimed=%v, released=%v, current=%v", msg.Claimed, msg.Released, msg.Current)
			}
		case err, ok := <-mutationConsumer.Errors():
			if ok {
				log.Printf("Received kafka error %v", err.Error())
			}
		case <-programInterrupted:
			return nil
		}
	}
}

func executeDatabaseMsg(message *sarama.ConsumerMessage, db *sql.DB) {
	query := string(message.Value) //TODO parse as a transaction of 1..n messages
	_, err := db.Exec(query)
	if err != nil {
		log.Printf("Error while executing query %v, error: %v", query, err.Error())
	}
	// TODO return result on response topic
	log.Printf("Successfully executed %v, result ", query)
}

func mustHaveEnvironment(name string) string {
	val, set := os.LookupEnv(name)
	if !set {
		log.Fatalf("could not find required environment variable %v", name)
	}
	return val
}
