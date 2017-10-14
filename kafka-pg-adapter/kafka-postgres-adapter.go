package main

import (
	"database/sql"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	_ "github.com/lib/pq"
	"time"
	"fmt"
)

func main() {
	log.Println("Starting Kafka -> Postgres adapter, waiting 10 seconds for kafka and postgres to init")
	time.Sleep(10 * time.Second)
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
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Flush.Frequency = 1 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	brokers := []string{bootstrapServers}
	mutationConsumer, err := cluster.NewConsumer(brokers, "kluster-pg-adapter-"  + time.Now().Format(time.RFC3339), []string{mutationTopic}, config)
	if err != nil {
		return err
	}
	defer mutationConsumer.Close()

	producer, err := sarama.NewSyncProducer(brokers, &config.Config)
	if err != nil {
		log.Fatalf("Error creating kafka producer %v", err.Error())
	}

	programInterrupted := make(chan os.Signal, 1)
	signal.Notify(programInterrupted, os.Interrupt)

	log.Println("Listening for kafka messages")

	for {
		select {
		case msg, ok := <-mutationConsumer.Messages():
			if ok {
				log.Printf("Received kafka message, key=%v val=%v", string(msg.Key), string(msg.Value))
				executeDatabaseMsg(msg, db, producer, responseTopic)
				mutationConsumer.CommitOffsets()
			} else {
				log.Printf("incoming message channel closed, kafka consumer must be stopped")
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
			log.Printf("interrupt received, not consuming messages anymore")
			return nil
		}
	}
}

func executeDatabaseMsg(message *sarama.ConsumerMessage, db *sql.DB, producer sarama.SyncProducer, responseTopic string) {
	query := string(message.Value) //TODO parse as a transaction of 1..n messages
	res, err := db.Exec(query)
	var result string
	if err != nil {
		result = fmt.Sprintf("Error while executing query %v, error: %v", query, err.Error())
	} else {
		rows, _ := res.RowsAffected()
		result = fmt.Sprintf("Successfully executed query, rowsAffected=%v ", rows)
	}
	msg := &sarama.ProducerMessage{
		Topic: responseTopic,
		Key:   sarama.StringEncoder(message.Key),
		Value: sarama.StringEncoder(result),
	}
	producer.SendMessage(msg)
	log.Printf("Send result message: %v", result)
}

func mustHaveEnvironment(name string) string {
	val, set := os.LookupEnv(name)
	if !set {
		log.Fatalf("could not find required environment variable %v", name)
	}
	return val
}
