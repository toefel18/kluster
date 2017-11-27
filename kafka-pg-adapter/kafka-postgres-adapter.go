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
	"github.com/satori/go.uuid"
	"sync"
	"container/list"
)

func main() {
	log.Println("Starting Kafka -> Postgres adapter, waiting 10 seconds for kafka and postgres to init")
	time.Sleep(30 * time.Second)
	pgConnectionString := mustHaveEnvironment("DATABASE_ADDRESS")
	kafkaBootstrapServers := mustHaveEnvironment("KAFKA_BOOTSTRAP_SERVERS")
	kafkaOneTopic := mustHaveEnvironment("KAFKA_ONE_TOPIC")
	kafkaAllTopic := mustHaveEnvironment("KAFKA_ALL_TOPIC")
	kafkaResponseTopic := mustHaveEnvironment("KAFKA_RESPONSE_TOPIC")

	log.Printf("postgres connection string %v", pgConnectionString)

	db, err := sql.Open("postgres", pgConnectionString)
	if err != nil {
		log.Fatalf("Error connecting to postgres: %v", err.Error())
	}

	if res, err := db.Query("SELECT 2+2 as Count"); err != nil {
		log.Fatalf("Error executing query on postgres: %v", err.Error())
	} else {
		res.Close()
	}

	log.Printf("Connected to DB %v", pgConnectionString)

	var buffer *MessageBuffer

	go startConsuming(kafkaBootstrapServers, kafkaOneTopic, buffer, db, kafkaResponseTopic)
	go startConsuming(kafkaBootstrapServers, kafkaAllTopic, buffer, db, kafkaResponseTopic)
}

func startConsuming(bootstrapServers string, topic string, buffer *MessageBuffer, db *sql.DB, responseTopic string) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Flush.Frequency = 1 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	consumerGroup := fmt.Sprintf("kluster-pg-adapter-%v", uuid.NewV4())

	brokers := []string{bootstrapServers}
	consumer, err := cluster.NewConsumer(brokers, consumerGroup, []string{topic}, config)
	if err != nil {
		//qqqq fatal
		return
	}
	defer consumer.Close()

	producer, err := sarama.NewSyncProducer(brokers, &config.Config)
	if err != nil {
		log.Fatalf("Error creating kafka producer %v", err.Error())
	}

	// trap SIGINT to trigger a shutdown
	programInterrupted := make(chan os.Signal, 1)
	signal.Notify(programInterrupted, os.Interrupt)

	log.Println("Listening for kafka messages, consumerGroupId=" + consumerGroup)

	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				log.Printf("Received kafka message, key=%v val=%v", string(msg.Key), string(msg.Value))
				executeDatabaseMsg(consumer, buffer, msg, db, producer, responseTopic)
				//qqqq niet hier consumer.CommitOffsets()
			} else {
				log.Printf("Inbound message channel closed, kafka consumer must be stopped")
			}
		case msg, ok := <-consumer.Notifications():
			if ok {
				log.Printf("Received kafka balancing notification claimed=%v, released=%v, current=%v", msg.Claimed, msg.Released, msg.Current)
			}
		case err, ok := <-consumer.Errors():
			if ok {
				log.Printf("Received kafka error %v", err.Error())
			}
		case <-programInterrupted:
			log.Printf("Interrupt received, not consuming messages anymore")
			return //qqqq
		}
	}
}

func executeDatabaseMsg(consumer *cluster.Consumer, buffer *MessageBuffer, message *sarama.ConsumerMessage, db *sql.DB, producer sarama.SyncProducer, responseTopic string) {
	buffer.mux.Lock()
	defer buffer.mux.Unlock()

	/*qqqq
	allRequest = read request from ALL topic
	IF (allRequest is a CUD request) { // CUD = Create, Update or Delete, aka mutation aka write
	do mutation on local store
	IF (same request arrives from ONE topic within timeoutPeriod) { // same means allRequest.messageId == oneRequest.messageId
	send processing response to RESPONSE topic
	}
	} ELSE { // R request, R = Read
		IF (same request arrives from ONE topic within timeoutPeriod) {
		do query on local store
		send processing response to RESPONSE topic
	}
	}
	*/
	deleteExpiredReadMessage(buffer)

	msgId := string(message.Key)
	if _, ok := buffer.messageInfos[msgId]; ok {
		// received same message for 2d time
		if frontMsg := buffer.messages.Front(); string(frontMsg.Value.(*sarama.ConsumerMessage).Value) == msgId {
			// it is the 1st to be processed msgId
			buffer.messages.Remove(frontMsg)
			delete(buffer.messageInfos, msgId)
			doExecuteDatabaseMsg(consumer, message, db, producer, responseTopic)

			//qqqq recursive next msg
			nextElem := frontMsg.Next()
			if nextElem != nil {
				nextMsg := nextElem.Value.(*sarama.ConsumerMessage)
				if nextInfo, ok := buffer.messageInfos[string(nextMsg.Key)]; nextInfo.ReceiveCount > 1 {
					if !ok {
						//qqqq kan niet?
					}
					executeDatabaseMsg(consumer, buffer, nextMsg, db, producer, responseTopic)
				}
			}
		} else {
			//qqqq it is the 2d time received maar nog niet aan de beurt
			info := buffer.messageInfos[msgId]
			info.ReceiveCount++
		}
	} else {
		// received message for the 1st time
		buffer.messages.PushBack(message)
		buffer.messageInfos[msgId] = MessageInfo{Id: msgId, TimeStamp: message.Timestamp, ReceiveCount: 1}
	}
}

func doExecuteDatabaseMsg(consumer *cluster.Consumer, message *sarama.ConsumerMessage, db *sql.DB, producer sarama.SyncProducer, responseTopic string) {
	query := string(message.Value) //TODO parse as a transaction of 1..n messages
	res, err := db.Exec(query)
	var result string
	if err != nil { //qqqq retry infinitely if db not connected
		result = fmt.Sprintf("Error while executing query %v, error: %v", query, err.Error())
	} else {
		rows, _ := res.RowsAffected()
		result = fmt.Sprintf("Successfully executed query, rowsAffected=%v ", rows)
		consumer.CommitOffsets()
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

type MessageBuffer struct {
	messageInfos map[string]MessageInfo
	messages *list.List
	mux sync.Mutex
}

type MessageInfo struct {
	Id string
	TimeStamp time.Time
	ReceiveCount int
}

func deleteExpiredReadMessage(buffer *MessageBuffer ) {
    //qqqq
}