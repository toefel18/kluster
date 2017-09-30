package kluster

import (
	"time"

	"log"
	"strings"

	"strconv"

	"sync"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/oklog/ulid"
	"os/signal"
	"os"
)

type Client interface {
	Exec(stmtQuery string, expireIn time.Duration) FutureResult
	Close()
}

type Result interface {
	Success() bool
	RowsAffected() int64
	Rows() [][]interface{}
}

type FutureResult interface {
	// blocks until result is back or timed out
	WaitForSingle() (Result, error)
	// receive the on a separate channel, channel is closed if result is not available.
	ResultChannel() chan Result
}

type kafkaClient struct {
	mutationTopic string
	producer      sarama.SyncProducer
	resultTracker *kafkaResultTracker
}

type kafkaResultTracker struct {
	//syncs resultsToTrack modifications TODO make a channel?
	mutex *sync.Mutex
	//map of correlation-id to futureResult instances
	resultsToTrack map[string]*futureResult
	//set of keys (correlation-ids) that can be dropped because they are finished (only the first of n postgres results will be used for now)
	finished map[string]time.Time
	consumer *cluster.Consumer
}

type futureResult struct {
	// the query id, for internal use only
	queryId string
	// the latch indicating the result is ready, or closed on expiry
	resultReady chan Result
	// the timeout period
	expireIn time.Duration
	// the calculated time of expiry at the start of the request.
	expireAt time.Time
}

type result struct {
	success      bool
	raw          string
	rowsAffected int64
	rows         [][]interface{}
}

type TimeoutError struct {
	CorrelationId string
	TimedOutAt    time.Time
	TimedOutAfter time.Duration
}

func (t TimeoutError) Error() string {
	return "request with correlation id " + t.CorrelationId + " timed out at " + t.TimedOutAt.String() + ", after waiting " + t.TimedOutAfter.String()
}

func NewKafkaClient(bootstrapServers, mutationTopic, responseTopic string) Client {
	servers := strings.Split(bootstrapServers, ",")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 5
	config.Producer.Flush.Frequency = 1 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(servers, config)
	if err != nil {
		log.Fatalf("[kafkaClient] Error creating kafka producer %v", err.Error())
	}

	//consumers require cluster configuration due to consumer groups and rebalancing algo's
	clusterConfig := cluster.NewConfig()
	clusterConfig.Config = *config
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true

	consumerGroup := strconv.FormatUint(ulid.Now(), 10)
	consumer, err := cluster.NewConsumer(servers, consumerGroup, []string{responseTopic}, clusterConfig)
	if err != nil {
		log.Fatalf("[kafkaClient] Error creating kafka consumer %v", err.Error())
	}

	log.Println("[kafkaClient] Listening for kafka messages")

	resultTracker := &kafkaResultTracker{
		&sync.Mutex{},
		make(map[string]*futureResult),
		make(map[string]time.Time),
		consumer,
	}

	go resultTracker.consumeResults()

	return &kafkaClient{mutationTopic, producer, resultTracker}
}

func (c *kafkaClient) Exec(stmtQuery string, expireIn time.Duration) FutureResult {
	queryId := strconv.FormatUint(ulid.Now(), 10)
	msg := &sarama.ProducerMessage{
		Topic: c.mutationTopic,
		Key:   sarama.StringEncoder(queryId),
		Value: sarama.StringEncoder(stmtQuery),
	}
	future := c.resultTracker.track(queryId, expireIn)
	c.producer.SendMessage(msg)
	log.Printf("[kafkaClient] sent query for execution with id %v: %v", queryId, stmtQuery)
	return future
}

func (c *kafkaClient) Close() {
	c.producer.Close()
	c.resultTracker.consumer.Close()
}

func (c *kafkaResultTracker) track(queryId string, expireIn time.Duration) FutureResult {
	futureResult := &futureResult{
		queryId:     queryId,
		resultReady: make(chan Result),
		expireIn:    expireIn,
		expireAt:    time.Now().Add(expireIn),
	}
	c.mutex.Lock()
	c.resultsToTrack[queryId] = futureResult //TODO maybe post this via a channel depending on the worker loop implemenation
	c.mutex.Unlock()
	return futureResult
}

func (c *kafkaResultTracker) consumeResults() {
	programInterrupted := make(chan os.Signal, 1)
	signal.Notify(programInterrupted, os.Interrupt)

	log.Println("[kafkaResultTracker] Consuming query results from kafka")

	for {
		select {
		case msg, ok := <-c.consumer.Messages():
			if ok {
				log.Printf("[kafkaResultTracker] Received result message, key=%v val=%v", string(msg.Key), string(msg.Value))
				c.processResult(msg)
			} else {
				log.Printf("[kafkaResultTracker] incoming result channel closed, kafka consumer must be stopped")
				return
			}
		case err, ok := <-c.consumer.Errors():
			if ok {
				log.Printf("[kafkaResultTracker] Received kafka error %v", err.Error())
			}
		case <-programInterrupted:
			log.Printf("[kafkaResultTracker] interrupt received, not consuming results anymore")
			c.consumer.Close()
		}
	}
}

func (c *kafkaResultTracker) processResult(message *sarama.ConsumerMessage) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if finishTime, alreadyFinished := c.finished[string(message.Key)]; alreadyFinished {
		log.Printf("[kafkaResultTracker] received result for query with id %v, was query was already finished at %v", string(message.Key), finishTime.Format(time.RFC3339))
		return
	}
	if trackFuture, found := c.resultsToTrack[string(message.Key)]; found {
		log.Printf("[kafkaResultTracker] received result for query with id %v, completing result!", string(message.Key))
		// complete the future by sending a result on the channel
		trackFuture.resultReady <- &result{true, string(message.Value), 0, make([][]interface{}, 0)}
		// mark as finished
		c.finished[string(message.Key)] = time.Now()
		delete(c.resultsToTrack, string(message.Key))
		return
	} else {
		log.Printf("[kafkaResultTracker] received result for query with id %v, but this result has no tracker, dropping value raw: %v ", string(message.Key), string(message.Value))
	}
}

func (r *result) Success() bool {
	return r.success
}

func (r *result) RowsAffected() int64 {
	return r.rowsAffected
}

func (r *result) Rows() [][]interface{} {
	return r.rows
}

// will be called by clients
func (fr *futureResult) WaitForSingle() (Result, error) {
	log.Printf("[futureResult] Waiting sync for query %v to return", fr.queryId)
	timeout := time.NewTicker(fr.expireAt.Sub(time.Now()))

	select {
	case result := <- fr.ResultChannel():
		log.Printf("[futureResult] Query %v has produced a result %v", fr.queryId, result)
		timeout.Stop()
		return result, nil
	case <-timeout.C:
		log.Printf("[futureResult] Query %v has timed out", fr.queryId)
		return nil, TimeoutError{fr.queryId, fr.expireAt, fr.expireIn}
	}

}

func (fr *futureResult) ResultChannel() chan Result {
	return fr.resultReady
}
