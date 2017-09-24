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
	WaitForSingleSync() (Result, error)
	// receive the on a separate channel, channel is closed if result is not available.
	WaitForSingleAsync() chan Result
}

type kafkaClient struct {
	mutationTopic string
	producer      sarama.SyncProducer
	resultTracker *kafkaResultTracker
}

type kafkaResultTracker struct {
	//syncs resultsToTrack modifications TODO make a channel?
	mutex sync.Mutex
	//map of correlation-id to futureResult instances
	resultsToTrack map[string]*futureResult
	//set of keys (correlation-ids) that can be dropped because they are finished (only the first of n postgres results will be used for now)
	finished map[string]struct{}
	consumer *cluster.Consumer
}

type futureResult struct {
	// the correlation id, for internal use only
	correlationId string
	// the latch indicating the result is ready, or closed on expiry
	resultReady chan Result
	// the timeout period
	expireIn time.Duration
	// the calculated time of expiry at the start of the request.
	expireAt time.Time
}

type result struct {
	success      bool
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
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(servers, config)
	if err != nil {
		log.Fatalf("Error creating kafka producer %v", err.Error())
	}

	//consumers require cluster configuration due to consumer groups and rebalancing algo's
	clusterConfig := cluster.NewConfig()
	clusterConfig.Config = *config
	consumerGroup := strconv.FormatUint(ulid.Now(), 10)
	consumer, err := cluster.NewConsumer(servers, consumerGroup, []string{responseTopic}, clusterConfig)
	if err != nil {
		log.Fatalf("Error creating kafka consumer %v", err.Error())
	}

	return &kafkaClient{mutationTopic, producer, &kafkaResultTracker{consumer}}
}

func (c *kafkaClient) Exec(stmtQuery string, expireIn time.Duration) FutureResult {
	correlationId := strconv.FormatUint(ulid.Now(), 10)
	msg := &sarama.ProducerMessage{
		Topic: c.mutationTopic,
		Key:   sarama.StringEncoder(correlationId),
		Value: sarama.StringEncoder(stmtQuery),
	}
	c.producer.SendMessage(msg)
	return c.resultTracker.track(correlationId, expireIn)
}

func (c *kafkaClient) Close() {
	c.producer.Close()
	c.resultTracker.consumer.Close()
}

func (c *kafkaResultTracker) track(correlationId string, expireIn time.Duration) FutureResult {
	futureResult := &futureResult{
		correlationId: correlationId,
		resultReady:   make(chan Result),
		expireIn:      expireIn,
		expireAt:      time.Now().Add(expireIn),
	}
	c.mutex.Lock()
	c.resultsToTrack[correlationId] = futureResult //TODO maybe post this via a channel depending on the worker loop implemenation
	c.mutex.Unlock()
	return futureResult
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
func (fr *futureResult) WaitForSingleSync() (Result, error) {
	if result, ok := <-fr.WaitForSingleAsync(); ok {
		return result, nil
	} else {
		return nil, TimeoutError{fr.correlationId, fr.expireAt, fr.expireIn}
	}
}

func (fr *futureResult) WaitForSingleAsync() chan Result {
	return fr.resultReady
}
