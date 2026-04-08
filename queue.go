package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/goravel/framework/contracts/config"
	contractsfoundation "github.com/goravel/framework/contracts/foundation"
	contractsqueue "github.com/goravel/framework/contracts/queue"
	"github.com/goravel/framework/errors"
	"github.com/goravel/framework/queue/utils"
	"github.com/goravel/framework/support/color"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	_ contractsqueue.Driver            = &Queue{}
	_ contractsqueue.DriverWithReceive = &Queue{}
)

type Queue struct {
	ctx       context.Context
	cancel    context.CancelFunc
	producer  *kgo.Client
	jobStorer contractsqueue.JobStorer
	json      contractsfoundation.Json
	topicKey  *TopicKey
	baseOpts  []kgo.Opt

	consumers sync.Map // map[string]*consumerState
}

type consumerState struct {
	client   *kgo.Client
	migrator *delayMigrator
}

func NewQueue(ctx context.Context, config config.Config, queue contractsqueue.Queue, json contractsfoundation.Json, connection string) (*Queue, error) {
	clientConnection := config.GetString(fmt.Sprintf("queue.connections.%s.connection", connection), "default")

	producer, err := GetProducer(config, clientConnection)
	if err != nil {
		return nil, fmt.Errorf("failed to init kafka producer: %w", err)
	}

	baseOpts, err := BuildBaseOpts(config, clientConnection)
	if err != nil {
		return nil, fmt.Errorf("failed to build kafka opts: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	return &Queue{
		ctx:       ctx,
		cancel:    cancel,
		producer:  producer,
		jobStorer: queue.JobStorer(),
		json:      json,
		topicKey:  NewTopicKey(config.GetString("app.name", "goravel"), connection),
		baseOpts:  baseOpts,
	}, nil
}

func (q *Queue) Driver() string {
	return contractsqueue.DriverCustom
}

func (q *Queue) Push(task contractsqueue.Task, queue string) error {
	if !task.Delay.IsZero() {
		return q.later(task.Delay, task, queue)
	}

	payload, err := utils.TaskToJson(task, q.json)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: q.topicKey.Topic(queue),
		Key:   []byte(task.UUID),
		Value: []byte(payload),
	}

	return q.producer.ProduceSync(q.ctx, record).FirstErr()
}

func (q *Queue) Pop(queue string) (contractsqueue.ReservedJob, error) {
	ctx, cancel := context.WithTimeout(q.ctx, 100*time.Millisecond)
	defer cancel()

	jobs, err := q.Receive(ctx, queue, 1)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, errors.QueueDriverNoJobFound.Args(q.topicKey.Topic(queue))
	}

	return jobs[0], nil
}

func (q *Queue) Receive(ctx context.Context, queue string, count int) ([]contractsqueue.ReservedJob, error) {
	cs := q.getOrCreateConsumer(queue)
	if cs.client == nil {
		return nil, nil
	}

	cs.migrator.start()

	fetches := cs.client.PollRecords(ctx, count)
	cs.client.AllowRebalance()

	for _, err := range fetches.Errors() {
		if !errors.Is(err.Err, context.DeadlineExceeded) && !errors.Is(err.Err, context.Canceled) {
			return nil, err.Err
		}
	}

	records := fetches.Records()
	if len(records) == 0 {
		return nil, nil
	}

	jobs := make([]contractsqueue.ReservedJob, 0, len(records))
	for _, record := range records {
		rj, err := NewReservedJob(cs.client, record, q.jobStorer, q.json)
		if err != nil {
			q.sendToDLQ(ctx, record, err)
			if commitErr := cs.client.CommitRecords(ctx, record); commitErr != nil {
				color.Warningf("Failed to commit skipped record: %s\n", commitErr)
			}
			continue
		}
		jobs = append(jobs, rj)
	}

	return jobs, nil
}

func (q *Queue) Close() error {
	q.cancel()

	q.consumers.Range(func(key, value any) bool {
		cs := value.(*consumerState)
		cs.migrator.stop()
		cs.client.Close()
		return true
	})

	return nil
}

func (q *Queue) later(delay time.Time, task contractsqueue.Task, queue string) error {
	task.Delay = time.Time{}
	payload, err := utils.TaskToJson(task, q.json)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: q.topicKey.DelayTopic(queue),
		Key:   []byte(task.UUID),
		Value: []byte(payload),
		Headers: []kgo.RecordHeader{
			{Key: delayHeaderKey, Value: []byte(strconv.FormatInt(delay.Unix(), 10))},
		},
	}

	return q.producer.ProduceSync(q.ctx, record).FirstErr()
}

func (q *Queue) getOrCreateConsumer(queueName string) *consumerState {
	if cs, ok := q.consumers.Load(queueName); ok {
		return cs.(*consumerState)
	}

	opts := make([]kgo.Opt, len(q.baseOpts))
	copy(opts, q.baseOpts)
	opts = append(opts,
		kgo.ConsumerGroup(q.topicKey.GroupID(queueName)),
		kgo.ConsumeTopics(q.topicKey.Topic(queueName)),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		color.Warningf("Failed to create kafka consumer for queue [%s]: %s\n", queueName, err)
		cs := &consumerState{}
		q.consumers.Store(queueName, cs)
		return cs
	}

	migrator := newDelayMigrator(q.ctx, q.producer, q.topicKey, queueName, q.baseOpts)
	cs := &consumerState{client: client, migrator: migrator}
	q.consumers.Store(queueName, cs)

	return cs
}

func (q *Queue) sendToDLQ(ctx context.Context, record *kgo.Record, reason error) {
	dlqRecord := &kgo.Record{
		Topic: q.topicKey.DLQTopic(record.Topic),
		Key:   record.Key,
		Value: record.Value,
		Headers: append(record.Headers,
			kgo.RecordHeader{Key: "x-dlq-reason", Value: []byte(reason.Error())},
			kgo.RecordHeader{Key: "x-original-topic", Value: []byte(record.Topic)},
		),
	}
	if err := q.producer.ProduceSync(ctx, dlqRecord).FirstErr(); err != nil {
		color.Warningf("Failed to send to DLQ: %s\n", err)
	}
}

// TopicKey manages Kafka topic naming conventions.
type TopicKey struct {
	appName    string
	connection string
}

func NewTopicKey(appName string, connection string) *TopicKey {
	return &TopicKey{
		appName:    appName,
		connection: connection,
	}
}

func (t *TopicKey) Topic(queue string) string {
	return fmt.Sprintf("%s_queues_%s_%s", t.appName, t.connection, queue)
}

func (t *TopicKey) DelayTopic(queue string) string {
	return fmt.Sprintf("%s_delayed", t.Topic(queue))
}

func (t *TopicKey) DLQTopic(originalTopic string) string {
	return fmt.Sprintf("%s_dlq", originalTopic)
}

func (t *TopicKey) GroupID(queue string) string {
	return fmt.Sprintf("%s_%s_%s", t.appName, t.connection, queue)
}
