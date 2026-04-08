package kafka

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/goravel/framework/support/color"
	"github.com/twmb/franz-go/pkg/kgo"
)

const delayHeaderKey = "X-Delay-Until"

type delayMigrator struct {
	ctx      context.Context
	cancel   context.CancelFunc
	producer *kgo.Client
	consumer *kgo.Client
	topicKey *TopicKey
	queue    string
	started  atomic.Bool
}

func newDelayMigrator(parentCtx context.Context, producer *kgo.Client, topicKey *TopicKey, queue string, baseOpts []kgo.Opt) *delayMigrator {
	ctx, cancel := context.WithCancel(parentCtx)

	delayTopic := topicKey.DelayTopic(queue)
	groupID := topicKey.GroupID(queue) + "_delay"

	opts := make([]kgo.Opt, len(baseOpts))
	copy(opts, baseOpts)
	opts = append(opts,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(delayTopic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		color.Warningf("Failed to create delay migrator consumer for queue [%s]: %s\n", queue, err)
		cancel()
		return &delayMigrator{ctx: ctx, cancel: cancel}
	}

	return &delayMigrator{
		ctx:      ctx,
		cancel:   cancel,
		producer: producer,
		consumer: consumer,
		topicKey: topicKey,
		queue:    queue,
	}
}

// start begins the background goroutine that consumes from the delay topic.
func (m *delayMigrator) start() {
	if m.consumer == nil {
		return
	}
	if !m.started.CompareAndSwap(false, true) {
		return
	}

	go m.consumeLoop()
}

func (m *delayMigrator) consumeLoop() {
	mainTopic := m.topicKey.Topic(m.queue)
	delayTopic := m.topicKey.DelayTopic(m.queue)

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		pollCtx, cancel := context.WithTimeout(m.ctx, 2*time.Second)
		fetches := m.consumer.PollRecords(pollCtx, 100)
		cancel()

		for _, record := range fetches.Records() {
			delayUntil := parseDelayHeader(record)
			remaining := time.Until(delayUntil)

			switch {
			case remaining <= 0:
				// Ready: forward to main topic
				if err := m.forward(mainTopic, record); err != nil {
					color.Warningf("Failed to forward delayed message to [%s]: %s\n", mainTopic, err)
					continue // don't commit, will retry on next poll
				}

			case remaining <= 2*time.Second:
				// Almost ready: wait then forward
				select {
				case <-time.After(remaining):
				case <-m.ctx.Done():
					return
				}
				if err := m.forward(mainTopic, record); err != nil {
					color.Warningf("Failed to forward delayed message to [%s]: %s\n", mainTopic, err)
					continue
				}

			default:
				// Not ready: re-produce back to delay topic tail
				reRecord := &kgo.Record{
					Topic:   delayTopic,
					Key:     record.Key,
					Value:   record.Value,
					Headers: record.Headers,
				}
				if err := m.producer.ProduceSync(m.ctx, reRecord).FirstErr(); err != nil {
					color.Warningf("Failed to re-produce delayed message: %s\n", err)
					continue // don't commit, message stays at current offset
				}
			}

			// Only commit after successful forward or re-produce
			if err := m.consumer.CommitRecords(m.ctx, record); err != nil {
				color.Warningf("Failed to commit delay topic offset: %s\n", err)
			}
		}
	}
}

func (m *delayMigrator) forward(topic string, record *kgo.Record) error {
	r := &kgo.Record{
		Topic: topic,
		Key:   record.Key,
		Value: record.Value,
	}
	return m.producer.ProduceSync(m.ctx, r).FirstErr()
}

func (m *delayMigrator) stop() {
	m.cancel()
	if m.consumer != nil {
		m.consumer.Close()
	}
}

func parseDelayHeader(record *kgo.Record) time.Time {
	for _, h := range record.Headers {
		if h.Key == delayHeaderKey {
			ts, err := strconv.ParseInt(string(h.Value), 10, 64)
			if err != nil {
				color.Warningf("Invalid delay header in record (offset %d): %s\n", record.Offset, err)
				return time.Now()
			}
			return time.Unix(ts, 0)
		}
	}
	return time.Now()
}
