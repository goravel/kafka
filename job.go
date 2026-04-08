package kafka

import (
	"context"

	contractsfoundation "github.com/goravel/framework/contracts/foundation"
	contractsqueue "github.com/goravel/framework/contracts/queue"
	"github.com/goravel/framework/queue/utils"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ReservedJob struct {
	client *kgo.Client
	record *kgo.Record
	task   contractsqueue.Task
}

func NewReservedJob(client *kgo.Client, record *kgo.Record, jobStorer contractsqueue.JobStorer, json contractsfoundation.Json) (*ReservedJob, error) {
	task, err := utils.JsonToTask(string(record.Value), jobStorer, json)
	if err != nil {
		return nil, err
	}

	return &ReservedJob{
		client: client,
		record: record,
		task:   task,
	}, nil
}

func (r *ReservedJob) Delete() error {
	return r.client.CommitRecords(context.Background(), r.record)
}

func (r *ReservedJob) Task() contractsqueue.Task {
	return r.task
}
