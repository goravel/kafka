package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	contractsqueue "github.com/goravel/framework/contracts/queue"
	"github.com/goravel/framework/foundation/json"
	mocksconfig "github.com/goravel/framework/mocks/config"
	mocksqueue "github.com/goravel/framework/mocks/queue"
	"github.com/goravel/framework/queue/utils"
	"github.com/goravel/framework/support/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
)

type ReservedJobTestSuite struct {
	suite.Suite
	ctx           context.Context
	client        *kgo.Client
	mockJobStorer *mocksqueue.JobStorer
	docker        *Docker
	topic         string
}

func TestReservedJobTestSuite(t *testing.T) {
	if env.IsWindows() {
		t.Skip("Skipping tests of using docker")
	}

	suite.Run(t, &ReservedJobTestSuite{})
}

func (s *ReservedJobTestSuite) SetupSuite() {
	mockConfig := mocksconfig.NewConfig(s.T())
	docker := initDocker(mockConfig)

	brokers := fmt.Sprintf("%s:%d", docker.host, docker.port)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup("test-reserved-job-group"),
		kgo.ConsumeTopics("test-reserved-job-topic"),
		kgo.DisableAutoCommit(),
	)
	s.Require().NoError(err)

	s.ctx = context.Background()
	s.client = client
	s.docker = docker
	s.mockJobStorer = mocksqueue.NewJobStorer(s.T())
	s.topic = "test-reserved-job-topic"
}

func (s *ReservedJobTestSuite) TearDownSuite() {
	if s.client != nil {
		s.client.Close()
	}
	s.NoError(s.docker.Shutdown())
}

func (s *ReservedJobTestSuite) SetupTest() {
	producers.Clear()
}

func (s *ReservedJobTestSuite) TestNewReservedJob() {
	task := contractsqueue.Task{
		UUID: "865111de-ff50-4652-9733-72fea655f836",
		ChainJob: contractsqueue.ChainJob{
			Job: &MockJob{},
			Args: []contractsqueue.Arg{
				{Type: "[]string", Value: []string{"test", "test2", "test3"}},
			},
		},
	}
	payload, err := utils.TaskToJson(task, json.New())
	s.Require().NoError(err)

	record := &kgo.Record{
		Topic: s.topic,
		Value: []byte(payload),
	}

	s.mockJobStorer.EXPECT().Get("mock").Return(&MockJob{}, nil).Once()

	reservedJob, err := NewReservedJob(s.client, record, s.mockJobStorer, json.New())
	s.NoError(err)
	s.NotNil(reservedJob)
	s.Equal(s.client, reservedJob.client)
	s.Equal(record, reservedJob.record)
	s.Equal("865111de-ff50-4652-9733-72fea655f836", reservedJob.task.UUID)
	s.Equal("mock", reservedJob.task.Job.Signature())
}

func (s *ReservedJobTestSuite) TestDelete() {
	task := contractsqueue.Task{
		UUID: "delete-test",
		ChainJob: contractsqueue.ChainJob{
			Job: &MockJob{},
		},
	}
	payload, err := utils.TaskToJson(task, json.New())
	s.Require().NoError(err)

	record := &kgo.Record{
		Topic: s.topic,
		Value: []byte(payload),
	}
	s.Require().NoError(s.client.ProduceSync(s.ctx, record).FirstErr())

	time.Sleep(1 * time.Second)
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	fetches := s.client.PollRecords(ctx, 1)
	records := fetches.Records()
	s.Require().NotEmpty(records)

	s.mockJobStorer.EXPECT().Get("mock").Return(&MockJob{}, nil).Once()

	reservedJob, err := NewReservedJob(s.client, records[0], s.mockJobStorer, json.New())
	s.NoError(err)

	err = reservedJob.Delete()
	s.NoError(err)
}

func (s *ReservedJobTestSuite) TestTask() {
	task := contractsqueue.Task{
		UUID: "task-test",
		ChainJob: contractsqueue.ChainJob{
			Job:  &MockJob{},
			Args: []contractsqueue.Arg{{Type: "string", Value: "hello"}},
		},
	}
	payload, err := utils.TaskToJson(task, json.New())
	s.Require().NoError(err)

	record := &kgo.Record{
		Topic: s.topic,
		Value: []byte(payload),
	}

	s.mockJobStorer.EXPECT().Get("mock").Return(&MockJob{}, nil).Once()

	reservedJob, err := NewReservedJob(s.client, record, s.mockJobStorer, json.New())
	s.NoError(err)

	result := reservedJob.Task()
	s.Equal("task-test", result.UUID)
	s.Equal("mock", result.Job.Signature())
	s.Len(result.Args, 1)
	s.Equal("string", result.Args[0].Type)
}

func TestTaskToJson(t *testing.T) {
	task := contractsqueue.Task{
		UUID: "test-uuid",
		ChainJob: contractsqueue.ChainJob{
			Job: &MockJob{},
		},
	}

	result, err := utils.TaskToJson(task, json.New())

	assert.NoError(t, err)
	assert.Contains(t, result, `"uuid":"test-uuid"`)
	assert.Contains(t, result, `"signature":"mock"`)
}
