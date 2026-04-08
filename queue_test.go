package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	contractsqueue "github.com/goravel/framework/contracts/queue"
	"github.com/goravel/framework/errors"
	"github.com/goravel/framework/foundation/json"
	mocksconfig "github.com/goravel/framework/mocks/config"
	mocksqueue "github.com/goravel/framework/mocks/queue"
	"github.com/goravel/framework/support/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type QueueTestSuite struct {
	suite.Suite
	mockJobStorer *mocksqueue.JobStorer
	ctx           context.Context
	queue         *Queue
	docker        *Docker
}

func TestQueueTestSuite(t *testing.T) {
	if env.IsWindows() {
		t.Skip("Skipping tests of using docker")
	}

	suite.Run(t, &QueueTestSuite{})
}

func (s *QueueTestSuite) SetupSuite() {
	mockConfig := mocksconfig.NewConfig(s.T())
	docker := initDocker(mockConfig)

	// Mock for BuildBaseOpts (called by NewQueue -> GetProducer)
	mockGetProducer(mockConfig, docker)

	// Mock for NewQueue
	mockConfig.EXPECT().GetString("app.name", "goravel").Return("test").Once()
	mockConfig.EXPECT().GetString(fmt.Sprintf("queue.connections.%s.connection", testConnection), "default").Return(testConnection).Once()

	// Mock for BuildBaseOpts (called by NewQueue for baseOpts)
	brokers := fmt.Sprintf("%s:%d", docker.host, docker.port)
	mockBuildBaseOpts(mockConfig, testConnection, brokers, "")

	mockQueue := mocksqueue.NewQueue(s.T())
	s.mockJobStorer = mocksqueue.NewJobStorer(s.T())
	mockQueue.EXPECT().JobStorer().Return(s.mockJobStorer).Once()

	s.ctx = context.Background()

	queue, err := NewQueue(s.ctx, mockConfig, mockQueue, json.New(), testConnection)
	s.Require().NoError(err)

	s.docker = docker
	s.queue = queue
}

func (s *QueueTestSuite) TearDownSuite() {
	if s.queue != nil {
		s.NoError(s.queue.Close())
	}
	if s.docker != nil {
		s.NoError(s.docker.Shutdown())
	}
}

func (s *QueueTestSuite) SetupTest() {
	producers.Clear()
}

func (s *QueueTestSuite) TestDriver() {
	s.Equal("custom", s.queue.Driver())
}

func (s *QueueTestSuite) TestPush() {
	s.Run("no delay", func() {
		queue := "push-no-delay"
		task := contractsqueue.Task{
			UUID: "push-no-delay-uuid",
			ChainJob: contractsqueue.ChainJob{
				Job:  &MockJob{},
				Args: testArgs,
			},
		}

		s.NoError(s.queue.Push(task, queue))

		// Verify by consuming the message
		s.mockJobStorer.EXPECT().Get(task.Job.Signature()).Return(&MockJob{}, nil).Once()

		// Wait for message to be available
		time.Sleep(2 * time.Second)

		job, err := s.queue.Pop(queue)
		s.NoError(err)
		s.NotNil(job)
		s.Equal(task.Job.Signature(), job.Task().Job.Signature())
		s.Equal(task.UUID, job.Task().UUID)

		s.NoError(job.Delete())
	})

	s.Run("delay", func() {
		queue := "push-delay"
		task := contractsqueue.Task{
			UUID: "push-delay-uuid",
			ChainJob: contractsqueue.ChainJob{
				Job:   &MockJob{},
				Args:  testArgs,
				Delay: time.Now().Add(2 * time.Second),
			},
		}

		s.NoError(s.queue.Push(task, queue))

		// Should not be available immediately on main topic
		time.Sleep(500 * time.Millisecond)
		job, err := s.queue.Pop(queue)
		s.Nil(job)
		s.NotNil(err)

		// Wait for delay to pass + migration
		time.Sleep(3 * time.Second)

		// Trigger migration and check
		s.mockJobStorer.EXPECT().Get(task.Job.Signature()).Return(&MockJob{}, nil).Once()

		job, err = s.queue.Pop(queue)
		s.NoError(err)
		s.NotNil(job)
		s.Equal(task.UUID, job.Task().UUID)

		s.NoError(job.Delete())
	})
}

func (s *QueueTestSuite) TestPop() {
	s.Run("no job", func() {
		queue := "pop-no-job"
		job, err := s.queue.Pop(queue)

		s.NotNil(err)
		s.True(errors.Is(err, errors.QueueDriverNoJobFound))
		s.Nil(job)
	})

	s.Run("success", func() {
		queue := "pop-success"
		task := contractsqueue.Task{
			UUID: "pop-success-uuid",
			ChainJob: contractsqueue.ChainJob{
				Job:  &MockJob{},
				Args: testArgs,
			},
			Chain: []contractsqueue.ChainJob{
				{
					Job:  &MockJob{},
					Args: testArgs,
				},
			},
		}

		s.NoError(s.queue.Push(task, queue))
		time.Sleep(2 * time.Second)

		s.mockJobStorer.EXPECT().Get(task.Job.Signature()).Return(&MockJob{}, nil).Twice()

		job, err := s.queue.Pop(queue)
		s.NoError(err)
		s.NotNil(job)
		s.Equal(task.Job.Signature(), job.Task().Job.Signature())
		s.Equal(len(task.Args), len(job.Task().Args))
		s.Len(job.Task().Chain, 1)
		s.Equal(task.Chain[0].Job.Signature(), job.Task().Chain[0].Job.Signature())

		s.NoError(job.Delete())
	})
}

func (s *QueueTestSuite) TestReceive() {
	s.Run("no messages", func() {
		queue := "receive-empty"
		ctx, cancel := context.WithTimeout(s.ctx, 1*time.Second)
		defer cancel()

		jobs, err := s.queue.Receive(ctx, queue, 5)
		s.NoError(err)
		s.Empty(jobs)
	})

	s.Run("single message", func() {
		queue := "receive-single"
		task := contractsqueue.Task{
			UUID: "receive-single-uuid",
			ChainJob: contractsqueue.ChainJob{
				Job:  &MockJob{},
				Args: testArgs,
			},
		}

		s.NoError(s.queue.Push(task, queue))
		time.Sleep(2 * time.Second)

		s.mockJobStorer.EXPECT().Get(task.Job.Signature()).Return(&MockJob{}, nil).Once()

		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		defer cancel()

		jobs, err := s.queue.Receive(ctx, queue, 5)
		s.NoError(err)
		s.Len(jobs, 1)
		s.Equal(task.UUID, jobs[0].Task().UUID)

		s.NoError(jobs[0].Delete())
	})

	s.Run("multiple messages", func() {
		queue := "receive-multiple"
		tasks := make([]contractsqueue.Task, 3)
		for i := 0; i < 3; i++ {
			tasks[i] = contractsqueue.Task{
				UUID: fmt.Sprintf("receive-multi-uuid-%d", i),
				ChainJob: contractsqueue.ChainJob{
					Job:  &MockJob{},
					Args: testArgs,
				},
			}
			s.NoError(s.queue.Push(tasks[i], queue))
		}

		time.Sleep(2 * time.Second)

		s.mockJobStorer.EXPECT().Get("mock").Return(&MockJob{}, nil).Times(3)

		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		defer cancel()

		jobs, err := s.queue.Receive(ctx, queue, 5)
		s.NoError(err)
		s.Len(jobs, 3)

		for _, job := range jobs {
			s.NoError(job.Delete())
		}
	})
}

func (s *QueueTestSuite) TestReceiveCommitPreventsRedelivery() {
	queue := "receive-commit"
	task := contractsqueue.Task{
		UUID: "commit-test-uuid",
		ChainJob: contractsqueue.ChainJob{
			Job:  &MockJob{},
			Args: testArgs,
		},
	}

	s.NoError(s.queue.Push(task, queue))
	time.Sleep(2 * time.Second)

	s.mockJobStorer.EXPECT().Get(task.Job.Signature()).Return(&MockJob{}, nil).Once()

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	jobs, err := s.queue.Receive(ctx, queue, 1)
	s.NoError(err)
	s.Len(jobs, 1)

	// Commit (Delete)
	s.NoError(jobs[0].Delete())

	// Subsequent receive should return nothing
	ctx2, cancel2 := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel2()

	jobs2, err := s.queue.Receive(ctx2, queue, 1)
	s.NoError(err)
	s.Empty(jobs2)
}

func TestTopicKey(t *testing.T) {
	topicKey := NewTopicKey("test-app", "test-conn")

	t.Run("Topic", func(t *testing.T) {
		assert.Equal(t, "test-app_queues_test-conn_test-queue", topicKey.Topic("test-queue"))
	})

	t.Run("DelayTopic", func(t *testing.T) {
		assert.Equal(t, "test-app_queues_test-conn_test-queue_delayed", topicKey.DelayTopic("test-queue"))
	})

	t.Run("DLQTopic", func(t *testing.T) {
		assert.Equal(t, "test-app_queues_test-conn_test-queue_dlq", topicKey.DLQTopic("test-app_queues_test-conn_test-queue"))
	})

	t.Run("GroupID", func(t *testing.T) {
		assert.Equal(t, "test-app_test-conn_test-queue", topicKey.GroupID("test-queue"))
	})
}

var testArgs = []contractsqueue.Arg{
	{Type: "bool", Value: true},
	{Type: "int", Value: 1},
	{Type: "string", Value: "test"},
	{Type: "[]string", Value: []string{"test", "test2"}},
}

type MockJob struct{}

func (m *MockJob) Signature() string {
	return "mock"
}

func (m *MockJob) Handle(args ...any) error {
	return nil
}
