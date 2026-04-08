package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	contractsdocker "github.com/goravel/framework/contracts/testing/docker"
	mocksconfig "github.com/goravel/framework/mocks/config"
	"github.com/goravel/framework/process"
	"github.com/goravel/framework/support/env"
	testingdocker "github.com/goravel/framework/testing/docker"
	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	testConnection = "default"
	testHost       = "localhost"
	testPort       = 9092
	testBrokers    = "localhost:9092"
)

type DockerTestSuite struct {
	suite.Suite
	mockConfig *mocksconfig.Config
}

func TestDockerTestSuite(t *testing.T) {
	suite.Run(t, &DockerTestSuite{})
}

func (s *DockerTestSuite) SetupTest() {
	s.mockConfig = &mocksconfig.Config{}
}

func (s *DockerTestSuite) TestNewDocker() {
	processInstance := process.New()

	tests := []struct {
		name          string
		brokers       string
		expectedError bool
	}{
		{
			name:          "success",
			brokers:       testBrokers,
			expectedError: false,
		},
		{
			name:          "missing brokers",
			brokers:       "",
			expectedError: true,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			s.SetupTest()
			s.mockConfig.On("GetString", fmt.Sprintf("queue.connections.%s.connection", testConnection), "default").Return(testConnection).Once()
			s.mockConfig.On("GetString", fmt.Sprintf("kafka.%s.brokers", testConnection)).Return(test.brokers).Once()

			docker, err := NewDocker(s.mockConfig, processInstance, testConnection)

			if test.expectedError {
				s.Error(err)
				s.Nil(docker)
			} else {
				s.NoError(err)
				s.NotNil(docker)
			}
		})
	}
}

func (s *DockerTestSuite) TestBuildReadyShutdown() {
	if env.IsWindows() {
		s.T().Skip("Skipping tests of using docker")
	}

	docker := &Docker{
		connection: testConnection,
		config:     s.mockConfig,
		host:       "localhost",
		port:       9092,
		imageDriver: testingdocker.NewImageDriver(contractsdocker.Image{
			Repository:   "apache/kafka",
			Tag:          "latest",
			ExposedPorts: []string{"9092:9092"},
			Env: []string{
				"KAFKA_NODE_ID=1",
				"KAFKA_PROCESS_ROLES=broker,controller",
				"KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
				"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
				"KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
				"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			},
		}, process.New()),
		process: process.New(),
	}

	err := docker.Build()
	s.NoError(err)
	s.NotEmpty(docker.containerID)
	s.NotZero(docker.port)

	s.mockConfig.EXPECT().Add(fmt.Sprintf("kafka.%s.brokers", testConnection), fmt.Sprintf("%s:%d", docker.host, docker.port)).Once()

	err = docker.Ready()
	s.NoError(err)

	// Verify connectivity
	client, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("%s:%d", docker.host, docker.port)),
	)
	s.NoError(err)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.NoError(client.Ping(pingCtx))
	client.Close()

	err = docker.Shutdown()
	s.NoError(err)
}

func initDocker(mockConfig *mocksconfig.Config) *Docker {
	mockConfig.EXPECT().GetString(fmt.Sprintf("queue.connections.%s.connection", testConnection), "default").Return(testConnection).Once()
	mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.brokers", testConnection)).Return(testBrokers).Once()

	docker, err := NewDocker(mockConfig, process.New(), testConnection)
	if err != nil {
		panic(err)
	}

	if err := docker.Build(); err != nil {
		panic(err)
	}

	mockConfig.EXPECT().Add(fmt.Sprintf("kafka.%s.brokers", testConnection), fmt.Sprintf("%s:%d", docker.host, docker.port)).Once()

	if err := docker.Ready(); err != nil {
		panic(err)
	}

	return docker
}

func mockGetProducer(mockConfig *mocksconfig.Config, docker *Docker) {
	mockBuildBaseOpts(mockConfig, testConnection, fmt.Sprintf("%s:%d", docker.host, docker.port), "")
}

// mockBuildBaseOpts mocks all config calls for BuildBaseOpts with no SASL.
func mockBuildBaseOpts(mockConfig *mocksconfig.Config, connection, brokers, saslMechanism string) {
	mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.brokers", connection)).Return(brokers).Once()
	mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.mechanism", connection)).Return(saslMechanism).Once()
	mockConfig.EXPECT().Get(fmt.Sprintf("kafka.%s.tls", connection)).Return(nil).Once()
	mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.client_id", connection)).Return("").Once()
	mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.instance_id", connection)).Return("").Once()
	mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.compression", connection)).Return("").Once()
	mockConfig.EXPECT().GetInt(fmt.Sprintf("kafka.%s.session_timeout", connection)).Return(0).Once()
}
