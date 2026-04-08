package kafka

import (
	"fmt"
	"testing"

	mocksconfig "github.com/goravel/framework/mocks/config"
	"github.com/goravel/framework/support/env"
	"github.com/stretchr/testify/suite"
)

type ClientTestSuite struct {
	suite.Suite
	mockConfig *mocksconfig.Config
	docker     *Docker
	brokers    string
}

func TestClientTestSuite(t *testing.T) {
	if env.IsWindows() {
		t.Skip("Skipping tests of using docker")
	}

	suite.Run(t, &ClientTestSuite{})
}

func (s *ClientTestSuite) SetupSuite() {
	s.mockConfig = mocksconfig.NewConfig(s.T())
	docker := initDocker(s.mockConfig)
	s.docker = docker
	s.brokers = fmt.Sprintf("%s:%d", docker.host, docker.port)
}

func (s *ClientTestSuite) TearDownSuite() {
	s.NoError(s.docker.Shutdown())
}

func (s *ClientTestSuite) SetupTest() {
	producers.Clear()
}

func (s *ClientTestSuite) TestBuildBaseOpts() {
	s.Run("happy path", func() {
		mockBuildBaseOpts(s.mockConfig, testConnection, s.brokers, "")
		opts, err := BuildBaseOpts(s.mockConfig, testConnection)
		s.NoError(err)
		s.NotEmpty(opts)
	})

	s.Run("empty brokers", func() {
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.brokers", testConnection)).Return("").Once()
		opts, err := BuildBaseOpts(s.mockConfig, testConnection)
		s.Error(err)
		s.Nil(opts)
	})

	s.Run("with SASL PLAIN", func() {
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.brokers", testConnection)).Return(s.brokers).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.mechanism", testConnection)).Return("PLAIN").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.username", testConnection)).Return("user").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.password", testConnection)).Return("pass").Once()
		s.mockConfig.EXPECT().Get(fmt.Sprintf("kafka.%s.tls", testConnection)).Return(nil).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.client_id", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.instance_id", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.compression", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetInt(fmt.Sprintf("kafka.%s.session_timeout", testConnection)).Return(0).Once()

		opts, err := BuildBaseOpts(s.mockConfig, testConnection)
		s.NoError(err)
		s.NotEmpty(opts)
	})

	s.Run("with SASL SCRAM-SHA-256", func() {
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.brokers", testConnection)).Return(s.brokers).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.mechanism", testConnection)).Return("SCRAM-SHA-256").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.username", testConnection)).Return("user").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.password", testConnection)).Return("pass").Once()
		s.mockConfig.EXPECT().Get(fmt.Sprintf("kafka.%s.tls", testConnection)).Return(nil).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.client_id", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.instance_id", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.compression", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetInt(fmt.Sprintf("kafka.%s.session_timeout", testConnection)).Return(0).Once()

		opts, err := BuildBaseOpts(s.mockConfig, testConnection)
		s.NoError(err)
		s.NotEmpty(opts)
	})

	s.Run("with SASL OAUTHBEARER", func() {
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.brokers", testConnection)).Return(s.brokers).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.mechanism", testConnection)).Return("OAUTHBEARER").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.username", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.password", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.token", testConnection)).Return("my-token").Once()
		s.mockConfig.EXPECT().Get(fmt.Sprintf("kafka.%s.tls", testConnection)).Return(nil).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.client_id", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.instance_id", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.compression", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().GetInt(fmt.Sprintf("kafka.%s.session_timeout", testConnection)).Return(0).Once()

		opts, err := BuildBaseOpts(s.mockConfig, testConnection)
		s.NoError(err)
		s.NotEmpty(opts)
	})

	s.Run("with compression and client_id", func() {
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.brokers", testConnection)).Return(s.brokers).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.sasl.mechanism", testConnection)).Return("").Once()
		s.mockConfig.EXPECT().Get(fmt.Sprintf("kafka.%s.tls", testConnection)).Return(nil).Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.client_id", testConnection)).Return("my-app").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.instance_id", testConnection)).Return("worker-1").Once()
		s.mockConfig.EXPECT().GetString(fmt.Sprintf("kafka.%s.compression", testConnection)).Return("snappy").Once()
		s.mockConfig.EXPECT().GetInt(fmt.Sprintf("kafka.%s.session_timeout", testConnection)).Return(30000).Once()

		opts, err := BuildBaseOpts(s.mockConfig, testConnection)
		s.NoError(err)
		s.NotEmpty(opts)
	})
}

func (s *ClientTestSuite) TestGetProducer() {
	s.Run("creates and caches producer", func() {
		mockBuildBaseOpts(s.mockConfig, testConnection, s.brokers, "")
		client1, err := GetProducer(s.mockConfig, testConnection)
		s.NoError(err)
		s.NotNil(client1)

		// Second call should return cached instance (no more mock calls expected)
		client2, err := GetProducer(s.mockConfig, testConnection)
		s.NoError(err)
		s.Equal(client1, client2)
	})

	s.Run("invalid brokers", func() {
		connection := "invalid"
		mockBuildBaseOpts(s.mockConfig, connection, "invalid-host:9999", "")
		client, err := GetProducer(s.mockConfig, connection)
		s.NoError(err)
		s.Nil(client)
	})
}
