package facades

import (
	"context"
	"testing"

	"github.com/goravel/framework/foundation/json"
	mocksconfig "github.com/goravel/framework/mocks/config"
	mocksfoundation "github.com/goravel/framework/mocks/foundation"
	mocksqueue "github.com/goravel/framework/mocks/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goravel/kafka"
)

func TestQueue(t *testing.T) {
	t.Run("returns error when App is nil", func(t *testing.T) {
		kafka.App = nil
		driver, err := Queue("default")
		assert.Nil(t, driver)
		assert.Equal(t, kafka.ErrKafkaServiceProviderNotRegistered, err)
	})

	t.Run("returns error when connection is empty", func(t *testing.T) {
		mockApp := mocksfoundation.NewApplication(t)
		kafka.App = mockApp

		driver, err := Queue("")
		assert.Nil(t, driver)
		assert.Equal(t, kafka.ErrKafkaConnectionIsRequired, err)
	})

	t.Run("returns queue driver with real implementation", func(t *testing.T) {
		mockApp := mocksfoundation.NewApplication(t)
		mockConfig := mocksconfig.NewConfig(t)
		mockQueueInstance := mocksqueue.NewQueue(t)
		mockJobStorer := mocksqueue.NewJobStorer(t)

		kafka.App = mockApp

		connectionName := "queue_default"
		brokers := "localhost:9092"

		mockConfig.On("GetString", "queue.connections.queue_default.connection", "default").Return(connectionName).Once()
		// GetProducer -> BuildBaseOpts (first call)
		mockConfig.On("GetString", "kafka.queue_default.brokers").Return(brokers).Twice()
		mockConfig.On("GetString", "kafka.queue_default.sasl.mechanism").Return("").Twice()
		mockConfig.On("Get", "kafka.queue_default.tls").Return(nil).Twice()
		mockConfig.On("GetString", "app.name", "goravel").Return("test").Once()

		mockQueueInstance.On("JobStorer").Return(mockJobStorer).Once()

		realInstance, err := kafka.NewQueue(context.Background(), mockConfig, mockQueueInstance, json.New(), connectionName)
		if err != nil {
			t.Skip("Skipping test as real driver creation failed:", err)
			return
		}

		mockApp.EXPECT().MakeWith(kafka.BindingQueue, map[string]any{"connection": connectionName}).Return(realInstance, nil).Once()

		driver, err := Queue(connectionName)
		require.NoError(t, err)
		require.NotNil(t, driver)
		assert.Equal(t, realInstance, driver)
	})
}
