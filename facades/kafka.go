package facades

import (
	"github.com/goravel/framework/contracts/queue"

	"github.com/goravel/kafka"
)

func Queue(connection string) (queue.Driver, error) {
	if kafka.App == nil {
		return nil, kafka.ErrKafkaServiceProviderNotRegistered
	}
	if connection == "" {
		return nil, kafka.ErrKafkaConnectionIsRequired
	}

	instance, err := kafka.App.MakeWith(kafka.BindingQueue, map[string]any{"connection": connection})
	if err != nil {
		return nil, err
	}

	return instance.(*kafka.Queue), nil
}
