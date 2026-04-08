package kafka

import "github.com/goravel/framework/errors"

var (
	ErrKafkaServiceProviderNotRegistered = errors.New("please register kafka service provider").SetModule("Kafka")
	ErrKafkaConnectionIsRequired         = errors.New("connection is required").SetModule("Kafka")
)
