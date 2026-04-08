package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	contractsconfig "github.com/goravel/framework/contracts/config"
	contractsprocess "github.com/goravel/framework/contracts/process"
	contractsdocker "github.com/goravel/framework/contracts/testing/docker"
	supportdocker "github.com/goravel/framework/support/docker"
	testingdocker "github.com/goravel/framework/testing/docker"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Docker struct {
	config      contractsconfig.Config
	imageDriver contractsdocker.ImageDriver
	process     contractsprocess.Process
	connection  string
	host        string
	port        int
	containerID string
}

func NewDocker(config contractsconfig.Config, process contractsprocess.Process, connection string) (*Docker, error) {
	clientConnection := config.GetString(fmt.Sprintf("queue.connections.%s.connection", connection), "default")
	configPrefix := fmt.Sprintf("kafka.%s", clientConnection)
	brokersRaw := config.GetString(fmt.Sprintf("%s.brokers", configPrefix))
	if brokersRaw == "" {
		return nil, fmt.Errorf("kafka brokers not configured for connection [%s] at path '%s.brokers'", clientConnection, configPrefix)
	}

	return &Docker{
		connection: clientConnection,
		config:     config,
		host:       "localhost",
		port:       9092,
		imageDriver: testingdocker.NewImageDriver(contractsdocker.Image{
			Repository:   "apache/kafka",
			Tag:          "latest",
			ExposedPorts: []string{"9092"},
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
		}, process),
		process: process,
	}, nil
}

func (r *Docker) Build() error {
	if err := r.imageDriver.Build(); err != nil {
		return err
	}

	config := r.imageDriver.Config()
	r.containerID = config.ContainerID
	r.port, _ = strconv.Atoi(supportdocker.ExposedPort(config.ExposedPorts, strconv.Itoa(r.port)))

	return nil
}

func (r *Docker) Ready() error {
	for i := 0; i < 60; i++ {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(fmt.Sprintf("%s:%d", r.host, r.port)),
		)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = client.Ping(pingCtx)
		cancel()
		client.Close()

		if err == nil {
			r.resetConfigBrokers()
			return nil
		}

		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("kafka docker container failed to become ready")
}

func (r *Docker) Shutdown() error {
	return r.imageDriver.Shutdown()
}

func (r *Docker) resetConfigBrokers() {
	r.config.Add(fmt.Sprintf("kafka.%s.brokers", r.connection), fmt.Sprintf("%s:%d", r.host, r.port))
}
