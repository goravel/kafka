package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/goravel/framework/contracts/config"
	"github.com/goravel/framework/support/color"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	producers sync.Map
	mu        sync.Mutex
)

// GetProducer returns a cached Kafka producer client for the specified connection.
func GetProducer(config config.Config, connection string) (*kgo.Client, error) {
	if client, ok := producers.Load(connection); ok {
		return client.(*kgo.Client), nil
	}

	mu.Lock()
	defer mu.Unlock()

	if client, ok := producers.Load(connection); ok {
		return client.(*kgo.Client), nil
	}

	client, err := createProducer(config, connection)
	if err != nil {
		return nil, err
	}

	if client != nil {
		producers.Store(connection, client)
	}

	return client, nil
}

// BuildBaseOpts builds the common kgo.Opt slice (brokers, SASL, TLS, etc.) for a connection.
func BuildBaseOpts(config config.Config, connection string) ([]kgo.Opt, error) {
	configPrefix := fmt.Sprintf("kafka.%s", connection)
	brokersRaw := config.GetString(fmt.Sprintf("%s.brokers", configPrefix))
	if brokersRaw == "" {
		return nil, fmt.Errorf("kafka brokers not configured for connection [%s] at path '%s.brokers'", connection, configPrefix)
	}

	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	}

	// SASL
	mechanism := config.GetString(fmt.Sprintf("%s.sasl.mechanism", configPrefix))
	if mechanism != "" {
		username := config.GetString(fmt.Sprintf("%s.sasl.username", configPrefix))
		password := config.GetString(fmt.Sprintf("%s.sasl.password", configPrefix))

		switch strings.ToUpper(mechanism) {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{User: username, Pass: password}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha512Mechanism()))
		case "OAUTHBEARER":
			token := config.GetString(fmt.Sprintf("%s.sasl.token", configPrefix))
			opts = append(opts, kgo.SASL(oauth.Auth{Token: token}.AsMechanism()))
		}
	}

	// TLS
	tlsConfigRaw := config.Get(fmt.Sprintf("%s.tls", configPrefix))
	if tlsConfig, ok := tlsConfigRaw.(*tls.Config); ok && tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	// ClientID
	clientID := config.GetString(fmt.Sprintf("%s.client_id", configPrefix))
	if clientID != "" {
		opts = append(opts, kgo.ClientID(clientID))
	}

	// InstanceID (static group membership)
	instanceID := config.GetString(fmt.Sprintf("%s.instance_id", configPrefix))
	if instanceID != "" {
		opts = append(opts, kgo.InstanceID(instanceID))
	}

	// Compression
	compression := config.GetString(fmt.Sprintf("%s.compression", configPrefix))
	if compression != "" {
		if codec, ok := compressionCodec(compression); ok {
			opts = append(opts, kgo.ProducerBatchCompression(codec))
		}
	}

	// SessionTimeout
	sessionTimeout := config.GetInt(fmt.Sprintf("%s.session_timeout", configPrefix))
	if sessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(time.Duration(sessionTimeout)*time.Millisecond))
	}

	return opts, nil
}

func createProducer(config config.Config, connection string) (*kgo.Client, error) {
	opts, err := BuildBaseOpts(config, connection)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer for connection [%s]: %w", connection, err)
	}

	// Verify connectivity
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = client.Ping(pingCtx); err != nil {
		client.Close()
		color.Warningf("Failed to connect to kafka connection [%s]: %s\n", connection, err)

		return nil, nil
	}

	return client, nil
}

func compressionCodec(name string) (kgo.CompressionCodec, bool) {
	switch strings.ToLower(name) {
	case "gzip":
		return kgo.GzipCompression(), true
	case "snappy":
		return kgo.SnappyCompression(), true
	case "lz4":
		return kgo.Lz4Compression(), true
	case "zstd":
		return kgo.ZstdCompression(), true
	default:
		return kgo.NoCompression(), false
	}
}
