package main

import (
	"os"

	"github.com/goravel/framework/packages"
	"github.com/goravel/framework/packages/match"
	"github.com/goravel/framework/packages/modify"
	"github.com/goravel/framework/support/env"
	"github.com/goravel/framework/support/file"
	"github.com/goravel/framework/support/path"
	supportstubs "github.com/goravel/framework/support/stubs"
)

func main() {
	setup := packages.Setup(os.Args)
	kafkaConfig := `map[string]any{
        "default": map[string]any{
            "brokers":     config.Env("KAFKA_BROKERS", "localhost:9092"),
            "client_id":   config.Env("KAFKA_CLIENT_ID", ""),
            "instance_id": config.Env("KAFKA_INSTANCE_ID", ""),
            "compression": config.Env("KAFKA_COMPRESSION", ""),
            "session_timeout": config.Env("KAFKA_SESSION_TIMEOUT", 0),
            "sasl": map[string]any{
                "mechanism": config.Env("KAFKA_SASL_MECHANISM", ""),
                "username":  config.Env("KAFKA_SASL_USERNAME", ""),
                "password":  config.Env("KAFKA_SASL_PASSWORD", ""),
            },
        },
    }`
	queueConfig := `map[string]any{
        "driver": "custom",
        "connection": "default",
        "queue": "default",
        "via": func() (queue.Driver, error) {
            return kafkafacades.Queue("kafka") // The ` + "`kafka`" + ` value is the key of ` + "`connections`" + `
        },
    }`

	appConfigPath := path.Config("app.go")
	kafkaConfigPath := path.Config("kafka.go")
	queueConfigPath := path.Config("queue.go")
	kafkaServiceProvider := "&kafka.ServiceProvider{}"
	moduleImport := setup.Paths().Module().Import()
	configPackage := setup.Paths().Config().Package()
	facadesImport := setup.Paths().Facades().Import()
	facadesPackage := setup.Paths().Facades().Package()
	envPath := path.Base(".env")
	envExamplePath := path.Base(".env.example")
	queueContract := "github.com/goravel/framework/contracts/queue"
	kafkaFacades := "github.com/goravel/kafka/facades"
	envContent := `
KAFKA_BROKERS=localhost:9092
KAFKA_SASL_MECHANISM=
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
`

	setup.Install(
		// Add kafka service provider to app.go if not using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return !env.IsBootstrapSetup()
		}, modify.GoFile(appConfigPath).
			Find(match.Imports()).Modify(modify.AddImport(moduleImport)).
			Find(match.Providers()).Modify(modify.Register(kafkaServiceProvider))),

		// Add kafka service provider to providers.go if using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return env.IsBootstrapSetup()
		}, modify.RegisterProvider(moduleImport, kafkaServiceProvider)),

		// Create config/kafka.go if not exists
		modify.WhenFileNotExists(kafkaConfigPath, modify.File(kafkaConfigPath).Overwrite(supportstubs.DatabaseConfig(configPackage, facadesImport, facadesPackage))),

		// Add kafka configuration to kafka.go
		modify.GoFile(kafkaConfigPath).
			Find(match.Config("kafka")).Modify(modify.AddConfig("kafka", kafkaConfig, "// Kafka connections")),

		// Add kafka queue configuration to queue.go if queue config file exists
		modify.WhenFileExists(queueConfigPath,
			modify.GoFile(queueConfigPath).
				Find(match.Imports()).Modify(modify.AddImport(queueContract), modify.AddImport(kafkaFacades, "kafkafacades")).
				Find(match.Config("queue.connections")).Modify(modify.AddConfig("kafka", queueConfig)).
				Find(match.Config("queue")).Modify(modify.AddConfig("default", `"kafka"`)),
		),

		// Add configurations to the .env and .env.example files
		modify.WhenFileNotContains(envPath, "KAFKA_BROKERS", modify.File(envPath).Append(envContent)),
		modify.WhenFileNotContains(envExamplePath, "KAFKA_BROKERS", modify.File(envExamplePath).Append(envContent)),
	).Uninstall(
		// Remove kafka service provider from app.go if not using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return !env.IsBootstrapSetup()
		}, modify.GoFile(appConfigPath).
			Find(match.Providers()).Modify(modify.Unregister(kafkaServiceProvider)).
			Find(match.Imports()).Modify(modify.RemoveImport(moduleImport))),

		// Remove kafka service provider from providers.go if using bootstrap setup
		modify.When(func(_ map[string]any) bool {
			return env.IsBootstrapSetup()
		}, modify.UnregisterProvider(moduleImport, kafkaServiceProvider)),

		// Remove kafka configuration from queue.go if queue config file exists
		modify.WhenFileExists(queueConfigPath,
			modify.GoFile(queueConfigPath).
				Find(match.Config("queue")).Modify(modify.AddConfig("default", `"sync"`)).
				Find(match.Config("queue.connections")).Modify(modify.RemoveConfig("kafka")).
				Find(match.Imports()).Modify(modify.RemoveImport(queueContract), modify.RemoveImport(kafkaFacades, "kafkafacades")),
		),

		// Remove kafka configuration from kafka.go
		modify.GoFile(kafkaConfigPath).
			Find(match.Config("kafka")).Modify(modify.RemoveConfig("kafka")),

		// Remove config/kafka.go if it matches the default template
		modify.When(func(_ map[string]any) bool {
			content, err := file.GetContent(kafkaConfigPath)
			if err != nil {
				return false
			}
			return content == supportstubs.DatabaseConfig(configPackage, facadesImport, facadesPackage)
		}, modify.File(kafkaConfigPath).Remove()),
	).Execute()
}
