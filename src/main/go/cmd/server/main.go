package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"pipeline-perf/pkg/duration"
	"pipeline-perf/pkg/env"
	"time"

	"github.com/gofiber/fiber/v2"
)

const defaultConfigEndpoint = "http://pipeline-config:8060/config"

var defaultPollInterval = 5 * time.Second

func main() {
	log.SetOutput(os.Stdout)

	configURL := flag.String("config-url", env.GetEnvOrDefault("config-url", defaultConfigEndpoint), "URL of the config service")
	pollInterval := flag.Duration("poll-interval", defaultPollInterval, "Config polling interval")
	flag.Parse()

	configService := NewConfigService(*configURL, *pollInterval)
	defer configService.Stop()

	initialConfig := configService.GetConfig()
	log.Printf("Initial latency config: %+v", initialConfig.Latency)

	app := fiber.New()

	rand.New(rand.NewSource(time.Now().UnixNano()))

	app.Get("/", func(c *fiber.Ctx) error {
		roll := rand.Float64()
		var sleepDuration duration.Duration

		currentConfig := configService.GetConfig()
		latencyConfig := currentConfig.Latency

		switch {
		case roll >= 0.99:
			sleepDuration = latencyConfig.P99
		case roll >= 0.95:
			sleepDuration = latencyConfig.P95
		case roll >= 0.75:
			sleepDuration = latencyConfig.P75
		case roll >= 0.50:
			sleepDuration = latencyConfig.P50
		default:
			sleepDuration = latencyConfig.P25
		}

		log.Printf("Sleeping for %d (roll: %.4f)", time.Duration(sleepDuration), roll)

		time.Sleep(time.Duration(sleepDuration))
		return c.SendString("Hello, World 👋!")
	})

	log.Fatal(app.Listen(":5050"))
}
