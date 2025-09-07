package main

import (
	"flag"
	"log"
	"os"
	"sync"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/gofiber/fiber/v2"
)

var (
	config      map[string]any
	configMutex = &sync.RWMutex{}
	configPath  string
	lastModTime time.Time
)

func loadConfig(modTime time.Time) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}

	var newConfig map[string]any
	if err := yaml.Unmarshal(data, &newConfig); err != nil {
		return err
	}

	newConfig["updateTime"] = modTime

	configMutex.Lock()
	config = newConfig
	configMutex.Unlock()

	log.Printf("Config loaded/reloaded from %s", configPath)
	return nil
}

func pollConfig(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		stat, err := os.Stat(configPath)
		if err != nil {
			log.Printf("Error checking file: %v", err)
			continue
		}

		modTime := stat.ModTime()
		if modTime.After(lastModTime) {
			log.Printf("Config file modified, reloading...")
			if err := loadConfig(modTime); err != nil {
				log.Printf("Error reloading config: %v", err)
			} else {
				lastModTime = modTime
			}
		}
	}
}

func main() {
	flag.StringVar(&configPath, "config", "config.yaml", "Path to config file")
	port := flag.String("port", "8060", "Port to run the service on")
	pollInterval := flag.Duration("poll-interval", 1*time.Second, "Polling interval for config file changes")
	flag.Parse()

	// Get initial modification time
	if stat, err := os.Stat(configPath); err == nil {
		lastModTime = stat.ModTime()
	}

	if err := loadConfig(lastModTime); err != nil {
		log.Fatal("Failed to load initial config:", err)
	}

	// Start high-performance polling
	go pollConfig(*pollInterval)

	app := fiber.New()

	app.Get("/config", func(c *fiber.Ctx) error {
		configMutex.RLock()
		configCopy := config
		configMutex.RUnlock()

		return c.JSON(configCopy)
	})

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "healthy",
			"config": configPath,
		})
	})

	log.Printf("Config service starting on port %s with %v polling interval", *port, *pollInterval)
	log.Fatal(app.Listen(":" + *port))
}
