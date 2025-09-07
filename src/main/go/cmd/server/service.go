package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"pipeline-perf/pkg/duration"
	"sync"
	"sync/atomic"
	"time"
)

var defaultConfig = AppConfig{
	Latency: LatencyDistribution{
		P25: duration.Duration(10 * time.Millisecond),
		P50: duration.Duration(50 * time.Millisecond),
		P75: duration.Duration(100 * time.Millisecond),
		P95: duration.Duration(200 * time.Millisecond),
		P99: duration.Duration(500 * time.Millisecond),
	},
}

type LatencyDistribution struct {
	P25 duration.Duration `json:"p25"`
	P50 duration.Duration `json:"p50"`
	P75 duration.Duration `json:"p75"`
	P95 duration.Duration `json:"p95"`
	P99 duration.Duration `json:"p99"`
}

type AppConfig struct {
	Latency    LatencyDistribution `json:"endpointLatency"`
	UpdateTime time.Time           `json:"updateTime"`
}

type ConfigService struct {
	configPtr    atomic.Pointer[AppConfig]
	configURL    string
	pollInterval time.Duration
	httpClient   *http.Client
	stopChan     chan struct{}
}

var instance *ConfigService
var instanceOnce sync.Once

func GetInstance() *ConfigService {
	instanceOnce.Do(func() {
		instance = &ConfigService{
			configURL:    "http://localhost:8060/config",
			pollInterval: 5 * time.Second,
			httpClient: &http.Client{
				Timeout: 10 * time.Second,
			},
			stopChan: make(chan struct{}),
		}
		instance.initialize()
	})
	return instance
}

func NewConfigService(configURL string, pollInterval time.Duration) *ConfigService {
	cs := &ConfigService{
		configURL:    configURL,
		pollInterval: pollInterval,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		stopChan: make(chan struct{}),
	}
	cs.initialize()
	return cs
}

func (cs *ConfigService) initialize() {
	cs.configPtr.Store(&defaultConfig)

	if err := cs.fetchConfig(); err != nil {
		log.Printf("Failed to fetch initial config: %v, using defaults", err)
	}

	go cs.pollConfig()
}

func (cs *ConfigService) GetConfig() *AppConfig {
	return cs.configPtr.Load()
}

func (cs *ConfigService) fetchConfig() error {
	resp, err := cs.httpClient.Get(cs.configURL)
	if err != nil {
		return fmt.Errorf("failed to fetch config: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("config endpoint returned status %d", resp.StatusCode)
	}

	newConfig := defaultConfig
	if err := json.NewDecoder(resp.Body).Decode(&newConfig); err != nil {
		return fmt.Errorf("failed to decode config: %w", err)
	}

	currentConfig := cs.GetConfig()
	if newConfig.UpdateTime.After(currentConfig.UpdateTime) {
		cs.configPtr.Store(&newConfig)
		log.Printf("Config updated at %v", newConfig.UpdateTime)
		return nil
	}

	return nil
}

func (cs *ConfigService) pollConfig() {
	ticker := time.NewTicker(cs.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := cs.fetchConfig(); err != nil {
				log.Printf("Error fetching config: %v", err)
			}
		case <-cs.stopChan:
			return
		}
	}
}

func (cs *ConfigService) Stop() {
	close(cs.stopChan)
}
