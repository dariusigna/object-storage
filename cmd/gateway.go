package main

import (
	"context"
	"errors"
	"fmt"
	log "log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dariusigna/object-storage/internal/app"
	"github.com/dariusigna/object-storage/internal/gateway"
	"github.com/dariusigna/object-storage/internal/registrar"
	"github.com/dariusigna/object-storage/internal/registry"
	"github.com/moby/moby/client"
	"github.com/zeromicro/go-zero/core/hash"
)

func main() {
	if err := run(); err != nil {
		log.Error("Applications exists with error:", err)
		os.Exit(1)
	}
}

func run() error {
	log.Info("Server is starting...")
	log.SetLogLoggerLevel(log.LevelDebug) // Set log level from env or flags in production

	// Setup cancellation context
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Setup of the services
	// registry,registrar could be a separate microservices in a prod environment
	instanceRegistry := registry.NewRegistry(hash.NewConsistentHash())
	dockerCLI, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return fmt.Errorf("Could not create docker client: %v\n", err)
	}
	instanceRegistrar := registrar.NewRegistrar(dockerCLI, instanceRegistry)
	storage, err := gateway.NewObjectStorage(instanceRegistry)
	if err != nil {
		return fmt.Errorf("Could not create object storage: %v\n", err)
	}
	srv := app.NewServer(storage)
	server := &http.Server{
		Addr:         ":3000", // Read host and port from env or flags
		Handler:      srv,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	// In production, we will add metrics and tracing
	// with Prometheus, OpenTelemetry further triggering alerts for cases like high latency, high error rate, errors etc.

	// Continuously listen for docker events
	go instanceRegistrar.ListenForDockerEvents(ctx)

	// Start the server
	go func() {
		log.Info("Server is ready to handle requests at :3000")
		if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Could not listen on :3000: %v\n", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	<-ctx.Done()
	stop()

	log.Info("Server is shutting down...")
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err = server.Shutdown(ctx); err != nil {
		log.Error("Could not gracefully shutdown the server: %v\n", err)
		return err
	}

	log.Info("Server stopped")
	return nil
}
