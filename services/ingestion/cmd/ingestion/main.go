package main

import (
	"context"
	"errors"
	"io"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"pulseguard/services/ingestion/internal/cache"
	"pulseguard/services/ingestion/internal/config"
	"pulseguard/services/ingestion/internal/http"
	"pulseguard/services/ingestion/internal/queue"
	"pulseguard/services/ingestion/internal/service"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.ReadConfig("../../internal/config", "yaml")
	if err != nil {
		log.Fatal(err)
	}

	redis, err := cache.NewRedisProjectCache(context.Background(), cfg.Redis)
	if err != nil {
		log.Fatal(err)
	}

	injectService := service.NewErrInjectionService(redis)
	kafka := queue.NewKafkaQueue(cfg.Kafka)

	handler := http.NewHttpHandler(&injectService, kafka)
	server := &nethttp.Server{
		Addr:    cfg.Addr,
		Handler: handler,
	}

	shutdownChan := make(chan struct{})

	go shutdown(server, shutdownChan, kafka, redis)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
		log.Fatalf("server error: %v: ", err)
	}

	<-shutdownChan
}

func shutdown(server *nethttp.Server, shutdownChan chan struct{}, closers ...io.Closer) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
	<-sigchan

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			log.Printf("server shutdown error: %v", err)
		}
	}

	close(shutdownChan)
}
