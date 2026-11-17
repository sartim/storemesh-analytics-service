package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
	"storemesh-analytics-service/internal/projector"
)

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	brokers := os.Getenv("KAFKA_BROKERS")
	if databaseURL == "" || brokers == "" {
		log.Fatal("DATABASE_URL and KAFKA_BROKERS are required")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	group := os.Getenv("KAFKA_CONSUMER_GROUP")
	if group == "" {
		group = "storemesh-analytics"
	}
	ctx := context.Background()
	topics := []string{"storemesh.order.events"}
	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers: strings.Split(brokers, ","), Topic: topic, GroupID: group,
				MinBytes: 1, MaxBytes: 10e6, CommitInterval: 0,
			})
			defer reader.Close()
			for {
				message, err := reader.FetchMessage(ctx)
				if err != nil {
					log.Printf("fetch %s: %v", topic, err)
					time.Sleep(time.Second)
					continue
				}
				event := projector.Event{ID: header(message.Headers, "event-id"), Topic: topic, Type: header(message.Headers, "event-type"), Payload: message.Value}
				if event.ID == "" {
					log.Printf("skip %s offset %d: event-id header missing", topic, message.Offset)
					continue
				}
				if err := projector.Apply(ctx, db, event); err != nil {
					log.Printf("project %s: %v", event.ID, err)
					continue
				}
				if err := reader.CommitMessages(ctx, message); err != nil {
					log.Printf("commit %s: %v", event.ID, err)
				}
			}
		}(topic)
	}
	wg.Wait()
}

func header(headers []kafka.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
