package producer

import (
	"csvreader/internal/models"
	"csvreader/pkg/logger"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pquerna/ffjson/ffjson"
)

// Producer represents a Kafka producer.
type Producer struct {
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
}

// NewProducer creates a new Kafka producer.
func NewProducer(bootstrapServers, topic string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	logger.DebugAsync("Kafka producer created successfully")

	return &Producer{
		producer:     p,
		topic:        topic,
		deliveryChan: make(chan kafka.Event, 100), // buffer to avoid blocking
	}, nil
}

// ProduceBatch produces a batch of messages to Kafka.
// This approach enhances the performance and reliability of the batch processing by leveraging parallelism and efficient resource management
func (p *Producer) ProduceBatch(users []models.User, correlationID string) error {
	logger.InfoAsync("Starting batch production")

	// Number of workers
	const numWorkers = 10

	// Create channels
	userCh := make(chan models.User, len(users)) // Channel for users
	errCh := make(chan error, len(users))        // Channel for errors
	doneCh := make(chan struct{})                // Channel to signal completion

	// Create a wait group to synchronize goroutines
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for user := range userCh {
				payload, err := ffjson.Marshal(&user)
				if err != nil {
					errCh <- fmt.Errorf("failed to serialize payload: %w", err)
					continue
				}

				err = p.producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
					Value:          payload,
					Headers:        []kafka.Header{{Key: "correlation-id", Value: []byte(correlationID)}},
				}, p.deliveryChan)
				if err != nil {
					errCh <- fmt.Errorf("produce failed: %w", err)
					continue
				}
			}
		}()
	}

	// Send users to the userCh channel
	go func() {
		for _, user := range users {
			userCh <- user
		}
		close(userCh)
	}()

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	// Collect errors if any
	select {
	case <-doneCh:
		close(errCh)
	case err := <-errCh:
		return err
	}

	// Wait for all delivery reports
	for range users {
		if err := p.waitForDeliveryReport(); err != nil {
			return err
		}
	}

	logger.InfoAsync("Batch production completed")
	return nil
}

// waitForDeliveryReport waits for the delivery report.
func (p *Producer) waitForDeliveryReport() error {
	e := <-p.deliveryChan
	m, ok := e.(*kafka.Message)
	if !ok {
		logger.ErrorAsync("Failed to parse delivery report")
		return fmt.Errorf("failed to parse delivery report")
	}

	if m.TopicPartition.Error != nil {
		logger.ErrorAsync("Delivery failed:", m.TopicPartition.Error)
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}

// Close closes the producer.
func (p *Producer) Close() {
	logger.InfoAsync("Closing producer")
	close(p.deliveryChan)
	p.producer.Close()
}
