package producer

import (
	"csvreader/internal/models"
	"csvreader/pkg/logger"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pquerna/ffjson/ffjson"
)

type Producer struct {
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
}

// NewProducer creates a new Kafka producer instance and returns a pointer to Producer object.
// It takes 'bootstrapServers' and 'topic' as input parameters.
// The 'bootstrapServers' parameter is the comma-separated list of Kafka broker addresses.
// The 'topic' parameter is the name of the Kafka topic to produce messages to.
// It initializes the producer and returns any initialization error.
// Returns a pointer to Producer object and any error encountered during initialization.
func NewProducer(bootstrapServers, topic string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	logger.InfoAsync("Kafka producer created successfully")

	return &Producer{
		producer:     p,
		topic:        topic,
		deliveryChan: make(chan kafka.Event, 100), // buffer to avoid blocking
	}, nil
}

// ProduceBatch serializes a batch of users, produces Kafka messages with the payloads,
// and waits for delivery reports for each message. It takes a slice of models.User as the
// batch of users to be serialized and produced, and a string as the correlation ID for
// the messages. It returns an error if serialization, production, or delivery fails.
// The method logs an info message at the start of the batch production, and an info message
// when the batch production is completed.
// During batch production, it logs error messages if serialization or production fails.
// After all messages are produced, it waits for the delivery report for each message
// in the batch and returns an error if any delivery fails.
func (p *Producer) ProduceBatch(users []models.User, correlationID string) error {
	logger.InfoAsync("Starting batch production")
	for _, user := range users {
		payload, err := ffjson.Marshal(&user)

		if err != nil {
			logger.ErrorAsync("Failed to serialize payload:", err)
			return fmt.Errorf("failed to serialize payload: %w", err)
		}

		err = p.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
			Value:          payload,
			Headers:        []kafka.Header{{Key: "correlation-id", Value: []byte(correlationID)}},
		}, p.deliveryChan)
		if err != nil {
			logger.ErrorAsync("Produce failed:", err)
			return fmt.Errorf("produce failed: %w", err)
		}
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

func (p *Producer) waitForDeliveryReport() error {
	e := <-p.deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		logger.ErrorAsync("Delivery failed:", m.TopicPartition.Error)
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	//	logger.InfoAsync(fmt.Sprintf("Delivered message to topic %s [%d] at offset %v",
	//	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset))
	return nil
}

func (p *Producer) Close() {
	logger.InfoAsync("Closing producer")
	close(p.deliveryChan)
	p.producer.Close()
}
