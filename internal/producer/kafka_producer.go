package producer

import (
	"csvreader/internal/models"
	"csvreader/pkg/logger"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
}

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

func (p *Producer) Produce(value *models.User, correlationID string) error {
	logger.DebugAsync("Starting serialization")
	payload, err := json.Marshal(value)
	if err != nil {
		logger.ErrorAsync("Failed to serialize payload:", err)
		return fmt.Errorf("failed to serialize payload: %w", err)
	}

	logger.DebugAsync("Serialization successful, starting production")
	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        []kafka.Header{{Key: "correlation-id", Value: []byte(correlationID)}},
	}, p.deliveryChan)
	if err != nil {
		logger.ErrorAsync("Produce failed:", err)
		return fmt.Errorf("produce failed: %w", err)
	}

	logger.DebugAsync("Message produced, waiting for delivery report")
	return p.waitForDeliveryReport()
}

func (p *Producer) ProduceBatch(users []models.User, correlationID string) error {
	logger.InfoAsync("Starting batch production")
	for _, user := range users {
		payload, err := json.Marshal(&user) // da cambiare rallenta..
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
