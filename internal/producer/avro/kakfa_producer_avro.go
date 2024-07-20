package avro

import (
	"csvreader/internal/models"
	"csvreader/pkg/logger"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/linkedin/goavro/v2"
)

type Producer struct {
	producer     *kafka.Producer
	topic        string
	deliveryChan chan kafka.Event
	avroCodec    *goavro.Codec
}

func NewProducerAvro(bootstrapServers, topic, avroSchema string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create Avro codec: %v", err)
	}

	logger.InfoAsync("Kafka producer created successfully")

	return &Producer{
		producer:     p,
		topic:        topic,
		deliveryChan: make(chan kafka.Event, 1000), // Increased buffer size
		avroCodec:    codec,
	}, nil
}

func (p *Producer) ProduceAvro(user *models.User, correlationID string) error {
	logger.DebugAsync("Starting Avro serialization")
	payload, err := p.ConvertUserToAvro(user)
	if err != nil {
		logger.ErrorAsync("Failed to serialize payload:", err)
		return fmt.Errorf("failed to serialize payload: %w", err)
	}

	logger.DebugAsync("Serialization successful, starting production")
	err = p.produceAvroMessage(payload, correlationID)
	if err != nil {
		return fmt.Errorf("produce failed: %w", err)
	}

	logger.DebugAsync("Message produced, waiting for delivery report")
	return p.waitForAvroDeliveryReport()
}

func (p *Producer) ProduceBatchAvro(avroData [][]byte, correlationID string) error {
	logger.InfoAsync("Starting batch production")

	var wg sync.WaitGroup
	for _, data := range avroData {
		wg.Add(1)
		go func(data []byte) {
			defer wg.Done()
			err := p.produceAvroMessage(data, correlationID)
			if err != nil {
				logger.ErrorAsync("Produce failed:", err)
			}
		}(data)
	}

	wg.Wait()

	// Ensure all delivery reports are processed
	for range avroData {
		if err := p.waitForAvroDeliveryReport(); err != nil {
			return err
		}
	}

	logger.InfoAsync("Batch production completed")
	return nil
}

func (p *Producer) produceAvroMessage(payload []byte, correlationID string) error {
	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          payload,
		Headers:        []kafka.Header{{Key: "correlation-id", Value: []byte(correlationID)}},
	}, p.deliveryChan)
}

func (p *Producer) waitForAvroDeliveryReport() error {
	e := <-p.deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		logger.ErrorAsync("Delivery failed:", m.TopicPartition.Error)
		return fmt.Errorf("delivery failed: %w", m.TopicPartition.Error)
	}

	return nil
}

func (p *Producer) CloseAvro() {
	logger.InfoAsync("Closing producer")
	close(p.deliveryChan)
	p.producer.Close()
}

func (p *Producer) ConvertUserToAvro(user *models.User) ([]byte, error) {
	avroRecord := map[string]interface{}{
		"ID":         user.ID,
		"NomeUtente": user.NomeUtente,
		"Email":      user.Email,
	}

	binary, err := p.avroCodec.BinaryFromNative(nil, avroRecord)
	if err != nil {
		return nil, fmt.Errorf("failed to encode Avro record: %v", err)
	}

	return binary, nil
}

func (p *Producer) ConvertUsersToAvro(users []models.User) ([][]byte, error) {
	var avroData [][]byte
	for _, user := range users {
		binary, err := p.ConvertUserToAvro(&user)
		if err != nil {
			return nil, fmt.Errorf("failed to encode Avro record: %v", err)
		}
		avroData = append(avroData, binary)
	}
	return avroData, nil
}
