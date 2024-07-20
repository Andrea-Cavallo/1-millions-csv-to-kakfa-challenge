package main

import (
	"csvreader/internal/producer/json"
	"csvreader/internal/service"
	"csvreader/pkg/constants"
	"csvreader/pkg/logger"
	"csvreader/pkg/utils"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Design Pattern: FANOUT -<
// The advantage of the Fan-Out pattern is that tasks are executed in parallel by the workers
func main() {
	start := time.Now()

	// Generate a correlation ID
	correlationID := uuid.New().String()

	// Get users from the service (they are read from a CSV file and converted into a list of Users)
	users, err := service.GetUsers()
	if err != nil {
		logger.ErrorAsync(err)
	}

	elapsed := time.Since(start)
	defer logger.InfoAsync("Reading 1,000,000 users from CSV took ", elapsed)

	// Configure the Kafka producer instance
	kafkaProducerInstance, err := producer.NewProducer(constants.KafkaBootstrapServers, constants.KafkaTopic)
	if err != nil {
		logger.ErrorAsync("Failed to create kafkaProducerInstance:", err)
	}
	defer kafkaProducerInstance.Close()

	// Create a WaitGroup to wait for all the workers to finish
	var wg sync.WaitGroup

	// Add the number of workers to the WaitGroup
	wg.Add(constants.NumWorkers)

	// Main channel to send tasks
	mainCh := make(chan func())

	// Split the main channel into numWorkers channels
	channels := utils.Split(mainCh, constants.NumWorkers)

	// Start the workers
	for i := 0; i < constants.NumWorkers; i++ {
		go utils.Worker(channels[i], &wg)
	}

	// Send tasks to the main channel
	go func() {
		// Close the main channel when the function ends
		defer close(mainCh)

		// First task: Write users to JSON file
		mainCh <- func() {
			utils.WriteUsersToJSONFile(users, constants.JSONFileName)
		}

		// Second task: Convert users to Avro and write to file
		mainCh <- func() {
			avroUsers, err := utils.ConvertUsersToAvro(users)
			if err != nil {
				logger.ErrorAsync("Error converting users to Avro:", err)
				return
			}
			err = utils.WriteAvroToFile(avroUsers, constants.AvroFileName)
			if err != nil {
				logger.ErrorAsync("Error writing Avro file:", err)
				return
			}
		}

		// Third task: Send users to Kafka in batches
		mainCh <- func() {
			batches := utils.BatchUsers(users, constants.BatchSize)

			// Start time for sending batches to Kafka
			startBatchSend := time.Now()

			for _, batch := range batches {
				err := kafkaProducerInstance.ProduceBatch(batch, correlationID)
				if err != nil {
					logger.ErrorAsync("Error sending batch to Kafka:", err)
					return
				}
			}

			// Calculate elapsed time for sending batches to Kafka
			elapsedBatchSend := time.Since(startBatchSend)
			logger.InfoAsync("Sending batches to Kafka took ", elapsedBatchSend)
		}
	}()

	// Wait for all the workers to finish
	wg.Wait()
}
