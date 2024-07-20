package main

import (
	"csvreader/internal/producer"
	"csvreader/internal/producer/avro"
	"csvreader/internal/service"
	"csvreader/pkg/constants"
	"csvreader/pkg/logger"
	"csvreader/pkg/utils"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Design Pattern : FANOUT -<
// Il vantaggio del pattern Fan-Out è che i task vengono eseguiti in parallelo dai worker
func main() {
	start := time.Now()

	// Genera un correlation ID
	correlationID := uuid.New().String()

	// Ottengo gli utenti dal servizio (vengono letti da un file CSV e convertiti in una lista di Users)
	users, err := service.GetUsers()
	if err != nil {
		logger.ErrorAsync(err)
	}
	avroSchema := `
	{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "ID", "type": "int"},
			{"name": "NomeUtente", "type": "string"},
			{"name": "Email", "type": "string"}
		]
	}`
	elapsed := time.Since(start)
	defer logger.InfoAsync("La lettura di 1000000 di utenti dal CSV ha impiegato %s", elapsed)

	// Configura il kafkaProducerInstance Kafka
	kafkaProducerInstance, err := producer.NewProducer(constants.KafkaBootstrapServers, constants.KafkaTopic)
	if err != nil {
		logger.ErrorAsync("Failed to create JSON-kafkaProducerInstance:", err)
	}
	defer kafkaProducerInstance.Close()

	kafkaProducerInstanceAvro, err := avro.NewProducerAvro(constants.KafkaBootstrapServers, constants.KafkaTopic, avroSchema)
	if err != nil {
		logger.ErrorAsync("Failed to create AVRO-kafkaProducerInstance:", err)
	}
	defer kafkaProducerInstanceAvro.CloseAvro()

	// Crea un WaitGroup per aspettare che tutti i worker finiscano
	var wg sync.WaitGroup

	// Aggiunge il numero di worker al WaitGroup
	wg.Add(constants.NumWorkers)

	// Canale principale per inviare i task
	mainCh := make(chan func())

	// Divide il canale principale in numWorkers canali
	channels := utils.Split(mainCh, constants.NumWorkers)

	// Avvia i worker
	for i := 0; i < constants.NumWorkers; i++ {
		go utils.Worker(channels[i], &wg)
	}

	// Invio dei task al canale principale
	go func() {
		// Chiude il canale principale quando la funzione termina
		defer close(mainCh)

		// Primo task: Scrittura degli utenti su file JSON
		mainCh <- func() {
			utils.WriteUsersToJSONFile(users, constants.JSONFileName)
		}

		// Secondo task: Conversione degli utenti in Avro e scrittura su file
		mainCh <- func() {
			avroUsers, err := utils.ConvertUsersToAvro(users)
			if err != nil {
				logger.ErrorAsync("Errore nella conversione degli utenti in Avro:", err)
				return
			}
			err = utils.WriteAvroToFile(avroUsers, constants.AvroFileName)
			if err != nil {
				logger.ErrorAsync("Errore nella scrittura del file Avro:", err)
				return
			}
		}
		// Terzo task: Lettura degli utenti e stampa su console
		mainCh <- func() {
			err := kafkaProducerInstanceAvro.ProduceAvro(users, correlationID)
			if err != nil {
				return
			}
		}

		// Quarto task: Invio degli utenti a Kafka in batch
		mainCh <- func() {
			batches := utils.BatchUsers(users, constants.BatchSize)

			// Start time for sending batches to Kafka
			startBatchSend := time.Now()

			for _, batch := range batches {
				err := kafkaProducerInstance.ProduceBatch(batch, correlationID)
				//	err := kafkaProducerInstanceAvro.ProduceBatchAvro(batch, correlationID)
				if err != nil {
					logger.ErrorAsync("Errore nell'invio del batch a Kafka:", err)
					return
				}
			}

			// Calculate elapsed time for sending batches to Kafka
			elapsedBatchSend := time.Since(startBatchSend)
			logger.InfoAsync("Sending batches to Kafka took %s", elapsedBatchSend)
		}
	}()

	// Aspetta che tutti i worker finiscano
	wg.Wait()
}
