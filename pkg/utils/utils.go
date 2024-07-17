package utils

import (
	"csvreader/internal/models"
	"csvreader/pkg/constants"
	"csvreader/pkg/logger"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/linkedin/goavro/v2"
)

// ReadCSV reads a CSV file containing user data and returns a slice of models.User objects.
// It opens the file, reads the records using csv package, creates User objects from the records,
// and appends them to the users slice. It skips the first record, assuming it is a header.
// If an error occurs during file opening, records reading, or user creation, it returns the error.
// It uses the constants.UsersFile constant to specify the file path, and constants.Separator constant
// to set the CSV field separator. It also calls the safelyClose function to close the file safely.
func ReadCSV() ([]models.User, error) {
	file, err := os.Open(constants.UsersFile) // apro il file
	if err != nil {
		return nil, fmt.Errorf(constants.FileOpenErrMessage, err)
	}
	defer safelyClose(file) // chiudo per non sprecare risorse

	var users []models.User
	reader := csv.NewReader(file)
	reader.Comma = constants.Separator
	records, err := reader.ReadAll()

	if err != nil {
		return nil, fmt.Errorf(constants.RecordsReadErrMessage, err)
	}
	// Itero i record, saltando il primo (records[1:])
	for _, record := range records[1:] {
		user, err := createUserFromRecord(record)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}

	return users, nil
}

// safelyClose closes the file. If an error occurs during file closing, it logs the error.
func safelyClose(file *os.File) {
	err := file.Close()
	if err != nil {
		logger.ErrorAsync("Attenzione - Errore durante la chiusura del File!!: %v", err)
	}
}

// createUserFromRecord creates a models.User object from the given CSV record.
// It parses the integer value from the first element of the record and assigns it to the ID field of the User object.
// It assigns the second and third elements of the record to the NomeUtente and Email fields of the User object respectively.
// If an error occurs during the conversion of the ID or if the record does not have enough elements, it returns an error.
func createUserFromRecord(record []string) (models.User, error) {
	id, err := strconv.Atoi(record[0])
	if err != nil {
		return models.User{}, fmt.Errorf("errore durante la conversione dell'id: %v", err)
	}

	user := models.User{
		ID:         id,
		NomeUtente: record[1],
		Email:      record[2],
	}

	return user, nil
}

// DisplayUsersAsJSON prints the users in JSON format.
func DisplayUsersAsJSON(users []models.User) {
	jsonData, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		logger.ErrorAsync("Errore durante la conversione in JSON: %v", err)
	}
	fmt.Println(string(jsonData))
}
func WriteUsersToJSONFile(users []models.User, filename string) {
	jsonData, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		logger.ErrorAsync("Errore durante la conversione in JSON: %v", err)
	}

	file, err := os.Create(filename)
	if err != nil {
		logger.ErrorAsync("Errore durante la creazione del file: %v", err)
	}
	defer safelyClose(file)

	_, err = file.Write(jsonData)
	if err != nil {
		logger.ErrorAsync("Errore durante la scrittura nel file: %v", err)
	}

	fmt.Printf("Dati scritti nel file %s con successo.\n", filename)
}
func ConvertUsersToAvro(users []models.User) ([]byte, error) {
	// Definizione dello schema Avro per gli utenti
	schema := `
	{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "ID", "type": "int"},
			{"name": "NomeUtente", "type": "string"},
			{"name": "Email", "type": "string"}
		]
	}`

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("errore nella creazione del codec Avro: %v", err)
	}

	var avroData []byte
	for _, user := range users {
		// Conversione del singolo utente in una mappa compatibile con Avro
		avroRecord := map[string]interface{}{
			"ID":         user.ID,
			"NomeUtente": user.NomeUtente,
			"Email":      user.Email,
		}

		// Codifica del record Avro
		binary, err := codec.BinaryFromNative(nil, avroRecord)
		if err != nil {
			return nil, fmt.Errorf("errore nella codifica Avro: %v", err)
		}

		avroData = append(avroData, binary...)
	}

	return avroData, nil
}
func WriteAvroToFile(avroData []byte, filename string) error {
	err := os.WriteFile(filename, avroData, 0644)
	if err != nil {
		return fmt.Errorf("errore durante la scrittura dei dati Avro su file: %v", err)
	}
	return nil
}

func BatchUsers(users []models.User, batchSize int) [][]models.User {
	var batches [][]models.User
	for batchSize < len(users) {
		users, batches = users[batchSize:], append(batches, users[0:batchSize:batchSize])
	}
	return append(batches, users)
}
