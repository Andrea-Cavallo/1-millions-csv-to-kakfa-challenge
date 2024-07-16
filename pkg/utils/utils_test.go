package utils

import (
	"csvreader/internal/models"
	"os"
	"testing"
)

func TestSafelyClose(t *testing.T) {
	// Setup: create a temporary file for testing
	tempFile, err := os.CreateTemp("", "test*.txt")
	if err != nil {
		t.Fatalf("Errore durante la creazione del file temporaneo: %v", err)
	}

	// Test safelyClose
	safelyClose(tempFile)
	if _, err := tempFile.WriteString("test"); err == nil {
		t.Errorf("Atteso errore durante la scrittura su un file chiuso, ma non si è verificato")
	}
}

func TestCreateUserFromRecord(t *testing.T) {
	// Test valid record
	record := []string{"1", "user1", "user1@example.com"}
	expectedUser := models.User{ID: 1, NomeUtente: "user1", Email: "user1@example.com"}
	user, err := createUserFromRecord(record)
	if err != nil {
		t.Errorf("Errore durante la creazione dell'utente dal record: %v", err)
	}
	if user != expectedUser {
		t.Errorf("Utente atteso: %v, ottenuto: %v", expectedUser, user)
	}

	// Test invalid record
	invalidRecord := []string{"invalid_id", "user1", "user1@example.com"}
	_, err = createUserFromRecord(invalidRecord)
	if err == nil {
		t.Errorf("Atteso errore durante la conversione dell'id, ma non si è verificato")
	}
}
