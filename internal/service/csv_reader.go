package service

import (
	"csvreader/internal/models"
	"csvreader/pkg/utils"
)

// GetUsers retrieves users from a CSV file and returns a slice of model.User objects.
// It reads the CSV file in a separate goroutine and uses channels to handle errors and data.
// If an error occurs during CSV reading, it returns the error.
// If the CSV reading is successful, it returns the slice of users.
func GetUsers() ([]models.User, error) {
	usersChan := make(chan []models.User, 1)
	errorsChan := make(chan error, 1)

	go func() {
		users, err := utils.ReadCSV()
		if err != nil {
			errorsChan <- err
			close(usersChan)
			return
		}
		usersChan <- users
	}()

	select {
	case err := <-errorsChan:
		return nil, err
	case users := <-usersChan:
		return users, nil
	}
}
