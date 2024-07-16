package models

type User struct {
	ID         int    `json:"id" :"id"`
	NomeUtente string `json:"nome_utente" :"nome_utente"`
	Email      string `json:"email" :"email"`
}
