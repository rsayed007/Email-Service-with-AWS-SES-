package auth

import (
	"context"
	"fmt"

	"email-service/internal/repository"
	"golang.org/x/crypto/bcrypt"
)

type SMTPAuthenticator struct {
	clients *repository.ClientRepository
}

func NewSMTPAuthenticator(clients *repository.ClientRepository) *SMTPAuthenticator {
	return &SMTPAuthenticator{clients: clients}
}

// Authenticate validates SMTP PLAIN credentials and returns the matching client.
func (a *SMTPAuthenticator) Authenticate(ctx context.Context, username, password string) (*repository.Client, error) {
	client, err := a.clients.GetBySMTPUsername(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("smtp auth: unknown user")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(client.SMTPPasswordHash), []byte(password)); err != nil {
		return nil, fmt.Errorf("smtp auth: invalid password")
	}
	return client, nil
}

// HashPassword returns a bcrypt hash of the given password.
func HashPassword(password string, cost int) (string, error) {
	if cost <= 0 {
		cost = bcrypt.DefaultCost
	}
	h, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(h), nil
}
