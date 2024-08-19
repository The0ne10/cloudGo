package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

type PostgresTransactionLogger struct {
	events chan<- string
	errors <-chan error
	db     *sql.DB
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {

	connStr := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		config.host, config.user, config.password, config.dbName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("error pinging postgres: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("error verifying table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
}
