package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
)

var ErrorNoSuchKey = errors.New("no such key")

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

type EventType byte
type FileTransactionLogger struct {
	events       chan<- Event // Канал только для записи; для передачи событий
	errors       <-chan error // Канал только для чтения; для приема ошибок
	lastSequence uint64       // Последний исплользованный порядковый номер
	file         *os.File     // Местоположение файла журнала
}

type Event struct {
	Sequence  uint64    // Уникальный порядковый номер записи
	EventType EventType // Выполненное действие
	Key       string    // Ключ. затронутый этой транкзакцией
	Value     string    // Значение для транкзакции PUT
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error

	ReadEvents() (<-chan Event, <-chan error)
	Run()
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0775)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}
func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16) // Создать канал событий
	l.events = events

	errors := make(chan error, 1) // Создать канал ошибок
	l.errors = errors

	go func() {
		for e := range events { // Извлечь следующее собтыие Event
			l.lastSequence++                                                                               // Увеличить порядковый номер
			_, err := fmt.Fprintf(l.file, "%d\t%s\t%s\t%s\n", l.lastSequence, e.EventType, e.Key, e.Value) // Записать событие в журнал
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file) // Создать Scanner для чтения l.file
	outEvent := make(chan Event)        // Небуферезированный канал событий
	outError := make(chan error, 1)     // Буферизованный канал ошибок

	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			// Проверка целостности!
			// Порядковые номера последовательно увеличиваются?

			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			l.lastSequence = e.Sequence // Запомнить последний использованный порядковый номер
			outEvent <- e               // Отправить событие along
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}

var logger TransactionLogger

func initializeTransactionLog() error {
	var err error

	logger, err = NewFileTransactionLogger("./transactions.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors: // Получает ошибку
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}
	logger.Run()

	return err
}
