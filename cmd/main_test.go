package main

import (
	"errors"
	"testing"
)

func TestPut(t *testing.T) {
	// Инициализация
	key := "foo"
	value := "bar"

	// Выполнение
	err := Put(key, value)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	// Проверка, что значение сохранено правильно
	got, _ := Get(key)
	if got != value {
		t.Errorf("expected %s, got %s", value, got)
	}
}

func TestGet(t *testing.T) {
	// Инициализируем
	key := "foo"
	value := "bar"
	Put(key, value)

	// Проверка успешного получения значения
	got, err := Get(key)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if got != value {
		t.Errorf("expected %s, got %s", value, got)
	}

	// Проверка получения несуществующего ключа
	_, err = Get("unknown")
	if !errors.Is(err, ErrorNoSuchKey) {
		t.Errorf("expected %v, got %v", ErrorNoSuchKey, err)
	}
}

func TestDelete(t *testing.T) {
	// Инициализация
	key := "foo"
	value := "bar"
	Put(key, value)

	// Удаление ключа
	err := Delete(key)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	// Проверка, что ключ действительно удален
	_, err = Get(key)
	if !errors.Is(err, ErrorNoSuchKey) {
		t.Errorf("expected %v, got %v", ErrorNoSuchKey, err)
	}
}
