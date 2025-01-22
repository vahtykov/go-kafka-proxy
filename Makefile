# Переменные
BINARY_NAME=kafka-proxy
BUILD_DIR=bin
MAIN_FILE=cmd/api/main.go

# Версионирование
VERSION=$(shell git describe --tags --always --dirty)
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Go параметры
GOFLAGS=-v
LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME} -w -s"

.PHONY: all build clean test coverage lint docker docker-compose help install

# Цель по умолчанию
all: clean build

# Сборка приложения
build:
	@echo "Building ${BINARY_NAME}..."
	@mkdir -p ${BUILD_DIR}
	CGO_ENABLED=0 go build ${GOFLAGS} ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME} ${MAIN_FILE}

# Сборка для разных платформ
build-all: clean
	@echo "Building for multiple platforms..."
	@mkdir -p ${BUILD_DIR}
	GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME}-linux-amd64 ${MAIN_FILE}
	GOOS=windows GOARCH=amd64 go build ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME}-windows-amd64.exe ${MAIN_FILE}
	GOOS=darwin GOARCH=amd64 go build ${LDFLAGS} -o ${BUILD_DIR}/${BINARY_NAME}-darwin-amd64 ${MAIN_FILE}

# Очистка бинарных файлов
clean:
	@echo "Cleaning..."
	@rm -rf ${BUILD_DIR}

# Запуск тестов
test:
	go test -v ./...

# Проверка покрытия кода тестами
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# Проверка кода линтером
lint:
	golangci-lint run

# Сборка Docker образа
docker:
	docker build -t ${BINARY_NAME}:${VERSION} .

# Запуск всех сервисов через Docker Compose
docker-compose:
	docker-compose up -d

# Остановка Docker Compose
docker-compose-down:
	docker-compose down

# Запуск приложения локально
run:
	go run ${MAIN_FILE}

# Установка необходимых инструментов и зависимостей
install:
	@echo "Installing dependencies and tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/cosmtrek/air@latest
	go install github.com/swaggo/swag/cmd/swag@latest
	@echo "Installing project dependencies..."
	go mod download
	go mod tidy

# Установка только зависимостей проекта
deps:
	@echo "Installing project dependencies..."
	go mod download
	go mod tidy

# Помощь
help:
	@echo "Доступные команды:"
	@echo "  make build          - собрать приложение"
	@echo "  make build-all      - собрать для всех платформ"
	@echo "  make clean          - очистить бинарные файлы"
	@echo "  make test           - запустить тесты"
	@echo "  make coverage       - проверить покрытие кода"
	@echo "  make lint           - проверить код линтером"
	@echo "  make docker         - собрать Docker образ"
	@echo "  make docker-compose - запустить через Docker Compose"
	@echo "  make run            - запустить локально"
	@echo "  make install        - установить необходимые инструменты и зависимости"
	@echo "  make deps           - установить только зависимости проекта"
