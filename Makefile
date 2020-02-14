- run:
	@docker-compose down
	@docker-compose up -d --build

- test:
	@go test -v ./internal/handlers/interfaceHandler_test.go  